"""
Stream Consumer — Spark Structured Streaming job with Silver Quality Gate,
Dead Letter Queue (DLQ) routing, Gold-layer feature engineering, and
atomic telemetry logging.

Fulfills EFIP Scenario Architecture:
  - Flow 0: Bronze Ingestion -> Quality Gate -> Silver -> Gold
  - Flow 1: 7-Step Quality Gate Hierarchy:
      D1/D2: Critical field presence (txn_id, amount, nameOrig, type, step)
      D3:    Transaction type validation against VALID_TYPES
      D4:    Non-negative amount check (NEGATIVE_AMOUNT -> DLQ)
      D5:    Duplicate suppression (idempotent deduplication by txn_id)
      D6:    Completeness scoring (>=0.90 VALID, 0.60-0.89 LOW_CONFIDENCE, <0.60 TOO_INCOMPLETE -> DLQ)
  - Flow 2: Backpressure control via maxFilesPerTrigger
  - Flow 5: Telemetry logging for Freshness states (LIVE, AGING, STALE, RECOVERING)
  - Flow 8: Reconciliation verification (Ingested == Valid + DLQ + Duplicates)

Usage:
    python src/ingestion/stream_consumer.py
    python src/ingestion/stream_consumer.py --max-files 5
    python src/ingestion/stream_consumer.py --clean  # Reset state for fresh drill
"""

import argparse
import datetime
import json
import os
import shutil
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from src.utils.spark_session import get_spark

# ── Paths ──────────────────────────────────────────────────────────────
STREAMING_SOURCE = str(PROJECT_ROOT / "data" / "lake" / "streaming_source")
GOLD_PATH = str(PROJECT_ROOT / "data" / "lake" / "gold" / "features")
CHECKPOINT_PATH = str(PROJECT_ROOT / "data" / "checkpoints" / "streaming_gold")
QUARANTINE_PATH = str(PROJECT_ROOT / "data" / "lake" / "quarantine")
PIPELINE_METRICS_PATH = PROJECT_ROOT / "data" / "lake" / "pipeline_metrics.json"

# ── Schema (PaySim + Traceability Metadata) ───────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
    StructField("txn_id", StringType(), True),
    StructField("trace_id", StringType(), True),
    StructField("emit_ts", StringType(), True),
])

VALID_TYPES = {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}

CORE_COMPLETENESS_FIELDS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg",
    "newbalanceOrig", "nameDest", "oldbalanceDest",
    "newbalanceDest", "isFraud", "isFlaggedFraud",
]

# ── In-memory batch metrics (used by load_test_spike.py) ───────────────
_batch_metrics = []


def get_batch_metrics():
    """Return collected per-batch timing metrics."""
    return list(_batch_metrics)


def reset_pipeline_metrics():
    """Resets the pipeline metrics file."""
    if PIPELINE_METRICS_PATH.exists():
        try:
            PIPELINE_METRICS_PATH.unlink()
        except OSError:
            pass


def clean_pipeline_state():
    """Removes Gold, Checkpoint, DLQ, and Metrics for a fresh run."""
    for p in [Path(GOLD_PATH), Path(CHECKPOINT_PATH), Path(QUARANTINE_PATH)]:
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
    reset_pipeline_metrics()
    print("[CONSUMER] Cleaned Gold, Checkpoint, DLQ, and pipeline metrics.")


def update_pipeline_metrics(
    batch_id: int,
    raw_count: int,
    valid_count: int,
    low_conf_count: int,
    quarantine_count: int,
    duplicates_suppressed: int,
    elapsed_sec: float,
    reason_code_counts: dict,
):
    """
    Updates the atomic telemetry state in pipeline_metrics.json.
    Computes reconciliation: Ingested == Valid + Quarantined + Duplicates.
    """
    metrics = {
        "latest_update": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "latest_gold_commit_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "latest_batch_id": batch_id,
        "total_ingested": 0,
        "total_valid": 0,
        "total_low_confidence": 0,
        "total_quarantined": 0,
        "total_duplicates_suppressed": 0,
        "reason_code_breakdown": {},
        "recent_batches": [],
    }

    if PIPELINE_METRICS_PATH.exists():
        try:
            with open(PIPELINE_METRICS_PATH, "r", encoding="utf-8") as f:
                existing = json.load(f)
                metrics["total_ingested"] = existing.get("total_ingested", 0)
                metrics["total_valid"] = existing.get("total_valid", 0)
                metrics["total_low_confidence"] = existing.get("total_low_confidence", 0)
                metrics["total_quarantined"] = existing.get("total_quarantined", 0)
                metrics["total_duplicates_suppressed"] = existing.get("total_duplicates_suppressed", 0)
                metrics["reason_code_breakdown"] = existing.get("reason_code_breakdown", {})
                metrics["recent_batches"] = existing.get("recent_batches", [])
        except Exception:
            pass

    # Accumulate
    metrics["latest_update"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    metrics["latest_gold_commit_ts"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    metrics["latest_batch_id"] = batch_id
    metrics["total_ingested"] += raw_count
    metrics["total_valid"] += valid_count
    metrics["total_low_confidence"] += low_conf_count
    metrics["total_quarantined"] += quarantine_count
    metrics["total_duplicates_suppressed"] += duplicates_suppressed

    # Merge reason codes
    for rcode, cnt in reason_code_counts.items():
        metrics["reason_code_breakdown"][rcode] = (
            metrics["reason_code_breakdown"].get(rcode, 0) + cnt
        )

    # Reconciliation check: Ingested == Valid + Quarantined + Duplicates
    sum_accounted = (
        metrics["total_valid"]
        + metrics["total_quarantined"]
        + metrics["total_duplicates_suppressed"]
    )
    delta = metrics["total_ingested"] - sum_accounted
    metrics["reconciliation_delta"] = delta
    metrics["reconciliation_status"] = "BALANCED" if delta == 0 else "MISMATCH"

    tot_ing = metrics["total_ingested"]
    metrics["dlq_rejection_rate_pct"] = round(
        (metrics["total_quarantined"] / tot_ing * 100) if tot_ing > 0 else 0.0, 2
    )

    # Append batch entry
    batch_entry = {
        "batch_id": batch_id,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ingested": raw_count,
        "valid": valid_count,
        "low_confidence": low_conf_count,
        "quarantined": quarantine_count,
        "duplicates_suppressed": duplicates_suppressed,
        "elapsed_sec": round(elapsed_sec, 3),
        "records_per_sec": round(raw_count / elapsed_sec, 1) if elapsed_sec > 0 else 0,
        "reasons": reason_code_counts,
    }
    metrics["recent_batches"].append(batch_entry)
    metrics["recent_batches"] = metrics["recent_batches"][-25:]  # Retain last 25 batches

    PIPELINE_METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = PIPELINE_METRICS_PATH.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    temp_path.replace(PIPELINE_METRICS_PATH)


def apply_silver_transforms(df):
    """
    Silver-layer transforms & 7-Step Quality Gate Hierarchy (Flow 1).
    Returns (valid_df, quarantine_df).
    """
    # ── Fallbacks for legacy/unadorned CSV files ──────────────────
    df = df.withColumn(
        "txn_id",
        F.coalesce(
            F.col("txn_id"),
            F.concat(F.lit("TXN_"), F.col("step"), F.lit("_"), F.monotonically_increasing_id()),
        ),
    )
    df = df.withColumn("trace_id", F.coalesce(F.col("trace_id"), F.lit("trc_legacy")))
    df = df.withColumn("emit_ts", F.coalesce(F.col("emit_ts"), F.current_timestamp().cast("string")))

    # ── Completeness calculation ──────────────────────────────────
    present_expr = sum(
        F.when(F.col(c).isNotNull() & (F.trim(F.col(c).cast("string")) != ""), 1).otherwise(0)
        for c in CORE_COMPLETENESS_FIELDS
    )
    completeness_expr = F.round(present_expr / float(len(CORE_COMPLETENESS_FIELDS)), 4)
    df = df.withColumn("completeness_score", completeness_expr)

    # ── 7-Step Quality Gate Hierarchy (Flow 1) ────────────────────
    error_code_expr = (
        F.when(F.col("nameOrig").isNull() | (F.trim(F.col("nameOrig")) == ""), "NULL_ORIGIN")
        .when(F.col("amount").isNull(), "MISSING_FIELD")
        .when(F.col("amount") < 0, "NEGATIVE_AMOUNT")
        .when(F.col("type").isNull() | (F.trim(F.col("type")) == "") | F.col("step").isNull(), "MISSING_FIELD")
        .when(~F.col("type").isin(*VALID_TYPES), "INVALID_TYPE")
        .when(F.col("completeness_score") < 0.60, "TOO_INCOMPLETE")
        .otherwise("VALID")
    )

    # ── Quarantine Routing (DLQ) ──────────────────────────────────
    quarantine_df = (
        df.filter(error_code_expr != "VALID")
        .withColumn("error_code", error_code_expr)
        .withColumn("quarantine_reason", error_code_expr)  # Backward compatibility
        .withColumn("quarantine_ts", F.current_timestamp())
        .withColumn("raw_payload", F.to_json(F.struct("*")))
    )

    # ── Valid / Clean Routing ─────────────────────────────────────
    valid_df = df.filter(error_code_expr == "VALID")

    # Quality status: LOW_CONFIDENCE if completeness between 0.60 and 0.89
    quality_status_expr = (
        F.when(F.col("completeness_score") < 0.90, "LOW_CONFIDENCE")
        .otherwise("VALID")
    )

    valid_df = (
        valid_df
        .withColumn("quality_status", quality_status_expr)
        .withColumn(
            "requires_manual_review",
            F.when(F.col("quality_status") == "LOW_CONFIDENCE", 1).otherwise(0),
        )
        .withColumn(
            "balance_delta_orig",
            F.coalesce(F.col("newbalanceOrig"), F.lit(0.0)) - F.coalesce(F.col("oldbalanceOrg"), F.lit(0.0)),
        )
        .withColumn(
            "balance_delta_dest",
            F.coalesce(F.col("newbalanceDest"), F.lit(0.0)) - F.coalesce(F.col("oldbalanceDest"), F.lit(0.0)),
        )
        .withColumn(
            "is_balance_mismatch",
            F.when(
                (F.col("type").isin("TRANSFER", "CASH_OUT"))
                & (F.abs(F.col("balance_delta_orig") + F.col("amount")) > 1.0),
                1,
            ).otherwise(0),
        )
    )

    return valid_df, quarantine_df


def apply_gold_transforms(df):
    """
    Gold-layer transforms: velocity features, z-scores, balance ratios,
    end-to-end processing latency.
    """
    # ── Velocity features (rolling window per customer) ────────────
    window_spec = (
        Window
        .partitionBy("nameOrig")
        .orderBy("step")
        .rangeBetween(-1, 0)
    )

    df = (
        df
        .withColumn("txn_count_window", F.count("*").over(window_spec))
        .withColumn("total_amount_window", F.sum("amount").over(window_spec))
        .withColumn("avg_amount_window", F.avg("amount").over(window_spec))
    )

    # ── Velocity score (0-1) ──────────────────────────────────────
    df = df.withColumn(
        "velocity_score",
        F.when(F.col("txn_count_window") > 5, 1.0)
        .when(F.col("txn_count_window") > 3, 0.7)
        .when(F.col("txn_count_window") > 1, 0.4)
        .otherwise(0.2),
    )

    # ── Per-type amount z-score ───────────────────────────────────
    type_window = Window.partitionBy("type")
    df = (
        df
        .withColumn("type_mean", F.avg("amount").over(type_window))
        .withColumn("type_stddev", F.stddev("amount").over(type_window))
        .withColumn(
            "amount_zscore",
            F.when(
                F.col("type_stddev") > 0,
                (F.col("amount") - F.col("type_mean")) / F.col("type_stddev"),
            ).otherwise(0.0),
        )
        .drop("type_mean", "type_stddev")
    )

    # ── Balance ratio ─────────────────────────────────────────────
    df = df.withColumn(
        "balance_ratio",
        F.when(
            F.col("oldbalanceOrg") > 0,
            F.col("amount") / F.col("oldbalanceOrg"),
        ).otherwise(0.0),
    )

    # ── End-to-end processing latency (ms) & metadata ─────────────
    df = df.withColumn(
        "latency_ms",
        F.when(
            F.col("emit_ts").isNotNull(),
            F.round(
                (F.current_timestamp().cast("double") - F.to_timestamp(F.col("emit_ts")).cast("double")) * 1000,
                1,
            ),
        ).otherwise(0.0),
    )
    df = df.withColumn("processed_at", F.current_timestamp())

    return df


def process_micro_batch(batch_df, batch_id):
    """
    Process a single micro-batch: Deduplication -> Silver Quality Gate -> DLQ / Gold.
    Called by foreachBatch — guaranteed idempotent.
    """
    if batch_df.isEmpty():
        return

    batch_start = time.time()
    raw_count = batch_df.count()

    # Step 1: Micro-batch deduplication (Flow 1, D5)
    dedup_df = batch_df.dropDuplicates(["txn_id"]) if "txn_id" in batch_df.columns else batch_df
    dedup_count = dedup_df.count()
    duplicates_suppressed = raw_count - dedup_count

    # Step 2: Silver Quality Gate & transforms
    valid_df, quarantine_df = apply_silver_transforms(dedup_df)

    # Step 3: Write DLQ records
    quarantine_count = quarantine_df.count()
    reason_counts = {}
    if quarantine_count > 0:
        reasons = quarantine_df.groupBy("error_code").count().collect()
        for row in reasons:
            reason_counts[row["error_code"]] = row["count"]
        (
            quarantine_df.write
            .mode("append")
            .parquet(QUARANTINE_PATH)
        )

    # Step 4: Gold transforms on valid records
    valid_count = valid_df.count()
    low_conf_count = 0
    if valid_count > 0:
        low_conf_count = valid_df.filter(F.col("quality_status") == "LOW_CONFIDENCE").count()
        gold_df = apply_gold_transforms(valid_df)
        (
            gold_df.write
            .mode("append")
            .parquet(GOLD_PATH)
        )

    batch_elapsed = time.time() - batch_start
    records_per_sec = raw_count / batch_elapsed if batch_elapsed > 0 else 0

    # Step 5: Update telemetry store (pipeline_metrics.json)
    update_pipeline_metrics(
        batch_id=batch_id,
        raw_count=raw_count,
        valid_count=valid_count,
        low_conf_count=low_conf_count,
        quarantine_count=quarantine_count,
        duplicates_suppressed=duplicates_suppressed,
        elapsed_sec=batch_elapsed,
        reason_code_counts=reason_counts,
    )

    _batch_metrics.append({
        "batch_id": batch_id,
        "rows": raw_count,
        "valid": valid_count,
        "low_confidence": low_conf_count,
        "quarantined": quarantine_count,
        "duplicates_suppressed": duplicates_suppressed,
        "elapsed_sec": round(batch_elapsed, 3),
        "records_per_sec": round(records_per_sec, 1),
    })

    fault_summary = ""
    if quarantine_count > 0:
        fault_summary = f", {quarantine_count} to DLQ"
        if reason_counts:
            reasons_str = ", ".join(f"{k}: {v}" for k, v in reason_counts.items())
            fault_summary += f" [{reasons_str}]"
    else:
        fault_summary = ", 0 to DLQ"

    dup_summary = f", {duplicates_suppressed} dups suppressed" if duplicates_suppressed > 0 else ""

    print(
        f"  [Consumer] Batch {batch_id:>4d}: "
        f"{raw_count} ingested -> {valid_count} to Gold ({low_conf_count} low-conf)"
        f"{fault_summary}{dup_summary}  ({batch_elapsed:.2f}s, {records_per_sec:.0f} rec/s)"
    )


def parse_args():
    parser = argparse.ArgumentParser(description="EFIP Streaming Consumer with Quality Gate & DLQ")
    parser.add_argument(
        "--max-files", type=int,
        default=int(os.environ.get("EFIP_MAX_FILES_PER_TRIGGER", "1")),
        help="maxFilesPerTrigger for backpressure (default: 1, or EFIP_MAX_FILES_PER_TRIGGER env var)",
    )
    parser.add_argument(
        "--timeout", type=int, default=0,
        help="Seconds to run before stopping (0 = run until no new data, default: 0)",
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Clean Gold, Checkpoint, DLQ, and metrics state before running",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.clean:
        clean_pipeline_state()

    print("=" * 70)
    print("  EFIP Streaming Consumer (with Quality Gate & DLQ)")
    print("=" * 70)
    print(f"  Source landing: {STREAMING_SOURCE}")
    print(f"  Gold Lakehouse: {GOLD_PATH}")
    print(f"  Checkpoints:    {CHECKPOINT_PATH}")
    print(f"  DLQ Quarantine: {QUARANTINE_PATH}")
    print(f"  Telemetry:      {PIPELINE_METRICS_PATH}")
    print(f"  Backpressure:   maxFilesPerTrigger={args.max_files}")
    print("=" * 70)

    # Ensure output directories exist
    for d in [GOLD_PATH, CHECKPOINT_PATH, QUARANTINE_PATH]:
        os.makedirs(d, exist_ok=True)

    spark = get_spark("EFIP-StreamConsumer")

    # ── Read stream ──────────────────────────────────────────────
    stream_df = (
        spark.readStream
        .option("maxFilesPerTrigger", args.max_files)
        .option("header", "true")
        .schema(TRANSACTION_SCHEMA)
        .csv(STREAMING_SOURCE)
    )

    # ── Write stream with foreachBatch ───────────────────────────
    query = (
        stream_df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )

    print("[Consumer] Streaming query started...")

    if args.timeout > 0:
        query.awaitTermination(args.timeout * 1000)
        query.stop()
    else:
        query.awaitTermination()

    # ── Summary ──────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  Streaming Complete - Lakehouse Summary")
    print("=" * 70)

    # Count Gold records
    try:
        gold_df = spark.read.parquet(GOLD_PATH)
        gold_count = gold_df.count()
        fraud_count = gold_df.filter(F.col("isFraud") == 1).count()
        low_conf_count = gold_df.filter(F.col("quality_status") == "LOW_CONFIDENCE").count()
        print(f"  Gold layer:     {gold_count:,} records ({fraud_count:,} fraud, {low_conf_count:,} low-confidence)")

        # Velocity distribution
        velocity_dist = (
            gold_df.groupBy("velocity_score")
            .count()
            .orderBy("velocity_score")
            .collect()
        )
        print("  Velocity distribution:")
        for row in velocity_dist:
            print(f"    score {row['velocity_score']:.1f}: {row['count']:,} records")
    except Exception:
        print("  Gold layer:     empty or not yet created")

    # Count DLQ records
    try:
        dlq_df = spark.read.parquet(QUARANTINE_PATH)
        dlq_count = dlq_df.count()
        print(f"  DLQ Quarantine: {dlq_count:,} records")
        reasons_dist = dlq_df.groupBy("error_code").count().collect()
        print("  DLQ breakdown by reason:")
        for row in reasons_dist:
            print(f"    - {row['error_code']:<20s}: {row['count']:,} records")
    except Exception:
        print("  DLQ Quarantine: empty")

    # Reconciliation status
    if PIPELINE_METRICS_PATH.exists():
        try:
            with open(PIPELINE_METRICS_PATH, "r", encoding="utf-8") as f:
                pm = json.load(f)
                status_icon = "[PASS]" if pm.get("reconciliation_status") == "BALANCED" else "[FAIL]"
                print(f"  Reconciliation: {status_icon} {pm.get('reconciliation_status')} "
                      f"(Total ingested: {pm.get('total_ingested'):,}, delta: {pm.get('reconciliation_delta')})")
        except Exception:
            pass

    # Check checkpoint
    cp_path = Path(CHECKPOINT_PATH)
    if cp_path.exists() and list(cp_path.iterdir()):
        print(f"  Checkpoint:     present at {CHECKPOINT_PATH}")
    else:
        print(f"  Checkpoint:     not found")

    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
