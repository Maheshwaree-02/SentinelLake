"""
Load Test: Burst Spike Simulation

Drops a large number of micro-batch files into the streaming source directory
SIMULTANEOUSLY (no delay), then runs the consumer and measures how the
backpressure-controlled pipeline handles the burst.

This answers recruiter Q3: "How do you manage capacity under a sudden load spike?"

The answer: maxFilesPerTrigger caps how much the consumer pulls per micro-batch,
so a sudden burst queues up in the source directory rather than overwhelming the
Spark job. Processing lag rises smoothly and recovers — the job doesn't crash.

Usage:
    python scripts/load_test_spike.py --burst-size 50
    python scripts/load_test_spike.py --burst-size 200 --records-per-file 500
"""

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

STREAMING_SOURCE = PROJECT_ROOT / "data" / "lake" / "streaming_source"
GOLD_PATH = PROJECT_ROOT / "data" / "lake" / "gold" / "features"
CHECKPOINT_PATH = PROJECT_ROOT / "data" / "checkpoints" / "streaming_gold"
QUARANTINE_PATH = PROJECT_ROOT / "data" / "lake" / "quarantine"
RAW_CSV = PROJECT_ROOT / "data" / "raw" / "Paysim_dataset.csv"
RESULTS_DIR = PROJECT_ROOT / "docs" / "runbooks"


def parse_args():
    parser = argparse.ArgumentParser(description="Burst load test for streaming pipeline")
    parser.add_argument(
        "--burst-size", type=int, default=50,
        help="Number of files to drop simultaneously (default: 50)"
    )
    parser.add_argument(
        "--records-per-file", type=int, default=200,
        help="Records per micro-batch file (default: 200)"
    )
    parser.add_argument(
        "--max-files-per-trigger", type=int, default=2,
        help="Consumer's maxFilesPerTrigger setting (default: 2)"
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Clean streaming source, Gold, checkpoint, and DLQ before test"
    )
    return parser.parse_args()


def clean_all():
    """Remove all pipeline state for a fresh test."""
    for d in [STREAMING_SOURCE, GOLD_PATH, CHECKPOINT_PATH, QUARANTINE_PATH]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)
    print("[LOAD TEST] Cleaned all pipeline state")


def generate_burst(burst_size: int, records_per_file: int):
    """Drop burst_size files into streaming source simultaneously (no delay)."""
    print(f"[LOAD TEST] Loading source data...")
    df = pd.read_csv(RAW_CSV, nrows=burst_size * records_per_file)

    print(f"[LOAD TEST] Dropping {burst_size} files x {records_per_file} records "
          f"= {burst_size * records_per_file:,} total records SIMULTANEOUSLY...")

    t0 = time.time()
    for i in range(burst_size):
        start_idx = i * records_per_file
        end_idx = start_idx + records_per_file
        chunk = df.iloc[start_idx:end_idx]
        filename = f"batch_burst_{i:06d}.csv"
        chunk.to_csv(STREAMING_SOURCE / filename, index=False)

    drop_time = time.time() - t0
    print(f"[LOAD TEST] All {burst_size} files dropped in {drop_time:.2f}s "
          f"(simulating instant burst)")
    return drop_time


def run_consumer(max_files_per_trigger: int):
    """Run the streaming consumer and capture per-batch metrics."""
    from src.utils.spark_session import get_spark
    from src.ingestion.stream_consumer import (
        TRANSACTION_SCHEMA, process_micro_batch, get_batch_metrics, _batch_metrics
    )
    from pyspark.sql import functions as F

    # Clear previous metrics
    _batch_metrics.clear()

    spark = get_spark("EFIP-LoadTest")

    stream_df = (
        spark.readStream
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("header", "true")
        .schema(TRANSACTION_SCHEMA)
        .csv(str(STREAMING_SOURCE))
    )

    t0 = time.time()

    query = (
        stream_df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", str(CHECKPOINT_PATH))
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    total_time = time.time() - t0

    metrics = get_batch_metrics()

    # Gold summary
    try:
        gold_df = spark.read.parquet(str(GOLD_PATH))
        gold_count = gold_df.count()
        fraud_count = gold_df.filter(F.col("isFraud") == 1).count()
    except Exception:
        gold_count = 0
        fraud_count = 0

    spark.stop()
    return metrics, total_time, gold_count, fraud_count


def print_results(metrics, total_time, gold_count, fraud_count,
                  burst_size, records_per_file, max_files_per_trigger):
    """Print and save results."""
    total_records = burst_size * records_per_file
    total_batches = len(metrics)

    print("\n" + "=" * 70)
    print("  LOAD TEST RESULTS")
    print("=" * 70)
    print(f"  Burst config:     {burst_size} files x {records_per_file} records "
          f"= {total_records:,} total")
    print(f"  Backpressure:     maxFilesPerTrigger = {max_files_per_trigger}")
    print(f"  Total batches:    {total_batches}")
    print(f"  Total time:       {total_time:.2f}s")
    print(f"  Avg throughput:   {total_records / total_time:.0f} records/sec")
    print(f"  Gold layer:       {gold_count:,} records ({fraud_count:,} fraud)")
    print()

    if metrics:
        # Per-batch breakdown
        print("  Per-batch breakdown:")
        print(f"  {'Batch':>6s}  {'Rows':>6s}  {'Time(s)':>8s}  {'Rec/s':>8s}")
        print(f"  {'------':>6s}  {'------':>6s}  {'--------':>8s}  {'--------':>8s}")
        for m in metrics:
            print(f"  {m['batch_id']:>6d}  {m['rows']:>6d}  "
                  f"{m['elapsed_sec']:>8.3f}  {m['records_per_sec']:>8.0f}")

        # Lag analysis
        elapsed_times = [m["elapsed_sec"] for m in metrics]
        print()
        print(f"  Processing lag analysis:")
        print(f"    Min batch time:  {min(elapsed_times):.3f}s")
        print(f"    Max batch time:  {max(elapsed_times):.3f}s")
        print(f"    Avg batch time:  {sum(elapsed_times)/len(elapsed_times):.3f}s")
        print(f"    P95 batch time:  {sorted(elapsed_times)[int(len(elapsed_times)*0.95)]:.3f}s")

    print("=" * 70)

    # Save results to JSON
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results = {
        "test_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "config": {
            "burst_size": burst_size,
            "records_per_file": records_per_file,
            "total_records": total_records,
            "max_files_per_trigger": max_files_per_trigger,
        },
        "results": {
            "total_batches": total_batches,
            "total_time_sec": round(total_time, 2),
            "avg_throughput_rps": round(total_records / total_time, 0),
            "gold_count": gold_count,
            "fraud_count": fraud_count,
        },
        "lag_analysis": {
            "min_batch_sec": round(min(elapsed_times), 3) if metrics else 0,
            "max_batch_sec": round(max(elapsed_times), 3) if metrics else 0,
            "avg_batch_sec": round(sum(elapsed_times)/len(elapsed_times), 3) if metrics else 0,
        },
        "per_batch_metrics": metrics,
    }

    results_file = RESULTS_DIR / "load_test_results.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Results saved to: {results_file}")

    return results


def main():
    args = parse_args()

    print("=" * 70)
    print("  EFIP LOAD TEST — Burst Spike Simulation")
    print("=" * 70)
    print(f"  Burst size:       {args.burst_size} files")
    print(f"  Records/file:     {args.records_per_file}")
    print(f"  Total records:    {args.burst_size * args.records_per_file:,}")
    print(f"  Backpressure:     maxFilesPerTrigger = {args.max_files_per_trigger}")
    print("=" * 70)

    if args.clean:
        clean_all()

    # Phase 1: Drop burst
    generate_burst(args.burst_size, args.records_per_file)

    # Phase 2: Run consumer and measure
    print("\n[LOAD TEST] Starting consumer with backpressure...")
    metrics, total_time, gold_count, fraud_count = run_consumer(
        args.max_files_per_trigger
    )

    # Phase 3: Results
    print_results(
        metrics, total_time, gold_count, fraud_count,
        args.burst_size, args.records_per_file, args.max_files_per_trigger
    )


if __name__ == "__main__":
    main()
