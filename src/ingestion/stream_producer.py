"""
Stream Producer — Simulates a real-time transaction feed with fault injection.

Reads the raw PaySim CSV and emits micro-batch CSV files into a landing
directory (streaming_source) at a controlled rate with randomized jitter.

Metadata added per record:
  - txn_id:   Unique deterministic identifier per transaction (e.g. TXN_001_0000_00042)
  - trace_id: Micro-batch UUID tracing the batch from producer through lakehouse
  - emit_ts:  ISO UTC timestamp recorded at file creation (used to compute e2e latency)

Fault injection (Scenario Flow 1):
  - Injects realistic corrupted records controlled via --fault-rate or --scenario
  - Fault types:
      * NEGATIVE_AMOUNT:       amount < 0
      * NULL_ORIGIN:           nameOrig is empty / null
      * INVALID_TYPE:          type not in {PAYMENT, TRANSFER, CASH_OUT, CASH_IN, DEBIT}
      * TOO_INCOMPLETE:        multiple non-critical fields stripped (< 60% completeness)
      * DUPLICATE:             re-emits previously seen txn_ids to test idempotent merge

Usage:
    # Baseline clean stream
    python src/ingestion/stream_producer.py --rate 100 --batches 10 --delay 1.0

    # Normal stream with 5% faults to feed the DLQ live
    python src/ingestion/stream_producer.py --rate 100 --batches 10 --fault-rate 0.05

    # Preset scenarios from the EFIP Scenario Matrix
    python src/ingestion/stream_producer.py --scenario corrupt-spike   # 20% faults (triggers DLQ alarm)
    python src/ingestion/stream_producer.py --scenario burst           # 50 files dropped with 0 delay
    python src/ingestion/stream_producer.py --scenario duplicate       # Re-emits duplicates in batch 2
"""

import argparse
import datetime
import os
import random
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
STREAMING_SOURCE = PROJECT_ROOT / "data" / "lake" / "streaming_source"
RAW_CSV = PROJECT_ROOT / "data" / "raw" / "Paysim_dataset.csv"

# Valid PaySim types from Flow 1
VALID_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]

# Invalid types for injection
INVALID_INJECTION_TYPES = ["CRYPTO_ESCROW", "BITCOIN_PAYMENT", "WIRE_UNVERIFIED", "UNKNOWN_GATEWAY"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="EFIP Stream Producer with Fault Injection & Traceability"
    )
    parser.add_argument(
        "--rate", type=int, default=50,
        help="Records per micro-batch (default: 50)"
    )
    parser.add_argument(
        "--jitter", type=float, default=0.3,
        help="Random delay jitter factor 0-1 (default: 0.3)"
    )
    parser.add_argument(
        "--batches", type=int, default=0,
        help="Number of batches to emit (0 = entire dataset, default: 0)"
    )
    parser.add_argument(
        "--delay", type=float, default=2.0,
        help="Base delay between batches in seconds (default: 2.0)"
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Remove existing streaming source files before starting"
    )
    parser.add_argument(
        "--fault-rate", type=float, default=0.0,
        help="Fraction of records to corrupt (0.0 to 1.0, e.g. 0.05 = 5%%, default: 0.0)"
    )
    parser.add_argument(
        "--scenario", type=str, default="custom",
        choices=["custom", "normal", "corrupt", "corrupt-spike", "burst", "duplicate"],
        help="Scenario preset from EFIP matrix (normal, corrupt, corrupt-spike, burst, duplicate)"
    )
    return parser.parse_args()


def clean_source_dir(source_dir: Path):
    """Remove existing micro-batch files from the streaming source directory."""
    if source_dir.exists():
        csv_files = list(source_dir.glob("batch_*.csv"))
        for f in csv_files:
            try:
                f.unlink()
            except OSError:
                pass
        print(f"[PRODUCER] Cleaned {len(csv_files)} existing batch files in {source_dir}")


def add_traceability_metadata(
    df_chunk: pd.DataFrame, batch_num: int, trace_id: str
) -> pd.DataFrame:
    """
    Appends txn_id, trace_id, and emit_ts to each record.
    Preserves original columns in first positions for full backward compatibility.
    """
    df = df_chunk.copy()
    emit_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

    txn_ids = []
    for idx, row in df.iterrows():
        step_val = row.get("step", 0)
        # Deterministic unique ID: TXN_<step>_<batch>_<row_idx>
        txn_ids.append(f"TXN_{step_val}_{batch_num:04d}_{idx:06d}")

    df["txn_id"] = txn_ids
    df["trace_id"] = trace_id
    df["emit_ts"] = emit_ts
    return df


def inject_faults(
    df_chunk: pd.DataFrame, fault_rate: float, seed: int = None
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Injects synthetic corruption into a fraction of records to exercise the
    Quality Gate and DLQ (Flow 1).

    Fault categories:
      1. NEGATIVE_AMOUNT:  amount < 0
      2. NULL_ORIGIN:      nameOrig is null/empty
      3. INVALID_TYPE:     transaction type is unrecognized
      4. TOO_INCOMPLETE:   multiple fields wiped (< 60% completeness)

    Returns:
        (corrupted_df, fault_summary_counts)
    """
    df = df_chunk.copy()
    total_records = len(df)
    if fault_rate <= 0 or total_records == 0:
        return df, {}

    if seed is not None:
        random.seed(seed)

    num_faults = max(1, int(round(total_records * fault_rate)))
    # Pick random indices to corrupt
    candidate_indices = list(df.index)
    target_indices = random.sample(candidate_indices, min(num_faults, len(candidate_indices)))

    fault_counts: Dict[str, int] = {
        "NEGATIVE_AMOUNT": 0,
        "NULL_ORIGIN": 0,
        "INVALID_TYPE": 0,
        "TOO_INCOMPLETE": 0,
    }

    # Rotate through fault types to ensure balanced testing of DLQ reason codes
    fault_types = ["NEGATIVE_AMOUNT", "NULL_ORIGIN", "INVALID_TYPE", "TOO_INCOMPLETE"]

    for i, idx in enumerate(target_indices):
        fault_type = fault_types[i % len(fault_types)]
        fault_counts[fault_type] += 1

        if fault_type == "NEGATIVE_AMOUNT":
            current_amt = float(df.at[idx, "amount"]) if pd.notna(df.at[idx, "amount"]) else 100.0
            df.at[idx, "amount"] = -abs(current_amt if current_amt != 0 else 500.0)

        elif fault_type == "NULL_ORIGIN":
            df.at[idx, "nameOrig"] = None

        elif fault_type == "INVALID_TYPE":
            df.at[idx, "type"] = random.choice(INVALID_INJECTION_TYPES)

        elif fault_type == "TOO_INCOMPLETE":
            # Wipe 6 non-critical fields (leaving 5/11 fields = 45% completeness < 60%)
            fields_to_wipe = [
                "oldbalanceOrg", "newbalanceOrig", "nameDest",
                "oldbalanceDest", "newbalanceDest", "isFlaggedFraud"
            ]
            for f in fields_to_wipe:
                if f in df.columns:
                    df.at[idx, f] = None

    return df, fault_counts


def apply_scenario_config(args):
    """Overrides command-line flags based on preset scenario name."""
    if args.scenario == "normal":
        args.fault_rate = 0.0
        args.rate = 100
        args.delay = 1.0
        print("[SCENARIO] Selected: NORMAL (Clean stream, 0% faults, 100 rec/batch, delay=1.0s)")

    elif args.scenario == "corrupt":
        args.fault_rate = 0.05
        args.rate = 100
        args.delay = 1.0
        print("[SCENARIO] Selected: CORRUPT (5% faults across 4 reason codes to exercise DLQ)")

    elif args.scenario == "corrupt-spike":
        args.fault_rate = 0.20
        args.rate = 100
        args.delay = 1.0
        print("[SCENARIO] Selected: CORRUPT-SPIKE (20% faults to trigger DLQ alarm > 5%)")

    elif args.scenario == "burst":
        args.rate = 200
        args.batches = 25
        args.delay = 0.0
        args.jitter = 0.0
        print("[SCENARIO] Selected: BURST (5,000 records dropped instantly with zero delay)")

    elif args.scenario == "duplicate":
        args.rate = 50
        args.batches = 3
        args.delay = 1.0
        args.fault_rate = 0.0
        print("[SCENARIO] Selected: DUPLICATE (Batch 2 will re-emit duplicate txn_ids from Batch 1)")


def main():
    args = parse_args()
    apply_scenario_config(args)

    if not RAW_CSV.exists():
        print(f"[ERROR] Raw CSV not found at: {RAW_CSV}")
        print("        Ensure Paysim_dataset.csv is present in data/raw/")
        sys.exit(1)

    STREAMING_SOURCE.mkdir(parents=True, exist_ok=True)

    if args.clean:
        clean_source_dir(STREAMING_SOURCE)

    print(f"[PRODUCER] Loading {RAW_CSV}...")
    df = pd.read_csv(RAW_CSV)
    total_rows = len(df)
    print(f"[PRODUCER] Loaded {total_rows:,} rows from source dataset")

    # Order by step for temporal arrival realism
    df = df.sort_values("step").reset_index(drop=True)

    chunk_size = args.rate
    max_batches = args.batches if args.batches > 0 else (total_rows + chunk_size - 1) // chunk_size

    print("=" * 70)
    print("  EFIP STREAM PRODUCER")
    print("=" * 70)
    print(f"  Target landing zone:  {STREAMING_SOURCE}")
    print(f"  Batch size (rate):    {chunk_size} records/file")
    print(f"  Planned batches:      {max_batches}")
    print(f"  Base delay:           {args.delay}s (jitter: ±{args.jitter})")
    print(f"  Fault injection rate: {args.fault_rate * 100:.1f}%")
    print("=" * 70)

    batch_num = 0
    start_time = time.time()
    total_records_emitted = 0
    total_faults_injected = 0
    cumulative_fault_breakdown: Dict[str, int] = {}
    previous_batch_df: pd.DataFrame = None

    for i in range(0, total_rows, chunk_size):
        if batch_num >= max_batches:
            break

        chunk = df.iloc[i : i + chunk_size].copy()
        trace_id = f"trc_{uuid.uuid4().hex[:12]}"

        # Scenario: duplicate injection
        if args.scenario == "duplicate" and batch_num == 1 and previous_batch_df is not None:
            # Re-inject 10 duplicate rows from previous batch with IDENTICAL txn_ids
            num_dups = min(10, len(previous_batch_df))
            duplicate_rows = previous_batch_df.head(num_dups).copy()
            # Replace bottom rows of current chunk with duplicates
            chunk = pd.concat([chunk.iloc[:-num_dups], duplicate_rows]).reset_index(drop=True)
            print(f"  [Batch {batch_num:>4d}] Injected {num_dups} duplicate records from Batch 0")

        # Step 1: Add metadata
        chunk = add_traceability_metadata(chunk, batch_num, trace_id)

        # Step 2: Inject faults if configured
        chunk, faults = inject_faults(chunk, args.fault_rate)

        # Record metrics
        batch_faults = sum(faults.values())
        total_faults_injected += batch_faults
        for ftype, count in faults.items():
            cumulative_fault_breakdown[ftype] = cumulative_fault_breakdown.get(ftype, 0) + count

        # Step 3: Write micro-batch to landing zone
        batch_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:19]
        filename = f"batch_{batch_num:06d}_{batch_ts}.csv"
        output_path = STREAMING_SOURCE / filename
        chunk.to_csv(output_path, index=False)

        previous_batch_df = chunk.copy()
        batch_num += 1
        total_records_emitted += len(chunk)
        elapsed = time.time() - start_time

        fault_detail = ""
        if batch_faults > 0:
            active_faults = [f"{k}: {v}" for k, v in faults.items() if v > 0]
            fault_detail = f" | [FAULTS: {batch_faults} ({', '.join(active_faults)})]"

        print(
            f"  [Batch {batch_num:>4d}/{max_batches}] "
            f"{len(chunk):>5d} rows -> {filename} "
            f"(elapsed: {elapsed:>5.1f}s){fault_detail}"
        )

        # Step 4: Inter-batch pacing with jitter
        if batch_num < max_batches and args.delay > 0:
            jitter_offset = random.uniform(-args.jitter, args.jitter) * args.delay
            sleep_time = max(0.01, args.delay + jitter_offset)
            time.sleep(sleep_time)

    total_elapsed = time.time() - start_time
    print("=" * 70)
    print("  PRODUCER EMISSION COMPLETE")
    print("=" * 70)
    print(f"  Total batches emitted:   {batch_num}")
    print(f"  Total records emitted:   {total_records_emitted:,}")
    print(f"  Total faults injected:   {total_faults_injected:,} "
          f"({(total_faults_injected / total_records_emitted * 100) if total_records_emitted else 0:.1f}%)")
    if cumulative_fault_breakdown:
        print("  Fault breakdown by type:")
        for ftype, count in cumulative_fault_breakdown.items():
            print(f"    - {ftype:<20s}: {count:>5d}")
    print(f"  Elapsed wall-clock time: {total_elapsed:.2f}s")
    print(f"  Average throughput:      {total_records_emitted / total_elapsed:.1f} rec/s")
    print(f"  Landing directory:       {STREAMING_SOURCE}")
    print("=" * 70)


if __name__ == "__main__":
    main()
