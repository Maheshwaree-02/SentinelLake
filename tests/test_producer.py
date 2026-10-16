"""
Unit tests for the upgraded Stream Producer (Phase 1).
Verifies:
  - Traceability metadata generation (txn_id, trace_id, emit_ts)
  - Synthetic fault injection across all 4 categories
  - Preset scenario configuration handling
  - CSV landing zone cleanup
"""

import datetime
from pathlib import Path
import pandas as pd
import pytest

from src.ingestion.stream_producer import (
    add_traceability_metadata,
    inject_faults,
    clean_source_dir,
    apply_scenario_config,
    VALID_TYPES,
    INVALID_INJECTION_TYPES,
)


@pytest.fixture
def clean_sample_df():
    """Returns a 10-row clean DataFrame matching PaySim structure."""
    rows = []
    for i in range(10):
        rows.append({
            "step": 1,
            "type": "PAYMENT" if i % 2 == 0 else "TRANSFER",
            "amount": float(100 * (i + 1)),
            "nameOrig": f"C{1000 + i}",
            "oldbalanceOrg": 5000.0,
            "newbalanceOrig": 5000.0 - (100 * (i + 1)),
            "nameDest": f"M{2000 + i}",
            "oldbalanceDest": 0.0,
            "newbalanceDest": float(100 * (i + 1)),
            "isFraud": 0,
            "isFlaggedFraud": 0,
        })
    return pd.DataFrame(rows)


def test_add_traceability_metadata(clean_sample_df):
    """Test that txn_id, trace_id, and emit_ts are properly added."""
    batch_num = 3
    trace_id = "trc_test123456"

    tagged_df = add_traceability_metadata(clean_sample_df, batch_num, trace_id)

    assert "txn_id" in tagged_df.columns
    assert "trace_id" in tagged_df.columns
    assert "emit_ts" in tagged_df.columns

    # Verify all trace_ids match
    assert (tagged_df["trace_id"] == trace_id).all()

    # Verify txn_ids are unique and formatted correctly
    assert tagged_df["txn_id"].nunique() == len(clean_sample_df)
    assert tagged_df["txn_id"].iloc[0].startswith("TXN_1_0003_")

    # Verify emit_ts parses as valid ISO timestamp
    ts_str = tagged_df["emit_ts"].iloc[0]
    parsed_dt = datetime.datetime.fromisoformat(ts_str)
    assert parsed_dt.year >= 2024


def test_inject_faults_zero_rate(clean_sample_df):
    """Zero fault rate should return DataFrame unmodified with zero faults."""
    df, faults = inject_faults(clean_sample_df, fault_rate=0.0)
    assert faults == {}
    assert (df["amount"] > 0).all()
    assert df["nameOrig"].notna().all()


def test_inject_faults_active_rate(clean_sample_df):
    """Active fault rate should inject expected error types and record counts."""
    # 50% fault rate on 10 rows = 5 faults
    df, faults = inject_faults(clean_sample_df, fault_rate=0.50, seed=42)

    assert sum(faults.values()) == 5

    # Check for negative amount
    has_negative_amount = (df["amount"] < 0).any()
    # Check for null origin
    has_null_origin = df["nameOrig"].isna().any()
    # Check for invalid type
    has_invalid_type = df["type"].isin(INVALID_INJECTION_TYPES).any()

    # At least some of these must be true given 5 rotating faults
    assert has_negative_amount or has_null_origin or has_invalid_type


def test_scenario_presets():
    """Test scenario preset parameter overrides."""
    class DummyArgs:
        scenario = "corrupt-spike"
        fault_rate = 0.0
        rate = 50
        delay = 2.0
        jitter = 0.3
        batches = 0

    args = DummyArgs()
    apply_scenario_config(args)

    assert args.fault_rate == 0.20
    assert args.rate == 100
    assert args.delay == 1.0


def test_clean_source_dir(tmp_path):
    """Test that batch_*.csv files are cleaned from source directory."""
    test_dir = tmp_path / "streaming_source"
    test_dir.mkdir()

    # Create dummy files
    f1 = test_dir / "batch_000000_123.csv"
    f2 = test_dir / "batch_000001_124.csv"
    other = test_dir / "other_file.txt"
    f1.write_text("data")
    f2.write_text("data")
    other.write_text("important")

    clean_source_dir(test_dir)

    assert not f1.exists()
    assert not f2.exists()
    assert other.exists()  # Non-batch file should remain
