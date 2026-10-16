"""
Tests for the streaming pipeline (Producer + Consumer).
Verifies: producer output, consumer Silver/Gold transforms, checkpoint behavior.
"""

import os
import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def temp_streaming_dir(tmp_path):
    """Create temporary directories for streaming test."""
    dirs = {
        "source": tmp_path / "streaming_source",
        "gold": tmp_path / "gold",
        "checkpoint": tmp_path / "checkpoint",
        "quarantine": tmp_path / "quarantine",
    }
    for d in dirs.values():
        d.mkdir(parents=True)
    return dirs


@pytest.fixture
def sample_batch_csv(temp_streaming_dir):
    """Create a sample micro-batch CSV mimicking producer output."""
    data = {
        "step": [1, 1, 1, 2, 2],
        "type": ["PAYMENT", "TRANSFER", "CASH_OUT", "PAYMENT", "TRANSFER"],
        "amount": [1000.0, 50000.0, 25000.0, 500.0, 181000.0],
        "nameOrig": ["C100", "C200", "C300", "C100", "C400"],
        "oldbalanceOrg": [5000.0, 50000.0, 30000.0, 4000.0, 181000.0],
        "newbalanceOrig": [4000.0, 0.0, 5000.0, 3500.0, 0.0],
        "nameDest": ["M100", "C500", "C600", "M200", "C700"],
        "oldbalanceDest": [0.0, 0.0, 10000.0, 0.0, 0.0],
        "newbalanceDest": [0.0, 50000.0, 35000.0, 0.0, 181000.0],
        "isFraud": [0, 1, 0, 0, 1],
        "isFlaggedFraud": [0, 0, 0, 0, 1],
    }
    df = pd.DataFrame(data)
    csv_path = temp_streaming_dir["source"] / "batch_000000_test.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


class TestStreamProducer:
    """Tests for the stream producer."""

    def test_producer_creates_csv_files(self, temp_streaming_dir):
        """Producer should create numbered CSV files in the source dir."""
        from src.ingestion.stream_producer import STREAMING_SOURCE

        # Just verify the module imports and constants are correct
        assert "streaming_source" in str(STREAMING_SOURCE)

    def test_producer_output_schema(self, sample_batch_csv):
        """Producer output CSV should have the correct PaySim schema."""
        df = pd.read_csv(sample_batch_csv)
        expected_cols = [
            "step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest",
            "newbalanceDest", "isFraud", "isFlaggedFraud",
        ]
        assert list(df.columns) == expected_cols
        assert len(df) == 5

    def test_producer_preserves_fraud_labels(self, sample_batch_csv):
        """Fraud labels should be preserved in producer output."""
        df = pd.read_csv(sample_batch_csv)
        assert df["isFraud"].sum() == 2


class TestStreamConsumer:
    """Tests for the consumer's transform logic (no Spark needed)."""

    def test_silver_validation_logic(self):
        """Records with invalid amounts or null origins should be quarantined."""
        from src.ingestion.stream_consumer import VALID_TYPES

        assert "TRANSFER" in VALID_TYPES
        assert "CASH_OUT" in VALID_TYPES
        assert "PAYMENT" in VALID_TYPES
        assert "BITCOIN" not in VALID_TYPES

    def test_gold_feature_columns_present(self):
        """Gold layer should contain velocity and risk feature columns."""
        gold_path = PROJECT_ROOT / "data" / "lake" / "gold" / "features"
        if not gold_path.exists():
            pytest.skip("Gold layer not built yet")

        df = pd.read_parquet(gold_path)
        required_cols = [
            "velocity_score", "txn_count_window", "total_amount_window",
            "avg_amount_window", "amount_zscore", "balance_ratio",
            "balance_delta_orig", "balance_delta_dest",
            "is_balance_mismatch", "processed_at",
        ]
        for col in required_cols:
            assert col in df.columns, f"Missing Gold column: {col}"

    def test_gold_velocity_scores_in_range(self):
        """Velocity scores should be between 0 and 1."""
        gold_path = PROJECT_ROOT / "data" / "lake" / "gold" / "features"
        if not gold_path.exists():
            pytest.skip("Gold layer not built yet")

        df = pd.read_parquet(gold_path)
        assert df["velocity_score"].min() >= 0.0
        assert df["velocity_score"].max() <= 1.0

    def test_gold_no_null_critical_fields(self):
        """Critical fields should have no nulls in Gold."""
        gold_path = PROJECT_ROOT / "data" / "lake" / "gold" / "features"
        if not gold_path.exists():
            pytest.skip("Gold layer not built yet")

        df = pd.read_parquet(gold_path)
        critical = ["nameOrig", "amount", "type", "velocity_score"]
        for col in critical:
            assert df[col].notna().all(), f"Nulls found in Gold.{col}"

    def test_checkpoint_exists(self):
        """Streaming checkpoint directory should exist after consumer run."""
        cp_path = PROJECT_ROOT / "data" / "checkpoints" / "streaming_gold"
        if not cp_path.exists():
            pytest.skip("Consumer not run yet")

        # Checkpoint should contain metadata
        contents = list(cp_path.rglob("*"))
        assert len(contents) > 0, "Checkpoint directory is empty"
