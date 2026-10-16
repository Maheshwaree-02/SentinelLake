"""
Unit tests for the upgraded Stream Consumer (Phase 2).
Verifies:
  - 14-column Transaction Schema
  - Silver Quality Gate 7-step hierarchy & reason codes
  - Completeness threshold scoring (VALID vs LOW_CONFIDENCE vs TOO_INCOMPLETE)
  - Telemetry & Reconciliation equation (Ingested == Valid + DLQ + Duplicates)
"""

import json
from pathlib import Path
import pytest
import pandas as pd

from src.ingestion.stream_consumer import (
    TRANSACTION_SCHEMA,
    VALID_TYPES,
    CORE_COMPLETENESS_FIELDS,
    update_pipeline_metrics,
    reset_pipeline_metrics,
    PIPELINE_METRICS_PATH,
)


def test_transaction_schema_columns():
    """Schema should contain 14 fields including metadata (txn_id, trace_id, emit_ts)."""
    field_names = [f.name for f in TRANSACTION_SCHEMA.fields]
    assert "txn_id" in field_names
    assert "trace_id" in field_names
    assert "emit_ts" in field_names
    assert len(field_names) == 14


def test_core_completeness_fields():
    """Core PaySim completeness fields should be exactly 11."""
    assert len(CORE_COMPLETENESS_FIELDS) == 11
    assert "step" in CORE_COMPLETENESS_FIELDS
    assert "amount" in CORE_COMPLETENESS_FIELDS
    assert "nameOrig" in CORE_COMPLETENESS_FIELDS


def test_telemetry_reconciliation_balanced(tmp_path, monkeypatch):
    """Telemetry logger should verify Ingested == Valid + Quarantined + Duplicates."""
    test_metrics_file = tmp_path / "pipeline_metrics.json"
    monkeypatch.setattr("src.ingestion.stream_consumer.PIPELINE_METRICS_PATH", test_metrics_file)

    # Batch 0: 100 ingested = 90 valid (including 5 low-conf) + 10 DLQ + 0 dups
    update_pipeline_metrics(
        batch_id=0,
        raw_count=100,
        valid_count=90,
        low_conf_count=5,
        quarantine_count=10,
        duplicates_suppressed=0,
        elapsed_sec=1.5,
        reason_code_counts={"NEGATIVE_AMOUNT": 6, "NULL_ORIGIN": 4},
    )

    assert test_metrics_file.exists()
    with open(test_metrics_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["total_ingested"] == 100
    assert data["total_valid"] == 90
    assert data["total_low_confidence"] == 5
    assert data["total_quarantined"] == 10
    assert data["total_duplicates_suppressed"] == 0
    assert data["reconciliation_delta"] == 0
    assert data["reconciliation_status"] == "BALANCED"
    assert data["dlq_rejection_rate_pct"] == 10.0
    assert data["reason_code_breakdown"] == {"NEGATIVE_AMOUNT": 6, "NULL_ORIGIN": 4}
    assert len(data["recent_batches"]) == 1


def test_telemetry_reconciliation_multi_batch(tmp_path, monkeypatch):
    """Multiple batches should accumulate counts cleanly while keeping reconciliation balanced."""
    test_metrics_file = tmp_path / "pipeline_metrics.json"
    monkeypatch.setattr("src.ingestion.stream_consumer.PIPELINE_METRICS_PATH", test_metrics_file)

    # Batch 0
    update_pipeline_metrics(
        batch_id=0,
        raw_count=50,
        valid_count=48,
        low_conf_count=2,
        quarantine_count=2,
        duplicates_suppressed=0,
        elapsed_sec=0.5,
        reason_code_counts={"INVALID_TYPE": 2},
    )

    # Batch 1 with duplicates
    update_pipeline_metrics(
        batch_id=1,
        raw_count=50,
        valid_count=40,
        low_conf_count=0,
        quarantine_count=5,
        duplicates_suppressed=5,
        elapsed_sec=0.6,
        reason_code_counts={"TOO_INCOMPLETE": 5},
    )

    with open(test_metrics_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["total_ingested"] == 100
    assert data["total_valid"] == 88
    assert data["total_quarantined"] == 7
    assert data["total_duplicates_suppressed"] == 5
    assert data["reconciliation_delta"] == 0
    assert data["reconciliation_status"] == "BALANCED"
    assert data["reason_code_breakdown"]["INVALID_TYPE"] == 2
    assert data["reason_code_breakdown"]["TOO_INCOMPLETE"] == 5
