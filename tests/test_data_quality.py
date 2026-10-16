"""
Tests for the data quality gate (Phase C).
Covers: all-fields-present, missing-critical-field, borderline-completeness.
"""

import pytest
from src.etl.data_quality import (
    classify_record, classify_batch, RecordStatus, ALL_FIELDS
)


@pytest.fixture
def complete_record():
    """A perfectly formed record with all fields."""
    return {
        "step": 1,
        "type": "TRANSFER",
        "amount": 1000.0,
        "nameOrig": "C100",
        "oldbalanceOrg": 5000.0,
        "newbalanceOrig": 4000.0,
        "nameDest": "C200",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 1000.0,
        "isFraud": 0,
        "isFlaggedFraud": 0
    }


def test_complete_record_is_valid(complete_record):
    """A record with all fields should be VALID with 100% completeness."""
    result = classify_record(complete_record)
    assert result.status == RecordStatus.VALID
    assert result.completeness == 1.0
    assert result.reason is None
    assert not result.missing_fields


def test_missing_critical_field_is_quarantined(complete_record):
    """Missing a critical field (amount) should route to QUARANTINE."""
    record = complete_record.copy()
    record["amount"] = None

    result = classify_record(record)
    assert result.status == RecordStatus.QUARANTINE
    assert result.reason == "MISSING_CRITICAL_FIELD"
    assert "amount" in result.missing_fields


def test_empty_string_critical_field_is_quarantined(complete_record):
    """An empty string in a critical field (nameOrig) should route to QUARANTINE."""
    record = complete_record.copy()
    record["nameOrig"] = "   "

    result = classify_record(record)
    assert result.status == RecordStatus.QUARANTINE
    assert result.reason == "MISSING_CRITICAL_FIELD"
    assert "nameOrig" in result.missing_fields


def test_borderline_completeness_is_low_confidence(complete_record):
    """Missing ~30% of fields (non-critical) should flag as LOW_CONFIDENCE."""
    record = complete_record.copy()
    # Remove 3 non-critical fields (8/11 = 72% completeness, below 90% threshold)
    record["isFraud"] = None
    record["isFlaggedFraud"] = None
    record["newbalanceDest"] = None

    result = classify_record(record)
    assert result.status == RecordStatus.LOW_CONFIDENCE
    assert result.completeness == round(8 / 11, 4)
    assert len(result.missing_fields) == 3


def test_low_completeness_is_quarantined():
    """Missing >40% of fields (even if critical present) should route to QUARANTINE."""
    # Only 4/11 fields present (36% completeness, below 60% threshold)
    record = {f: None for f in ALL_FIELDS}
    record["step"] = 1
    record["type"] = "PAYMENT"
    record["amount"] = 50.0
    record["nameOrig"] = "C999"

    result = classify_record(record)
    assert result.status == RecordStatus.QUARANTINE
    assert result.reason == "INSUFFICIENT_DATA"
    assert len(result.missing_fields) == 7


def test_classify_batch(complete_record):
    """Batch classification should correctly group records."""
    valid_rec = complete_record.copy()
    
    quarantine_rec = complete_record.copy()
    quarantine_rec["type"] = None
    
    low_conf_rec = complete_record.copy()
    low_conf_rec["oldbalanceOrg"] = None
    low_conf_rec["newbalanceOrig"] = None
    low_conf_rec["nameDest"] = None

    batch = [valid_rec, quarantine_rec, low_conf_rec]
    results = classify_batch(batch)

    assert len(results["valid"]) == 1
    assert len(results["quarantine"]) == 1
    assert len(results["low_confidence"]) == 1
    
    # Check that the tuple unpacking works
    rec, res = results["quarantine"][0]
    assert res.reason == "MISSING_CRITICAL_FIELD"
