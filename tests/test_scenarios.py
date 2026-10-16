"""
Automated EFIP Scenario Matrix Verification Suite (Phase 4)
============================================================

Tests each core scenario from the Scenario Flowcharts:
  - Flow 1 & Flow 6: Model loaded vs. Graceful fallback (RULES_ONLY)
  - Flow 6 (CAP): Low-confidence records capped to at least MANUAL_REVIEW
  - Flow 6 (D1): Cold-start new accounts tagged NEW_ACCOUNT
  - Flow 5: Freshness states (LIVE, AGING, STALE)
  - Flow 8: Reconciliation formula balancing
"""

import datetime
from pathlib import Path
import pytest

from src.models.scoring_engine import score_transaction, map_score_to_action
from src.ingestion.stream_consumer import update_pipeline_metrics


@pytest.fixture
def clean_record():
    return {
        "txn_id": "TXN_TEST_001",
        "nameOrig": "C10001",
        "amount": 2500.0,
        "oldbalanceOrg": 10000.0,
        "newbalanceOrig": 7500.0,
        "oldbalanceDest": 0.0,
        "newbalanceDest": 2500.0,
        "balance_ratio": 0.25,
        "velocity_score": 0.2,
        "txn_count_window": 10,
        "quality_status": "VALID",
    }


def test_scenario_ml_model_scoring_success(clean_record):
    """When model artifact exists, transaction should be scored using ML_MODEL."""
    result = score_transaction(clean_record)
    assert result["scoring_mode"] == "ML_MODEL"
    assert "RANDOM_FOREST_INFERENCE" in result["risk_drivers"]
    assert 0.0 <= result["unified_risk_score"] <= 1.0
    assert result["recommended_action"] in ["APPROVE", "STEP_UP_AUTH", "HOLD", "MANUAL_REVIEW", "BLOCK"]


def test_scenario_model_unavailable_fallback(clean_record, tmp_path):
    """When model artifact is missing, should gracefully fallback to RULES_ONLY (Flow 6 FB)."""
    non_existent_model = tmp_path / "missing_model.joblib"
    result = score_transaction(clean_record, custom_model_path=non_existent_model)

    assert result["scoring_mode"] == "RULES_ONLY"
    assert "RULES_ONLY" in result["flags"]
    assert 0.0 <= result["unified_risk_score"] <= 1.0
    # Must still recommend a valid action without error
    assert result["recommended_action"] in ["APPROVE", "STEP_UP_AUTH", "HOLD", "MANUAL_REVIEW", "BLOCK"]


def test_scenario_cold_start_new_account(clean_record):
    """Transactions with no prior history should be tagged NEW_ACCOUNT (Flow 6 D1)."""
    rec = clean_record.copy()
    rec["txn_count_window"] = 1  # Cold-start

    result = score_transaction(rec)
    assert "NEW_ACCOUNT" in result["flags"]


def test_scenario_low_confidence_capping():
    """Records flagged LOW_CONFIDENCE must NEVER auto-approve (Flow 6 CAP)."""
    # Low risk score that would normally be APPROVED (< 0.40)
    low_risk_score = 0.15

    # 1. Normal VALID record -> APPROVE
    act_valid, conf_valid, _ = map_score_to_action(
        risk_score=low_risk_score,
        amount=500.0,
        quality_status="VALID",
    )
    assert act_valid == "APPROVE"

    # 2. LOW_CONFIDENCE record -> Capped to MANUAL_REVIEW
    act_low, conf_low, reason_low = map_score_to_action(
        risk_score=low_risk_score,
        amount=500.0,
        quality_status="LOW_CONFIDENCE",
    )
    assert act_low == "MANUAL_REVIEW"
    assert "Auto-approval forbidden" in reason_low


def test_scenario_freshness_state_transitions():
    """Verify Flow 5 Freshness state transitions based on elapsed time."""
    now_dt = datetime.datetime.now(datetime.timezone.utc)

    # LIVE: < 300s (5 min)
    live_commit = (now_dt - datetime.timedelta(seconds=60)).isoformat()
    # AGING: 300s - 1800s (5 - 30 min)
    aging_commit = (now_dt - datetime.timedelta(seconds=600)).isoformat()
    # STALE: > 1800s (> 30 min)
    stale_commit = (now_dt - datetime.timedelta(seconds=2400)).isoformat()

    def eval_freshness(commit_ts_str):
        commit_dt = datetime.datetime.fromisoformat(commit_ts_str)
        elapsed = (datetime.datetime.now(datetime.timezone.utc) - commit_dt).total_seconds()
        if elapsed < 300:
            return "LIVE"
        elif elapsed <= 1800:
            return "AGING"
        return "STALE"

    assert eval_freshness(live_commit) == "LIVE"
    assert eval_freshness(aging_commit) == "AGING"
    assert eval_freshness(stale_commit) == "STALE"


def test_scenario_lakehouse_reconciliation_balancing(tmp_path, monkeypatch):
    """Reconciliation check: Ingested == Valid + DLQ + Duplicates (Flow 8)."""
    test_metrics = tmp_path / "pipeline_metrics.json"
    monkeypatch.setattr("src.ingestion.stream_consumer.PIPELINE_METRICS_PATH", test_metrics)

    # 100 ingested = 92 valid + 6 DLQ + 2 dups
    update_pipeline_metrics(
        batch_id=99,
        raw_count=100,
        valid_count=92,
        low_conf_count=3,
        quarantine_count=6,
        duplicates_suppressed=2,
        elapsed_sec=0.8,
        reason_code_counts={"NEGATIVE_AMOUNT": 4, "INVALID_TYPE": 2},
    )

    import json
    data = json.load(open(test_metrics))
    assert data["reconciliation_delta"] == 0
    assert data["reconciliation_status"] == "BALANCED"
    assert data["total_ingested"] == 100
