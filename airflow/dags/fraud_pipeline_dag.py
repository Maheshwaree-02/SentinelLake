"""
Enterprise Fraud Detection Pipeline — Airflow DAG
==================================================

Orchestrates the end-to-end fraud detection pipeline across the medallion
architecture layers (Bronze → Silver → Gold) and downstream ML scoring /
recommendation generation.

Pipeline stages
---------------
1. bronze_ingestion        – Ingest raw PaySim CSV into the Bronze layer
                             (Delta / Parquet format).
2. silver_transformation   – Schema validation, deduplication, enrichment
                             with balance-delta columns.
3. gold_feature_engineering – Velocity features, z-scores, window
                              aggregations for ML consumption.
4. model_scoring           – Apply the trained Random Forest model and
                             produce a unified_risk_score.
5. recommendation_generation – Rule-based engine that maps risk scores
                               to actionable recommendations.
6. data_quality_check      – Row-count validation and null-percentage
                             checks on pipeline outputs.

Task dependency graph
---------------------
    bronze >> silver >> gold >> [model_scoring, data_quality_check]
                                          ↘               ↙
                                    recommendation_generation

Notes
-----
* The actual heavy-lifting (PySpark / Delta Lake) runs on Databricks.
  Each task here is a **simulation stub** that logs intended actions so the
  DAG can be tested locally before wiring up Databricks operators.
* DAG is scheduled to run daily with ``catchup=False``.

Author : Data Engineering Team
Created: 2024-01-01
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
logger = logging.getLogger("fraud_pipeline")

# ---------------------------------------------------------------------------
# Default arguments applied to every task in the DAG
# ---------------------------------------------------------------------------
default_args: Dict[str, Any] = {
    "owner": "fraud-detection-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _log_stage(stage_name: str, details: str) -> None:
    """Centralised logging helper for pipeline stages."""
    logger.info("=" * 60)
    logger.info("STAGE : %s", stage_name)
    logger.info("DETAIL: %s", details)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# PythonOperator-style callable  (used as a fallback example)
# ---------------------------------------------------------------------------

def _run_data_quality_check(**context: Any) -> Dict[str, Any]:
    """Validate output row counts and null-percentage thresholds.

    This callable is invoked via a classic ``PythonOperator`` to
    demonstrate the non-TaskFlow approach.

    Parameters
    ----------
    context : dict
        Airflow template context (provides ``ds``, ``ts``, etc.).

    Returns
    -------
    dict
        A summary of the quality-check results.
    """
    execution_date = context.get("ds", "unknown")
    _log_stage(
        "data_quality_check",
        f"Running data-quality validations for execution date {execution_date}",
    )

    # ------------------------------------------------------------------
    # Simulation: In production these queries would run against the Gold
    # layer tables on Databricks / Delta Lake.
    # ------------------------------------------------------------------
    checks = {
        "gold_transactions_row_count": {"expected_min": 1_000, "status": "PASS"},
        "gold_transactions_null_pct_amount": {"threshold": 0.01, "status": "PASS"},
        "gold_transactions_null_pct_risk_score": {"threshold": 0.05, "status": "PASS"},
        "scored_output_row_count": {"expected_min": 1_000, "status": "PASS"},
    }

    for check_name, result in checks.items():
        logger.info("  ✓ %s — %s", check_name, result["status"])

    logger.info("All data-quality checks passed for %s.", execution_date)
    return checks


# ---------------------------------------------------------------------------
# DAG definition using the TaskFlow API
# ---------------------------------------------------------------------------

@dag(
    dag_id="enterprise_fraud_detection_pipeline",
    description=(
        "Daily end-to-end fraud detection pipeline: ingestion → transformation "
        "→ feature engineering → scoring → recommendations."
    ),
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fraud-detection", "medallion-architecture", "ml-pipeline"],
    doc_md=__doc__,
    max_active_runs=1,
)
def fraud_pipeline_dag() -> None:
    """Instantiate the Fraud Detection pipeline DAG."""

    # ------------------------------------------------------------------
    # Task 1 — Bronze Ingestion
    # ------------------------------------------------------------------
    @task(
        task_id="bronze_ingestion",
        doc_md="Ingest the raw PaySim CSV into the Bronze layer as Delta/Parquet.",
    )
    def bronze_ingestion() -> Dict[str, str]:
        """Read raw PaySim CSV and persist as Bronze-layer Delta/Parquet.

        Returns
        -------
        dict
            Metadata about the ingested dataset (path, row count, etc.).
        """
        _log_stage(
            "bronze_ingestion",
            "Reading raw PaySim CSV → writing Bronze Delta table",
        )

        # Simulated actions ------------------------------------------------
        logger.info("  → Locating source file: data/raw/paysim_transactions.csv")
        logger.info("  → Inferring schema from CSV header")
        logger.info("  → Writing to: bronze/transactions/ (Delta format)")
        logger.info("  → Adding ingestion metadata columns (_ingested_at, _source_file)")

        result = {
            "source": "data/raw/paysim_transactions.csv",
            "destination": "bronze/transactions/",
            "format": "delta",
            "status": "SUCCESS",
        }
        logger.info("Bronze ingestion completed: %s", result)
        return result

    # ------------------------------------------------------------------
    # Task 2 — Silver Transformation
    # ------------------------------------------------------------------
    @task(
        task_id="silver_transformation",
        doc_md="Schema validation, deduplication, and balance-delta enrichment.",
    )
    def silver_transformation(bronze_meta: Dict[str, str]) -> Dict[str, str]:
        """Clean and enrich Bronze data into the Silver layer.

        Parameters
        ----------
        bronze_meta : dict
            Metadata returned by the bronze_ingestion task.

        Returns
        -------
        dict
            Metadata about the Silver-layer output.
        """
        _log_stage(
            "silver_transformation",
            f"Transforming Bronze data from {bronze_meta.get('destination', 'N/A')}",
        )

        logger.info("  → Validating schema against expected columns")
        logger.info("  → Removing duplicate transaction records")
        logger.info("  → Computing balance deltas (orig & dest)")
        logger.info("  → Casting amount/balance columns to DecimalType(18,2)")
        logger.info("  → Writing to: silver/transactions/ (Delta format)")

        result = {
            "source": bronze_meta.get("destination", "bronze/transactions/"),
            "destination": "silver/transactions/",
            "format": "delta",
            "status": "SUCCESS",
        }
        logger.info("Silver transformation completed: %s", result)
        return result

    # ------------------------------------------------------------------
    # Task 3 — Gold Feature Engineering
    # ------------------------------------------------------------------
    @task(
        task_id="gold_feature_engineering",
        doc_md="Generate velocity features, z-scores, and window aggregations.",
    )
    def gold_feature_engineering(silver_meta: Dict[str, str]) -> Dict[str, str]:
        """Engineer ML-ready features from Silver data into the Gold layer.

        Parameters
        ----------
        silver_meta : dict
            Metadata returned by the silver_transformation task.

        Returns
        -------
        dict
            Metadata about the Gold-layer feature table.
        """
        _log_stage(
            "gold_feature_engineering",
            f"Engineering features from {silver_meta.get('destination', 'N/A')}",
        )

        logger.info("  → Computing 1h / 6h / 24h velocity features per nameOrig")
        logger.info("  → Calculating z-scores for amount relative to customer mean")
        logger.info("  → Building sliding-window aggregations (count, sum, avg)")
        logger.info("  → Joining with customer-profile dimension table")
        logger.info("  → Writing to: gold/fraud_features/ (Delta format)")

        result = {
            "source": silver_meta.get("destination", "silver/transactions/"),
            "destination": "gold/fraud_features/",
            "format": "delta",
            "status": "SUCCESS",
        }
        logger.info("Gold feature engineering completed: %s", result)
        return result

    # ------------------------------------------------------------------
    # Task 4 — Model Scoring
    # ------------------------------------------------------------------
    @task(
        task_id="model_scoring",
        doc_md="Score transactions using the trained Random Forest model.",
    )
    def model_scoring(gold_meta: Dict[str, str]) -> Dict[str, str]:
        """Apply the Random Forest fraud model and generate risk scores.

        Parameters
        ----------
        gold_meta : dict
            Metadata returned by the gold_feature_engineering task.

        Returns
        -------
        dict
            Metadata about the scored output table.
        """
        _log_stage(
            "model_scoring",
            f"Scoring features from {gold_meta.get('destination', 'N/A')}",
        )

        logger.info("  → Loading trained Random Forest model from MLflow registry")
        logger.info("  → Reading Gold feature table")
        logger.info("  → Generating fraud_probability via model.predict_proba()")
        logger.info("  → Computing unified_risk_score (model + rule blending)")
        logger.info("  → Writing to: gold/scored_transactions/ (Delta format)")

        result = {
            "source": gold_meta.get("destination", "gold/fraud_features/"),
            "destination": "gold/scored_transactions/",
            "model_version": "rf_v2.3",
            "format": "delta",
            "status": "SUCCESS",
        }
        logger.info("Model scoring completed: %s", result)
        return result

    # ------------------------------------------------------------------
    # Task 5 — Recommendation Generation
    # ------------------------------------------------------------------
    @task(
        task_id="recommendation_generation",
        doc_md="Apply rule-based engine to generate fraud-response actions.",
    )
    def recommendation_generation(
        scoring_meta: Dict[str, str],
        dq_results: Dict[str, Any],
    ) -> Dict[str, str]:
        """Map risk scores to actionable recommendations.

        Parameters
        ----------
        scoring_meta : dict
            Metadata returned by the model_scoring task.
        dq_results : dict
            Results from the data_quality_check task (must pass before
            recommendations are generated).

        Returns
        -------
        dict
            Metadata about the recommendations output.
        """
        _log_stage(
            "recommendation_generation",
            f"Generating recommendations from {scoring_meta.get('destination', 'N/A')}",
        )

        # Verify upstream DQ passed
        failed_checks = [
            k for k, v in dq_results.items() if v.get("status") != "PASS"
        ]
        if failed_checks:
            raise ValueError(
                f"Data-quality checks failed for: {failed_checks}. "
                "Aborting recommendation generation."
            )

        logger.info("  → Data-quality gate: ALL CHECKS PASSED ✓")
        logger.info("  → Reading scored transactions")
        logger.info("  → Applying rule-based engine:")
        logger.info("      • risk_score ≥ 0.85 → BLOCK transaction")
        logger.info("      • risk_score ≥ 0.60 → REVIEW by analyst")
        logger.info("      • risk_score ≥ 0.40 → ENHANCED monitoring")
        logger.info("      • risk_score <  0.40 → ALLOW")
        logger.info("  → Writing to: gold/fraud_recommendations/ (Delta format)")

        result = {
            "source": scoring_meta.get("destination", "gold/scored_transactions/"),
            "destination": "gold/fraud_recommendations/",
            "format": "delta",
            "status": "SUCCESS",
        }
        logger.info("Recommendation generation completed: %s", result)
        return result

    # ------------------------------------------------------------------
    # Task 6 — Data Quality Check  (classic PythonOperator example)
    # ------------------------------------------------------------------
    dq_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=_run_data_quality_check,
        doc_md="Validate output row counts and null-percentage thresholds.",
    )

    # ------------------------------------------------------------------
    # Wire up task dependencies
    # ------------------------------------------------------------------
    # TaskFlow tasks are linked via XCom (return value → parameter), but
    # we still need to set explicit dependencies for the PythonOperator
    # and the fan-in at recommendation_generation.
    #
    # Desired graph:
    #   bronze >> silver >> gold >> [model_scoring, data_quality_check]
    #                                        ↘               ↙
    #                                  recommendation_generation
    # ------------------------------------------------------------------

    bronze_meta = bronze_ingestion()
    silver_meta = silver_transformation(bronze_meta)
    gold_meta = gold_feature_engineering(silver_meta)

    # model_scoring depends on gold (via XCom)
    scoring_meta = model_scoring(gold_meta)

    # data_quality_check (PythonOperator) depends on gold
    gold_meta >> dq_check  # type: ignore[operator]

    # recommendation_generation fans in from scoring + dq_check
    # We pass dq_check's return value via XCom by referencing ti.xcom_pull
    # For the TaskFlow task, we pass scoring_meta directly and pull
    # dq_results from the PythonOperator's XCom.

    @task(
        task_id="recommendation_generation_wrapper",
        doc_md="Wrapper that pulls DQ results and delegates to recommendation logic.",
    )
    def _recommendation_wrapper(
        scoring_meta: Dict[str, str],
        **context: Any,
    ) -> Dict[str, str]:
        """Thin wrapper to bridge PythonOperator XCom into TaskFlow."""
        ti = context["ti"]
        dq_results: Dict[str, Any] = ti.xcom_pull(task_ids="data_quality_check") or {}
        return recommendation_generation.function(scoring_meta, dq_results)

    rec_result = _recommendation_wrapper(scoring_meta)
    dq_check >> rec_result  # type: ignore[operator]


# Instantiate the DAG
fraud_pipeline_dag()
