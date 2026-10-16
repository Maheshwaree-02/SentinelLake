# Disaster Recovery & Resilience Runbook
**Last Updated**: October 10, 2026

## Objective
This runbook defines the Recovery Time Objective (RTO) and Recovery Point Objective (RPO) for the Enterprise Fraud Detection pipeline. It provides measured drills for recovering from Airflow task failures, streaming disruptions, and data corruption.

---

## 1. Task-Level Failure (Airflow)
**Scenario**: The Silver or Gold Databricks job fails mid-run due to an API timeout, spot instance preemption, or network glitch.

**Configuration Strategy**:
The Airflow DAG (`fraud_pipeline_dag.py`) is configured with enterprise-grade exponential backoff:
```python
"retries": 3,
"retry_delay": timedelta(minutes=2),
"retry_exponential_backoff": True,
"max_retry_delay": timedelta(minutes=15),
```

**Recovery Procedure**:
1. None required. Airflow will auto-retry in 2 mins, then 4 mins, then 8 mins.
2. Because all pipeline tasks (especially the PySpark `foreachBatch` consumer) are explicitly designed to be **idempotent**, the retry simply overwrites or safely skips already-processed data.

**Measured Impact**:
* RPO: 0 (No data loss)
* RTO: < 5 minutes (Auto-recovers on next retry)

---

## 2. Streaming Disruption (Checkpoint Recovery)
**Scenario**: The continuous streaming job crashes or is intentionally stopped for maintenance.

**Recovery Procedure**:
1. Restart the stream consumer.
2. Spark Structured Streaming reads the Delta `_checkpoint` directory.

**Measured Drill Results (Oct 9, 2026)**:
* We stopped the consumer at Batch 4 (5,000 records).
* We dropped 2,500 new records into the source directory.
* We restarted the consumer.
* **Result**: The consumer instantly read the checkpoint, bypassed Batches 0-4, and resumed exactly at **Batch 5**. 
* **RPO**: 0 (Exactly-once semantics guaranteed by checkpointing).
* **RTO**: ~4 seconds to initialize and resume processing.

---

## 3. Total Gold Layer Corruption (Full Rebuild)
**Scenario**: A bad code deployment (e.g., incorrect feature logic) corrupts the Gold layer features. We need to wipe Gold and rebuild it from the Silver layer.

**Recovery Procedure**:
1. Delete the corrupted Gold table and the streaming Checkpoint directory.
2. Restart the consumer with `availableNow=True` to trigger a historical backfill from the Silver/Bronze source directory.

**Measured Drill Results (Oct 10, 2026)**:
* **Initial State**: 10,000 records in 50 micro-batch files in the source directory.
* **Action**: Deleted `data/lake/gold/features` and `data/checkpoints/streaming_gold`.
* **Execution**: Re-ran the consumer with `maxFilesPerTrigger=10`.
* **Result**: 
  ```text
  Batch 0: 2000 ingested, 2000 to Gold (193 rec/s)
  Batch 1: 2000 ingested, 2000 to Gold (467 rec/s)
  Batch 2: 2000 ingested, 2000 to Gold (618 rec/s)
  Batch 3: 2000 ingested, 2000 to Gold (682 rec/s)
  Batch 4: 2000 ingested, 2000 to Gold (786 rec/s)
  ```
* **Recovery Time**: Rebuilt 10,000 Gold records from scratch in **36.20 seconds**.
* **RPO**: 0 (Full history recovered from immutable source).
* **RTO**: ~36 seconds per 10k records.

---

## Interview Talking Points

1. **"Our pipeline is designed for idempotency. If a Databricks job fails due to spot instance preemption, Airflow's exponential backoff automatically retries it, and the idempotent code ensures zero data duplication."**
2. **"We use Spark Structured Streaming checkpoints. I proved this in a drill: after killing the job and dropping new files, the restarted job resumed exactly at Batch 5 without reprocessing older files."**
3. **"In the event of Gold layer corruption (e.g. bad feature engineering logic deployed), our RPO is zero because the Silver layer is immutable. I ran a rebuild drill that wiped Gold and the checkpoint, and the pipeline rebuilt 10,000 records from Silver in 36 seconds."**
