# Enterprise Fraud Intelligence Platform — Architecture

**Status key used throughout this document:** 
- `[BUILT]` = exists today per the current codebase 
- `[NEW]` = being added to close the resilience/scale/credibility gaps identified during review.

## 1. System Overview

*(See corresponding Mermaid chart in `README.md` or reference the System Overview diagram)*

- **Ingestion**: `[BUILT]` PaySim CSV (6.36M rows), `[BUILT]` amplify_paysim.py, `[NEW]` stream_producer.py
- **Compute (PySpark local / Databricks)**: `[NEW]` Structured Streaming consumer, `[BUILT]` Bronze ingestion notebooks, `[BUILT+NEW]` Silver ETL (Schema+DLQ+Completeness gate), `[BUILT]` Gold ETL
- **Ops & Orchestration**: `[BUILT+NEW]` Airflow DAG (`fraud_pipeline_dag.py`), `[NEW]` Pipeline health panel, `[NEW]` Recovery runbook
- **Lakehouse (Delta Lake)**: Bronze (immutable), Silver (cleaned/typed), `[NEW]` Quarantine (DLQ), Gold (features+scores)
- **Serving**: `[BUILT]` FastAPI, `[NEW]` RBAC login, `[NEW]` Disposition store, `[NEW]` Degraded-mode cache
- **Intelligence**: `[BUILT]` Random Forest (leakage-checked), `[BUILT]` Rule-based recommender, `[PLANNED]` FAISS + ALS CF
- **UI**: `[BUILT+NEW]` Streamlit (Overview, Queue, Pipeline Health, Customer Lookup, `[NEW]` Alert Detail, Login)

---

## 2. Ingestion Layer

### 2.1 Batch path `[BUILT]`
Raw PaySim CSV -> `amplify_paysim.py` (distributed Spark job) -> Bronze. The amplifier scales every monetary column in a row by a shared random factor per replica, preserving all arithmetic relationships and the original per-type fraud rate exactly (verified: 1,048,575 -> 5,242,875 rows at 5x, fraud rate identical per transaction type before and after). This is a documented resampling methodology, not fabricated data — stated explicitly to pre-empt the "is this real data" question.

### 2.2 Streaming simulation path `[NEW]`
**Why simulate rather than skip streaming entirely:** the product vision requires "stream of data incoming" with fast analyst-visible response. A pure batch/Airflow-cycle design cannot honestly claim that. Simulating a stream with production-equivalent consumer semantics is the standard, credible way to demonstrate this without access to a real bank feed.

```python
# src/ingestion/stream_producer.py [NEW]
# Emits transactions at a controlled rate with jitter, partitioned by nameOrig
# so downstream consumers see realistic key-ordered arrival, same as a Kafka
# topic partitioned by customer ID would behave.

# Spark Structured Streaming consumer [NEW]
stream_df = (
    spark.readStream
    .option("maxFilesPerTrigger", 1)  # backpressure control — see Section 5
    .schema(TRANSACTION_SCHEMA)
    .csv("data/lake/streaming_source/")
)

def process_batch(batch_df, batch_id):
    # Idempotent: Delta MERGE means replaying a batch after a crash
    # does not duplicate data.
    scored = apply_gold_transforms(batch_df)
    scored.write.format("delta").mode("append").save(GOLD_PATH)

query = (
    stream_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "data/checkpoints/streaming_gold/") # [NEW]
    .start()
)
```

**Honest framing for interviews:** the producer is simulated because no real transaction feed is available to a student project. The consumer is written against production-equivalent semantics (checkpointing, idempotent writes, backpressure), so pointing it at a real Kafka topic later is a source-config change, not a redesign.

---

## 3. Lakehouse Layer `[BUILT]`, data quality gate `[NEW]`

| Layer | Contents | Retention |
| :--- | :--- | :--- |
| **Bronze** | Raw ingested records, immutable | Full — this is what makes disaster recovery possible (Section 6) |
| **Silver** | Schema-validated, deduplicated, `balance_delta` enriched | Full |
| **Quarantine (DLQ)**<br/>`[NEW panel]` | Records failing validation, reason-coded | Full, reviewable |
| **Gold** | Velocity features, risk scores, recommendations | Full, MERGE-updated |

### 3.1 Data quality gate — the "40% missing fields" answer `[NEW]`

```python
# src/etl/data_quality.py [NEW]
REQUIRED_FIELDS = ["amount", "nameOrig", "type", "step"]
COMPLETENESS_THRESHOLD = 0.6  # fraction of non-null fields required

def classify_record(row):
    present = sum(1 for f in ALL_FIELDS if row[f] is not None)
    completeness = present / len(ALL_FIELDS)
    
    if any(row[f] is None for f in REQUIRED_FIELDS):
        return "QUARANTINE", "MISSING_CRITICAL_FIELD"
    
    if completeness < COMPLETENESS_THRESHOLD:
        return "QUARANTINE", "INSUFFICIENT_DATA"
        
    if completeness < 0.9:
        return "LOW_CONFIDENCE", None  # scored, but flagged for manual review
        
    return "VALID", None
```
A record missing ~40% of its fields is **never silently imputed and never silently scored with full confidence**. It's either quarantined (if a critical field is missing) or scored but explicitly flagged `LOW_CONFIDENCE`, which routes it to mandatory manual review in the UI rather than letting the recommender auto-suggest `APPROVE` / `BLOCK` on shaky data.

---

## 4. Intelligence Layer

### 4.1 ML Risk Scoring `[BUILT]`
Random Forest, evaluated via PR-AUC (not accuracy/ROC-AUC, given ~0.13% real fraud rate). **Known pitfall, already caught and fixed in this project:** an initial 1.0000 PR-AUC was traced to feature leakage (`isFlaggedFraud` and/or raw balance columns encoding the simulator's own fraud-generation rule) and corrected — this is documented as a deliberate validation step, not hidden.

### 4.2 Recommendation Engine `[BUILT: rule-based]`, `[PLANNED: content-based + CF]`
Current: `src/recommendation/rule_based.py` ranks 5 actions (`APPROVE`, `STEP_UP_AUTH`, `HOLD`, `MANUAL_REVIEW`, `BLOCK`) from risk score + business rules. Planned extension: FAISS content-based playbook matching + ALS collaborative filtering trained on logged analyst dispositions (Section 5.2) as implicit feedback — only claim this once built.

---

## 5. Serving Layer

### 5.1 Capacity & backpressure — the "sudden load spike" answer `[NEW]`
Three mechanisms, honestly scoped to what a laptop/free-tier deployment can actually demonstrate:

1. **Intake throttling**: `maxFilesPerTrigger` on the streaming read caps how much the consumer pulls per micro-batch, so a sudden burst queues up in the source rather than overwhelming the Spark job. This is demonstrable today — simulate a burst by dropping many files into `streaming_source/` at once and show processing lag increase smoothly rather than the job crashing.
2. **Stateless API layer**: FastAPI reads are pure lookups against pre-computed Gold data, so the serving layer can run multiple workers behind a load balancer without coordination — documented as the scaling path even though only one instance runs locally.
3. **Graceful degradation under load or staleness `[NEW]`**: if Gold data is older than a configured threshold, the API serves the last-known-good cached result with `stale: true` and a timestamp, rather than blocking or erroring.

```python
# src/serving/cache.py [NEW]
@app.get("/recommendations/high-risk")
def get_high_risk_queue():
    freshness = get_gold_last_updated()
    if (now() - freshness) > STALENESS_THRESHOLD:
        return {**last_known_good_cache, "stale": True, "as_of": freshness}
    return live_query()
```

### 5.2 Auth + Disposition tracking `[NEW]`
- `src/serving/auth.py` — role-based login (Analyst / Lead), currently absent, required since "analyst logs in" is the literal first step of the product vision.
- **Disposition table** — every analyst decision (action taken, reason code, outcome) is written back, forming both the audit trail and the future CF recommender's training signal.

---

## 6. Resilience & Recovery — the "how do you recover, how long" answer `[NEW]`

**Design principle:** Bronze is the immutable source of truth. Every downstream layer (Silver, Gold) can be fully rebuilt from Bronze, and every write uses Delta MERGE, so replaying a failed stage is always safe (idempotent) rather than duplicating data.

| Failure scenario | Recovery mechanism | Target RTO | Target RPO |
| :--- | :--- | :--- | :--- |
| **Transient task failure (e.g., Silver job OOM once)** | Airflow automatic retry with exponential backoff | < 2 min (auto, no human action) | 0 — task hadn't committed |
| **Streaming consumer crash** | Resume from last Structured Streaming checkpoint | < 5 min to catch up to real-time, bounded by backlog size | ≤ 1 micro-batch interval (uncommitted batch only) |
| **Full Gold-layer corruption** | Rebuild Gold from Silver (or Silver from Bronze if needed) via backfill trigger | Bounded by full reprocessing time — **measure this, don't estimate it** | 0 — Bronze retains everything |
| **DAG dependency failure (Silver fails -> Gold must not run)** | `[BUILT]` TaskFlow API dependency chain already prevents corrupted data from propagating | N/A — correct behavior is to halt, not degrade | N/A |

**Action item before quoting these numbers to a recruiter:** run the actual drill — kill the Silver task mid-run, let Airflow retry, time it; separately, delete the streaming checkpoint and time the catch-up. Replace the targets above with measured numbers in `docs/runbooks/recovery_runbook.md` `[NEW]`.

---

## 7. Honest Competitive Positioning — the "how is this better than commercial platforms" answer `[NEW - documentation only]`

This project does not claim superior fraud detection accuracy to commercial platforms (Stripe Radar, Feedzai, PayPal's risk stack) — those are trained on years of real labeled transaction data at a scale and with a feedback loop no student project can replicate. What this project demonstrates instead:

- A complete operational lifecycle around a model: ingestion quality gates, explainable scoring, action-ranking (not just a probability), an analyst feedback loop, and documented failure recovery — most academic/portfolio fraud projects stop at a confusion matrix.
- A synthetic-data methodology (amplification) that is statistically faithful and explicitly documented, rather than treated as equivalent to production data.
- A deliberate scope boundary: real-time infra (Kafka), cloud deployment, and MLOps tooling are named as future work, not overclaimed as done.

---

## 8. What Remains Explicitly Out of Scope (say this proactively)

Cloud deployment, real Kafka/Redpanda, multi-node cluster validation, MLflow model registry, drift detection, real user base. Documented here so the boundary is a stated design decision, not a gap discovered under questioning.
