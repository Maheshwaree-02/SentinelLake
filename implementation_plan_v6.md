# Enterprise Fraud Intelligence Platform — Implementation Plan (v6)

Supersedes all prior phase plans in this conversation (v4, v5, the DE/RS Enhancement Plan). This version's sole addition over v5 is the resilience/scale/capacity/data-quality workstream needed to credibly answer a strict recruiter's operational questions. Everything from v5 (DE + RS + BDA coverage) still applies underneath this.

**Primary goal, unchanged:** Data Engineer placement portfolio. RS and BDA coursework are satisfied through the same build, not separate projects.

## Status as of Oct 7, 2026

| Component | Status |
| :--- | :--- |
| Real PaySim1 dataset confirmed (6,362,620 rows, step 1–743, 0.129% fraud rate) | ✅ Done |
| `amplify_paysim.py` built, tested on real data | ✅ Done |
| Bronze → Silver → Gold (Delta) | ✅ Done |
| PySpark window-function velocity features, `balance_delta` | ✅ Done |
| Random Forest model, PR-AUC evaluation | ✅ Done — **leakage bug found and fixed** (initial 1.0000 AUC traced to feature leakage, corrected) |
| Rule-based recommendation engine (5 actions) | ✅ Done |
| FastAPI serving (`/health`, `/recommendations/customer/{id}`, `/recommendations/high-risk`) | ✅ Done |
| Docker + docker-compose (FastAPI + Postgres) | ✅ Done |
| Streamlit dashboard (Overview, High-Risk Queue, Pipeline Health, Customer Lookup) | ✅ Done |
| Airflow DAG, TaskFlow API, Bronze→Silver→Gold→Scoring→Recommendation with fail-fast dependency | ✅ Done |
| **Everything below this line** | ⏳ **Planned — this document** |

---

## Phase A: Realistic Streaming Simulation (Days 1–2.5)
**Answers recruiter Q1** ("did you run on a real stream or simulate it").

| File | What it does |
| :--- | :--- |
| `src/ingestion/stream_producer.py` | Reads amplified dataset, emits records at a controlled rate with randomized jitter, keyed/partitioned by `nameOrig` to mimic Kafka partition-key behavior |
| `src/ingestion/stream_consumer.py` | Spark Structured Streaming job reading the producer's output directory, with `checkpointLocation` set and `foreachBatch` writing idempotent Delta MERGE updates to Gold |

**Verification:**
```bash
python src/ingestion/stream_producer.py --rate 50 --jitter 0.3 &
python src/ingestion/stream_consumer.py
# Confirm: Gold row count increases continuously, not in one jump
```

**Interview-ready claim this unlocks:** "The consumer is written against production-equivalent semantics — checkpointed, idempotent, backpressure-controlled. Pointing it at a real Kafka topic is a source-config change, not a rewrite."

---

## Phase B: Capacity & Backpressure (Days 3–3.5)
**Answers recruiter Q3** ("how do you manage capacity under a sudden load spike").

| File | What it does |
| :--- | :--- |
| `src/ingestion/stream_consumer.py` (extended) | Add `maxFilesPerTrigger` config, tunable via env var |
| `scripts/load_test_spike.py` `[NEW]` | Drops a burst of files into the streaming source directory simultaneously, to demonstrate throttled processing rather than failure |
| `docs/runbooks/capacity_test.md` `[NEW]` | Records the before/after processing-lag numbers from the spike test |

**Verification:**
```bash
python scripts/load_test_spike.py --burst-size 500
# Watch consumer processing lag — should rise and recover smoothly, not crash
```

---

## Phase C: Data Completeness Gate (Days 4–4.5)
**Answers recruiter Q4** ("what if a transaction has 40% missing info").

| File | What it does |
| :--- | :--- |
| `src/etl/data_quality.py` | Completeness scoring + 3-way classification: `VALID`, `LOW_CONFIDENCE`, `QUARANTINE` (see architecture.md §3.1) |
| `tests/test_data_quality.py` | Covers: all-fields-present, missing-critical-field, borderline-completeness cases |

**Verification:**
```bash
python -m pytest tests/test_data_quality.py -v
```

---

## Phase D: Recovery Runbook + Measured RTO/RPO (Days 5–6)
**Answers recruiter Q2** ("how do you recover, how long does it take").

1. Configure Airflow task-level retries with exponential backoff on every DAG task.
2. Run three recovery drills and record real numbers (do not estimate):
   - Kill the Silver Airflow task mid-run → confirm auto-retry recovers it, time it
   - Delete the streaming checkpoint → restart consumer, time the catch-up to real-time
   - Simulate Gold corruption → trigger a backfill rebuild from Silver, time the full rebuild
3. Write `docs/runbooks/recovery_runbook.md` with the measured numbers replacing the targets in `architecture.md` §6.

```bash
# Drill 1
airflow tasks test fraud_pipeline_dag silver_etl 2026-10-07 &
kill -9 $!   # mid-run
# observe Airflow UI: does it retry automatically, how long until green?
```

---

## Phase E: Serving Resilience (Days 7–8)

| File | What it does |
| :--- | :--- |
| `src/serving/auth.py` `[NEW]` | Role-based login (Analyst / Lead), session handling |
| `src/serving/cache.py` `[NEW]` | Freshness check + last-known-good fallback with `stale: true` flag |
| `src/serving/disposition.py` `[NEW]` | Endpoint + table for analyst decisions (action, reason code, outcome) — audit trail + future CF training signal |
| `streamlit_app/pages/alert_detail.py` `[NEW]` | Investigation screen: transaction timeline, score breakdown, recommendation card, decision panel |
| `streamlit_app/pages/login.py` `[NEW]` | Role-based entry point |

**Verification:**
```bash
uvicorn src.serving.main:app --reload
# Stop the pipeline, wait past staleness threshold, confirm API still serves
# cached results with stale: true rather than erroring
```

---

## Phase F: Pipeline Health Panel Upgrade (Days 9–9.5)
Extends the existing Streamlit "Pipeline Health" page (currently freshness-only per the walkthrough) with:

- DLQ count + reason-code breakdown, trend over time
- Bronze → Silver → Gold row-count reconciliation (delta = potential silent loss, flagged)
- Retry/backfill trigger button, wired to the Airflow API

---

## Phase G: Documentation Pass (Days 10–10.5)

- `docs/runbooks/recovery_runbook.md` — measured RTO/RPO from Phase D
- `docs/competitive_positioning.md` — the honest "how is this different from commercial platforms" writeup (`architecture.md` §7, expanded)
- `README.md` lessons-learned section — the truncation catch and the leakage catch, with before/after numbers

---

## Net Addition
~10 days on top of everything already built. This is the last planned phase before the project is considered interview-hardened against the six operational questions a strict recruiter is likely to ask.

### Rating Trajectory (for reference)

| Checkpoint | Rating |
| :--- | :--- |
| Before DE/RS enhancements | 6.5–7/10 |
| After DE/RS enhancements (Phase 3.5/5 of v5) | 7.5/10 (projected) |
| **After this resilience workstream (this document)** | **8/10** (projected — verify by actually running the recovery drills and recording real numbers, not estimates) |
| Ceiling for a solo, non-deployed fresher project | ~8.5/10 — crossing into 9+ requires real cloud deployment, visible CI, and external validation (see `architecture.md` §8) |
