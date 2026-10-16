# Developer & Operations Workflow Guide (v2)

This supersedes the original Developer Workflow Guide. Sections 1–6 are the original, unchanged, verified-working workflow. Sections 7+ are new — operational drills and the analyst-facing workflow that make this project's resilience/scale claims demonstrable rather than theoretical.

## 1. Local Development Environment Setup [unchanged]

```bash
cd C:\Users\bharg\Enterprise_fraud_detection
python -m venv .venv
.venv\Scripts\activate       # Windows
pip install -r requirements.txt
cp env.example .env
```

## 2. Data Pipeline Workflow (PySpark / Lakehouse) [unchanged]

1. Raw data: `data/raw/Paysim_dataset.csv`
2. Run `notebooks/01_eda_and_baseline.ipynb`, then `notebooks/02_pyspark_big_data.ipynb` for Bronze→Gold transforms
3. Output lands in `data/recommendations_sample/`
4. Synthetic UI data: `python scripts/generate_rich_sample.py`

**Before trusting any numbers out of this pipeline:** confirm the source file is the full dataset, not an Excel-truncated copy —
```bash
python -c "import pandas as pd; df = pd.read_csv('data/raw/Paysim_dataset.csv'); print(df['step'].max())"
# Expect: 743 — if step.max() is far below 743, the file was truncated
```

## 3. Running the Serving Layer (FastAPI) [unchanged, see §7.3 for new auth]

```bash
uvicorn src.serving.main:app --host 0.0.0.0 --port 8000 --reload
# Swagger UI: http://localhost:8000/docs
```

## 4. Running the Interactive Dashboard (Streamlit) [unchanged, see §7.5 for new pages]

```bash
streamlit run streamlit_app/app.py
# http://localhost:8501
```

## 5. Airflow Orchestration Workflow [unchanged, see §7.1 for retry config]

```bash
cd airflow
docker-compose -f docker-compose.airflow.yml up airflow-init    # once
docker-compose -f docker-compose.airflow.yml up -d
# UI: http://localhost:8080 — admin / admin
```

## 6. Extending the Project [unchanged]

* New API endpoints: `src/serving/main.py` + `src/serving/schemas.py`
* Recommendation logic: `src/recommendation/rule_based.py`
* New dashboard pages: `streamlit_app/app.py` sidebar router

---

## 7. New Operational Workflows

### 7.1 Running the Streaming Simulation

```bash
# Terminal 1 — producer: emits records at a controlled rate, partitioned by nameOrig
python src/ingestion/stream_producer.py --rate 50 --jitter 0.3

# Terminal 2 — consumer: checkpointed Structured Streaming job
python src/ingestion/stream_consumer.py
```
Confirm it's actually streaming, not batch-in-disguise: Gold row counts should climb continuously in small increments, not jump once. Check the checkpoint directory exists and updates: `data/checkpoints/streaming_gold/`.

### 7.2 Recovery Drills (run these, record the real numbers — don't estimate)

**Drill 1 — transient task failure (Airflow auto-retry):**
```bash
airflow tasks test fraud_pipeline_dag silver_etl 2026-10-07 &
kill -9 $!
# Watch the Airflow UI: does the task retry automatically? Time to green?
```

**Drill 2 — streaming consumer crash + checkpoint recovery:**
```bash
# Stop the consumer mid-run (Ctrl+C), then:
rm -rf data/checkpoints/streaming_gold/offsets/<latest>    # simulate partial checkp
python src/ingestion/stream_consumer.py
# Time how long until Gold catches up to the producer's current position
```

**Drill 3 — full Gold rebuild from Bronze (disaster recovery):**
```bash
rm -rf data/lake/gold/
python -m src.etl.gold_etl --full-rebuild
# Time the full rebuild on your current dataset size
```

Record all three times in `docs/runbooks/recovery_runbook.md`. These become your real, defensible RTO numbers — replace every estimate in `architecture.md` §6 with what you actually measured.

### 7.3 Capacity / Load Spike Test

```bash
python scripts/load_test_spike.py --burst-size 500
```
Watch the consumer's processing lag metric in the Pipeline Health panel — it should rise and recover smoothly (backpressure working), not error out or crash.

### 7.4 Data Completeness / DLQ Test

```bash
python -m pytest tests/test_data_quality.py -v
```
Manually verify in the DLQ panel: a record missing a critical field (`amount`, `nameOrig`, `type`) lands in quarantine with reason `MISSING_CRITICAL_FIELD`; a record that's ~40% incomplete but has all critical fields is scored but flagged `LOW_CONFIDENCE`, visible as a badge in the Alert Detail view, not silently treated as a normal case.

### 7.5 Analyst-Facing Workflow (manual walkthrough, for demo/viva)

1. **Login** (`streamlit_app/pages/login.py`) — select role: Analyst or Lead
2. **Queue** — see live, auto-refreshing alerts sorted by risk band; staleness banner shows data freshness
3. **Claim an alert** — locks it to you
4. **Alert Detail** — review transaction timeline, risk score breakdown, recommended action + reasoning, similar historical cases
5. **Decide** — Approve / Step-up / Hold / Review / Block, with a required reason code
6. **If Block (confirmed fraud)** — "Notify Bank" fires (stubbed webhook/email), status flips to `BLOCKED`, incident record created
7. **Check Pipeline Health** (Lead view, or any analyst) — freshness, DLQ trend, reconciliation counts, retry/backfill trigger

### 7.6 Pre-Interview Checklist

Before presenting this project, confirm you can do all of the following live, not just describe them:
- [ ] Run the streaming producer+consumer and show Gold updating in near-real-time
- [ ] Show the measured recovery-drill numbers (not estimates) from `docs/runbooks/recovery_runbook.md`
- [ ] Show a burst load test and the backpressure-smoothed processing lag graph
- [ ] Show a deliberately malformed/incomplete record landing in DLQ with the correct reason code
- [ ] Explain the leakage bug (1.0000 AUC) and walk through how you diagnosed and fixed it
- [ ] State clearly, unprompted if needed, what's out of scope (cloud, real Kafka, MLOps) and why
