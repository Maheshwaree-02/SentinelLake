# EFIP Professor Viva Defense & Live Demonstration Runbook
**Platform:** Enterprise Fraud Intelligence Platform (EFIP)  
**Target Roles / Evaluation:** Data Engineer / Lakehouse Platform / ML Systems

---

## 1. Executive Summary & Opening Statement (1 Minute Hook)

> *"Professor, traditional fraud detection models treat fraud as a binary 0 or 1 classification problem. In production financial systems, that causes massive business damage: false declines alienate high-value customers, while latency in feature pipelines lets coordinated rings drain accounts.*
>
> *I designed and implemented an end-to-end **Lakehouse-backed Enterprise Fraud Intelligence Platform**. Under the hood, it runs PySpark Structured Streaming with an audit-ready 7-Step Quality Gate, Dead Letter Queue (DLQ), and idempotent Delta/Parquet storage.*
>
> *On top, it couples an ML model (trained with class-balancing and evaluated on PR-AUC) with a 5-tier Action Recommendation Engine and a dual-console operational interface: one for the Reliability Data Engineer monitoring freshness SLAs and reconciliation, and one for the Fraud Analyst providing zero-noise alert triage with closed-loop disposition feedback."*

---

## 2. Live Viva Demonstration Run Sheet

Use this sequence to guide your live demonstration:

| Demo Moment | Terminal Command / Action | Screen to Show | What Happens Live | What to Explain to Professor |
| :--- | :--- | :--- | :--- | :--- |
| **1. Baseline Health & Reconciliation** | `streamlit run streamlit_app/app.py` | **⚙️ Data Engineer Ops Console** | Reconciliation badge shows `BALANCED (Δ = 0)`; DLQ breakdown chart shows categorized errors. | *"Our Silver layer enforces zero silent data loss. Every ingested record is mathematically accounted for: $\text{Ingested} = \text{Valid} + \text{DLQ} + \text{Duplicates}$."* |
| **2. Live Stream with Faults** | `python src/ingestion/stream_producer.py --rate 100 --batches 2 --scenario corrupt`<br>`python src/ingestion/stream_consumer.py` | Terminal + **⚙️ Data Engineer Ops Console** | Consumer terminal logs `190 to Gold, 10 to DLQ [NEGATIVE_AMOUNT, NULL_ORIGIN, INVALID_TYPE, TOO_INCOMPLETE]`. DLQ table updates live. | *"Our stream producer injects realistic edge cases. Notice that rather than crashing Spark or silently corrupting features, bad rows are isolated into Parquet DLQ with audit reason codes."* |
| **3. Freshness & Staleness SLA (Flow 5)** | Stop producer, wait $>45\text{s}$ | **Sidebar Badge** & **🚨 Analyst Terminal** | Badge turns from `🟢 LIVE (< 15s)` $\rightarrow$ `🟠 AGING` $\rightarrow$ `🔴 STALE (> 45s)`. Warning banner appears on Analyst Terminal. | *"If upstream data stops arriving, our 4-state freshness engine alerts downstream analysts that risk scores may be lagging before decisions are made on stale features."* |
| **4. Zero-Noise Analyst Triage (Flow 0)** | Switch to **🚨 Analyst Command Terminal** | **🚨 Analyst Command Terminal** | Header displays: `84.6% Auto-Approved`. Priority queue displays only actionable items (`BLOCK`, `MANUAL_REVIEW`, `HOLD`). | *"Enterprise fraud teams cannot review millions of benign $5 transactions. Our engine filters out the noise, auto-approving low-risk transactions and ranking alerts by financial risk exposure."* |
| **5. Explainability & Balance Trajectory** | Select any high-risk alert in the queue | Right panel in **🚨 Analyst Terminal** | Displays Explainability Tags (`[ACCOUNT_DRAIN_PATTERN]`, `[VELOCITY_BURST]`) and **Balance Waterfall Chart**. | *"Analysts get instant behavioral context: they can see the account's starting balance plummet to zero in a waterfall graph, proving an account drain attack."* |
| **6. Closed-Loop Disposition (Flow 7 & 9)** | Select decision (e.g. `False Positive`), enter note, click **Submit Disposition** | **🚨 Analyst Terminal** | Success toast: decision appended to `data/lake/analyst_dispositions.csv`. | *"This closes the machine learning lifecycle: human analyst decisions are captured as versioned labels to calibrate thresholds and retrain future models."* |
| **7. ML Model Fallback (Flow 6 FB)** | Run `python -m pytest tests/test_scenarios.py -k fallback -v` | Terminal test | Passes: when model is offline, scores fallback to `RULES_ONLY` without throwing 500 error. | *"If our ML model artifact is corrupted or fails inference, the system gracefully falls back to a rules-only heuristic, logging a platform alert while keeping operations alive."* |

---

## 3. The Top 10 Toughest Professor Questions & Model Answers

### Q1: "PaySim has a 0.13% fraud rate. How did you handle extreme class imbalance?"
> **Answer:** *"If you train a classifier on raw accuracy with a 0.13% fraud rate, predicting 0 for every transaction yields a deceptive 99.87% accuracy while detecting zero fraud.  
> We handled this in two ways:  
> 1. In our Random Forest model (`train_baseline_model.py`), we applied `class_weight='balanced'`, which inversely weights classes by frequency so fraud errors are penalized heavily.  
> 2. We evaluated the model using **PR-AUC (Precision-Recall AUC)** and **Precision@K** rather than ROC-AUC or accuracy. Our trained model achieved a PR-AUC of **0.9761** on a holdout test set."*

---

### Q2: "What is the difference between your ML Model and your Recommendation Engine?"
> **Answer:** *"The ML model outputs a continuous probability of fraud between 0.0 and 1.0 based on behavioral patterns. However, financial institutions do not make decisions on raw probability alone—they must weigh customer friction against financial risk.  
> Our **Recommendation Engine (Flow 6)** maps risk scores and transaction context into a 5-tier intervention matrix:
> - Low risk ($< 0.40$) $\rightarrow$ `APPROVE` (silent, zero friction)
> - Moderate risk ($0.40 - 0.59$) $\rightarrow$ `STEP_UP_AUTH` (customer OTP challenge)
> - High-risk account drain ($0.60 - 0.69$) $\rightarrow$ `HOLD` (pause funds)
> - Complex anomalies ($0.70 - 0.84$) $\rightarrow$ `MANUAL_REVIEW` (investigator queue)
> - Severe fraud ($\ge 0.85$) $\rightarrow$ `BLOCK` (immediate prevention)*  
> *Additionally, if a record has partial data (`LOW_CONFIDENCE`), our engine enforces a hard cap forbidding auto-approval."*

---

### Q3: "What happens if a transaction arrives with 40% missing data?"
> **Answer:** *"It goes through our 7-step Silver Quality Gate (Flow 1).  
> 1. If any **critical field** (`amount`, `nameOrig`, `type`, `step`) is missing or null, or if the amount is negative, the record is immediately quarantined to the Dead Letter Queue (DLQ) with error codes like `MISSING_FIELD` or `NEGATIVE_AMOUNT`. It never enters Gold.  
> 2. If all critical fields are present, we compute total completeness. If completeness is $< 60\%$, it is quarantined with code `TOO_INCOMPLETE`.  
> 3. If completeness is between $60\%$ and $89\%$, it is marked `LOW_CONFIDENCE`. It enters Silver and Gold, but our scoring engine applies **Flow 6 Rule Capping (CAP)**: it is strictly forbidden from auto-approval and forced to `MANUAL_REVIEW`."*

---

### Q4: "How does your pipeline handle sudden traffic spikes or broker backlogs?"
> **Answer:** *"We use PySpark Structured Streaming with `maxFilesPerTrigger` backpressure control.  
> In our load test drill (`scripts/load_test_spike.py`), we injected a burst of 50 micro-batch files in 0.18 seconds. Rather than overwhelming executor memory and crashing the JVM, Spark smoothly throttled intake to at most 1–2 files per trigger, processing all 10,000 records in bounded micro-batches without a single dropped row."*

---

### Q5: "How do you ensure zero data loss during crash recovery?"
> **Answer:** *"We rely on Spark Structured Streaming **checkpointing** combined with **idempotent operations**.  
> The checkpoint stores offsets of committed micro-batches. If the consumer dies mid-batch, upon restart it replays only uncommitted offsets. Because our writes are deduplicated by `txn_id`, re-evaluating a batch produces identical state without generating duplicates. We proved this in a drill: killing the consumer and restarting it resulted in zero duplicate rows and zero missing records."*

---

### Q6: "What is your Recovery Time Objective (RTO) and Recovery Point Objective (RPO)?"
> **Answer:** *"We measured these in our disaster recovery drills:  
> - **Transient task failure:** Airflow exponential backoff retries within 2 minutes. $\text{RTO} < 5\text{ min}$, $\text{RPO} = 0$.  
> - **Streaming process restart:** Checkpoint resume takes ~4 seconds. $\text{RTO} = 4\text{s}$, $\text{RPO} = 0$.  
> - **Catastrophic Gold table corruption:** In Phase D, we wiped the entire Gold table and checkpoint, replaying all 10,000 records from the immutable source landing zone in **36.20 seconds**. $\text{RTO} \approx 36\text{s}$, $\text{RPO} = 0$."*

---

### Q7: "What happens if your ML model artifact is missing or corrupted?"
> **Answer:** *"Our scoring engine implements **Flow 6 Graceful Degradation (FB)**.  
> When `get_model()` fails to find or deserialize `rf_fraud_model.joblib`, the system does NOT throw a 500 error or crash the pipeline. It falls back to our deterministic heuristic risk engine, sets `scoring_mode = 'RULES_ONLY'`, logs a high-priority data platform alert, and surfaces a warning on the operations console."*

---

### Q8: "How do you detect silent data loss in the Lakehouse?"
> **Answer:** *"We enforce an atomic **Reconciliation Equation** after every micro-batch:
> $$\text{Total Ingested} = \text{Valid committed to Gold} + \text{Quarantined in DLQ} + \text{Duplicates Suppressed}$$
> If $\Delta \neq 0$, `pipeline_metrics.json` flags status `RECONCILIATION: MISMATCH` and alerts the on-call engineer in Console A."*

---

### Q9: "Why use Streamlit and FastAPI together?"
> **Answer:** *"They serve two completely different stakeholders:
> - **FastAPI (`src/serving/`):** A headless, low-latency microservice for payment gateways and core banking backends. It is secured with API Key headers (`X-API-Key`) and has an in-memory TTL caching decorator that serves repeated queries in $<1\text{ms}$ during traffic spikes.  
> - **Streamlit (`streamlit_app/`):** The human operational console. It provides the visual dual-console workspace for Data Engineers monitoring Lakehouse health and Fraud Analysts investigating alerts."*

---

### Q10: "How does the platform close the machine learning feedback loop?"
> **Answer:** *"In production, machine learning models drift as fraudsters change their attack patterns.  
> When an analyst reviews an alert in our Analyst Terminal and submits a decision (e.g. `Confirmed Fraud` vs. `False Positive`), that action is appended with ground-truth audit metadata to `data/lake/analyst_dispositions.csv`. This human-in-the-loop data is ingested into our retraining pipeline (`Flow 9`) to measure precision, recalibrate decision thresholds, and periodically retrain the model artifact."*
