# Project Walkthrough: Enterprise Fraud Intelligence Platform

Welcome to the detailed walkthrough of the Enterprise Fraud Intelligence Platform. This document explains the "why" and "how" behind each component of the project, taking you from raw data to a fully functional interactive dashboard.

## 1. Executive Summary
Financial fraud is a massive challenge, and simply predicting "fraud" or "not fraud" isn't enough for operations teams. This platform was built to not only detect fraud using Machine Learning but to **action** it. By combining an ML model with a rule-based recommendation engine, the platform tells fraud analysts exactly what to do (e.g., Block the transaction, require Step-Up Authentication, or Approve it).

## 2. The Data Engineering Lakehouse (Medallion Architecture)

We process a 6.36 million row dataset (PaySim) using PySpark. To ensure data quality and performance, we utilize the Medallion Architecture via Delta Lake.

### Phase 1: Bronze Layer (Raw Ingestion)
* **What it does**: Ingests the raw CSV file into Parquet/Delta format.
* **Why**: CSVs are slow to query and lack schema enforcement. Moving to a columnar format like Parquet provides immediate read-performance benefits and compression.

### Phase 2: Silver Layer (Cleansing & Enrichment)
* **What it does**: 
  * Validates data types.
  * Filters out corrupted records (e.g., negative amounts).
  * Enriches the data by calculating `balance_delta` (difference between old and new balances) to identify balance mismatches.
* **Why**: Machine Learning models require clean data. The Silver layer acts as the source of truth for all downstream analytics and modeling.

### Phase 3: Gold Layer (Feature Engineering)
* **What it does**: Aggregates data to create business-level features. We use PySpark Window functions to calculate "Velocity Features" (e.g., how many transactions has this user made in the last 1 hour? What is the total amount?).
* **Why**: Fraudsters often perform many rapid, small transactions. Velocity features are the strongest predictors of this behavior.

## 3. The Intelligence Layer

### Phase 4: Machine Learning Risk Scoring
* **Model**: Random Forest Classifier.
* **Process**: We train the model on the Gold layer features. Because fraud is highly imbalanced (very few actual fraud cases compared to legitimate ones), we evaluate the model using **PR-AUC** (Precision-Recall Area Under Curve) rather than standard accuracy.
* **Output**: The model produces a raw probability score (0.0 to 1.0) representing the likelihood of fraud.

### Phase 5: Rule-Based Recommendation Engine
* **The Problem**: A risk score of "0.85" is ambiguous to a customer service rep. Should they block the card or just call the customer?
* **The Solution**: We built `src/recommendation/rule_based.py`. It takes the ML Risk Score and overlays business context (Transaction Amount, New Device flags, etc.) to rank 5 possible actions:
  1. `APPROVE`
  2. `STEP_UP_AUTH` (e.g., send an SMS OTP)
  3. `HOLD`
  4. `MANUAL_REVIEW`
  5. `BLOCK`
* **Output**: A definitive action and a confidence score.

## 4. The Serving Layer

### Phase 6: FastAPI Backend
* **Location**: `src/serving/main.py`
* **What it does**: Exposes the pre-computed recommendations and metrics via RESTful endpoints.
* **Endpoints**:
  * `/health`: System status.
  * `/recommendations/customer/{name_orig}`: Fetches history and recommended actions for a specific user.
  * `/recommendations/high-risk`: Pulls a queue of transactions that require immediate attention.
* **Containerization**: Packaged using Docker and Docker Compose (`Dockerfile`, `docker-compose.yml`) to ensure it runs consistently across any environment.

## 5. Orchestration & Monitoring

### Phase 7: Airflow DAG
* **Location**: `airflow/dags/fraud_pipeline_dag.py`
* **What it does**: Automates the execution of the Bronze -> Silver -> Gold -> Scoring -> Recommendation steps. 
* **Design**: Uses the modern TaskFlow API (`@task`). It ensures that if the Silver layer fails, the Gold layer won't run, preventing corrupted data from reaching the API.

### Phase 8: Interactive Streamlit Dashboard
* **Location**: `streamlit_app/app.py`
* **What it does**: Provides a front-end for Fraud Analysts.
* **Features**:
  * **Overview**: High-level KPIs and Plotly charts showing action distributions.
  * **High-Risk Queue**: A triage interface for analysts to review blocked or held transactions.
  * **Pipeline Health**: Monitors data freshness to ensure the batch jobs ran successfully.
  * **Customer Lookup**: A deep-dive view into a specific customer's risk profile.

## Summary of Key Achievements
1. **Scalability**: PySpark usage ensures the pipeline can handle billions of rows, not just millions.
2. **Actionability**: Moving beyond a simple ML output to a ranked recommendation engine bridges the gap between Data Science and Operations.
3. **Production-Ready**: With Airflow for orchestration, Docker for serving, and FastAPI/Streamlit for consumption, the project mirrors a modern enterprise data architecture.
