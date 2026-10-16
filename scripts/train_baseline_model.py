"""
Train Baseline Random Forest Fraud Model
========================================

Trains a lightweight Random Forest model on PaySim data with class weighting
to address extreme fraud class imbalance (0.13% fraud rate).
Saves model artifact to src/models/rf_fraud_model.joblib for production scoring.

Usage:
    python scripts/train_baseline_model.py
"""

import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, average_precision_score
from sklearn.model_selection import train_test_split

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_CSV = PROJECT_ROOT / "data" / "raw" / "Paysim_dataset.csv"
MODEL_SAVE_PATH = PROJECT_ROOT / "src" / "models" / "rf_fraud_model.joblib"


def train_model():
    if not RAW_CSV.exists():
        print(f"[ERROR] Dataset not found at: {RAW_CSV}")
        sys.exit(1)

    print(f"[MODEL] Loading sample from {RAW_CSV}...")
    # Load 50,000 rows for fast training
    df = pd.read_csv(RAW_CSV, nrows=50000)

    # Feature engineering matching scoring engine
    df["balance_ratio"] = np.where(df["oldbalanceOrg"] > 0, df["amount"] / df["oldbalanceOrg"], 0.0)
    df["velocity_score"] = 0.4  # Baseline sample velocity

    feature_cols = [
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "balance_ratio",
        "velocity_score",
    ]

    X = df[feature_cols].fillna(0.0)
    y = df["isFraud"].astype(int)

    print(f"[MODEL] Dataset shape: {X.shape}, Fraud cases: {y.sum():,} ({y.mean()*100:.2f}%)")

    # Time/index-based split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y if y.sum() > 1 else None
    )

    # Address class imbalance using balanced class weighting
    print("[MODEL] Training RandomForestClassifier (n_estimators=35, max_depth=6, class_weight='balanced')...")
    clf = RandomForestClassifier(
        n_estimators=35,
        max_depth=6,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X_train, y_train)

    # Evaluation
    y_pred_proba = clf.predict_proba(X_test)[:, 1]
    y_pred = clf.predict(X_test)

    pr_auc = average_precision_score(y_test, y_pred_proba)
    print(f"\n[EVALUATION] PR-AUC (Precision-Recall AUC): {pr_auc:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, zero_division=0))

    # Save artifact
    MODEL_SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(clf, MODEL_SAVE_PATH)
    print(f"[MODEL] Model artifact saved to: {MODEL_SAVE_PATH}")


if __name__ == "__main__":
    train_model()
