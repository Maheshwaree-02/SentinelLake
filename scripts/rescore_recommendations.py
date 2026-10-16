"""
Re-score recommendations using the rule-based engine.
The original Databricks export has all 40K rows as 'APPROVE' because the
recommendation engine wasn't applied to the export. This script fixes that.
"""

import sys
from pathlib import Path

# Fix imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from src.recommendation.rule_based import recommend_action

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "recommendations_sample"
CSV_FILE = list(DATA_DIR.glob("part-*.csv"))[0]


def main():
    print(f"Loading {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE)
    print(f"Loaded {len(df):,} rows")
    print(f"Before: {df['recommended_action'].value_counts().to_dict()}")

    # Re-score each row
    actions = []
    confidences = []
    for _, row in df.iterrows():
        result = recommend_action(
            risk_score=row["unified_risk_score"],
            amount=row["amount"],
        )
        actions.append(result["recommended_action"])
        confidences.append(result["confidence"])

    df["recommended_action"] = actions
    df["confidence"] = confidences

    # Save
    df.to_csv(CSV_FILE, index=False)
    scored_path = DATA_DIR / "recommendations_scored.csv"
    df.to_csv(scored_path, index=False)

    print(f"\nAfter re-scoring:")
    print(df["recommended_action"].value_counts())
    print(f"\nSaved to {CSV_FILE}")
    print(f"Saved to {scored_path}")


if __name__ == "__main__":
    main()
