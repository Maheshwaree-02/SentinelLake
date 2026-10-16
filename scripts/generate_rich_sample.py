"""
Generate a realistic recommendation dataset with diverse risk scores and all 5 action types.
The original Databricks export had nearly uniform 0.07 risk scores.
This creates a more representative sample for the dashboard and API.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
from src.recommendation.rule_based import recommend_action

np.random.seed(42)
N = 40000

# Generate customer IDs
customer_ids = [f"C{np.random.randint(100000000, 999999999)}" for _ in range(N)]

# Generate amounts with realistic distribution (log-normal)
amounts = np.random.lognormal(mean=8, sigma=2, size=N).clip(0.01, 500000)

# Generate risk scores with a realistic distribution:
# ~70% low risk (0.0-0.3), ~15% medium (0.3-0.6), ~10% elevated (0.6-0.85), ~5% high (0.85-1.0)
risk_scores = np.concatenate([
    np.random.beta(2, 15, size=int(N * 0.70)),      # low risk, peaks ~0.1
    np.random.beta(5, 5, size=int(N * 0.15)) * 0.6 + 0.2,  # medium, 0.2-0.8
    np.random.beta(6, 3, size=int(N * 0.10)) * 0.3 + 0.6,  # elevated, 0.6-0.9
    np.random.beta(8, 2, size=int(N * 0.05)) * 0.2 + 0.8,  # high, 0.8-1.0
])
np.random.shuffle(risk_scores)
risk_scores = risk_scores.clip(0.0, 1.0)

# Generate fraud labels (correlated with risk score)
# Higher risk = higher probability of being actual fraud
fraud_probs = (risk_scores ** 2) * 0.8  # quadratic scaling
labels = (np.random.random(N) < fraud_probs).astype(int)

# Apply recommendation engine
actions = []
confidences = []
for risk, amt in zip(risk_scores, amounts):
    result = recommend_action(
        risk_score=float(risk),
        amount=float(amt),
        customer_risk=float(np.random.random() * 0.5),
        is_new_device=np.random.random() < 0.1,
        is_unusual_location=np.random.random() < 0.05,
    )
    actions.append(result["recommended_action"])
    confidences.append(result["confidence"])

df = pd.DataFrame({
    "nameOrig": customer_ids,
    "amount": np.round(amounts, 2),
    "unified_risk_score": np.round(risk_scores, 6),
    "recommended_action": actions,
    "confidence": confidences,
    "label": labels,
})

# Save
output_dir = Path(__file__).resolve().parent.parent / "data" / "recommendations_sample"
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / "recommendations_scored.csv"
df.to_csv(output_path, index=False)

# Also overwrite the original CSV
orig_csv = list(output_dir.glob("part-*.csv"))
if orig_csv:
    df.to_csv(orig_csv[0], index=False)

print(f"Generated {len(df):,} recommendations")
print(f"\nAction Distribution:")
print(df["recommended_action"].value_counts().to_string())
print(f"\nFraud Labels: {labels.sum():,} fraud out of {N:,} ({labels.mean()*100:.2f}%)")
print(f"\nRisk Score Stats:")
print(df["unified_risk_score"].describe().to_string())
print(f"\nSaved to {output_path}")
