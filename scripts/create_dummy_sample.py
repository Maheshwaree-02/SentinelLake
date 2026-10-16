import pandas as pd
from pathlib import Path

data = {
    "nameOrig": ["C1001", "C1002", "C1003", "C1004", "C1005"] * 20,
    "amount": [1500, 45000, 120000, 8500, 250000] * 20,
    "unified_risk_score": [0.08, 0.35, 0.72, 0.12, 0.91] * 20,
    "recommended_action": ["APPROVE", "STEP_UP_AUTH", "MANUAL_REVIEW", "APPROVE", "BLOCK"] * 20,
    "confidence": [0.88, 0.71, 0.84, 0.90, 0.96] * 20,
    "label": [0, 0, 1, 0, 1] * 20
}

df = pd.DataFrame(data)

output_path = Path("data/recommendations_sample")
output_path.mkdir(parents=True, exist_ok=True)

df.to_parquet(output_path / "sample.parquet", index=False)
print("Dummy sample created successfully")
print(df.head())