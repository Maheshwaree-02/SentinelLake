import pandas as pd
from pathlib import Path

class RecommendationStore:
    def __init__(self, data_path: str):
        path = Path(data_path)

        # Read all CSV files in the folder
        csv_files = list(path.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {data_path}")

        dfs = [pd.read_csv(f) for f in csv_files]
        self.df = pd.concat(dfs, ignore_index=True)

        # Clean types
        self.df["amount"] = pd.to_numeric(self.df["amount"], errors="coerce")
        self.df["unified_risk_score"] = pd.to_numeric(self.df["unified_risk_score"], errors="coerce")
        self.df["confidence"] = pd.to_numeric(self.df["confidence"], errors="coerce")
        self.df["label"] = pd.to_numeric(self.df["label"], errors="coerce")

        print(f"Loaded {len(self.df):,} recommendations")
        print(self.df.head(3))

    def get_by_customer(self, name_orig: str, limit: int = 10):
        result = self.df[self.df["nameOrig"] == name_orig].head(limit)
        return result.to_dict(orient="records")

    def get_high_risk(self, threshold: float = 0.5, limit: int = 20):
        result = (
            self.df[self.df["unified_risk_score"] >= threshold]
            .sort_values("unified_risk_score", ascending=False)
            .head(limit)
        )
        return result.to_dict(orient="records")

    def get_action_distribution(self):
        return self.df["recommended_action"].value_counts().to_dict()

    def total_count(self):
        return len(self.df)