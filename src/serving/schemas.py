from pydantic import BaseModel
from typing import Optional

class RecommendationResponse(BaseModel):
    nameOrig: str
    amount: float
    unified_risk_score: float
    recommended_action: str
    confidence: float
    label: Optional[int] = None

class HealthResponse(BaseModel):
    status: str
    total_recommendations: int