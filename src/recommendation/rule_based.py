"""
Rule-based Recommendation Engine
Given a risk score + transaction context → recommend the best action
"""

from typing import Dict, Any

def recommend_action(
    risk_score: float,
    amount: float,
    customer_risk: float = 0.0,
    is_new_device: bool = False,
    is_unusual_location: bool = False
) -> Dict[str, Any]:
    """
    Returns a dictionary with:
    - recommended_action
    - confidence
    - reason
    - all_scores (for ranking view)
    """

    # Candidate actions
    actions = {
        "APPROVE": 0.0,
        "STEP_UP_AUTH": 0.0,
        "HOLD": 0.0,
        "MANUAL_REVIEW": 0.0,
        "BLOCK": 0.0
    }

    # Base scoring from risk
    if risk_score >= 0.95:
        actions["BLOCK"] = 0.95 + (risk_score - 0.95)
        actions["MANUAL_REVIEW"] = 0.70
    elif risk_score >= 0.80:
        actions["MANUAL_REVIEW"] = 0.85
        actions["BLOCK"] = 0.60
        actions["STEP_UP_AUTH"] = 0.50
    elif risk_score >= 0.55:
        actions["STEP_UP_AUTH"] = 0.75
        actions["MANUAL_REVIEW"] = 0.45
        actions["HOLD"] = 0.30
    elif risk_score >= 0.30:
        actions["STEP_UP_AUTH"] = 0.40
        actions["APPROVE"] = 0.55
    else:
        actions["APPROVE"] = 0.90

    # Context boosts
    if amount > 100_000:
        actions["BLOCK"] += 0.15
        actions["MANUAL_REVIEW"] += 0.10
        actions["APPROVE"] -= 0.25

    if amount > 50_000:
        actions["MANUAL_REVIEW"] += 0.08
        actions["STEP_UP_AUTH"] += 0.05

    if customer_risk > 0.6:
        actions["MANUAL_REVIEW"] += 0.12
        actions["BLOCK"] += 0.08

    if is_new_device:
        actions["STEP_UP_AUTH"] += 0.15
        actions["APPROVE"] -= 0.10

    if is_unusual_location:
        actions["STEP_UP_AUTH"] += 0.12
        actions["MANUAL_REVIEW"] += 0.08

    # Normalize scores to 0-1 range (simple clip)
    for k in actions:
        actions[k] = max(0.0, min(1.0, actions[k]))

    # Rank
    ranked = sorted(actions.items(), key=lambda x: x[1], reverse=True)
    best_action, best_score = ranked[0]

    # Reason
    reasons = []
    if risk_score >= 0.90:
        reasons.append(f"Very high fraud risk ({risk_score:.2f})")
    elif risk_score >= 0.70:
        reasons.append(f"Elevated fraud risk ({risk_score:.2f})")
    if amount > 100_000:
        reasons.append(f"High transaction amount (₹{amount:,.0f})")
    if is_new_device:
        reasons.append("New/unrecognized device")
    if is_unusual_location:
        reasons.append("Unusual location")

    reason = " | ".join(reasons) if reasons else "Normal transaction pattern"

    return {
        "recommended_action": best_action,
        "confidence": round(best_score, 3),
        "reason": reason,
        "all_scores": {k: round(v, 3) for k, v in ranked}
    }


# Quick test
if __name__ == "__main__":
    print(recommend_action(0.92, 85000, customer_risk=0.4, is_new_device=True))
    print(recommend_action(0.12, 2500))
    print(recommend_action(0.78, 45000, is_unusual_location=True))