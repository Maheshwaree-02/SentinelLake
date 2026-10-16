from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import uvicorn

from schemas import RecommendationResponse, HealthResponse
from database import RecommendationStore
from auth import verify_api_key
from cache import ttl_cache
from disposition import DispositionRequest, DispositionResponse, save_disposition

# ---------- Config ----------
PARQUET_PATH = Path(__file__).parent.parent.parent / "data" / "recommendations_sample"

app = FastAPI(
    title="Fraud Intelligence Platform API",
    description="Risk scoring + Action recommendation service",
    version="1.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load data at startup
store = RecommendationStore(str(PARQUET_PATH))

@app.get("/health", response_model=HealthResponse)
def health():
    return {
        "status": "ok",
        "total_recommendations": store.total_count()
    }

@app.get("/recommendations/customer/{name_orig}", response_model=list[RecommendationResponse])
@ttl_cache(ttl_seconds=30)
def get_customer_recommendations(
    name_orig: str, 
    limit: int = Query(10, ge=1, le=50),
    api_key: str = Depends(verify_api_key)
):
    results = store.get_by_customer(name_orig, limit)
    if not results:
        raise HTTPException(status_code=404, detail="Customer not found")
    return results

@app.get("/recommendations/high-risk", response_model=list[RecommendationResponse])
@ttl_cache(ttl_seconds=15)
def get_high_risk(
    threshold: float = Query(0.4, ge=0, le=1), 
    limit: int = Query(20, ge=1, le=100),
    api_key: str = Depends(verify_api_key)
):
    return store.get_high_risk(threshold, limit)

@app.get("/metrics/action-distribution")
@ttl_cache(ttl_seconds=60)
def action_distribution(api_key: str = Depends(verify_api_key)):
    return store.get_action_distribution()

@app.post("/disposition", response_model=DispositionResponse, status_code=201)
def record_disposition(
    disposition: DispositionRequest,
    api_key: str = Depends(verify_api_key)
):
    """
    Record an analyst's feedback (e.g. true fraud vs false positive).
    Closes the ML loop.
    """
    return save_disposition(disposition)

@app.get("/")
def root():
    return {
        "message": "Fraud Intelligence API (Secured & Cached)",
        "docs": "/docs",
        "health": "/health"
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)