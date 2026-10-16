import os
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

# In production this would be hashed and stored in Secret Manager / DB
# For this project, we read from env or fallback to a dummy key
VALID_API_KEYS = {
    os.environ.get("EFIP_API_KEY", "efip-dev-key-123"),
    "efip-analyst-key-456"
}

def verify_api_key(api_key_header: str = Security(api_key_header)):
    """
    Dependency to verify API keys for protected endpoints.
    Answers recruiter Q about API security.
    """
    if api_key_header not in VALID_API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return api_key_header
