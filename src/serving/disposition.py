from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import csv
from pathlib import Path
import os

DISPOSITION_FILE = Path(__file__).parent.parent.parent / "data" / "lake" / "analyst_dispositions.csv"

class DispositionRequest(BaseModel):
    transaction_id: str
    customer_id: str
    action_taken: str  # e.g., "APPROVED", "BLOCKED", "ESCALATED"
    analyst_notes: Optional[str] = None
    is_fraud: bool

class DispositionResponse(BaseModel):
    status: str
    message: str
    timestamp: str

def save_disposition(disposition: DispositionRequest) -> DispositionResponse:
    """
    Saves an analyst's feedback (disposition) to close the ML loop.
    In production this writes to Postgres or Kafka. Here it appends to a CSV
    so we can re-train the model on it later.
    """
    file_exists = DISPOSITION_FILE.exists()
    
    # Ensure directory exists
    os.makedirs(DISPOSITION_FILE.parent, exist_ok=True)
    
    with open(DISPOSITION_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "transaction_id", "customer_id", 
                "action_taken", "is_fraud", "analyst_notes"
            ])
            
        now = datetime.utcnow().isoformat()
        writer.writerow([
            now,
            disposition.transaction_id,
            disposition.customer_id,
            disposition.action_taken,
            disposition.is_fraud,
            disposition.analyst_notes or ""
        ])
        
    return DispositionResponse(
        status="success",
        message=f"Disposition recorded for transaction {disposition.transaction_id}",
        timestamp=now
    )
