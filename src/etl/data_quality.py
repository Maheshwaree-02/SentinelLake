"""
Data Quality Gate — Completeness scoring and 3-way record classification.

Answers recruiter Q4: "What happens if a transaction has 40% missing info?"

Classification logic:
  - VALID: All critical fields present, >= 90% total completeness
  - LOW_CONFIDENCE: All critical fields present, but 60-90% total completeness
    -> Scored by the pipeline but flagged for mandatory manual review
  - QUARANTINE: Missing a critical field OR < 60% total completeness
    -> Routed to Dead Letter Queue with a reason code, never reaches Gold

Design principle: A record missing ~40% of its fields is NEVER silently
imputed and NEVER silently scored with full confidence.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple


class RecordStatus(str, Enum):
    VALID = "VALID"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    QUARANTINE = "QUARANTINE"


# All fields in the PaySim transaction schema
ALL_FIELDS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg",
    "newbalanceOrig", "nameDest", "oldbalanceDest",
    "newbalanceDest", "isFraud", "isFlaggedFraud",
]

# Fields that MUST be present — without these, the record cannot be scored
REQUIRED_FIELDS = ["amount", "nameOrig", "type", "step"]

# Minimum fraction of non-null fields to avoid quarantine
COMPLETENESS_THRESHOLD = 0.60

# Fraction below which a record is flagged LOW_CONFIDENCE
HIGH_CONFIDENCE_THRESHOLD = 0.90


@dataclass
class QualityResult:
    """Result of a data quality assessment on a single record."""
    status: RecordStatus
    completeness: float
    reason: Optional[str] = None
    missing_fields: Optional[list] = None


def classify_record(row: dict) -> QualityResult:
    """
    Classify a single record based on field completeness.

    Args:
        row: Dictionary representing a transaction record.

    Returns:
        QualityResult with status, completeness score, and reason code.

    Examples:
        >>> classify_record({"step": 1, "type": "TRANSFER", "amount": 1000,
        ...     "nameOrig": "C100", "oldbalanceOrg": 5000, "newbalanceOrig": 4000,
        ...     "nameDest": "C200", "oldbalanceDest": 0, "newbalanceDest": 1000,
        ...     "isFraud": 0, "isFlaggedFraud": 0})
        QualityResult(status=<RecordStatus.VALID: 'VALID'>, completeness=1.0, ...)

        >>> classify_record({"step": 1, "type": None, "amount": 1000,
        ...     "nameOrig": "C100", ...})
        QualityResult(status=<RecordStatus.QUARANTINE: 'QUARANTINE'>,
                      reason='MISSING_CRITICAL_FIELD', ...)
    """
    # Count present (non-null, non-empty) fields
    missing = []
    present_count = 0
    for field in ALL_FIELDS:
        value = row.get(field)
        if value is not None and value != "" and str(value).strip() != "":
            present_count += 1
        else:
            missing.append(field)

    completeness = present_count / len(ALL_FIELDS) if ALL_FIELDS else 0.0

    # Check 1: Are all critical fields present?
    missing_critical = [f for f in REQUIRED_FIELDS if f in missing]
    if missing_critical:
        return QualityResult(
            status=RecordStatus.QUARANTINE,
            completeness=round(completeness, 4),
            reason="MISSING_CRITICAL_FIELD",
            missing_fields=missing_critical,
        )

    # Check 2: Is overall completeness above the minimum threshold?
    if completeness < COMPLETENESS_THRESHOLD:
        return QualityResult(
            status=RecordStatus.QUARANTINE,
            completeness=round(completeness, 4),
            reason="INSUFFICIENT_DATA",
            missing_fields=missing,
        )

    # Check 3: Is completeness high enough for full confidence?
    if completeness < HIGH_CONFIDENCE_THRESHOLD:
        return QualityResult(
            status=RecordStatus.LOW_CONFIDENCE,
            completeness=round(completeness, 4),
            reason=None,
            missing_fields=missing,
        )

    # All checks passed
    return QualityResult(
        status=RecordStatus.VALID,
        completeness=round(completeness, 4),
        reason=None,
        missing_fields=missing if missing else None,
    )


def classify_batch(records: list) -> dict:
    """
    Classify a batch of records, returning categorized lists.

    Args:
        records: List of dicts, each representing a transaction.

    Returns:
        Dict with keys 'valid', 'low_confidence', 'quarantine',
        each containing a list of (record, QualityResult) tuples.
    """
    results = {
        "valid": [],
        "low_confidence": [],
        "quarantine": [],
    }

    for record in records:
        result = classify_record(record)
        if result.status == RecordStatus.VALID:
            results["valid"].append((record, result))
        elif result.status == RecordStatus.LOW_CONFIDENCE:
            results["low_confidence"].append((record, result))
        else:
            results["quarantine"].append((record, result))

    return results


def print_quality_summary(results: dict):
    """Print a human-readable summary of quality gate results."""
    total = sum(len(v) for v in results.values())
    valid = len(results["valid"])
    low_conf = len(results["low_confidence"])
    quarantined = len(results["quarantine"])

    print(f"  Data Quality Gate Summary:")
    print(f"    Total records:     {total}")
    print(f"    VALID:             {valid} ({valid/total*100:.1f}%)")
    print(f"    LOW_CONFIDENCE:    {low_conf} ({low_conf/total*100:.1f}%)")
    print(f"    QUARANTINE:        {quarantined} ({quarantined/total*100:.1f}%)")

    if quarantined > 0:
        reasons = {}
        for _, qr in results["quarantine"]:
            reasons[qr.reason] = reasons.get(qr.reason, 0) + 1
        print(f"    Quarantine reasons:")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"      {reason}: {count}")


# ---------------------------------------------------------------------------
# Quick self-test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Complete record
    complete = {f: "value" for f in ALL_FIELDS}
    complete["step"] = 1
    complete["amount"] = 1000
    r = classify_record(complete)
    print(f"Complete record: {r.status.value} (completeness={r.completeness})")

    # Missing critical field
    missing_crit = dict(complete)
    missing_crit["amount"] = None
    r = classify_record(missing_crit)
    print(f"Missing amount:  {r.status.value} (reason={r.reason})")

    # 40% missing (but critical fields present)
    partial = {f: None for f in ALL_FIELDS}
    partial["step"] = 1
    partial["type"] = "TRANSFER"
    partial["amount"] = 500
    partial["nameOrig"] = "C100"
    partial["oldbalanceOrg"] = 5000
    partial["newbalanceOrig"] = 4500
    partial["nameDest"] = "C200"
    # 7/11 fields present = 63.6% completeness
    r = classify_record(partial)
    print(f"63% complete:    {r.status.value} (completeness={r.completeness})")

    # Very incomplete (only critical fields)
    minimal = {f: None for f in ALL_FIELDS}
    minimal["step"] = 1
    minimal["type"] = "PAYMENT"
    minimal["amount"] = 100
    minimal["nameOrig"] = "C999"
    # 4/11 fields = 36.4% completeness
    r = classify_record(minimal)
    print(f"36% complete:    {r.status.value} (reason={r.reason})")

    # Batch test
    print("\nBatch test:")
    batch = [complete, missing_crit, partial, minimal]
    results = classify_batch(batch)
    print_quality_summary(results)
