"""
Enterprise Fraud Intelligence Platform (EFIP) — Unified Dashboard v7.0
========================================================================

Three-page consolidated layout designed for two personas:

  Page 1 — Analyst Workstation:
      Priority alert queue, investigation panel with explainability tags,
      inline Customer 360 history, balance waterfall, and disposition desk.

  Page 2 — Pipeline Operations:
      4-state freshness engine (5 min / 30 min SLA thresholds),
      Lakehouse reconciliation audit, DLQ forensic inspector, batch history.

  Page 3 — Executive Dashboard:
      KPI metrics, action distribution bar, fraud-vs-legit donut,
      expandable high-risk analytics with threshold tuner & risk histogram.
"""

import csv
import datetime
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RECOMMENDATIONS_DIR = DATA_DIR / "recommendations_sample"
GOLD_DIR = DATA_DIR / "lake" / "gold" / "features"
DLQ_DIR = DATA_DIR / "lake" / "quarantine"
METRICS_FILE = DATA_DIR / "lake" / "pipeline_metrics.json"
DISPOSITION_FILE = DATA_DIR / "lake" / "analyst_dispositions.csv"

# ---------------------------------------------------------------------------
# Streamlit Page Config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="EFIP — Fraud Intelligence Platform",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Styling & Badges
# ---------------------------------------------------------------------------
def _badge(label: str, colour: str) -> str:
    """Return an HTML snippet for a coloured badge."""
    return (
        f'<span style="background-color:{colour};color:#fff;'
        f'padding:3px 10px;border-radius:12px;font-weight:600;font-size:0.85em;">'
        f"{label}</span>"
    )


def _risk_color(score: float) -> str:
    if pd.isna(score):
        return "#95a5a6"
    if score < 0.30:
        return "#2ecc71"
    if score < 0.60:
        return "#f1c40f"
    if score < 0.85:
        return "#e67e22"
    return "#e74c3c"


def _format_staleness(seconds: float) -> str:
    """Human-readable staleness: '42 s', '3.2 min', '1.5 hr'."""
    if seconds < 60:
        return f"{seconds:.0f} s"
    if seconds < 3600:
        return f"{seconds / 60:.1f} min"
    return f"{seconds / 3600:.1f} hr"


# ---------------------------------------------------------------------------
# Telemetry & Freshness Engine (Flow 5 & Flow 8)
# ---------------------------------------------------------------------------
def get_pipeline_telemetry() -> Dict[str, Any]:
    """Reads pipeline_metrics.json and computes live 4-state freshness.

    Thresholds (tuned for realistic demo cadence):
      LIVE   : < 5 minutes   (300 s)
      AGING  : 5 – 30 minutes (300 – 1800 s)
      STALE  : > 30 minutes   (> 1800 s)
    """
    default_metrics = {
        "status": "STALE",
        "badge_color": "#e74c3c",
        "staleness_sec": None,
        "total_ingested": 0,
        "total_valid": 0,
        "total_low_confidence": 0,
        "total_quarantined": 0,
        "total_duplicates_suppressed": 0,
        "reconciliation_status": "UNKNOWN",
        "reconciliation_delta": 0,
        "dlq_rejection_rate_pct": 0.0,
        "reason_code_breakdown": {},
        "recent_batches": [],
        "latest_commit_ts": None,
        "latest_batch_id": None,
    }

    if not METRICS_FILE.exists():
        return default_metrics

    try:
        with open(METRICS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        commit_ts_str = data.get("latest_gold_commit_ts")
        if commit_ts_str:
            commit_dt = datetime.datetime.fromisoformat(commit_ts_str)
            now_dt = datetime.datetime.now(datetime.timezone.utc)
            staleness_sec = max(0.0, (now_dt - commit_dt).total_seconds())
            data["staleness_sec"] = staleness_sec
            data["latest_commit_ts"] = commit_ts_str

            # Realistic SLA thresholds (previously 15 s / 45 s — caused
            # the dashboard to show STALE immediately after any demo pause).
            if staleness_sec < 300:          # < 5 minutes
                data["status"] = "LIVE"
                data["badge_color"] = "#2ecc71"
            elif staleness_sec <= 1800:      # 5 – 30 minutes
                data["status"] = "AGING"
                data["badge_color"] = "#f1c40f"
            else:                            # > 30 minutes
                data["status"] = "STALE"
                data["badge_color"] = "#e74c3c"
        else:
            data["status"] = "STALE"
            data["badge_color"] = "#e74c3c"

        return data
    except Exception:
        return default_metrics


# ---------------------------------------------------------------------------
# Data Loaders (Live Gold Lakehouse + Historical Sample)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=5)
def load_live_gold_records() -> pd.DataFrame:
    """Loads live Gold layer records from Parquet with risk scores."""
    if not GOLD_DIR.exists():
        return pd.DataFrame()
    files = list(GOLD_DIR.glob("*.parquet"))
    if not files:
        return pd.DataFrame()

    try:
        df = pd.read_parquet(GOLD_DIR)
        if df.empty:
            return df

        # Ensure txn_id exists
        if "txn_id" not in df.columns:
            df["txn_id"] = [f"TXN_G_{i:06d}" for i in range(len(df))]

        # Compute unified_risk_score if not already present
        if "unified_risk_score" not in df.columns:
            # Risk formula combining velocity, z-score, balance ratio, and fraud label
            v_score = df["velocity_score"] if "velocity_score" in df.columns else 0.2
            z_score = df["amount_zscore"].clip(0, 3) / 3.0 if "amount_zscore" in df.columns else 0.0
            b_ratio = df["balance_ratio"].clip(0, 1) if "balance_ratio" in df.columns else 0.0
            is_fraud = df["isFraud"] if "isFraud" in df.columns else 0

            # Weighted blend
            raw_risk = (0.4 * v_score) + (0.3 * z_score) + (0.3 * b_ratio)
            # If confirmed fraud label, scale to high risk band
            df["unified_risk_score"] = (raw_risk * (1 - is_fraud) + (0.92 + 0.06 * raw_risk) * is_fraud).clip(0.01, 0.99).round(3)

        # Compute recommended_action based on Flow 6
        if "recommended_action" not in df.columns:
            actions = []
            confidences = []
            for _, r in df.iterrows():
                score = r["unified_risk_score"]
                amt = r.get("amount", 0.0)
                q_status = r.get("quality_status", "VALID")

                # Flow 6 score bands:
                # <0.40 APPROVE, 0.40-0.59 STEP_UP_AUTH, 0.60-0.69 HOLD, 0.70-0.84 MANUAL_REVIEW, >=0.85 BLOCK
                if score >= 0.85 or (amt > 200000 and score >= 0.70):
                    act = "BLOCK"
                    conf = 0.94
                elif score >= 0.70:
                    act = "MANUAL_REVIEW"
                    conf = 0.88
                elif score >= 0.60:
                    act = "HOLD"
                    conf = 0.82
                elif score >= 0.40:
                    act = "STEP_UP_AUTH"
                    conf = 0.78
                else:
                    act = "APPROVE"
                    conf = 0.91

                # Flow 6 cap: LOW_CONFIDENCE cannot auto-approve
                if q_status == "LOW_CONFIDENCE" and act in ["APPROVE", "STEP_UP_AUTH"]:
                    act = "MANUAL_REVIEW"
                    conf = 0.65

                actions.append(act)
                confidences.append(conf)

            df["recommended_action"] = actions
            df["confidence"] = confidences

        if "label" not in df.columns and "isFraud" in df.columns:
            df["label"] = df["isFraud"]

        return df
    except Exception as e:
        st.error(f"Error loading live Gold data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_historical_recommendations() -> pd.DataFrame:
    """Loads pre-scored historical sample CSVs."""
    if not RECOMMENDATIONS_DIR.exists():
        return pd.DataFrame()
    csv_files = sorted(RECOMMENDATIONS_DIR.glob("*.csv"))
    if not csv_files:
        return pd.DataFrame()

    frames = []
    for fp in csv_files:
        try:
            frames.append(pd.read_csv(fp))
        except Exception:
            pass
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    if "txn_id" not in df.columns:
        df["txn_id"] = [f"TXN_H_{i:06d}" for i in range(len(df))]
    if "quality_status" not in df.columns:
        df["quality_status"] = "VALID"
    return df


@st.cache_data(ttl=5)
def load_dlq_records() -> pd.DataFrame:
    """Loads quarantined records directly from DLQ Parquet files."""
    if not DLQ_DIR.exists():
        return pd.DataFrame()
    files = list(DLQ_DIR.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    try:
        return pd.read_parquet(DLQ_DIR)
    except Exception:
        return pd.DataFrame()


def save_analyst_disposition(
    txn_id: str,
    customer_id: str,
    action_taken: str,
    is_fraud: bool,
    notes: str,
    risk_score: float,
    recommended_action: str,
):
    """Appends an analyst disposition decision to the feedback store."""
    DISPOSITION_FILE.parent.mkdir(parents=True, exist_ok=True)
    file_exists = DISPOSITION_FILE.exists()
    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

    with open(DISPOSITION_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "transaction_id", "customer_id",
                "action_taken", "is_fraud", "analyst_notes",
                "original_risk_score", "ai_recommended_action",
            ])
        writer.writerow([
            now_str, txn_id, customer_id, action_taken,
            is_fraud, notes, risk_score, recommended_action,
        ])


# ---------------------------------------------------------------------------
# Sidebar Navigation & Live Telemetry Card
# ---------------------------------------------------------------------------
telemetry = get_pipeline_telemetry()

st.sidebar.markdown("## 🛡️ EFIP Navigation")

# Unified Freshness Badge (Flow 5) at top of sidebar
status_text = telemetry["status"]
badge_col = telemetry["badge_color"]
stale_str = (
    f" ({_format_staleness(telemetry['staleness_sec'])})"
    if telemetry["staleness_sec"] is not None
    else ""
)
st.sidebar.markdown(
    f"**Pipeline SLA:** {_badge(f'{status_text}{stale_str}', badge_col)}",
    unsafe_allow_html=True,
)
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Select Console",
    [
        "🚨 Analyst Workstation",
        "⚙️ Pipeline Operations",
        "📊 Executive Dashboard",
    ],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🎛️ Session Controls")
auto_refresh = st.sidebar.checkbox("🔄 Live Stream Polling (5 s)", value=False)
data_source_mode = st.sidebar.selectbox(
    "Active Data Feed",
    ["🔴 Live Gold Lakehouse", "📁 Historical Sample"],
    index=0,
)

if auto_refresh:
    time.sleep(5)
    st.rerun()

# Determine active dataset
if data_source_mode == "🔴 Live Gold Lakehouse":
    active_df = load_live_gold_records()
    if active_df.empty:
        # Fallback to historical if gold not yet created
        active_df = load_historical_recommendations()
        st.sidebar.caption("⚠️ Live Gold is empty. Showing historical baseline.")
    else:
        st.sidebar.caption(f"Connected to live Gold Parquet ({len(active_df):,} records)")
else:
    active_df = load_historical_recommendations()
    st.sidebar.caption(f"Connected to historical sample ({len(active_df):,} records)")


# ===================================================================
# PAGE 1 — ANALYST WORKSTATION
# (Merges: Analyst Terminal + Customer 360 + Alert Detail)
# ===================================================================
if page == "🚨 Analyst Workstation":
    # ── Freshness SLA Warning Banner (Flow 5) ──
    if telemetry["status"] == "STALE":
        stale_display = _format_staleness(telemetry["staleness_sec"]) if telemetry["staleness_sec"] else "unknown"
        st.error(
            f"🚨 **CRITICAL SLA ALERT:** Pipeline is **STALE** (no commit for {stale_display}). "
            "Decisions may rest on delayed feature data. Notify the on-call data engineer."
        )
    elif telemetry["status"] == "AGING":
        aging_display = _format_staleness(telemetry["staleness_sec"]) if telemetry["staleness_sec"] else "unknown"
        st.warning(
            f"⚠️ **PIPELINE NOTICE:** Stream latency is **AGING** ({aging_display} since last commit). "
            "Incoming transactions may experience processing delay."
        )

    st.title("🚨 Fraud Analyst Workstation")
    st.caption(
        "One-stop investigation console. The noise filter auto-approves low-risk "
        "transactions so you only review what matters. Select an alert to see full "
        "investigation details, customer history, and submit your disposition — all "
        "without leaving this page."
    )

    if active_df.empty:
        st.info("No transaction data available in active feed.")
    else:
        # ── Noise Reduction Banner (Flow 0) ──
        total_txns = len(active_df)
        auto_approved_count = len(active_df[active_df["recommended_action"] == "APPROVE"])
        noise_reduction_pct = (auto_approved_count / total_txns * 100) if total_txns > 0 else 0

        # Action Queue: Non-APPROVE records
        action_queue = active_df[active_df["recommended_action"] != "APPROVE"].copy()

        # Metrics bar
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(
            "Actionable Queue",
            f"{len(action_queue):,} alerts",
            delta=f"{len(action_queue)/total_txns*100:.1f}% of traffic",
        )
        m2.metric(
            "Noise Filtered",
            f"{auto_approved_count:,} txns",
            delta=f"{noise_reduction_pct:.1f}% Auto-Approved",
        )
        m3.metric("Critical Blocks", len(active_df[active_df["recommended_action"] == "BLOCK"]))
        m4.metric("Manual Reviews", len(active_df[active_df["recommended_action"] == "MANUAL_REVIEW"]))

        st.markdown(
            f"*{_badge('NOISE FILTER ACTIVE', '#15803d')} &nbsp; "
            f"Auto-approved {auto_approved_count:,} low-risk transactions. "
            f"Human attention focused on {len(action_queue):,} elevated-risk items.*",
            unsafe_allow_html=True,
        )
        st.markdown("---")

        if action_queue.empty:
            st.success("🎉 All pending transactions cleared! Zero high-risk alerts in queue.")
        else:
            # Ranking: Score * Amount (Risk Exposure)
            action_queue["risk_exposure"] = action_queue["unified_risk_score"] * action_queue["amount"]
            action_queue = action_queue.sort_values("risk_exposure", ascending=False).reset_index(drop=True)

            # ── 2-Column Split: Queue on Left, Deep-Dive on Right ──
            col_queue, col_detail = st.columns([1, 2])

            with col_queue:
                st.subheader("🚨 Priority Alert Feed")
                st.caption(
                    "Alerts ranked by Risk Exposure (risk score × amount). "
                    "Highest-monetary-impact items appear first."
                )

                # Filter by action
                filter_action = st.selectbox(
                    "Filter by Recommended Action:",
                    ["ALL", "BLOCK", "MANUAL_REVIEW", "HOLD", "STEP_UP_AUTH"],
                    index=0,
                )
                filtered_queue = (
                    action_queue
                    if filter_action == "ALL"
                    else action_queue[action_queue["recommended_action"] == filter_action]
                )

                # Alert selector
                txn_options = filtered_queue["txn_id"].tolist()
                if not txn_options:
                    st.info(f"No alerts matching {filter_action}.")
                    selected_txn_id = None
                else:
                    selected_txn_id = st.selectbox(
                        "Select Alert to Investigate:",
                        txn_options,
                        format_func=lambda tid: (
                            f"{tid} | "
                            f"${filtered_queue[filtered_queue['txn_id']==tid]['amount'].iloc[0]:,.0f} | "
                            f"{filtered_queue[filtered_queue['txn_id']==tid]['recommended_action'].iloc[0]}"
                        ),
                    )

            with col_detail:
                if selected_txn_id:
                    alert_row = active_df[active_df["txn_id"] == selected_txn_id].iloc[0]

                    score = alert_row["unified_risk_score"]
                    action = alert_row["recommended_action"]
                    amt = alert_row["amount"]
                    cust = alert_row["nameOrig"]
                    q_status = alert_row.get("quality_status", "VALID")
                    color = _risk_color(score)

                    st.subheader(f"🔎 Investigation: `{selected_txn_id}`")

                    # Status Header
                    st.markdown(
                        f"**Customer:** `{cust}` &nbsp;&nbsp;|&nbsp;&nbsp; "
                        f"**Amount:** `${amt:,.2f}` &nbsp;&nbsp;|&nbsp;&nbsp; "
                        f"**Unified Risk:** {_badge(f'{score:.3f}', color)} &nbsp;&nbsp;|&nbsp;&nbsp; "
                        f"**AI Action:** `{action}`",
                        unsafe_allow_html=True,
                    )

                    # ── Explainability Tags ──
                    st.markdown("##### 🧠 Why Was This Flagged?")
                    st.caption(
                        "Each pill shows a specific risk driver the scoring engine identified. "
                        "Multiple pills compound to push the risk score higher."
                    )
                    driver_pills = []
                    if score >= 0.85:
                        driver_pills.append(_badge("CRITICAL_RISK_BAND", "#e74c3c"))
                    if amt > 100000:
                        driver_pills.append(_badge("HIGH_VALUE_OUTLIER", "#e67e22"))
                    if alert_row.get("velocity_score", 0.2) >= 0.7:
                        driver_pills.append(_badge("VELOCITY_BURST", "#e74c3c"))
                    if alert_row.get("balance_ratio", 0.0) >= 0.8:
                        driver_pills.append(_badge("ACCOUNT_DRAIN_PATTERN", "#b91c1c"))
                    if alert_row.get("is_balance_mismatch", 0) == 1:
                        driver_pills.append(_badge("BALANCE_MATH_MISMATCH", "#f39c12"))
                    if q_status == "LOW_CONFIDENCE":
                        driver_pills.append(_badge("DATA_MISSING_CAPPED", "#9b59b6"))

                    if not driver_pills:
                        driver_pills.append(_badge("ELEVATED_HEURISTIC_RISK", "#f1c40f"))

                    st.markdown(" ".join(driver_pills), unsafe_allow_html=True)
                    st.markdown("<br>", unsafe_allow_html=True)

                    # ── Balance Trajectory Waterfall ──
                    if "oldbalanceOrg" in alert_row and "newbalanceOrig" in alert_row:
                        st.markdown("##### 📉 Balance Waterfall — This Transaction")
                        st.caption(
                            "Shows the sender's account balance before the transaction, "
                            "the amount debited, and the resulting balance. A near-zero "
                            "final balance is a classic account-drain indicator."
                        )
                        old_bal = float(alert_row["oldbalanceOrg"])
                        new_bal = float(alert_row["newbalanceOrig"])
                        fig_bal = go.Figure(go.Waterfall(
                            name="Balance Delta",
                            orientation="v",
                            measure=["absolute", "relative", "total"],
                            x=["Starting Balance", "Transaction Amount", "Final Balance"],
                            textposition="outside",
                            text=[f"${old_bal:,.0f}", f"-${amt:,.0f}", f"${new_bal:,.0f}"],
                            y=[old_bal, -amt, new_bal],
                            connector={"line": {"color": "rgb(63, 63, 63)"}},
                        ))
                        fig_bal.update_layout(height=240, margin=dict(l=10, r=10, t=20, b=20))
                        st.plotly_chart(fig_bal, use_container_width=True)

                    # ── Inline Customer 360 History ──
                    st.markdown("---")
                    st.markdown(f"##### 📜 Customer 360 — All Transactions for `{cust}`")
                    cust_history = active_df[active_df["nameOrig"] == cust]
                    st.caption(
                        f"Showing {len(cust_history)} transaction(s) for this account. "
                        "Review velocity, amounts, and prior actions to spot patterns."
                    )

                    if len(cust_history) > 0:
                        # Summary metrics
                        ch1, ch2, ch3 = st.columns(3)
                        ch1.metric("Transaction Count", len(cust_history))
                        ch2.metric("Total Volume", f"${cust_history['amount'].sum():,.2f}")
                        ch3.metric("Peak Risk Score", f"{cust_history['unified_risk_score'].max():.3f}")

                        display_cols = [
                            c for c in ["txn_id", "amount", "unified_risk_score",
                                        "recommended_action", "confidence"]
                            if c in cust_history.columns
                        ]
                        st.dataframe(
                            cust_history[display_cols],
                            use_container_width=True,
                            height=min(180, 35 * len(cust_history) + 40),
                        )

                    # ── Analyst Disposition Feedback Desk ──
                    st.markdown("---")
                    st.markdown("##### 📝 Disposition Desk — Your Verdict")
                    st.caption(
                        "Submit your investigation outcome. This creates a ground-truth "
                        "label that feeds back into the next model re-training cycle."
                    )
                    with st.form(f"disposition_form_{selected_txn_id}"):
                        decision = st.radio(
                            "Select Analyst Intervention:",
                            [
                                "Confirm Fraud (Block Account & Report)",
                                "False Positive (Release Funds to Customer)",
                                "Challenge Customer (Trigger 2FA / Mock OTP)",
                                "Escalate to Tier 2 Fraud Team",
                            ],
                            horizontal=True,
                        )
                        analyst_notes = st.text_area(
                            "Analyst Investigation Rationale",
                            placeholder="e.g. Spoke with customer via phone, transaction confirmed legitimate.",
                        )
                        submitted = st.form_submit_button("💾 Submit Disposition & Close Ticket")

                        if submitted:
                            is_fraud_decision = "Confirm Fraud" in decision
                            save_analyst_disposition(
                                txn_id=selected_txn_id,
                                customer_id=cust,
                                action_taken=decision,
                                is_fraud=is_fraud_decision,
                                notes=analyst_notes,
                                risk_score=score,
                                recommended_action=action,
                            )
                            st.success(
                                f"✅ Disposition recorded for `{selected_txn_id}`! "
                                "Ground-truth label appended for model calibration."
                            )


# ===================================================================
# PAGE 2 — PIPELINE OPERATIONS
# (Same content as old Engineer Ops Console, fixed thresholds)
# ===================================================================
elif page == "⚙️ Pipeline Operations":
    st.title("⚙️ Pipeline Operations Console")
    st.caption(
        "Real-time telemetry for the streaming Lakehouse pipeline. "
        "Monitors 4-state freshness SLA, Lakehouse reconciliation integrity, "
        "and DLQ quarantine forensics."
    )

    # ── 1. Freshness State Engine (Flow 5) ──
    st.subheader("1. Pipeline Freshness — Is Data Current?")
    st.caption(
        "The freshness engine checks how long ago the consumer last committed "
        "records to the Gold layer. Thresholds: **LIVE** < 5 min, "
        "**AGING** 5–30 min, **STALE** > 30 min."
    )

    st_col1, st_col2, st_col3, st_col4 = st.columns(4)

    status_badge_html = _badge(telemetry["status"], telemetry["badge_color"])
    st_col1.markdown(f"**Current State:** {status_badge_html}", unsafe_allow_html=True)
    st_col2.metric(
        "Time Since Last Commit",
        _format_staleness(telemetry["staleness_sec"]) if telemetry["staleness_sec"] is not None else "N/A",
    )
    st_col3.metric(
        "Latest Micro-Batch ID",
        f"Batch #{telemetry['latest_batch_id']}" if telemetry["latest_batch_id"] is not None else "None",
    )
    st_col4.metric("Backpressure Intake Limit", "1-2 files / trigger")

    # Explanation banner
    if telemetry["status"] == "LIVE":
        st.success(
            "🟢 **LIVE:** Last Gold commit arrived within the 5-minute SLA window. "
            "Streaming consumer is healthy."
        )
    elif telemetry["status"] == "AGING":
        st.warning(
            "🟠 **AGING:** 5–30 minutes since last commit. "
            "Possible backpressure draining a burst, or consumer paused between runs."
        )
    else:
        st.error(
            "🔴 **STALE:** No commit for over 30 minutes. "
            "Check upstream producer or restart the consumer process."
        )

    st.markdown("---")

    # ── 2. Lakehouse Reconciliation Auditor (Flow 8) ──
    st.subheader("2. Reconciliation Audit — Do the Numbers Add Up?")
    st.caption(
        "Every record ingested must land in exactly one of three buckets: "
        "Gold (valid), DLQ (quarantined), or Duplicates (suppressed). "
        "A delta of 0 means the pipeline is perfectly balanced."
    )
    recon_status = telemetry["reconciliation_status"]
    recon_delta = telemetry["reconciliation_delta"]
    recon_color = "#2ecc71" if recon_status == "BALANCED" else "#e74c3c"

    st.markdown(
        f"**Audit Formula:** `Total Ingested = Gold + DLQ + Duplicates` &nbsp;&nbsp;|&nbsp;&nbsp; "
        f"**Audit Status:** {_badge(f'{recon_status} (delta = {recon_delta})', recon_color)}",
        unsafe_allow_html=True,
    )

    rc1, rc2, rc3, rc4 = st.columns(4)
    rc1.metric("Total Ingested", f"{telemetry['total_ingested']:,} rows")
    rc2.metric("Committed to Gold", f"{telemetry['total_valid']:,} rows")
    rc3.metric("Diverted to DLQ", f"{telemetry['total_quarantined']:,} rows")
    rc4.metric(
        "DLQ Rejection Rate",
        f"{telemetry['dlq_rejection_rate_pct']:.2f}%",
        delta="SLA Target: < 10.0%",
    )

    st.markdown("---")

    # ── 3. Dead Letter Queue (DLQ) Forensic Inspector ──
    st.subheader("3. DLQ Inspector — Why Were Records Quarantined?")
    st.caption(
        "Records that fail the 7-step quality gate land here. "
        "The bar chart shows the breakdown by error code so you can "
        "identify systemic upstream issues (e.g. corrupt amounts, "
        "missing fields, duplicate bursts)."
    )
    dlq_df = load_dlq_records()

    if dlq_df.empty:
        st.info(
            "DLQ is currently empty (0 quarantined records). "
            "Run the producer with `--scenario corrupt` to exercise the quality gate."
        )
    else:
        q1, q2 = st.columns([1, 1])

        with q1:
            st.markdown("##### Quarantine Reason Breakdown")
            reason_counts = dlq_df["error_code"].value_counts().reset_index()
            reason_counts.columns = ["Error Code", "Quarantined Count"]
            fig_dlq = px.bar(
                reason_counts,
                x="Error Code",
                y="Quarantined Count",
                color="Error Code",
                color_discrete_sequence=px.colors.qualitative.Bold,
                title="Count of DLQ Records by Error Code",
            )
            fig_dlq.update_layout(height=280, showlegend=False)
            st.plotly_chart(fig_dlq, use_container_width=True)
            st.caption(
                "Each bar = one error code from the quality gate. "
                "Taller bars indicate a recurring upstream data issue."
            )

        with q2:
            st.markdown("##### Quarantined Record Samples")
            cols_to_show = [
                c for c in ["error_code", "amount", "type", "nameOrig", "txn_id", "quarantine_ts"]
                if c in dlq_df.columns
            ]
            st.dataframe(dlq_df[cols_to_show].head(15), use_container_width=True, height=280)
            st.caption("First 15 quarantined records. Inspect error_code to diagnose root cause.")

    st.markdown("---")

    # ── 4. Recent Micro-Batch Performance ──
    st.subheader("4. Micro-Batch History — Latency & Throughput Log")
    st.caption(
        "Each row is one consumer micro-batch. Tracks ingest count, "
        "valid/quarantined split, and processing duration."
    )
    if telemetry.get("recent_batches"):
        batches_df = pd.DataFrame(telemetry["recent_batches"])
        st.dataframe(batches_df, use_container_width=True, height=200)
    else:
        st.info("No batch execution history logged yet. Run the consumer to generate entries.")


# ===================================================================
# PAGE 3 — EXECUTIVE DASHBOARD
# (Merges: Executive Overview + High-Risk Analytics)
# ===================================================================
elif page == "📊 Executive Dashboard":
    st.title("📊 Executive Dashboard")
    st.caption(
        "High-level KPIs, intervention action distribution, and fraud-vs-legit "
        "class balance. Expand the High-Risk Analytics section below for "
        "threshold tuning and risk-score distribution analysis."
    )

    if active_df.empty:
        st.info("No transaction data loaded.")
    else:
        total_txns = len(active_df)
        fraud_cases = int(active_df["label"].sum()) if "label" in active_df.columns else 0
        fraud_rate = (fraud_cases / total_txns * 100) if total_txns > 0 else 0
        avg_risk = active_df["unified_risk_score"].mean()

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Transactions", f"{total_txns:,}")
        k2.metric("Fraud Cases Identified", f"{fraud_cases:,}")
        k3.metric("Fraud Rate", f"{fraud_rate:.2f}%")
        k4.metric("Average Risk Score", f"{avg_risk:.3f}")

        st.markdown("---")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("AI Intervention Breakdown")
            st.caption(
                "How the scoring engine distributed recommended actions. "
                "APPROVE = low risk, BLOCK = highest risk. Ideally most "
                "transactions are APPROVE (green)."
            )
            action_counts = active_df["recommended_action"].value_counts().reset_index()
            action_counts.columns = ["Action", "Count"]
            fig_action = px.bar(
                action_counts,
                x="Action",
                y="Count",
                color="Action",
                color_discrete_map={
                    "APPROVE": "#2ecc71",
                    "STEP_UP_AUTH": "#3498db",
                    "HOLD": "#f1c40f",
                    "MANUAL_REVIEW": "#e67e22",
                    "BLOCK": "#e74c3c",
                },
                title="Transactions per Recommended Action",
            )
            fig_action.update_layout(showlegend=False)
            st.plotly_chart(fig_action, use_container_width=True)

        with c2:
            st.subheader("Fraud vs. Legitimate Split")
            st.caption(
                "Ground-truth class balance from the PaySim dataset. "
                "The 'Fraud' slice represents confirmed fraudulent transactions."
            )
            if "label" in active_df.columns:
                label_counts = active_df["label"].value_counts().reset_index()
                label_counts.columns = ["Label", "Count"]
                label_counts["Label"] = label_counts["Label"].map({0: "Legitimate", 1: "Fraud"})
                fig_pie = px.pie(
                    label_counts,
                    names="Label",
                    values="Count",
                    color="Label",
                    color_discrete_map={"Legitimate": "#2ecc71", "Fraud": "#e74c3c"},
                    hole=0.4,
                    title="Class Distribution (Donut Chart)",
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No ground-truth labels available for class distribution.")

        # ── Expandable: High-Risk Analytics ──
        st.markdown("---")
        with st.expander("📈 High-Risk Analytics & Threshold Tuning", expanded=False):
            st.caption(
                "Adjust the risk threshold slider to see how many transactions "
                "would be surfaced at different cut-off points. Use this to "
                "calibrate alert volume vs. coverage trade-offs."
            )

            threshold = st.slider(
                "Minimum Unified Risk Score:",
                min_value=0.0,
                max_value=1.0,
                value=0.40,
                step=0.05,
            )
            high_risk_df = active_df[active_df["unified_risk_score"] >= threshold]

            hr1, hr2, hr3 = st.columns(3)
            hr1.metric("Transactions Above Threshold", f"{len(high_risk_df):,}")
            hr2.metric(
                "% of Total Traffic",
                f"{len(high_risk_df) / total_txns * 100:.1f}%" if total_txns > 0 else "0%",
            )
            fraud_in_high = (
                int(high_risk_df["label"].sum()) if "label" in high_risk_df.columns else 0
            )
            hr3.metric(
                "Fraud Capture Rate",
                f"{fraud_in_high / max(fraud_cases, 1) * 100:.1f}%",
                help="% of all confirmed fraud cases captured above this threshold",
            )

            st.dataframe(
                high_risk_df[
                    [c for c in ["txn_id", "nameOrig", "amount", "unified_risk_score",
                                 "recommended_action", "confidence"] if c in high_risk_df.columns]
                ].head(100),
                use_container_width=True,
                height=300,
            )

            st.subheader("Risk Score Distribution — Fraud Overlay Histogram")
            st.caption(
                "Blue bars = legitimate transactions, red bars = confirmed fraud. "
                "A well-calibrated model pushes fraud to the right (high scores) "
                "and legitimate traffic to the left (low scores)."
            )
            if "label" in active_df.columns:
                hist_df = active_df.copy()
                hist_df["Class"] = hist_df["label"].map({0: "Legitimate", 1: "Fraud"})
                fig_hist = px.histogram(
                    hist_df,
                    x="unified_risk_score",
                    color="Class",
                    nbins=40,
                    barmode="overlay",
                    color_discrete_map={"Legitimate": "#3498db", "Fraud": "#e74c3c"},
                    opacity=0.75,
                    title="Where Do Fraud and Legit Transactions Land on the Risk Scale?",
                    labels={"unified_risk_score": "Unified Risk Score", "count": "Number of Transactions"},
                )
                fig_hist.update_layout(
                    xaxis_title="Unified Risk Score (0 = safe, 1 = certain fraud)",
                    yaxis_title="Number of Transactions",
                )
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info("No ground-truth labels available for overlay histogram.")


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.caption(
    "EFIP Enterprise Fraud Intelligence Platform · v7.0\n\n"
    f"Artifact Dir: `data/lake`"
)
