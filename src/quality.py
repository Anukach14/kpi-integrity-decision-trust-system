import pandas as pd
import numpy as np

def quality_checks(events: pd.DataFrame) -> pd.DataFrame:
    """
    Build daily data-quality signals and combine them into a KPI Trust Score (0–100).

    Why this is portfolio-grade:
    - It mimics how real analytics teams prevent leaders from reacting to broken metrics.
    - It produces human-readable 'reason tags' for low-trust days.
    """

    df = events.copy()
    df["date"] = df["event_ts"].dt.floor("D")

    # -------------------------
    # 1) COMPLETENESS (Purchase tracking outage signal)
    # -------------------------
    purch = df[df["event_name"].isin(["purchase", "in_app_purchase"])]

    purchase_events = purch.groupby("date").size().rename("purchase_events")
    # Use rolling median as baseline expectation (robust to spikes)
    baseline_purchase = purchase_events.rolling(7, min_periods=1).median().replace(0, np.nan)

    score_completeness = (purchase_events / baseline_purchase).clip(0, 1).fillna(0)
    # If purchase events collapse, completeness score drops

    # -------------------------
    # 2) SCHEMA CONSISTENCY (Schema drift: purchase renamed -> in_app_purchase)
    # -------------------------
    has_purchase = (df[df["event_name"] == "purchase"].groupby("date").size() > 0).astype(int)
    has_iap = (df[df["event_name"] == "in_app_purchase"].groupby("date").size() > 0).astype(int)

    # If day contains in_app_purchase but no purchase, likely schema drift started (mapping not updated)
    schema_drift_flag = ((has_iap == 1) & (has_purchase == 0)).astype(int)
    score_schema = np.where(schema_drift_flag == 1, 0.60, 1.00)

    score_schema = pd.Series(score_schema, index=schema_drift_flag.index).rename("score_schema")

    # -------------------------
    # 3) UNIQUENESS (Duplicate events: pipeline rerun)
    # -------------------------
    dup_mask = df.duplicated(subset=["user_id", "event_ts", "event_name", "amount"], keep=False)
    dup_count = df[dup_mask].groupby("date").size().rename("duplicate_events")

    # Convert duplicates into a score (more duplicates => lower score)
    dup_baseline = dup_count.rolling(7, min_periods=1).median().fillna(0)
    score_uniqueness = np.exp(-(dup_count / (dup_baseline + 1))).rename("score_uniqueness")

    # -------------------------
    # 4) VOLUME ANOMALY (Bot spike inflating sessions/DAU)
    # -------------------------
    sessions = df[df["event_name"] == "session_start"].groupby("date").size().rename("session_events")
    z = (sessions - sessions.mean()) / (sessions.std(ddof=0) if sessions.std(ddof=0) != 0 else 1.0)
    volume_anomaly_flag = (z.abs() > 2.8).astype(int).rename("volume_anomaly_flag")

    score_volume = np.where(volume_anomaly_flag == 1, 0.55, 1.00)
    score_volume = pd.Series(score_volume, index=volume_anomaly_flag.index).rename("score_volume")

    # -------------------------
    # 5) VALIDITY (simple: negative amounts, impossible values)
    # -------------------------
    neg_amount = purch[purch["amount"].fillna(0) < 0].groupby("date").size().rename("neg_amount_events")
    score_validity = np.where(neg_amount.fillna(0) > 0, 0.0, 1.0)
    score_validity = pd.Series(score_validity, index=neg_amount.index).rename("score_validity")

    # -------------------------
    # Combine into single daily frame (fill missing dates)
    # -------------------------
    all_dates = pd.date_range(df["date"].min(), df["date"].max(), freq="D", tz="UTC")

    q = pd.DataFrame({"date": all_dates})
    q = q.set_index("date")

    q["purchase_events"] = purchase_events
    q["session_events"] = sessions
    q["duplicate_events"] = dup_count
    q["schema_drift_flag"] = schema_drift_flag
    q["volume_anomaly_flag"] = volume_anomaly_flag
    q["neg_amount_events"] = neg_amount

    q["score_completeness"] = score_completeness
    q["score_schema"] = score_schema
    q["score_uniqueness"] = score_uniqueness
    q["score_volume"] = score_volume
    q["score_validity"] = score_validity

    q = q.fillna(0)

    # -------------------------
    # Trust Score (weighted & interpretable)
    # -------------------------
    q["trust_score"] = (
        0.30 * q["score_completeness"] +
        0.20 * q["score_schema"] +
        0.20 * q["score_uniqueness"] +
        0.15 * q["score_volume"] +
        0.15 * q["score_validity"]
    ) * 100

    # -------------------------
    # Human readable reason tag (this prevents “AI looking” outputs)
    # -------------------------
    reasons = []
    for _, r in q.iterrows():
        tags = []
        if r["score_completeness"] < 0.70:
            tags.append("possible_purchase_outage")
        if r["score_schema"] < 1.00:
            tags.append("schema_drift_purchase_name")
        if r["score_volume"] < 1.00:
            tags.append("traffic_spike_possible_bots")
        if r["duplicate_events"] > 0:
            tags.append("duplicates_detected")
        if r["score_validity"] < 1.00:
            tags.append("invalid_amount_values")

        reasons.append(", ".join(tags) if tags else "ok")

    q["reason"] = reasons

    return q.reset_index()
