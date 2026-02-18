import pandas as pd
import numpy as np


def quality_checks(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()
    df["date"] = df["event_ts"].dt.floor("D")

    purch = df[df["event_name"].isin(["purchase", "in_app_purchase"])]

    # Completeness
    purchase_events = purch.groupby("date").size().rename("purchase_events")
    baseline_purchase = purchase_events.rolling(7, min_periods=1).median().replace(0, np.nan)
    score_completeness = (purchase_events / baseline_purchase).clip(0, 1).fillna(0)

    # Schema drift
    has_purchase = (df[df["event_name"] == "purchase"].groupby("date").size() > 0).astype(int)
    has_iap = (df[df["event_name"] == "in_app_purchase"].groupby("date").size() > 0).astype(int)
    schema_drift_flag = ((has_iap == 1) & (has_purchase == 0)).astype(int)
    score_schema = pd.Series(np.where(schema_drift_flag == 1, 0.60, 1.00), index=schema_drift_flag.index)

    # Uniqueness (duplicates)
    dup_mask = df.duplicated(subset=["user_id", "event_ts", "event_name", "amount"], keep=False)
    dup_count = df[dup_mask].groupby("date").size().rename("duplicate_events")
    dup_baseline = dup_count.rolling(7, min_periods=1).median().fillna(0)
    score_uniqueness = np.exp(-(dup_count / (dup_baseline + 1)))

    # Volume anomaly (bot spike)
    sessions = df[df["event_name"] == "session_start"].groupby("date").size().rename("session_events")
    z = (sessions - sessions.mean()) / (sessions.std(ddof=0) if sessions.std(ddof=0) != 0 else 1.0)
    volume_anomaly_flag = (z.abs() > 2.8).astype(int)
    score_volume = pd.Series(np.where(volume_anomaly_flag == 1, 0.55, 1.00), index=volume_anomaly_flag.index)

    # Validity (negative amounts)
    neg_amount = purch[purch["amount"].fillna(0) < 0].groupby("date").size().rename("neg_amount_events")
    score_validity = pd.Series(np.where(neg_amount.fillna(0) > 0, 0.0, 1.0), index=neg_amount.index)

    # Build daily frame
    all_dates = pd.date_range(df["date"].min(), df["date"].max(), freq="D", tz="UTC")
    q = pd.DataFrame({"date": all_dates}).set_index("date")

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

    # Trust score
    q["trust_score"] = (
        0.30 * q["score_completeness"] +
        0.20 * q["score_schema"] +
        0.20 * q["score_uniqueness"] +
        0.15 * q["score_volume"] +
        0.15 * q["score_validity"]
    ) * 100

    # Reason tags (human readable)
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
