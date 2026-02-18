import pandas as pd
import numpy as np


def compute_daily_kpis(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()
    df["event_date"] = df["event_ts"].dt.floor("D")

    dau = (
        df[df["event_name"] == "session_start"]
        .groupby("event_date")["user_id"]
        .nunique()
        .rename("dau")
    )

    purchase_events = df[df["event_name"].isin(["purchase", "in_app_purchase"])]

    purchasers = (
        purchase_events
        .groupby("event_date")["user_id"]
        .nunique()
        .rename("purchasers")
    )

    revenue = (
        purchase_events
        .groupby("event_date")["amount"]
        .sum(min_count=1)
        .fillna(0)
        .rename("revenue")
    )

    kpis = pd.concat([dau, purchasers, revenue], axis=1).fillna(0)

    kpis["conversion_rate"] = np.where(
        kpis["dau"] > 0,
        kpis["purchasers"] / kpis["dau"],
        0.0
    )

    # D1 retention proxy
    active = (
        df[df["event_name"] == "session_start"]
        [["event_date", "user_id"]]
        .drop_duplicates()
    )

    yday = active.rename(columns={"event_date": "prev_date"})
    active["prev_date"] = active["event_date"] - pd.Timedelta(days=1)

    retained = (
        active.merge(yday, on=["prev_date", "user_id"], how="inner")
        .groupby("event_date")["user_id"]
        .nunique()
        .rename("retained_from_yday")
    )

    active_yday = (
        yday.groupby("prev_date")["user_id"]
        .nunique()
        .rename("active_yday")
    )

    retention = pd.concat([retained, active_yday], axis=1).fillna(0)

    kpis["d1_retention_proxy"] = np.where(
        retention["active_yday"] > 0,
        retention["retained_from_yday"] / retention["active_yday"],
        0.0
    )

    kpis["revenue_per_dau"] = np.where(
        kpis["dau"] > 0,
        kpis["revenue"] / kpis["dau"],
        0.0
    )

    return (
        kpis.reset_index()
        .sort_values("event_date")
    )
