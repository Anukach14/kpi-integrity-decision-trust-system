import numpy as np
import pandas as pd


def generate_events(
    seed: int = 7,
    start_date: str = "2025-11-01",
    days: int = 35,
    users: int = 12000,
    missing_purchase_days=(14, 15),
    schema_drift_day: int = 18,
    bot_spike_day: int = 22,
    timezone_shift_day: int = 27,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    start = pd.Timestamp(start_date, tz="UTC")

    join_probs = _softmax(-np.arange(days) / 10)
    join_day = rng.choice(np.arange(days), size=users, p=join_probs)
    user_ids = np.arange(1, users + 1)

    activity_prop = rng.beta(2, 6, size=users)
    purchase_prop = rng.beta(1.2, 25, size=users)

    rows = []
    for uid, jd, a, p in zip(user_ids, join_day, activity_prop, purchase_prop):
        life = int(rng.integers(3, 28))
        for d in range(jd, min(days, jd + life)):
            date = start + pd.Timedelta(days=int(d))

            sessions = rng.poisson(lam=1 + 6 * a)
            for _ in range(sessions):
                ts = date + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
                rows.append((uid, ts, "session_start", None))

                completes = rng.binomial(3, 0.25 + 0.35 * a)
                for __ in range(completes):
                    ts2 = ts + pd.Timedelta(minutes=int(rng.integers(1, 25)))
                    rows.append((uid, ts2, "level_complete", None))

            if rng.random() < (0.015 + 0.08 * p + 0.02 * a):
                amount = float(np.round(rng.lognormal(mean=1.2, sigma=0.7), 2))
                ts = date + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
                rows.append((uid, ts, "purchase", amount))

    df = pd.DataFrame(rows, columns=["user_id", "event_ts", "event_name", "amount"])
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)

    df = inject_failures(
        df=df,
        rng=rng,
        start=start,
        users=users,
        days=days,
        missing_purchase_days=missing_purchase_days,
        schema_drift_day=schema_drift_day,
        bot_spike_day=bot_spike_day,
        timezone_shift_day=timezone_shift_day,
        seed=seed,
    )

    return df.sort_values("event_ts").reset_index(drop=True)


def inject_failures(
    df: pd.DataFrame,
    rng: np.random.Generator,
    start: pd.Timestamp,
    users: int,
    days: int,
    missing_purchase_days,
    schema_drift_day: int,
    bot_spike_day: int,
    timezone_shift_day: int,
    seed: int,
) -> pd.DataFrame:
    # 1) Missing purchase events on outage days
    for di in list(missing_purchase_days):
        day_start = start + pd.Timedelta(days=int(di))
        day_end = day_start + pd.Timedelta(days=1)
        mask = (
            (df["event_ts"] >= day_start)
            & (df["event_ts"] < day_end)
            & (df["event_name"].isin(["purchase", "in_app_purchase"]))
        )
        drop = mask & (rng.random(len(df)) < 0.85)
        df = df.loc[~drop].copy()

    # 2) Schema drift: purchase renamed to in_app_purchase
    drift_start = start + pd.Timedelta(days=int(schema_drift_day))
    mask = (df["event_ts"] >= drift_start) & (df["event_name"] == "purchase")
    df.loc[mask, "event_name"] = "in_app_purchase"

    # 3) Bot spike: inject many session_start events
    bot_start = start + pd.Timedelta(days=int(bot_spike_day))
    bot_users = rng.integers(1, users + 1, size=2200)
    bot_rows = []
    for uid in bot_users:
        for _ in range(int(rng.integers(10, 40))):
            ts = bot_start + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
            bot_rows.append((uid, ts, "session_start", None))
    bot_df = pd.DataFrame(bot_rows, columns=df.columns)
    bot_df["event_ts"] = pd.to_datetime(bot_df["event_ts"], utc=True)
    df = pd.concat([df, bot_df], ignore_index=True)

    # 4) Timezone shift: +1 hour
    tz_start = start + pd.Timedelta(days=int(timezone_shift_day))
    tz_end = tz_start + pd.Timedelta(days=1)
    mask = (df["event_ts"] >= tz_start) & (df["event_ts"] < tz_end)
    df.loc[mask, "event_ts"] = df.loc[mask, "event_ts"] + pd.Timedelta(hours=1)

    # 5) Duplicates on a random day
    dup_day = int(rng.integers(5, max(6, days - 5)))
    dup_start = start + pd.Timedelta(days=dup_day)
    dup_end = dup_start + pd.Timedelta(days=1)
    day_df = df[(df["event_ts"] >= dup_start) & (df["event_ts"] < dup_end)]
    if len(day_df) > 0:
        dup_sample = day_df.sample(min(1200, len(day_df)), random_state=seed)
        df = pd.concat([df, dup_sample], ignore_index=True)

    return df


def _softmax(x):
    e = np.exp(x - np.max(x))
    return e / e.sum()
