from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def _save(fig, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def plot_kpi_vs_trust(kpis: pd.DataFrame, quality: pd.DataFrame, out_dir: Path):
    """
    Story plot 1:
    “KPI moved — but can we trust it?”
    Overlay conversion rate with trust score and annotate low-trust reasons.
    """
    df = kpis.merge(
        quality[["date", "trust_score", "reason"]],
        left_on="event_date",
        right_on="date",
        how="left"
    )

    x = df["event_date"]

    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(x, df["conversion_rate"] * 100)
    ax1.set_ylabel("Conversion rate (%)")
    ax1.set_title("Conversion moved — but was it real? (Overlay KPI Trust Score)")
    ax1.grid(True, alpha=0.25)

    ax2 = ax1.twinx()
    ax2.plot(x, df["trust_score"])
    ax2.set_ylabel("KPI Trust Score (0–100)")
    ax2.set_ylim(0, 105)

    # Annotate the 3 lowest trust days
    worst = df.nsmallest(3, "trust_score")
    for _, r in worst.iterrows():
        ax2.annotate(
            r["reason"],
            (r["event_date"], r["trust_score"]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8
        )

    _save(fig, out_dir / "kpi_vs_trust.png")


def plot_quality_heatmap(quality: pd.DataFrame, out_dir: Path):
    """
    Story plot 2:
    Heatmap of quality signals — shows WHICH dimension caused distrust.
    """
    cols = ["score_completeness", "score_schema", "score_uniqueness", "score_volume", "score_validity"]
    q = quality.copy()

    mat = q[cols].to_numpy().T

    fig, ax = plt.subplots(figsize=(12, 3.8))
    im = ax.imshow(mat, aspect="auto")

    ax.set_yticks(np.arange(len(cols)))
    ax.set_yticklabels([c.replace("score_", "") for c in cols])

    labels = pd.to_datetime(q["date"]).dt.strftime("%m-%d")
    ax.set_xticks(np.arange(len(labels)))
    ax.set_xticklabels(labels, rotation=90, fontsize=7)

    ax.set_title("Root-cause map: which data-quality dimension broke the KPI?")
    fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02)

    _save(fig, out_dir / "quality_heatmap.png")


def plot_decision_impact(kpis: pd.DataFrame, quality: pd.DataFrame, out_dir: Path):
    """
    Story plot 3:
    “If you reacted to the reported KPI, you’d make the wrong decision.”
    We show a 'corrected' conversion adjusted by completeness to demonstrate
    how tracking outages can create fake drops.
    """
    df = kpis.merge(
        quality[["date", "trust_score", "reason", "score_completeness"]],
        left_on="event_date",
        right_on="date",
        how="left"
    )

    # crude correction: if completeness is low, reported conversion is likely biased downward
    completeness = df["score_completeness"].clip(0.35, 1.0).replace(0, 0.35)
    df["conversion_corrected"] = (df["conversion_rate"] / completeness).clip(0, 1)

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(df["event_date"], df["conversion_rate"] * 100, label="Reported conversion (%)")
    ax.plot(df["event_date"], df["conversion_corrected"] * 100, label="Corrected (adjusted for completeness) (%)")

    ax.set_title("Decision impact: reported drop vs corrected view (tracking issues ≠ real drop)")
    ax.set_ylabel("Conversion rate (%)")
    ax.grid(True, alpha=0.25)
    ax.legend()

    worst = df.nsmallest(2, "trust_score")
    for _, r in worst.iterrows():
        ax.annotate(
            f"Low trust: {r['reason']}",
            (r["event_date"], r["conversion_rate"] * 100),
            textcoords="offset points",
            xytext=(10, 15),
            ha="left",
            fontsize=8
        )

    _save(fig, out_dir / "decision_impact.png")


def make_cover_image(kpis: pd.DataFrame, quality: pd.DataFrame, asset_dir: Path):
    """
    Creates a cover image to use as:
      - LinkedIn post image
      - GitHub repo header image
      - Portfolio thumbnail

    Design goal:
    A hiring manager should understand the project in 3 seconds.
    """
    df = kpis.merge(
        quality[["date", "trust_score", "reason"]],
        left_on="event_date",
        right_on="date",
        how="left"
    )

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.plot(df["event_date"], df["conversion_rate"] * 100, label="Conversion (%)")
    ax1.set_ylabel("Conversion (%)")
    ax1.grid(True, alpha=0.25)

    ax2 = ax1.twinx()
    ax2.plot(df["event_date"], df["trust_score"], label="Trust score")
    ax2.set_ylabel("Trust score (0–100)")
    ax2.set_ylim(0, 105)

    ax1.set_title("KPI Integrity & Decision Trust System\nWhen KPIs lie, decisions fail", fontsize=16, pad=12)

    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")

    # Put short “story line” text
    ax1.text(
        0.01, -0.18,
        "Story: KPI dropped on low-trust days (outage / schema drift / bot spike).\n"
        "Outcome: Trust score flags unreliable days before stakeholders act.",
        transform=ax1.transAxes,
        fontsize=10
    )

    asset_dir.mkdir(parents=True, exist_ok=True)
    _save(fig, asset_dir / "cover.png")
