from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def _save(fig, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def _primary_reason(reason: str) -> str:
    """Pick one short tag to annotate, so plots stay readable."""
    if not isinstance(reason, str) or reason.strip() == "":
        return "ok"
    if reason.strip() == "ok":
        return "ok"
    # keep just the first tag (most important)
    return reason.split(",")[0].strip()


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

    # KPI line: solid + thicker (business metric)
    ax1.plot(
        x,
        df["conversion_rate"] * 100,
        label="Conversion rate (%)",
        linewidth=2.8
    )
    ax1.set_ylabel("Conversion rate (%)")
    ax1.set_title("Conversion moved — but was it real? (Overlay KPI Trust Score)")
    ax1.grid(True, alpha=0.25)

    ax2 = ax1.twinx()

    # Trust line: dashed (reliability signal)
    ax2.plot(
        x,
        df["trust_score"],
        label="KPI Trust Score",
        linestyle="--",
        linewidth=2.2
    )
    ax2.set_ylabel("KPI Trust Score (0–100)")
    ax2.set_ylim(0, 105)

    # Trust threshold: dotted decision rule
    ax2.axhline(70, linestyle=":", linewidth=2, alpha=0.8)
    ax2.text(x.iloc[1], 72, "Trust threshold (investigate below)", fontsize=9)

    # Annotate only the 2 lowest trust days (avoid clutter)
    worst = df.nsmallest(2, "trust_score").copy()
    for _, r in worst.iterrows():
        tag = _primary_reason(r["reason"])
        ax2.annotate(
            tag,
            (r["event_date"], r["trust_score"]),
            textcoords="offset points",
            xytext=(0, 16),
            ha="center",
            fontsize=9,
            bbox=dict(boxstyle="round,pad=0.25", alpha=0.9),
            arrowprops=dict(arrowstyle="->", alpha=0.6)
        )

    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")

    _save(fig, out_dir / "kpi_vs_trust.png")


def plot_quality_heatmap(quality: pd.DataFrame, out_dir: Path):
    """
    Story plot 2:
    Heatmap of quality signals — shows WHICH dimension caused distrust.
    """
    cols = ["score_completeness", "score_schema", "score_uniqueness", "score_volume", "score_validity"]
    q = quality.copy()

    # Make sure these are within 0..1 for display
    for c in cols:
        q[c] = q[c].clip(0, 1)

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

    completeness = df["score_completeness"].clip(0.35, 1.0).replace(0, 0.35)
    df["conversion_corrected"] = (df["conversion_rate"] / completeness).clip(0, 1)

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(df["event_date"], df["conversion_rate"] * 100, label="Reported conversion (%)", linewidth=2.6)
    ax.plot(df["event_date"], df["conversion_corrected"] * 100, label="Corrected (adjusted for completeness) (%)", linewidth=2.2)

    ax.set_title("Decision impact: reported drop vs corrected view (tracking issues ≠ real drop)")
    ax.set_ylabel("Conversion rate (%)")
    ax.grid(True, alpha=0.25)
    ax.legend()

    # Annotate only the single worst trust day (avoid bottom text mess)
    worst = df.nsmallest(1, "trust_score").iloc[0]
    tag = _primary_reason(worst["reason"])
    ax.annotate(
        f"Low trust → {tag}",
        (worst["event_date"], worst["conversion_rate"] * 100),
        textcoords="offset points",
        xytext=(10, 18),
        ha="left",
        fontsize=9,
        bbox=dict(boxstyle="round,pad=0.25", alpha=0.9),
        arrowprops=dict(arrowstyle="->", alpha=0.6)
    )

    _save(fig, out_dir / "decision_impact.png")


def make_cover_image(kpis: pd.DataFrame, quality: pd.DataFrame, asset_dir: Path):
    """
    Cover image for LinkedIn/GitHub.
    Goal: understand the story in 3 seconds.
    """
    df = kpis.merge(
        quality[["date", "trust_score", "reason"]],
        left_on="event_date",
        right_on="date",
        how="left"
    )

    fig, ax1 = plt.subplots(figsize=(12, 6))

    # KPI: solid + bold
    ax1.plot(
        df["event_date"],
        df["conversion_rate"] * 100,
        label="Conversion (%)",
        linewidth=3.0
    )
    ax1.set_ylabel("Conversion (%)")
    ax1.grid(True, alpha=0.25)

    ax2 = ax1.twinx()

    # Trust: dashed for contrast
    ax2.plot(
        df["event_date"],
        df["trust_score"],
        label="Trust score",
        linestyle="--",
        linewidth=2.4
    )
    ax2.set_ylabel("Trust score (0–100)")
    ax2.set_ylim(0, 105)

    # Trust threshold: dotted
    ax2.axhline(70, linestyle=":", linewidth=2.2, alpha=0.85)
    ax2.text(df["event_date"].iloc[1], 72, "Trust threshold (investigate below)", fontsize=10)

    ax1.set_title("KPI Integrity & Decision Trust System\nWhen KPIs lie, decisions fail", fontsize=16, pad=12)

    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")

    ax1.text(
        0.01, -0.18,
        "Story: KPI dropped on low-trust days (outage / schema drift / bot spike).\n"
        "Outcome: Trust score flags unreliable days before stakeholders act.",
        transform=ax1.transAxes,
        fontsize=10
    )

    asset_dir.mkdir(parents=True, exist_ok=True)
    _save(fig, asset_dir / "cover.png")
