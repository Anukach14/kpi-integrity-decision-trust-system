from pathlib import Path

from src.data_gen import generate_events
from src.kpis import compute_daily_kpis
from src.quality import quality_checks
from src.viz import (
    plot_kpi_vs_trust,
    plot_quality_heatmap,
    plot_decision_impact,
    make_cover_image,
)


def main():
    base = Path(".")
    data_dir = base / "data"
    fig_dir = base / "outputs" / "figures"
    rep_dir = base / "outputs" / "reports"
    asset_dir = base / "assets"

    data_dir.mkdir(exist_ok=True)
    fig_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    asset_dir.mkdir(exist_ok=True)

    print("1) Generating synthetic event data (with injected failures)...")
    events = generate_events()
    events.to_csv(data_dir / "events.csv", index=False)

    print("2) Computing daily KPIs...")
    kpis = compute_daily_kpis(events)
    kpis.to_csv(data_dir / "daily_kpis.csv", index=False)

    print("3) Running data quality checks + trust score...")
    q = quality_checks(events)
    q.to_csv(data_dir / "daily_quality.csv", index=False)

    print("4) Creating storytelling visuals...")
    plot_kpi_vs_trust(kpis, q, fig_dir)
    plot_quality_heatmap(q, fig_dir)
    plot_decision_impact(kpis, q, fig_dir)

    print("5) Creating LinkedIn/GitHub cover image...")
    make_cover_image(kpis, q, asset_dir)

    print("6) Writing decision memo...")
    (rep_dir / "decision_memo.md").write_text(build_decision_memo(kpis, q), encoding="utf-8")

    print("\nDONE.")
    print("Open:")
    print(" - outputs/figures/ (3 storytelling visuals)")
    print(" - assets/cover.png (LinkedIn/GitHub thumbnail)")
    print(" - outputs/reports/decision_memo.md")


def build_decision_memo(kpis, q):
    df = kpis.merge(q[["date", "trust_score", "reason"]], left_on="event_date", right_on="date", how="left")
    low = df.nsmallest(3, "trust_score")[["event_date", "trust_score", "reason", "conversion_rate", "dau", "purchasers", "revenue"]]
    high = df.nlargest(3, "trust_score")[["event_date", "trust_score", "reason", "conversion_rate", "dau", "purchasers", "revenue"]]

    def md_table(t):
        lines = []
        lines.append("| date | trust | reason | conv% | dau | purchasers | revenue |")
        lines.append("|---|---:|---|---:|---:|---:|---:|")
        for _, r in t.iterrows():
            lines.append(
                f"| {r['event_date'].date()} | {r['trust_score']:.1f} | {r['reason']} | {100*r['conversion_rate']:.2f} | "
                f"{int(r['dau'])} | {int(r['purchasers'])} | {r['revenue']:.2f} |"
            )
        return "\n".join(lines)

    return f"""# Decision Memo — KPI Integrity & Trust

## Executive summary
A conversion drop was observed, but **low trust scores** indicate the movement is likely caused by
tracking or data issues (outages / schema drift / bots / duplicates), not true performance.

**Recommendation:** Avoid acting on KPI movement when trust < 70. Investigate instrumentation first.

## Lowest trust days (investigate before acting)
{md_table(low)}

## Highest trust days (baseline / safe to compare)
{md_table(high)}

## Evidence (see visuals)
- `outputs/figures/kpi_vs_trust.png`
- `outputs/figures/quality_heatmap.png`
- `outputs/figures/decision_impact.png`

## Real company next steps
1. Confirm event naming mapping (purchase vs in_app_purchase) and update tracking/ETL.
2. Investigate missing purchase events on outage days (SDK / pipeline).
3. Add bot filtering rules for traffic spikes and backfill metrics.
4. Automate alerts when trust score < 70.
"""


if __name__ == "__main__":
    main()
