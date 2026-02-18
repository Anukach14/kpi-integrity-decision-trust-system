# Decision Memo — KPI Integrity & Trust

## Executive summary
A conversion drop was observed, but **low trust scores** indicate the movement is likely caused by
tracking or data issues (outages / schema drift / bots / duplicates), not true performance.

**Recommendation:** Avoid acting on KPI movement when trust < 70. Investigate instrumentation first.

## Lowest trust days (investigate before acting)
| date | trust | reason | conv% | dau | purchasers | revenue |
|---|---:|---|---:|---:|---:|---:|
| 2025-11-16 | 46.5 | possible_purchase_outage, duplicates_detected, invalid_amount_values | 0.36 | 6121 | 22 | 72.44 |
| 2025-11-15 | 46.7 | possible_purchase_outage, duplicates_detected, invalid_amount_values | 0.37 | 6203 | 23 | 117.34 |
| 2025-11-23 | 54.9 | traffic_spike_possible_bots, duplicates_detected, invalid_amount_values | 2.00 | 5953 | 119 | 590.97 |

## Highest trust days (baseline / safe to compare)
| date | trust | reason | conv% | dau | purchasers | revenue |
|---|---:|---|---:|---:|---:|---:|
| 2025-11-17 | 72.9 | duplicates_detected, invalid_amount_values | 2.93 | 5978 | 175 | 809.40 |
| 2025-11-21 | 72.5 | duplicates_detected, invalid_amount_values | 2.69 | 5214 | 140 | 639.05 |
| 2025-11-12 | 72.4 | duplicates_detected, invalid_amount_values | 2.59 | 6258 | 162 | 674.40 |

## Evidence (see visuals)
- `outputs/figures/kpi_vs_trust.png`
- `outputs/figures/quality_heatmap.png`
- `outputs/figures/decision_impact.png`

## Real company next steps
1. Confirm event naming mapping (purchase vs in_app_purchase) and update tracking/ETL.
2. Investigate missing purchase events on outage days (SDK / pipeline).
3. Add bot filtering rules for traffic spikes and backfill metrics.
4. Automate alerts when trust score < 70.
