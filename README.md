# KPI Integrity & Decision Trust System

This repository contains a small analytics pipeline designed to evaluate the **reliability of daily KPIs** derived from event-level telemetry.

The goal is not to optimise a metric, but to determine **whether a metric movement should be trusted before decisions are made**.

The project simulates common real-world data issues (tracking outages, schema drift, bot traffic, duplicate ingestion) and combines multiple quality signals into a single daily **KPI Trust Score (0–100)**.

---

## Project overview

**Input**
- Synthetic event stream (`session_start`, `level_complete`, `purchase`, `in_app_purchase`)
- Event timestamps stored in UTC

**Outputs**
- Daily KPIs (DAU, conversion rate, revenue, retention proxy)
- Daily data-quality signals
- A weighted trust score indicating metric reliability
- Diagnostic plots and a short decision memo

---

## Why this project exists

In production analytics, KPI movement is often caused by:
- instrumentation outages
- schema changes
- traffic anomalies
- ingestion duplication

Dashboards typically surface the KPI value but not its **reliability**.  
This project demonstrates a simple, interpretable approach to making KPI reliability explicit.

---

## Pipeline structure

1. **Event generation**
   - Generates user activity with heterogeneous behaviour
   - Injects controlled data failures:
     - missing purchase events
     - schema drift (`purchase` → `in_app_purchase`)
     - bot-driven session spikes
     - timestamp shifts
     - duplicate ingestion

2. **KPI computation**
   - DAU
   - purchasers
   - revenue
   - conversion rate
   - D1 retention proxy
   - revenue per DAU

3. **Quality checks**
   - completeness
   - schema consistency
   - uniqueness
   - volume anomaly detection
   - basic validity checks

4. **Trust scoring**
   - Weighted combination of quality signals
   - Produces a daily score between 0 and 100
   - Lower scores indicate higher decision risk

5. **Visual diagnostics**
   - KPI vs trust overlay
   - quality signal heatmap
   - reported vs adjusted KPI comparison

---

## How to run

### Create a virtual environment

**Windows**
```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python run_all.py
