# Ad-Streams: Real-Time Propensity Scoring on Snowflake

A complete real-time digital marketing propensity scoring pipeline — built entirely on Snowflake. Streaming ingestion, incremental feature engineering, ML model registry, sub-100ms serving, LLM-powered recommendations, and a deployed Next.js dashboard.

![Architecture: 2017 six-system stack vs 2026 Snowflake platform](ad_streams_blog_images/01_architecture_comparison_gemini.png)

> **Note:** Snowflake Datastream is currently in **Private Preview**. The ingestion layer (Step 2) requires access to this feature. If your account doesn't have Datastream enabled, you can skip it and load data directly via the event simulator (Step 4).

---

## Architecture

```
[Event Simulator]  →  [Datastream Topics]  →  [Snowpipe Streaming]
                                                       │
                                              ┌────────┴────────┐
                                              │  Bronze Tables   │  (raw_marketing_events, raw_conversion_events)
                                              └────────┬────────┘
                                                       │ CHANGE_TRACKING
                                              ┌────────┴────────┐
                                              │  Dynamic Tables  │  (unified → enriched → features → propensity)
                                              └────────┬────────┘
                                                       │ Custom Incrementalization (MERGE INTO SELF)
                                              ┌────────┴────────┐
                                              │Interactive Table │  + Interactive Warehouse (sub-100ms reads)
                                              └────────┬────────┘
                                                       │
                                              ┌────────┴────────┐
                                              │  App Runtime     │  Next.js dashboard + Cortex AI recommendations
                                              └─────────────────┘
```

---

## Prerequisites

- A Snowflake account (Enterprise edition or higher)
- `ACCOUNTADMIN` or a role with `CREATE DATABASE`, `CREATE WAREHOUSE` privileges
- **[Private Preview]** Datastream access (optional — simulator works without it)
- Snowflake CLI (`snow`) installed for App Runtime deployment
- Python 3.9+ for notebook and feature store scripts

---

## Setup Steps

### Step 1: Create Database, Schema, and Warehouse

```sql
-- Run sql/00_setup.sql
-- Creates: DEMO_<USER>.AD_STREAMS schema, AD_STREAMS_WH warehouse,
-- ML_REGISTRY / ML_EXPERIMENTS / FEATURE_STORE schemas
```

```bash
snow sql -f sql/00_setup.sql
```

### Step 2: Datastream + Snowpipe Streaming (Private Preview)

> ⚠️ **Snowflake Datastream is in Private Preview.** Skip this step if your account doesn't have access. The event simulator in Step 4 will generate data directly into bronze tables.

```bash
chmod +x scripts/01_datastream_setup.sh
./scripts/01_datastream_setup.sh
```

This creates:
- A Kafka-compatible Datastream broker
- Topics for marketing events and conversion events
- Snowpipe Streaming pipes landing data into bronze tables

### Step 3: Bronze Tables

```bash
snow sql -f sql/02_bronze.sql
```

Creates `raw_marketing_events` and `raw_conversion_events` with change tracking enabled.

### Step 4: Event Simulator

```bash
snow sql -f sql/03_simulator.sql
```

Creates a stored procedure + scheduled task that generates realistic ad events (impressions, clicks, paid search, conversions) every minute. This works whether or not you have Datastream — it inserts directly into the bronze tables.

### Step 5: Dynamic Table Pipeline

```bash
snow sql -f sql/04_dynamic_tables.sql
```

Creates the incremental pipeline:
1. `dt_events_unified` — merges marketing + conversion events
2. `dt_events_enriched` — adds channel mapping, device info
3. `dt_user_features` — rolling 1h/24h/7d aggregates via **Custom Incrementalization** (`MERGE INTO SELF` with `CHANGES(INFORMATION => APPEND_ONLY)`)
4. `dt_user_propensity` — calls the registered model for scoring

> **Note:** Step 5b (model training) must complete before `dt_user_propensity` can refresh.

### Step 5b: Train and Register the ML Model

Open `notebooks/propensity_model_dev.ipynb` in Snowflake Notebooks (or run locally):

```bash
python notebooks/_run_experiment_local.py
```

This:
- Races 4 model families (LogReg, RandomForest, XGBoost, LightGBM) as ML Experiment runs
- Registers the champion as `ML_REGISTRY.AD_PROPENSITY_MODEL`
- Sets the default version for the Dynamic Table to call

### Step 6: Serving Layer

```bash
snow sql -f sql/05_serving.sql
snow sql -f sql/07_campaign_dts.sql
```

Creates:
- `propensity_dashboard` — **Interactive Table** clustered by `(score_bucket, user_id)`
- `AD_STREAMS_INTERACTIVE_WH` — **Interactive Warehouse** for sub-100ms point lookups
- Campaign performance, attribution, and reach/frequency Dynamic Tables

### Step 6b: MLOps + Online Feature Store

```bash
snow sql -f sql/08_mlops.sql
python sql/09_online_feature_store.py
```

Sets up:
- Model Monitor (drift detection)
- Inference logging
- Scheduled weekly retrain task
- **Postgres Online Feature Store** for ~10ms real-time reads

### Step 7: Deploy the Dashboard (App Runtime)

```bash
cd app
snow app deploy
```

Deploys a Next.js application inside Snowflake with:
- Propensity scoring dashboard
- Campaign performance + heatmaps
- Multi-touch attribution (first/last/linear/time-decay)
- Audience segment builder
- AI campaign optimizer (Cortex `AI_COMPLETE`)

---

## Verify

```sql
-- Check DT pipeline health
SHOW DYNAMIC TABLES IN SCHEMA AD_STREAMS;

-- Check propensity scores
SELECT * FROM dt_user_propensity ORDER BY propensity_score DESC LIMIT 10;

-- Check Interactive Table serving (should be <100ms)
SELECT * FROM propensity_dashboard WHERE user_id = 'U0001';

-- Check event throughput
SELECT event_type, COUNT(*) FROM raw_marketing_events GROUP BY event_type;

-- Check model registry
SHOW VERSIONS IN MODEL ML_REGISTRY.AD_PROPENSITY_MODEL;
```

---

## Teardown

```sql
ALTER TASK generate_events_task SUSPEND;
ALTER TASK weekly_retrain_task SUSPEND;
DROP SCHEMA DEMO_ATAHIR.AD_STREAMS CASCADE;
DROP SCHEMA DEMO_ATAHIR.ML_REGISTRY CASCADE;
DROP SCHEMA DEMO_ATAHIR.ML_EXPERIMENTS CASCADE;
DROP SCHEMA DEMO_ATAHIR.FEATURE_STORE CASCADE;
DROP WAREHOUSE AD_STREAMS_WH;
DROP WAREHOUSE AD_STREAMS_INTERACTIVE_WH;
```

---

## Technologies Used

| Snowflake Feature | What It Replaces | Status |
|---|---|---|
| **Datastream** | Apache Kafka | ⚠️ Private Preview |
| **Snowpipe Streaming** | Kafka Connect | GA |
| **Dynamic Tables** (incremental) | Kafka Streams | GA |
| **Custom Incrementalization** (`MERGE INTO SELF`) | KTable window aggregations | GA |
| **Interactive Tables** + Interactive Warehouse | Apache Cassandra | GA |
| **Model Registry** + ML Experiments | MLflow + custom serving | GA |
| **Postgres Online Feature Store** | Redis / Feast online store | GA |
| **App Runtime** (Next.js) | Flask / custom deployment | GA |
| **Cortex AI** (`AI_COMPLETE`) | External LLM API + hosting | GA |

---

## Project Structure

```
├── README.md                    ← You are here
├── BLOG.md                      ← Substack blog post (narrative version)
├── ad_streams_blog_images/      ← Diagrams and visuals
├── sql/
│   ├── 00_setup.sql             ← Database, schema, warehouse creation
│   ├── 02_bronze.sql            ← Raw event tables with change tracking
│   ├── 03_simulator.sql         ← Event generation task
│   ├── 04_dynamic_tables.sql    ← Incremental pipeline (features + scoring)
│   ├── 05_serving.sql           ← Interactive Table + Interactive Warehouse
│   ├── 06_feature_store.py      ← Feature store entity/view definitions
│   ├── 07_campaign_dts.sql      ← Campaign analytics Dynamic Tables
│   ├── 08_mlops.sql             ← Model monitor, retrain task, inference log
│   └── 09_online_feature_store.py ← Postgres online serving setup
├── notebooks/
│   ├── propensity_model_dev.ipynb  ← Model experimentation notebook
│   └── _run_experiment_local.py    ← CLI runner for experiments
├── app/                         ← Next.js dashboard (App Runtime)
│   ├── app.yml                  ← Snowflake App Runtime config
│   ├── snowflake.yml            ← Snow CLI config
│   └── src/                     ← React components and API routes
├── scripts/
│   └── 01_datastream_setup.sh   ← Datastream broker + topic creation
└── ad-streams-architecture.drawio ← Architecture diagram (draw.io)
```

---

## Blog Post

The narrative version of this project is in [BLOG.md](BLOG.md) — a Substack article titled *"Roads? Where We're Going, We Don't Need Roads"* that tells the story of rebuilding a 2017 ad-tech pipeline in under an hour on Snowflake.

---

## License

MIT
