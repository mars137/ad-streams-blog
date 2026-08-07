# Ad-Streams on Snowflake

Real-time digital marketing propensity scoring — entirely on Snowflake.

## Architecture

```
[Event Simulator]  →  [Datastream Topics]  →  [Snowpipe Streaming]
                                                       │
                                              ┌────────┴────────┐
                                              │  Bronze Tables   │  (raw_marketing_events, raw_conversion_events)
                                              └────────┬────────┘
                                                       │ CHANGE_TRACKING
                                              ┌────────┴────────┐
                                              │  Silver DTs      │  (dt_events_unified, dt_events_enriched)
                                              └────────┬────────┘
                                                       │ APPEND_ONLY CHANGES
                                              ┌────────┴────────┐
                                              │  Gold DTs        │  (dt_user_features → dt_user_propensity)
                                              └────────┬────────┘  TARGET_LAG = 30s
                                                       │ Stream on DT
                                              ┌────────┴────────┐
                                              │Interactive Table │  (user_propensity_serving)
                                              └────────┬────────┘
                                                       │
                                              ┌────────┴────────┐
                                              │  App Runtime     │  (Next.js dashboard)
                                              └─────────────────┘
```

## Quick Start

### 1. Setup
```sql
-- Run in Snowsight or CoCo
@sql/00_setup.sql
```

### 2. Datastream (optional — can skip for demo)
```bash
chmod +x scripts/01_datastream_setup.sh
./scripts/01_datastream_setup.sh
```

### 3. Bronze tables
```sql
@sql/02_bronze.sql
```

### 4. Event simulator
```sql
@sql/03_simulator.sql
-- This creates a stored procedure + 1-min task that generates events
```

### 5. Dynamic Table pipeline
```sql
@sql/04_dynamic_tables.sql
-- Creates: dt_events_unified → dt_events_enriched → dt_user_features → dt_user_propensity
```

### 5. Dynamic Table pipeline
```sql
@sql/04_dynamic_tables.sql
-- Creates: dt_events_unified → dt_events_enriched → dt_user_features → dt_user_propensity
-- (dt_user_propensity calls the registered Model Registry model — run step 5b first)
```

### 5b. Train, experiment, and register the model
```
notebooks/propensity_model_dev.ipynb
-- Races 4 model families (LogReg, RandomForest, XGBoost, LightGBM) as ML Experiment
-- runs, registers the champion as ML_REGISTRY.AD_PROPENSITY_MODEL, sets PROD default.
```

### 6. Serving layer + campaign analytics
```sql
@sql/05_serving.sql        -- propensity_dashboard (Interactive Table) + Interactive Warehouse
@sql/07_campaign_dts.sql   -- campaign / attribution / reach-frequency DTs
```

### 6b. MLOps + real-time serving
```sql
@sql/08_mlops.sql          -- Model Monitor + inference log + scheduled retrain
```
```
sql/09_online_feature_store.py  -- Postgres Online Feature Store (10ms real-time reads)
```

### 7. App Runtime (optional)
```bash
cd app
snow app deploy
```

## Verify

```sql
-- Check DT pipeline is running
SHOW DYNAMIC TABLES IN SCHEMA DEMO_ATAHIR.AD_STREAMS;

-- Check propensity scores are being computed
SELECT * FROM dt_user_propensity ORDER BY propensity_score DESC LIMIT 10;

-- Check Interactive Table serving layer
SELECT * FROM user_propensity_serving WHERE user_id = 'U0001';

-- Check event flow
SELECT event_type, COUNT(*) FROM raw_marketing_events GROUP BY event_type;
```

## Teardown

```sql
ALTER TASK generate_events_task SUSPEND;
ALTER TASK sync_propensity_to_serving SUSPEND;
DROP SCHEMA DEMO_ATAHIR.AD_STREAMS CASCADE;
DROP WAREHOUSE AD_STREAMS_WH;
```

## Technologies Used

| Snowflake Feature | Replaces |
|---|---|
| Datastream | Apache Kafka |
| Snowpipe Streaming | Kafka Connect |
| Dynamic Tables (INCREMENTAL) | Kafka Streams |
| Custom Incrementalization (MERGE INTO SELF) | KTable aggregations |
| Interactive Table (Hybrid) | Apache Cassandra |
| App Runtime (Next.js) | Flask |
| Notebook + ML Experiments + Model Registry | External ML model + experiment tracking |
| Postgres Online Feature Store | Redis / online store |
