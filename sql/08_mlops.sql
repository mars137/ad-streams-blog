-- =============================================================================
-- Ad-Streams on Snowflake: MLOps Layer
-- Operationalizes AD_PROPENSITY_MODEL (Model Registry) with:
--   1. An inference log (predictions joined to later-observed conversions)
--   2. A Model Monitor for drift + performance in Snowsight
--   3. A scheduled retraining task that re-runs the experiment notebook
-- =============================================================================

USE SCHEMA DEMO_ATAHIR.ML_REGISTRY;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Inference log: capture each scoring run's predictions alongside the
--    eventual ground-truth label. The monitor reads from this table.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS propensity_inference_log (
    user_id             VARCHAR(20),
    predicted_score     FLOAT,
    predicted_label     NUMBER(1,0),          -- 1 if score >= 0.5
    actual_converted    NUMBER(1,0),          -- observed from features (ground truth)
    scored_at           TIMESTAMP_NTZ,
    model_version       VARCHAR(20) DEFAULT 'V1'
);

-- Append the current scores joined to observed conversions.
-- In production this runs on a task; here it can be called ad hoc or scheduled.
CREATE OR REPLACE PROCEDURE log_propensity_inferences()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO DEMO_ATAHIR.ML_REGISTRY.propensity_inference_log
        (user_id, predicted_score, predicted_label, actual_converted, scored_at, model_version)
    SELECT
        p.user_id,
        p.propensity_score,
        IFF(p.propensity_score >= 0.5, 1, 0),
        IFF(f.conversions_total > 0, 1, 0),
        CURRENT_TIMESTAMP(),
        'V1'
    FROM DEMO_ATAHIR.AD_STREAMS.dt_user_propensity p
    JOIN DEMO_ATAHIR.AD_STREAMS.dt_user_features f ON p.user_id = f.user_id;
    RETURN 'Logged ' || SQLROWCOUNT || ' inference rows';
END;
$$;

-- Task: snapshot inferences every hour for monitoring
CREATE OR REPLACE TASK log_inferences_task
    WAREHOUSE = AD_STREAMS_WH
    SCHEDULE = '60 MINUTE'
AS
    CALL DEMO_ATAHIR.ML_REGISTRY.log_propensity_inferences();

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Model Monitor: drift + performance over the inference log.
--    Surfaces metrics in Snowsight (AI & ML » Models » Monitors).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MODEL MONITOR IF NOT EXISTS ad_propensity_monitor
    WITH
        MODEL = DEMO_ATAHIR.ML_REGISTRY.AD_PROPENSITY_MODEL
        VERSION = V1
        FUNCTION = 'PREDICT_PROBA'
        SOURCE = DEMO_ATAHIR.ML_REGISTRY.propensity_inference_log
        BASELINE = DEMO_ATAHIR.ML_REGISTRY.propensity_inference_log
        TIMESTAMP_COLUMN = scored_at
        PREDICTION_SCORE_COLUMNS = (predicted_score)
        PREDICTION_CLASS_COLUMNS = (predicted_label)
        ACTUAL_CLASS_COLUMNS = (actual_converted)
        ID_COLUMNS = (user_id)
        WAREHOUSE = AD_STREAMS_WH
        REFRESH_INTERVAL = '1 hour'
        AGGREGATION_WINDOW = '1 day';

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Scheduled retraining: re-run the experiment + register a new version
--    weekly. Promote by flipping the registry default (no pipeline DDL change):
--      ALTER MODEL AD_PROPENSITY_MODEL SET DEFAULT_VERSION = V2;
--    Requires the notebook propensity_model_dev to be created in Snowflake
--    (Workspaces) and referenced by name below.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TASK retrain_propensity_task
    WAREHOUSE = AD_STREAMS_WH
    SCHEDULE = 'USING CRON 0 6 * * 1 UTC'   -- Mondays 06:00 UTC
AS
    EXECUTE NOTEBOOK DEMO_ATAHIR.AD_STREAMS.propensity_model_dev();

-- Resume both tasks. The retraining task calls the notebook registered above,
-- so it needs propensity_model_dev to exist in AD_STREAMS (see README step 6).
ALTER TASK log_inferences_task RESUME;
ALTER TASK retrain_propensity_task RESUME;

-- =============================================================================
-- Promotion workflow (manual or CI/CD):
--   1. Retraining logs AD_PROPENSITY_MODEL V2 with fresh metrics
--   2. Compare V2 vs V1 metrics (SHOW VERSIONS IN MODEL ...)
--   3. ALTER MODEL AD_PROPENSITY_MODEL SET DEFAULT_VERSION = V2;
--   4. dt_user_propensity automatically scores with V2 on next refresh
-- =============================================================================
