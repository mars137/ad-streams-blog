-- =============================================================================
-- Ad-Streams on Snowflake: Dynamic Table Pipeline
-- Silver (dedup + enrich) → Gold (features + propensity scoring)
-- Uses: multi-table pipeline, custom incrementalization, MERGE INTO SELF
-- =============================================================================

USE SCHEMA DEMO_ATAHIR.AD_STREAMS;

-- ─────────────────────────────────────────────────────────────────────────────
-- SILVER LAYER
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Unified event stream: union marketing + conversion, SCD Type 1 dedup
CREATE OR REPLACE DYNAMIC TABLE dt_events_unified
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = AD_STREAMS_WH
    REFRESH_MODE = INCREMENTAL
AS
SELECT
    event_id,
    user_id,
    event_type,
    campaign_id,
    event_ts,
    NULL::FLOAT AS conversion_value
FROM raw_marketing_events

UNION ALL

SELECT
    event_id,
    user_id,
    event_type,
    campaign_id,
    event_ts,
    conversion_value
FROM raw_conversion_events

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY event_ts DESC
) = 1;


-- 2. Enriched events: stream-static join with dim_campaigns
--    Custom incremental: INSERT INTO SELF from CHANGES() + LEFT JOIN dim
CREATE OR REPLACE DYNAMIC TABLE dt_events_enriched (
    event_id        VARCHAR(36),
    user_id         VARCHAR(20),
    event_type      VARCHAR(2),
    campaign_id     VARCHAR(20),
    campaign_name   VARCHAR(100),
    channel         VARCHAR(20),
    event_ts        TIMESTAMP_NTZ,
    conversion_value FLOAT
)
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = AD_STREAMS_WH
    REFRESH USING (
        INSERT INTO SELF
        SELECT
            e.event_id,
            e.user_id,
            e.event_type,
            e.campaign_id,
            c.campaign_name,
            c.channel,
            e.event_ts,
            e.conversion_value
        FROM dt_events_unified CHANGES(INFORMATION => APPEND_ONLY) AS e
        LEFT JOIN dim_campaigns AS c ON e.campaign_id = c.campaign_id
    );


-- ─────────────────────────────────────────────────────────────────────────────
-- GOLD LAYER: Feature Engineering
-- Pattern: MERGE INTO SELF — accumulates rolling window counts per user
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE DYNAMIC TABLE dt_user_features (
    user_id                         VARCHAR(20),
    clicks_1h                       INT,
    clicks_24h                      INT,
    clicks_7d                       INT,
    impressions_1h                  INT,
    impressions_24h                 INT,
    impressions_7d                  INT,
    paid_search_1h                  INT,
    paid_search_24h                 INT,
    paid_search_7d                  INT,
    conversions_total               INT,
    last_conversion_ts              TIMESTAMP_NTZ,
    time_since_last_conversion_hrs  FLOAT,
    event_velocity_24h              FLOAT,
    last_updated                    TIMESTAMP_NTZ
)
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = AD_STREAMS_WH
    REFRESH USING (
        MERGE INTO SELF AS tgt
        USING (
            SELECT
                user_id,
                -- Window: 1 hour
                SUM(CASE WHEN event_type = 'CL' AND event_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP()) THEN 1 ELSE 0 END) AS new_clicks_1h,
                SUM(CASE WHEN event_type = 'IM' AND event_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP()) THEN 1 ELSE 0 END) AS new_impr_1h,
                SUM(CASE WHEN event_type = 'PS' AND event_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP()) THEN 1 ELSE 0 END) AS new_ps_1h,
                -- Window: 24 hours
                SUM(CASE WHEN event_type = 'CL' AND event_ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN 1 ELSE 0 END) AS new_clicks_24h,
                SUM(CASE WHEN event_type = 'IM' AND event_ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN 1 ELSE 0 END) AS new_impr_24h,
                SUM(CASE WHEN event_type = 'PS' AND event_ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN 1 ELSE 0 END) AS new_ps_24h,
                -- Window: 7 days (accumulating)
                SUM(CASE WHEN event_type = 'CL' THEN 1 ELSE 0 END) AS new_clicks_7d,
                SUM(CASE WHEN event_type = 'IM' THEN 1 ELSE 0 END) AS new_impr_7d,
                SUM(CASE WHEN event_type = 'PS' THEN 1 ELSE 0 END) AS new_ps_7d,
                -- Conversions
                SUM(CASE WHEN event_type = 'CN' THEN 1 ELSE 0 END) AS new_conversions,
                MAX(CASE WHEN event_type = 'CN' THEN event_ts END) AS latest_conversion_ts,
                -- Velocity: events per hour in this batch
                COUNT(*) AS batch_event_count
            FROM dt_events_enriched CHANGES(INFORMATION => APPEND_ONLY)
            GROUP BY user_id
        ) AS src
        ON tgt.user_id = src.user_id
        WHEN MATCHED THEN UPDATE SET
            tgt.clicks_1h                      = src.new_clicks_1h,
            tgt.clicks_24h                     = src.new_clicks_24h,
            tgt.clicks_7d                      = tgt.clicks_7d + src.new_clicks_7d,
            tgt.impressions_1h                 = src.new_impr_1h,
            tgt.impressions_24h                = src.new_impr_24h,
            tgt.impressions_7d                 = tgt.impressions_7d + src.new_impr_7d,
            tgt.paid_search_1h                 = src.new_ps_1h,
            tgt.paid_search_24h                = src.new_ps_24h,
            tgt.paid_search_7d                 = tgt.paid_search_7d + src.new_ps_7d,
            tgt.conversions_total              = tgt.conversions_total + src.new_conversions,
            tgt.last_conversion_ts             = COALESCE(src.latest_conversion_ts, tgt.last_conversion_ts),
            tgt.time_since_last_conversion_hrs = DATEDIFF('hour',
                COALESCE(src.latest_conversion_ts, tgt.last_conversion_ts),
                CURRENT_TIMESTAMP()),
            tgt.event_velocity_24h             = src.batch_event_count / GREATEST(DATEDIFF('hour', tgt.last_updated, CURRENT_TIMESTAMP()), 1),
            tgt.last_updated                   = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            user_id, clicks_1h, clicks_24h, clicks_7d,
            impressions_1h, impressions_24h, impressions_7d,
            paid_search_1h, paid_search_24h, paid_search_7d,
            conversions_total, last_conversion_ts,
            time_since_last_conversion_hrs, event_velocity_24h, last_updated
        ) VALUES (
            src.user_id,
            src.new_clicks_1h, src.new_clicks_24h, src.new_clicks_7d,
            src.new_impr_1h, src.new_impr_24h, src.new_impr_7d,
            src.new_ps_1h, src.new_ps_24h, src.new_ps_7d,
            src.new_conversions, src.latest_conversion_ts,
            DATEDIFF('hour', COALESCE(src.latest_conversion_ts, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP()),
            src.batch_event_count,
            CURRENT_TIMESTAMP()
        )
    );


-- ─────────────────────────────────────────────────────────────────────────────
-- GOLD LAYER: Propensity Scoring
-- Calls the registered Model Registry model AD_PROPENSITY_MODEL (champion picked
-- via ML Experiments in notebooks/propensity_model_dev.ipynb).
-- The model method is IMMUTABLE, so this DT stays INCREMENTAL. We carry the
-- deterministic last_updated from upstream (NOT CURRENT_TIMESTAMP) to avoid
-- forcing a FULL refresh.
-- Promote a new model version by flipping the registry default — no DDL change here.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE DYNAMIC TABLE dt_user_propensity
    TARGET_LAG = '1 minute'
    WAREHOUSE = AD_STREAMS_WH
AS
WITH scored AS (
    SELECT
        f.user_id,
        f.last_updated,
        DEMO_ATAHIR.ML_REGISTRY.AD_PROPENSITY_MODEL!PREDICT_PROBA(
            f.clicks_1h, f.clicks_24h, f.clicks_7d,
            f.impressions_1h, f.impressions_24h, f.impressions_7d,
            f.paid_search_1h, f.paid_search_24h, f.paid_search_7d,
            f.event_velocity_24h
        ):output_feature_1::FLOAT AS propensity_score
    FROM dt_user_features f
)
SELECT
    user_id,
    propensity_score,
    CASE WHEN propensity_score >= 0.7 THEN 'HIGH'
         WHEN propensity_score >= 0.4 THEN 'MED'
         ELSE 'LOW' END AS score_bucket,
    last_updated
FROM scored;
