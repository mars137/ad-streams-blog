-- =============================================================================
-- Ad-Streams on Snowflake: Bronze Layer
-- Raw landing tables with CHANGE_TRACKING for Dynamic Table CHANGES() clause
-- =============================================================================

USE SCHEMA DEMO_ATAHIR.AD_STREAMS;
USE WAREHOUSE AD_STREAMS_WH;

CREATE OR REPLACE TABLE raw_marketing_events (
    event_id        VARCHAR(36)     NOT NULL,
    user_id         VARCHAR(20)     NOT NULL,
    event_type      VARCHAR(2)      NOT NULL,   -- CL=click, IM=impression, PS=paid search
    campaign_id     VARCHAR(20),
    event_ts        TIMESTAMP_NTZ   NOT NULL,
    properties      VARIANT,
    _ingested_at    TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
    CHANGE_TRACKING = TRUE
    DATA_RETENTION_TIME_IN_DAYS = 7;

CREATE OR REPLACE TABLE raw_conversion_events (
    event_id        VARCHAR(36)     NOT NULL,
    user_id         VARCHAR(20)     NOT NULL,
    event_type      VARCHAR(2)      NOT NULL,   -- CN=conversion
    campaign_id     VARCHAR(20),
    event_ts        TIMESTAMP_NTZ   NOT NULL,
    conversion_value FLOAT,
    properties      VARIANT,
    _ingested_at    TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
    CHANGE_TRACKING = TRUE
    DATA_RETENTION_TIME_IN_DAYS = 7;
