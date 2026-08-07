-- =============================================================================
-- Ad-Streams on Snowflake: Serving Layer
-- Interactive Table + Interactive Warehouse (replaces Hybrid Table + stream/task)
-- =============================================================================

USE SCHEMA DEMO_ATAHIR.AD_STREAMS;

-- Dynamic Interactive Table: auto-refreshes from dt_user_propensity
-- CLUSTER BY optimizes for dashboard queries (funnel by bucket, lookup by user_id)
CREATE OR REPLACE INTERACTIVE TABLE propensity_dashboard
  CLUSTER BY (score_bucket, user_id)
  TARGET_LAG = '1 minute'
  WAREHOUSE = AD_STREAMS_WH
AS
  SELECT user_id, propensity_score, score_bucket, last_updated
  FROM dt_user_propensity;

-- Interactive Warehouse: dedicated low-latency engine with data caching
-- 5s query timeout, sub-second for cached queries, auto-resume
CREATE OR REPLACE INTERACTIVE WAREHOUSE AD_STREAMS_INTERACTIVE_WH
  TABLES (DEMO_ATAHIR.AD_STREAMS.propensity_dashboard)
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_RESUME = TRUE;

ALTER WAREHOUSE AD_STREAMS_INTERACTIVE_WH RESUME;

-- Fallback: queries exceeding 5s are retried on standard warehouse
ALTER WAREHOUSE AD_STREAMS_INTERACTIVE_WH SET FALLBACK_WAREHOUSE = AD_STREAMS_WH;

-- =============================================================================
-- Usage:
--   USE WAREHOUSE AD_STREAMS_INTERACTIVE_WH;
--   SELECT * FROM propensity_dashboard WHERE user_id = 'U0015';  -- <100ms
--   SELECT score_bucket, COUNT(*) FROM propensity_dashboard GROUP BY score_bucket;
-- =============================================================================
