-- =============================================================================
-- Ad-Streams on Snowflake: Campaign Analytics Dynamic Tables
-- Powers: Campaign Performance, Attribution, Reach & Frequency pages
-- =============================================================================

USE SCHEMA DEMO_ATAHIR.AD_STREAMS;

-- Campaign Performance (hourly KPIs per campaign/channel)
CREATE OR REPLACE DYNAMIC TABLE dt_campaign_performance
  TARGET_LAG = '1 minute'
  WAREHOUSE = AD_STREAMS_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT
  campaign_id, campaign_name, channel,
  DATE_TRUNC('hour', event_ts) AS hour_ts,
  COUNT(*) AS total_events,
  SUM(CASE WHEN event_type = 'IM' THEN 1 ELSE 0 END) AS impressions,
  SUM(CASE WHEN event_type = 'CL' THEN 1 ELSE 0 END) AS clicks,
  SUM(CASE WHEN event_type = 'CN' THEN 1 ELSE 0 END) AS conversions,
  SUM(CASE WHEN event_type = 'CN' THEN conversion_value ELSE 0 END) AS revenue,
  COUNT(DISTINCT user_id) AS unique_users
FROM dt_events_enriched
GROUP BY 1, 2, 3, 4;

-- Attribution (user journey paths for credit assignment)
CREATE OR REPLACE DYNAMIC TABLE dt_attribution
  TARGET_LAG = '1 minute'
  WAREHOUSE = AD_STREAMS_WH
  REFRESH_MODE = INCREMENTAL
AS
WITH user_journeys AS (
  SELECT user_id,
    ARRAY_AGG(OBJECT_CONSTRUCT('channel', channel, 'campaign', campaign_name,
              'event_type', event_type, 'ts', event_ts::VARCHAR))
      WITHIN GROUP (ORDER BY event_ts) AS touchpoints,
    COUNT(*) AS touchpoint_count,
    MAX(CASE WHEN event_type = 'CN' THEN 1 ELSE 0 END) AS converted,
    SUM(CASE WHEN event_type = 'CN' THEN conversion_value ELSE 0 END) AS conversion_value
  FROM dt_events_enriched
  GROUP BY user_id
)
SELECT * FROM user_journeys WHERE converted = 1;

-- Reach & Frequency (per-user exposure counts)
CREATE OR REPLACE DYNAMIC TABLE dt_reach_frequency
  TARGET_LAG = '1 minute'
  WAREHOUSE = AD_STREAMS_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT
  campaign_id, campaign_name, channel, user_id,
  COUNT(*) AS frequency,
  MIN(event_ts) AS first_touch,
  MAX(event_ts) AS last_touch,
  COUNT(DISTINCT DATE_TRUNC('hour', event_ts)) AS active_hours
FROM dt_events_enriched
WHERE event_type IN ('CL', 'IM')
GROUP BY 1, 2, 3, 4;

-- Interactive Table for dashboard queries (sub-second via IW)
CREATE OR REPLACE INTERACTIVE TABLE campaign_dashboard
  CLUSTER BY (channel, campaign_id)
  TARGET_LAG = '1 minute'
  WAREHOUSE = AD_STREAMS_WH
AS SELECT * FROM dt_campaign_performance;

ALTER WAREHOUSE AD_STREAMS_INTERACTIVE_WH ADD TABLES (DEMO_ATAHIR.AD_STREAMS.campaign_dashboard);
