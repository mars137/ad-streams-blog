-- =============================================================================
-- Ad-Streams on Snowflake: Snowpipe Streaming Pipes
--
-- Snowpipe Streaming (high-performance architecture) ingests rows through a
-- PIPE object rather than writing to the table directly. The pipe is the
-- server-side layer that handles schema validation and in-flight transforms.
--
-- The producer (scripts/02_streaming_producer.py) opens a channel against each
-- pipe and calls append_rows(). Offset tokens give exactly-once delivery, so a
-- restarted producer resumes from the last committed row instead of
-- duplicating it.
--
-- Bronze tables keep CHANGE_TRACKING = TRUE (see 02_bronze.sql). The pipes do
-- not change that, so the downstream CHANGES(INFORMATION => APPEND_ONLY) reads
-- in 04_dynamic_tables.sql stay incremental.
-- =============================================================================

USE SCHEMA DEMO_ATAHIR.AD_STREAMS;

-- Marketing events: impressions, clicks, paid search.
-- Explicit SELECT rather than MATCH_BY_COLUMN_NAME so event_ts lands as
-- TIMESTAMP_NTZ and properties lands as a real VARIANT.
CREATE OR REPLACE PIPE raw_marketing_events_pipe
AS COPY INTO raw_marketing_events
     (event_id, user_id, event_type, campaign_id, event_ts, properties)
  FROM (
    SELECT $1:event_id::VARCHAR, $1:user_id::VARCHAR, $1:event_type::VARCHAR,
           $1:campaign_id::VARCHAR, $1:event_ts::TIMESTAMP_NTZ,
           $1:properties::VARIANT
    FROM TABLE (DATA_SOURCE(TYPE => 'STREAMING'))
  );

-- Conversion events, which carry a revenue value.
CREATE OR REPLACE PIPE raw_conversion_events_pipe
AS COPY INTO raw_conversion_events
     (event_id, user_id, event_type, campaign_id, event_ts,
      conversion_value, properties)
  FROM (
    SELECT $1:event_id::VARCHAR, $1:user_id::VARCHAR, $1:event_type::VARCHAR,
           $1:campaign_id::VARCHAR, $1:event_ts::TIMESTAMP_NTZ,
           $1:conversion_value::FLOAT, $1:properties::VARIANT
    FROM TABLE (DATA_SOURCE(TYPE => 'STREAMING'))
  );

SHOW PIPES LIKE 'raw_%_pipe';
