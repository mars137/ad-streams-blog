-- =============================================================================
-- Ad-Streams on Snowflake: Datastream Pipes (topic -> table)
--
-- These pipes land Datastream topic messages into the bronze tables. They are
-- the Kafka-protocol ingestion path, and are independent of the Snowpipe
-- Streaming pipes in 02b_pipes.sql, which write to the same tables via the SDK.
-- Either path can run; both keep CHANGE_TRACKING intact for the downstream
-- Dynamic Tables.
--
-- IMPORTANT: a Datastream message is NOT the payload at top level. Each row the
-- DATA_SOURCE table function emits looks like:
--
--     { "data": <the message value>, "metadata": { topic, partition, offset, timestamp } }
--
-- So MATCH_BY_COLUMN_NAME does not work here: it would look for event_id at the
-- top level, where only `data` and `metadata` exist. The payload has to be
-- pulled out explicitly with PARSE_JSON($1:data):<field>.
--
-- START_FROM defaults to 'latest', meaning a new pipe ignores everything already
-- in the topic. 'earliest' is used below so the pipe picks up the retained
-- history and then transitions to live tailing.
--
-- Producing to a topic requires a running broker. On this account the local
-- broker exits shortly after registering, because the assigned GMS pod runs a
-- newer GS build than the installed client binary. See README for detail.
-- =============================================================================

USE SCHEMA DEMO_ATAHIR.AD_STREAMS;

CREATE OR REPLACE PIPE MARKETING_PIPE
AS COPY INTO raw_marketing_events
     (event_id, user_id, event_type, campaign_id, event_ts, properties)
FROM (
  SELECT PARSE_JSON($1:data):event_id::VARCHAR,
         PARSE_JSON($1:data):user_id::VARCHAR,
         PARSE_JSON($1:data):event_type::VARCHAR,
         PARSE_JSON($1:data):campaign_id::VARCHAR,
         PARSE_JSON($1:data):event_ts::TIMESTAMP_NTZ,
         PARSE_JSON($1:data):properties::VARIANT
  FROM TABLE(DATA_SOURCE(
    TYPE              => 'DATASTREAM',
    DATASTREAM_SYSTEM => 'AD_STREAMS_DS',
    TOPIC             => 'marketing_events',
    START_FROM        => 'earliest'
  ))
);

CREATE OR REPLACE PIPE CONVERSION_PIPE
AS COPY INTO raw_conversion_events
     (event_id, user_id, event_type, campaign_id, event_ts,
      conversion_value, properties)
FROM (
  SELECT PARSE_JSON($1:data):event_id::VARCHAR,
         PARSE_JSON($1:data):user_id::VARCHAR,
         PARSE_JSON($1:data):event_type::VARCHAR,
         PARSE_JSON($1:data):campaign_id::VARCHAR,
         PARSE_JSON($1:data):event_ts::TIMESTAMP_NTZ,
         PARSE_JSON($1:data):conversion_value::FLOAT,
         PARSE_JSON($1:data):properties::VARIANT
  FROM TABLE(DATA_SOURCE(
    TYPE              => 'DATASTREAM',
    DATASTREAM_SYSTEM => 'AD_STREAMS_DS',
    TOPIC             => 'conversion_events',
    START_FROM        => 'earliest'
  ))
);

-- Pipe health, backlog, and last error:
--   SELECT SYSTEM$PIPE_STATUS('MARKETING_PIPE');
SHOW PIPES LIKE '%_PIPE';
