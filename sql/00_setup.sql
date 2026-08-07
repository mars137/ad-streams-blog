-- =============================================================================
-- Ad-Streams on Snowflake: Setup
-- Database, schema, warehouse, and seed data
-- =============================================================================

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS DEMO_ATAHIR;
CREATE SCHEMA IF NOT EXISTS DEMO_ATAHIR.AD_STREAMS;
USE SCHEMA DEMO_ATAHIR.AD_STREAMS;

CREATE WAREHOUSE IF NOT EXISTS AD_STREAMS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE AD_STREAMS_WH;

-- Dimension: campaigns for stream-static join enrichment
CREATE OR REPLACE TABLE dim_campaigns (
    campaign_id     VARCHAR(20)     PRIMARY KEY,
    campaign_name   VARCHAR(100),
    channel         VARCHAR(20),
    advertiser      VARCHAR(100),
    created_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO dim_campaigns VALUES
    ('C001', 'Spring Sale Display',  'display', 'Acme Corp', CURRENT_TIMESTAMP()),
    ('C002', 'Brand Search',         'search',  'Acme Corp', CURRENT_TIMESTAMP()),
    ('C003', 'Retargeting Social',   'social',  'Acme Corp', CURRENT_TIMESTAMP()),
    ('C004', 'Email Nurture',        'email',   'Acme Corp', CURRENT_TIMESTAMP()),
    ('C005', 'Video Pre-roll',       'display', 'Acme Corp', CURRENT_TIMESTAMP());
