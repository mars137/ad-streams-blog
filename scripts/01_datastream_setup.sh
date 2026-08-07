#!/bin/bash
# =============================================================================
# Ad-Streams on Snowflake: Datastream Setup
# Creates Snowflake Datastream (Kafka-compatible) with two topics
# =============================================================================
set -e

echo "Creating Datastream..."
snow datastream create AD_STREAMS_DS \
    --database DEMO_ATAHIR \
    --schema AD_STREAMS \
    --comment "Ad-Streams propensity scoring demo"

echo "Creating marketing_events topic..."
snow datastream topic create marketing_events \
    --datastream AD_STREAMS_DS \
    --partitions 3 \
    --retention-days 1

echo "Creating conversion_events topic..."
snow datastream topic create conversion_events \
    --datastream AD_STREAMS_DS \
    --partitions 3 \
    --retention-days 1

echo "Creating consumer group..."
snow datastream broker-group create AD_STREAMS_CONSUMER \
    --datastream AD_STREAMS_DS

echo "Datastream setup complete. Topics:"
snow datastream topic list --datastream AD_STREAMS_DS
