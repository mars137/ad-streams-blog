#!/bin/bash
# =============================================================================
# Ad-Streams on Snowflake: Datastream Setup
# Creates Snowflake Datastream (Kafka-compatible) with two topics
# =============================================================================
set -e

echo "Creating Datastream..."
snow datastream create AD_STREAMS_DS \
    --database DEMO_ATAHIR \
    --schema AD_STREAMS

echo "Creating marketing_events topic..."
snow datastream topic create marketing_events \
    --datastream AD_STREAMS_DS \
    --partition-count 3

echo "Creating conversion_events topic..."
snow datastream topic create conversion_events \
    --datastream AD_STREAMS_DS \
    --partition-count 3

echo "Creating consumer group..."
# --single-broker-mode creates TYPE 'standalone', which is what a locally
# launched broker (snow datastream broker launch) attaches to.
snow datastream broker-group create AD_STREAMS_CONSUMER \
    --datastream AD_STREAMS_DS \
    --single-broker-mode

echo "Datastream setup complete. Topics:"
snow datastream topic list --datastream AD_STREAMS_DS
