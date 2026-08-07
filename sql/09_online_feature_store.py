"""
Ad-Streams Online Feature Store (Postgres-backed) — real-time serving.

Provisions the Postgres online service, then registers two online feature views:
  - USER_FEATURES_ONLINE   : live rolling features (stream FV, <2s freshness)
  - USER_PROPENSITY_ONLINE : freshest model-produced score per user (batch online FV)

Served via the REST query API at ~10ms p50. The registered model
(AD_PROPENSITY_MODEL) remains the single source of truth; Postgres serves its
outputs for low-latency app reads.

NOTE: The Postgres online store is in preview ("do not use in production").
On DEMO_ATAHIR it previously timed out at 900s during cluster provisioning;
this script polls and reports clearly. Requires snowflake-ml-python >= 1.41.
"""
import sys
import time
from snowflake.snowpark import Session
from snowflake.ml.feature_store import (
    FeatureStore, Entity, CreationMode,
    FeatureView, Feature, StreamSource, StreamConfig,
)
from snowflake.ml.feature_store.feature_view import OnlineConfig, OnlineStoreType
from snowflake.ml.feature_store.spec.enums import FeatureAggregationMethod
from snowflake.snowpark.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, TimestampTimeZone,
)


def get_session():
    return Session.builder.configs({
        "connection_name": "demo_atahir",
        "database": "DEMO_ATAHIR", "schema": "AD_STREAMS", "warehouse": "AD_STREAMS_WH",
    }).create()


def provision(session):
    fs = FeatureStore(
        session=session, database="DEMO_ATAHIR", name="FEATURE_STORE",
        default_warehouse="AD_STREAMS_WH", creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
    )
    try:
        fs.register_entity(Entity(name="AD_USER", join_keys=["USER_ID"], desc="Ad-streams user"))
    except Exception as e:
        print(f"entity (may exist): {e}")

    print("Creating Postgres online service...", flush=True)
    try:
        fs.create_online_service("ACCOUNTADMIN", "ACCOUNTADMIN")
    except Exception as e:
        print(f"create_online_service returned: {e}", flush=True)

    # Poll up to ~18 min
    for i in range(36):
        status = session.sql(
            "SELECT SYSTEM$GET_FEATURE_STORE_ONLINE_SERVICE_STATUS('DEMO_ATAHIR.FEATURE_STORE')"
        ).collect()[0][0]
        print(f"[{i}] {status}", flush=True)
        if '"status":"RUNNING"' in status:
            return fs, True
        if '"status":"ERROR"' in status:
            return fs, False
        time.sleep(30)
    return fs, False


def register_online_views(fs, session):
    user_entity = fs.get_entity("AD_USER")

    # Batch online FV over the model-scored propensity DT
    prop_src = session.table("DEMO_ATAHIR.AD_STREAMS.DT_USER_PROPENSITY")
    propensity_fv = FeatureView(
        name="USER_PROPENSITY_ONLINE", entities=[user_entity],
        feature_df=prop_src, timestamp_col="LAST_UPDATED", refresh_freq="1 minute",
        online_config=OnlineConfig(enable=True, target_lag="10s", store_type=OnlineStoreType.POSTGRES),
        desc="Freshest model-produced propensity score per user (real-time serving)",
    )
    fs.register_feature_view(propensity_fv, version="V1", overwrite=True)
    print("Registered USER_PROPENSITY_ONLINE/V1", flush=True)

    # Stream FV for live rolling features
    event_stream = StreamSource(
        name="AD_EVENTS",
        schema=StructType([
            StructField("USER_ID", StringType()),
            StructField("EVENT_TS", TimestampType(TimestampTimeZone.NTZ)),
            StructField("EVENT_TYPE", StringType()),
            StructField("CAMPAIGN_ID", StringType()),
            StructField("CONVERSION_VALUE", DoubleType()),
        ]),
        desc="Real-time ad events",
    )
    try:
        fs.register_stream_source(event_stream)
    except Exception as e:
        print(f"stream source (may exist): {e}", flush=True)

    backfill = session.sql("""
        SELECT USER_ID, EVENT_TS, EVENT_TYPE, CAMPAIGN_ID, CONVERSION_VALUE
        FROM DEMO_ATAHIR.AD_STREAMS.DT_EVENTS_ENRICHED
    """)

    def passthrough(df):
        return df

    features = [
        Feature.count("EVENT_TYPE", "1h").alias("EVENTS_1H"),
        Feature.count("EVENT_TYPE", "24h").alias("EVENTS_24H"),
        Feature.sum("CONVERSION_VALUE", "24h").alias("CONVERSION_VALUE_24H"),
        Feature.max("EVENT_TS", "48h").alias("LATEST_EVENT_48H"),
    ]
    stream_fv = FeatureView(
        name="USER_FEATURES_ONLINE", entities=[user_entity],
        stream_config=StreamConfig(
            stream_source=event_stream, transformation_fn=passthrough, backfill_df=backfill
        ),
        timestamp_col="EVENT_TS", refresh_freq="1 minute", feature_granularity="1 minute",
        features=features,
        online_config=OnlineConfig(enable=True, target_lag="10s", store_type=OnlineStoreType.POSTGRES),
        feature_aggregation_method=FeatureAggregationMethod.CONTINUOUS,
        desc="Live rolling user features (<2s freshness)",
    )
    fs.register_feature_view(stream_fv, version="V1", overwrite=True)
    print("Registered USER_FEATURES_ONLINE/V1", flush=True)

    status = fs.get_online_service_status()
    print("Endpoints:", status.endpoints, flush=True)


def main():
    session = get_session()
    print("Account:", session.get_current_account(), flush=True)
    fs, running = provision(session)
    if not running:
        print("\nONLINE SERVICE NOT RUNNING — stopping. Batch + Interactive Table path unaffected.", flush=True)
        session.close()
        sys.exit(2)
    print("\nOnline service RUNNING. Registering online feature views...", flush=True)
    register_online_views(fs, session)
    print("\nReal-time Postgres serving ready.", flush=True)
    session.close()


if __name__ == "__main__":
    main()
