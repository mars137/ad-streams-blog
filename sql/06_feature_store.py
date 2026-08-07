"""
Ad-Streams Feature Store Setup
================================
Creates the Online Feature Store with:
- Postgres-backed online service (10ms p50 reads)
- Stream Source for real-time event ingestion
- Stream Feature View with continuous aggregation (rolling windows, <2s freshness)
- Real-time Feature View for on-demand propensity scoring
- Feature Group bundling everything for unified serving

Requires: snowflake-ml-python >= 1.41
"""
import time
from snowflake.snowpark import Session
from snowflake.ml.feature_store import (
    FeatureStore,
    FeatureView,
    Entity,
    Feature,
    FeatureGroup,
    CreationMode,
    StreamSource,
    StreamConfig,
)
from snowflake.ml.feature_store.feature_view import OnlineConfig, OnlineStoreType
from snowflake.ml.feature_store.spec.enums import FeatureAggregationMethod
from snowflake.snowpark.types import (
    StructType, StructField, StringType, FloatType,
    TimestampType, TimestampTimeZone, DoubleType,
)


def get_session():
    """Create Snowpark session from connection config."""
    return Session.builder.configs({
        "connection_name": "demo_atahir",
        "database": "DEMO_ATAHIR",
        "schema": "AD_STREAMS",
        "warehouse": "AD_STREAMS_WH",
    }).create()


def setup_feature_store(session: Session):
    """Initialize Feature Store and register entity."""
    fs = FeatureStore(
        session=session,
        database="DEMO_ATAHIR",
        name="FEATURE_STORE",
        default_warehouse="AD_STREAMS_WH",
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
    )

    user_entity = Entity(
        name="AD_USER",
        join_keys=["USER_ID"],
        desc="Ad-streams marketing user",
    )
    fs.register_entity(user_entity)
    print("Entity AD_USER registered.")
    return fs, user_entity


def create_online_service(fs: FeatureStore):
    """Provision Postgres-backed online service."""
    print("Creating online service (this takes a few minutes)...")
    fs.create_online_service("ACCOUNTADMIN", "ACCOUNTADMIN")

    status = fs.get_online_service_status()
    while status.status != "RUNNING":
        print(f"  Status: {status.status} - waiting...")
        time.sleep(30)
        status = fs.get_online_service_status()

    print(f"Online service RUNNING. Endpoints: {status.endpoints}")
    return status


def register_stream_source(fs: FeatureStore):
    """Register the event stream source schema."""
    event_stream = StreamSource(
        name="AD_EVENTS",
        schema=StructType([
            StructField("USER_ID", StringType()),
            StructField("EVENT_TS", TimestampType(TimestampTimeZone.NTZ)),
            StructField("EVENT_TYPE", StringType()),
            StructField("CAMPAIGN_ID", StringType()),
            StructField("CONVERSION_VALUE", FloatType()),
        ]),
        desc="Real-time ad marketing and conversion events",
    )
    fs.register_stream_source(event_stream)
    print("StreamSource AD_EVENTS registered.")
    return event_stream


def create_stream_feature_view(fs, session, user_entity, event_stream):
    """Create Stream Feature View with continuous aggregation."""
    # Backfill from existing enriched events
    backfill_df = session.sql("""
        SELECT USER_ID, EVENT_TS, EVENT_TYPE, CAMPAIGN_ID, CONVERSION_VALUE
        FROM DEMO_ATAHIR.AD_STREAMS.DT_EVENTS_ENRICHED
    """)

    stream_cfg = StreamConfig(
        stream_source=event_stream,
        backfill_df=backfill_df,
    )

    features = [
        Feature.sum("CASE WHEN EVENT_TYPE='CL' THEN 1 ELSE 0 END", "1h").alias("CLICKS_1H"),
        Feature.sum("CASE WHEN EVENT_TYPE='CL' THEN 1 ELSE 0 END", "24h").alias("CLICKS_24H"),
        Feature.sum("CASE WHEN EVENT_TYPE='CL' THEN 1 ELSE 0 END", "7d").alias("CLICKS_7D"),
        Feature.sum("CASE WHEN EVENT_TYPE='IM' THEN 1 ELSE 0 END", "1h").alias("IMPRESSIONS_1H"),
        Feature.sum("CASE WHEN EVENT_TYPE='IM' THEN 1 ELSE 0 END", "24h").alias("IMPRESSIONS_24H"),
        Feature.sum("CASE WHEN EVENT_TYPE='PS' THEN 1 ELSE 0 END", "1h").alias("PAID_SEARCH_1H"),
        Feature.sum("CASE WHEN EVENT_TYPE='PS' THEN 1 ELSE 0 END", "24h").alias("PAID_SEARCH_24H"),
        Feature.sum("CASE WHEN EVENT_TYPE='CN' THEN 1 ELSE 0 END", "7d").alias("CONVERSIONS_7D"),
        Feature.count("EVENT_TYPE", "24h").alias("EVENT_COUNT_24H"),
    ]

    user_features_fv = FeatureView(
        name="USER_BEHAVIOR_WINDOWED",
        entities=[user_entity],
        stream_config=stream_cfg,
        timestamp_col="EVENT_TS",
        refresh_freq="1 minute",
        feature_granularity="1 minute",
        features=features,
        online_config=OnlineConfig(
            enable=True,
            target_lag="10s",
            store_type=OnlineStoreType.POSTGRES,
        ),
        feature_aggregation_method=FeatureAggregationMethod.CONTINUOUS,
        desc="Real-time rolling window features with continuous aggregation",
    )

    registered_fv = fs.register_feature_view(user_features_fv, version="V1", overwrite=True)
    print("Stream Feature View USER_BEHAVIOR_WINDOWED V1 registered.")
    return registered_fv


def create_realtime_propensity_fv(fs, user_entity, registered_stream_fv):
    """Create Real-time Feature View for on-demand propensity scoring."""
    from snowflake.ml.feature_store import RealtimeConfig, RequestSource
    import pandas as pd

    def compute_propensity(request_df: pd.DataFrame, features_df: pd.DataFrame) -> pd.DataFrame:
        """Logistic regression: P = 1 / (1 + exp(-z))"""
        import numpy as np
        z = (
            -2.1
            + 0.8  * features_df["CLICKS_1H"].fillna(0)
            + 0.4  * features_df["CLICKS_24H"].fillna(0)
            + 0.1  * features_df["CLICKS_7D"].fillna(0)
            + 0.3  * features_df["IMPRESSIONS_1H"].fillna(0)
            + 0.15 * features_df["IMPRESSIONS_24H"].fillna(0)
            + 0.2  * features_df["PAID_SEARCH_1H"].fillna(0)
            + 1.2  * features_df["PAID_SEARCH_24H"].fillna(0)
            + 2.5  * features_df["CONVERSIONS_7D"].fillna(0)
        )
        score = 1.0 / (1.0 + np.exp(-z))
        bucket = pd.Series(["LOW"] * len(score))
        bucket[score >= 0.4] = "MED"
        bucket[score >= 0.7] = "HIGH"
        return pd.DataFrame({"PROPENSITY_SCORE": score, "SCORE_BUCKET": bucket})

    request_source = RequestSource(
        schema=StructType([StructField("_DUMMY", DoubleType())]),
    )

    propensity_rtfv = FeatureView(
        name="USER_PROPENSITY_REALTIME",
        entities=[user_entity],
        realtime_config=RealtimeConfig(
            compute_fn=compute_propensity,
            sources=[request_source, registered_stream_fv],
            output_schema=StructType([
                StructField("PROPENSITY_SCORE", DoubleType()),
                StructField("SCORE_BUCKET", StringType()),
            ]),
        ),
        desc="On-demand propensity scoring from real-time features",
    )

    registered_propensity = fs.register_feature_view(propensity_rtfv, "V1")
    print("Real-time Feature View USER_PROPENSITY_REALTIME V1 registered.")
    return registered_propensity


def create_feature_group(fs, registered_stream_fv, registered_propensity_fv):
    """Bundle feature views into a Feature Group."""
    user_scoring_fg = FeatureGroup(
        name="USER_SCORING_FG",
        features=[registered_stream_fv, registered_propensity_fv],
        auto_prefix=False,
        desc="User behavior features + propensity score for serving and training",
    )
    registered_fg = fs.register_feature_group(user_scoring_fg, "V1")
    print("Feature Group USER_SCORING_FG V1 registered.")
    return registered_fg


def main():
    print("=" * 60)
    print("Ad-Streams Online Feature Store Setup")
    print("=" * 60)

    session = get_session()
    print(f"Connected: {session.get_current_account()}")

    # Step 3: Feature Store + Online Service
    fs, user_entity = setup_feature_store(session)
    create_online_service(fs)

    # Step 4: Stream Source + Stream Feature View
    event_stream = register_stream_source(fs)
    registered_fv = create_stream_feature_view(fs, session, user_entity, event_stream)

    # Step 5: Real-time Feature View
    registered_propensity = create_realtime_propensity_fv(fs, user_entity, registered_fv)

    # Step 6: Feature Group
    create_feature_group(fs, registered_fv, registered_propensity)

    print("\n" + "=" * 60)
    print("Setup complete! Feature Store is ready for serving.")
    print("=" * 60)

    session.close()


if __name__ == "__main__":
    main()
