-- =============================================================================
-- Ad-Streams on Snowflake: Event Simulator
-- Snowpark Python stored procedure + task to generate synthetic events.
--
-- Each user has a latent "affinity" q (derived deterministically from their id).
-- High-affinity users produce MORE clicks AND MORE conversions, so clicks are
-- genuinely predictive of conversion. Impressions are emitted at a FLAT rate for
-- everyone, so they carry NO affinity signal (neutral feature). This gives the ML
-- model a single clean signal to learn: clicks up -> propensity up.
--
-- spread_seconds controls how far back event timestamps are scattered:
--   * training backfill uses a 7-day spread (604800) so the 1h/24h/7d windows
--     are populated and not perfectly collinear;
--   * the continuous task uses a 5-min spread (300) so events look near-real-time.
-- =============================================================================

USE SCHEMA DEMO_ATAHIR.AD_STREAMS;

CREATE OR REPLACE PROCEDURE generate_events(n_events INT, n_users INT, spread_seconds INT DEFAULT 300)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import random
import uuid
from datetime import datetime, timedelta, timezone

def run(session, n_events: int, n_users: int, spread_seconds: int = 300) -> str:
    campaigns = ['C001', 'C002', 'C003', 'C004', 'C005']
    users = [f'U{i:04d}' for i in range(1, n_users + 1)]

    marketing_rows = []
    conversion_rows = []
    now = datetime.now(timezone.utc)

    for _ in range(n_events):
        user_id = random.choice(users)
        # Latent affinity in [0, 1), deterministic from the user number.
        q = (int(user_id[1:]) % 100) / 100.0

        r = random.random()
        if r < 0.35:
            # Impressions: FLAT 35% for everyone -> carries no affinity signal.
            event_type = 'IM'
        elif random.random() < 0.02 + 0.12 * q:
            # Conversion among non-impression events, scales strongly with q.
            event_type = 'CN'
        else:
            # Clicks scale with q; paid search is the remainder.
            event_type = 'CL' if random.random() < 0.35 + 0.55 * q else 'PS'

        campaign_id = random.choice(campaigns)
        event_ts = (now - timedelta(seconds=random.randint(0, spread_seconds))).strftime('%Y-%m-%d %H:%M:%S.%f')
        event_id = str(uuid.uuid4())

        if event_type == 'CN':
            conversion_rows.append(
                f"('{event_id}', '{user_id}', '{event_type}', '{campaign_id}', "
                f"'{event_ts}'::TIMESTAMP_NTZ, {round(random.uniform(10, 500), 2)}, NULL)"
            )
        else:
            marketing_rows.append(
                f"('{event_id}', '{user_id}', '{event_type}', '{campaign_id}', "
                f"'{event_ts}'::TIMESTAMP_NTZ, NULL)"
            )

    mkt_count = 0
    if marketing_rows:
        batch_size = 500
        for i in range(0, len(marketing_rows), batch_size):
            batch = marketing_rows[i:i+batch_size]
            values_str = ',\n'.join(batch)
            session.sql(f"""
                INSERT INTO raw_marketing_events
                    (event_id, user_id, event_type, campaign_id, event_ts, properties)
                VALUES {values_str}
            """).collect()
            mkt_count += len(batch)

    cn_count = 0
    if conversion_rows:
        batch_size = 500
        for i in range(0, len(conversion_rows), batch_size):
            batch = conversion_rows[i:i+batch_size]
            values_str = ',\n'.join(batch)
            session.sql(f"""
                INSERT INTO raw_conversion_events
                    (event_id, user_id, event_type, campaign_id, event_ts,
                     conversion_value, properties)
                VALUES {values_str}
            """).collect()
            cn_count += len(batch)

    return f"Generated {mkt_count} marketing + {cn_count} conversion events ({n_users} users, {spread_seconds}s spread)"
$$;

-- One-time training backfill (run a few times to build ~40K events). 7-day spread
-- populates the 1h/24h/7d windows so they are not perfectly collinear:
--   CALL generate_events(14000, 1000, 604800);

-- Continuous producer: 300 events across 1000 users every minute, 5-min spread so
-- events stay near-real-time. This is the reliable direct-INSERT path. (The
-- Datastream broker path is provisioned but blocked by a GMS preview gap on this
-- account -- the broker registers then exits ~75s on ACTION_TYPE_BROKER_HEALTHCHECK.)
CREATE OR REPLACE TASK generate_events_task
    WAREHOUSE = AD_STREAMS_WH
    SCHEDULE = '1 MINUTE'
    COMMENT = 'Continuous event producer (direct INSERT). Reliable fallback while Datastream GMS broker-healthcheck is a preview gap. Spread=300s keeps events near-real-time.'
AS
    CALL generate_events(300, 1000, 300);

ALTER TASK generate_events_task RESUME;
