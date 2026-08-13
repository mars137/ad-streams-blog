#!/usr/bin/env python3
"""
Ad-Streams on Snowflake: Snowpipe Streaming producer.

Streams synthetic ad events into the bronze tables through the pipes created by
sql/02b_pipes.sql, using the Snowpipe Streaming high-performance Python SDK.

    pip install snowpipe-streaming
    python scripts/02_streaming_producer.py --duration 300

Auth uses key-pair (the SDK does not accept passwords). Point PRIVATE_KEY_PATH
at a PKCS#8 key whose public half is registered on the user:

    ALTER USER <you> SET RSA_PUBLIC_KEY_2 = '<public key body>';

Design notes, following the SDK's documented best practices:

  * One client per pipe. A client is bound to a single pipe.
  * Long-lived channels with deterministic names, so a restart reattaches to
    the same channel rather than leaking new ones.
  * A monotonic offset token per channel. On restart we read the last committed
    token back from Snowflake and skip anything already durable, which is what
    makes delivery exactly-once instead of at-least-once.
  * properties is passed as a native dict. The SDK stores a serialized JSON
    string as a literal string, so passing a str here would put quoted JSON in
    the VARIANT column instead of an object.
  * Backoff on throttling and channel reopen on invalidation.
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from snowflake.ingest.streaming import (
    StreamingIngestClient,
    StreamingIngestError,
)

DATABASE = os.environ.get("AD_STREAMS_DB", "DEMO_ATAHIR")
SCHEMA = os.environ.get("AD_STREAMS_SCHEMA", "AD_STREAMS")
ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "SFSENORTHAMERICA-DEMO_ATAHIR")
USER = os.environ.get("SNOWFLAKE_USER", "ATAHIR")
PRIVATE_KEY_PATH = os.environ.get(
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    str(Path.home() / ".snowflake" / "keys" / "ad_streams_rsa_key.p8"),
)

MARKETING_PIPE = "RAW_MARKETING_EVENTS_PIPE"
CONVERSION_PIPE = "RAW_CONVERSION_EVENTS_PIPE"

# Deterministic channel names. Reusing these on restart is what lets the SDK
# hand us back the last committed offset instead of starting over.
MARKETING_CHANNEL = "ad-streams-marketing-p0"
CONVERSION_CHANNEL = "ad-streams-conversion-p0"

CAMPAIGNS = [f"CAMP_{i:03d}" for i in range(1, 21)]
CHANNELS = ["search", "social", "display", "video", "email"]
# Clicks and impressions dominate real ad traffic; conversions are rare.
MARKETING_TYPES = ["IM"] * 70 + ["CL"] * 25 + ["PS"] * 5

_shutdown = False


def _handle_signal(signum, _frame):
    """Flip the shutdown flag so the main loop can close channels cleanly."""
    global _shutdown
    _shutdown = True
    print(f"\nSignal {signum} received, draining and closing channels...")


def load_profile() -> str:
    """
    Write the SDK profile to a file and return its path.

    profile_json takes a path, not a JSON string. Passing the JSON inline fails
    with "File name too long", and the error text echoes the private key, so
    keep the key on disk and hand over the path.
    """
    key_path = Path(PRIVATE_KEY_PATH)
    if not key_path.exists():
        sys.exit(
            f"Private key not found at {key_path}.\n"
            "Generate one and register the public half:\n"
            "  openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM "
            f"-out {key_path} -nocrypt\n"
            "  ALTER USER <you> SET RSA_PUBLIC_KEY_2 = '<public key body>';"
        )

    profile_path = key_path.parent / "ad_streams_profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "account": ACCOUNT,
                "user": USER,
                "url": f"https://{ACCOUNT.lower()}.snowflakecomputing.com:443",
                "private_key": key_path.read_text(),
            }
        )
    )
    profile_path.chmod(0o600)
    return str(profile_path)


def make_marketing_event(user_id: str, now: datetime) -> dict:
    event_type = random.choice(MARKETING_TYPES)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "campaign_id": random.choice(CAMPAIGNS),
        # Spread backwards a little so the 1h/24h/7d windows are not degenerate.
        "event_ts": (now - timedelta(seconds=random.randint(0, 300))).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ),
        # Native dict, not a JSON string: the SDK maps this to a real VARIANT.
        "properties": {
            "channel": random.choice(CHANNELS),
            "device": random.choice(["mobile", "desktop", "tablet"]),
        },
    }


def make_conversion_event(user_id: str, now: datetime) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": "CN",
        "campaign_id": random.choice(CAMPAIGNS),
        "event_ts": now.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "conversion_value": round(random.uniform(10.0, 500.0), 2),
        "properties": {"order_type": random.choice(["new", "repeat"])},
    }


def open_with_resume(client: StreamingIngestClient, channel_name: str):
    """
    Open a channel and return it with the next offset to use.

    The channel status carries the last offset Snowflake durably committed. We
    resume from that number rather than from zero, so rows already persisted are
    not re-sent after a crash.
    """
    channel, status = client.open_channel(channel_name)
    last = status.latest_committed_offset_token
    next_offset = int(last) + 1 if last is not None else 0
    if last is not None:
        print(f"  {channel_name}: resuming after committed offset {last}")
    else:
        print(f"  {channel_name}: new channel, starting at offset 0")
    return channel, next_offset


def append_with_retry(channel, row: dict, offset: int, channel_name: str,
                      client: StreamingIngestClient, max_attempts: int = 5):
    """
    Append one row, retrying transient failures with exponential backoff.

    Returns the channel, which may be a fresh object if we had to reopen after
    an invalidation.
    """
    delay = 0.5
    for attempt in range(1, max_attempts + 1):
        try:
            channel.append_row(row, str(offset))
            return channel
        except StreamingIngestError as exc:
            code = getattr(exc, "code", "")
            text = f"{code} {exc}"
            # Channel invalidated: another client claimed it, or it expired.
            # Reopen and let the caller continue from the committed offset.
            if "409" in text or "INVALID" in text.upper():
                print(f"  {channel_name}: channel invalidated, reopening")
                channel, _ = open_with_resume(client, channel_name)
                return channel
            # Throttling or a server blip: back off rather than hammering.
            if attempt < max_attempts:
                print(f"  {channel_name}: {text} (attempt {attempt}), "
                      f"retrying in {delay:.1f}s")
                time.sleep(delay)
                delay *= 2
                continue
            raise
    return channel


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stream synthetic ad events via Snowpipe Streaming."
    )
    parser.add_argument("--duration", type=int, default=60,
                        help="Seconds to run (0 runs until interrupted).")
    parser.add_argument("--rate", type=int, default=50,
                        help="Marketing events per second.")
    parser.add_argument("--users", type=int, default=1000,
                        help="Size of the synthetic user pool.")
    parser.add_argument("--conversion-rate", type=float, default=0.02,
                        help="Fraction of events that also emit a conversion.")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    profile = load_profile()
    user_pool = [f"USER_{i:06d}" for i in range(args.users)]

    print(f"Connecting to {DATABASE}.{SCHEMA} as {USER}")
    # One client per pipe: a client is bound to exactly one pipe.
    mkt_client = StreamingIngestClient(
        "ad-streams-marketing", DATABASE, SCHEMA, MARKETING_PIPE,
        profile_json=profile,
    )
    cnv_client = StreamingIngestClient(
        "ad-streams-conversion", DATABASE, SCHEMA, CONVERSION_PIPE,
        profile_json=profile,
    )

    mkt_channel, mkt_offset = open_with_resume(mkt_client, MARKETING_CHANNEL)
    cnv_channel, cnv_offset = open_with_resume(cnv_client, CONVERSION_CHANNEL)

    started = time.time()
    mkt_sent = cnv_sent = 0
    last_report = started

    try:
        while not _shutdown:
            if args.duration and (time.time() - started) >= args.duration:
                break
            tick = time.time()
            now = datetime.now(timezone.utc).replace(tzinfo=None)

            for _ in range(args.rate):
                user_id = random.choice(user_pool)
                mkt_channel = append_with_retry(
                    mkt_channel, make_marketing_event(user_id, now),
                    mkt_offset, MARKETING_CHANNEL, mkt_client,
                )
                mkt_offset += 1
                mkt_sent += 1

                if random.random() < args.conversion_rate:
                    cnv_channel = append_with_retry(
                        cnv_channel, make_conversion_event(user_id, now),
                        cnv_offset, CONVERSION_CHANNEL, cnv_client,
                    )
                    cnv_offset += 1
                    cnv_sent += 1

            # Report channel health, not just what we sent. A committed offset
            # that stops advancing, or a rising error count, is the early
            # signal that ingestion is broken.
            if time.time() - last_report >= 10:
                for name, ch in ((MARKETING_CHANNEL, mkt_channel),
                                 (CONVERSION_CHANNEL, cnv_channel)):
                    st = ch.get_channel_status()
                    print(f"  {name}: committed="
                          f"{st.latest_committed_offset_token} "
                          f"inserted={st.rows_inserted_count} "
                          f"errors={st.rows_error_count}"
                          + (f" lastError={st.last_error_message}"
                             if st.last_error_message else ""))
                last_report = time.time()

            # Pace to roughly --rate per second.
            elapsed = time.time() - tick
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
    finally:
        print(f"\nSent {mkt_sent} marketing + {cnv_sent} conversion events")
        for name, ch, cl in (
            (MARKETING_CHANNEL, mkt_channel, mkt_client),
            (CONVERSION_CHANNEL, cnv_channel, cnv_client),
        ):
            try:
                # Read status before closing: a closed channel rejects the call.
                st = ch.get_channel_status()
                print(f"  {name}: final committed offset "
                      f"{st.latest_committed_offset_token} "
                      f"({st.rows_inserted_count} rows inserted, "
                      f"{st.rows_error_count} errors)")
            except Exception as exc:  # noqa: BLE001 - best effort on shutdown
                print(f"  {name}: status unavailable ({exc})")
            try:
                ch.close()
            except Exception as exc:  # noqa: BLE001
                print(f"  {name}: close reported {exc}")
            try:
                cl.close()
            except Exception:  # noqa: BLE001
                pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
