import { NextResponse } from "next/server";
import { querySnowflake } from "@/lib/snowflake";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const userId = searchParams.get("user_id");
  const limit = searchParams.get("limit") || "50";

  let sql: string;
  if (userId) {
    sql = `SELECT event_id, user_id, event_type, campaign_name, channel, event_ts, conversion_value
           FROM dt_events_enriched
           WHERE user_id = '${userId.replace(/'/g, "''")}'
           ORDER BY event_ts DESC LIMIT ${parseInt(limit)}`;
  } else {
    sql = `SELECT event_id, user_id, event_type, campaign_name, channel, event_ts, conversion_value
           FROM dt_events_enriched
           ORDER BY event_ts DESC LIMIT ${parseInt(limit)}`;
  }

  const rows = await querySnowflake(sql);
  return NextResponse.json(rows);
}
