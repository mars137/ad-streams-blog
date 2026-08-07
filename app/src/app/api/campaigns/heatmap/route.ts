import { NextResponse } from "next/server";
import { queryInteractive } from "@/lib/snowflake";

export async function GET() {
  const rows = await queryInteractive(`
    SELECT hour_ts, channel, total_events
    FROM campaign_dashboard
    ORDER BY hour_ts, channel
  `);
  return NextResponse.json(rows);
}
