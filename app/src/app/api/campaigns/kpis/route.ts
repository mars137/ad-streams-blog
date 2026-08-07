import { NextResponse } from "next/server";
import { queryInteractive } from "@/lib/snowflake";

export async function GET() {
  const rows = await queryInteractive(`
    SELECT
      SUM(impressions) AS total_impressions,
      SUM(clicks) AS total_clicks,
      SUM(conversions) AS total_conversions,
      SUM(revenue) AS total_revenue,
      COUNT(DISTINCT unique_users) AS total_reach,
      DIV0(SUM(clicks), NULLIF(SUM(impressions), 0)) AS ctr,
      DIV0(SUM(conversions), NULLIF(SUM(clicks), 0)) AS conversion_rate,
      DIV0(SUM(revenue), NULLIF(SUM(conversions), 0)) AS avg_order_value
    FROM campaign_dashboard
  `);
  return NextResponse.json(rows[0] || {});
}
