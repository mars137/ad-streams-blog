import { NextResponse } from "next/server";
import { querySnowflake } from "@/lib/snowflake";

export async function GET() {
  const rows = await querySnowflake(`
    SELECT campaign_name, channel,
      COUNT(DISTINCT user_id) AS reach,
      AVG(frequency) AS avg_frequency,
      SUM(CASE WHEN frequency = 1 THEN 1 ELSE 0 END) AS freq_1,
      SUM(CASE WHEN frequency BETWEEN 2 AND 3 THEN 1 ELSE 0 END) AS freq_2_3,
      SUM(CASE WHEN frequency BETWEEN 4 AND 5 THEN 1 ELSE 0 END) AS freq_4_5,
      SUM(CASE WHEN frequency >= 6 THEN 1 ELSE 0 END) AS freq_6plus
    FROM DEMO_ATAHIR.AD_STREAMS.dt_reach_frequency
    GROUP BY 1, 2
    ORDER BY reach DESC
  `);
  return NextResponse.json(rows);
}
