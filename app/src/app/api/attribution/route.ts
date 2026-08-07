import { NextResponse } from "next/server";
import { querySnowflake } from "@/lib/snowflake";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const model = searchParams.get("model") || "linear";

  let sql: string;

  switch (model) {
    case "first_touch":
      sql = `
        SELECT GET_PATH(touchpoints[0], 'channel')::VARCHAR AS channel,
          COUNT(*) AS conversions,
          SUM(conversion_value) AS attributed_revenue
        FROM DEMO_ATAHIR.AD_STREAMS.dt_attribution
        GROUP BY 1 ORDER BY 3 DESC`;
      break;

    case "last_touch":
      sql = `
        SELECT GET_PATH(touchpoints[ARRAY_SIZE(touchpoints)-1], 'channel')::VARCHAR AS channel,
          COUNT(*) AS conversions,
          SUM(conversion_value) AS attributed_revenue
        FROM DEMO_ATAHIR.AD_STREAMS.dt_attribution
        GROUP BY 1 ORDER BY 3 DESC`;
      break;

    case "time_decay":
      sql = `
        WITH exploded AS (
          SELECT a.user_id, a.conversion_value, a.touchpoint_count,
            f.index AS touch_idx,
            GET_PATH(f.value, 'channel')::VARCHAR AS channel,
            GET_PATH(f.value, 'event_type')::VARCHAR AS event_type,
            -- Decay: more recent touchpoints get more weight
            EXP(-0.5 * (a.touchpoint_count - f.index - 1)) AS raw_weight
          FROM DEMO_ATAHIR.AD_STREAMS.dt_attribution a,
            LATERAL FLATTEN(a.touchpoints) f
          WHERE GET_PATH(f.value, 'event_type')::VARCHAR != 'CN'
        ),
        normalized AS (
          SELECT *, raw_weight / SUM(raw_weight) OVER (PARTITION BY user_id) AS weight
          FROM exploded
        )
        SELECT channel,
          COUNT(DISTINCT user_id) AS conversions,
          SUM(conversion_value * weight) AS attributed_revenue
        FROM normalized
        GROUP BY 1 ORDER BY 3 DESC`;
      break;

    default: // linear
      sql = `
        WITH exploded AS (
          SELECT a.user_id, a.conversion_value, a.touchpoint_count,
            GET_PATH(f.value, 'channel')::VARCHAR AS channel,
            GET_PATH(f.value, 'event_type')::VARCHAR AS event_type
          FROM DEMO_ATAHIR.AD_STREAMS.dt_attribution a,
            LATERAL FLATTEN(a.touchpoints) f
          WHERE GET_PATH(f.value, 'event_type')::VARCHAR != 'CN'
        )
        SELECT channel,
          COUNT(DISTINCT user_id) AS conversions,
          SUM(conversion_value / touchpoint_count) AS attributed_revenue
        FROM exploded
        GROUP BY 1 ORDER BY 3 DESC`;
  }

  const rows = await querySnowflake(sql);
  const total = rows.reduce((s: number, r: any) => s + (r.ATTRIBUTED_REVENUE || 0), 0);
  const withPct = rows.map((r: any) => ({
    ...r,
    CREDIT_PCT: total > 0 ? ((r.ATTRIBUTED_REVENUE / total) * 100).toFixed(1) : "0",
  }));

  return NextResponse.json(withPct);
}
