import { NextResponse } from "next/server";
import { querySnowflake } from "@/lib/snowflake";

interface Condition {
  field: string;
  op: string;
  value: string;
}

const ALLOWED_FIELDS = new Set([
  "score_bucket", "propensity_score", "clicks_1h", "clicks_24h", "clicks_7d",
  "impressions_1h", "impressions_24h", "conversions_total", "event_velocity_24h",
]);

const ALLOWED_OPS = new Set(["=", "!=", ">", ">=", "<", "<="]);

export async function POST(request: Request) {
  const { conditions } = await request.json() as { conditions: Condition[] };

  // Build WHERE clause with validation
  const whereClauses = conditions
    .filter((c) => ALLOWED_FIELDS.has(c.field) && ALLOWED_OPS.has(c.op))
    .map((c) => {
      const val = c.field === "score_bucket"
        ? `'${c.value.replace(/'/g, "''")}'`
        : parseFloat(c.value).toString();
      const col = c.field === "score_bucket" || c.field === "propensity_score"
        ? `p.${c.field}` : `f.${c.field}`;
      return `${col} ${c.op} ${val}`;
    });

  const whereStr = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

  const sql = `SELECT f.user_id, p.propensity_score, p.score_bucket, f.clicks_24h, f.conversions_total
FROM DEMO_ATAHIR.AD_STREAMS.dt_user_features f
JOIN DEMO_ATAHIR.AD_STREAMS.dt_user_propensity p ON f.user_id = p.user_id
${whereStr}
ORDER BY p.propensity_score DESC
LIMIT 50`;

  const countSql = `SELECT COUNT(*) AS cnt
FROM DEMO_ATAHIR.AD_STREAMS.dt_user_features f
JOIN DEMO_ATAHIR.AD_STREAMS.dt_user_propensity p ON f.user_id = p.user_id
${whereStr}`;

  const [users, countResult] = await Promise.all([
    querySnowflake(sql),
    querySnowflake(countSql),
  ]);

  const count = (countResult[0] as any)?.CNT || 0;

  return NextResponse.json({ count, users, sql });
}
