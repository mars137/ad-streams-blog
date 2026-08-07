import { NextResponse } from "next/server";
import { querySnowflake } from "@/lib/snowflake";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const userId = searchParams.get("user_id");
  const limit = searchParams.get("limit") || "20";

  try {
    let sql: string;
    if (userId) {
      sql = `SELECT * FROM DEMO_ATAHIR.AD_STREAMS.propensity_dashboard WHERE user_id = '${userId.replace(/'/g, "''")}'`;
    } else {
      sql = `SELECT * FROM DEMO_ATAHIR.AD_STREAMS.propensity_dashboard ORDER BY propensity_score DESC LIMIT ${parseInt(limit)}`;
    }

    const rows = await querySnowflake(sql);
    return NextResponse.json(rows);
  } catch (err: unknown) {
    // Fallback to dt_user_propensity if interactive table not accessible
    let sql: string;
    if (userId) {
      sql = `SELECT * FROM DEMO_ATAHIR.AD_STREAMS.dt_user_propensity WHERE user_id = '${userId!.replace(/'/g, "''")}'`;
    } else {
      sql = `SELECT * FROM DEMO_ATAHIR.AD_STREAMS.dt_user_propensity ORDER BY propensity_score DESC LIMIT ${parseInt(limit)}`;
    }
    const rows = await querySnowflake(sql);
    return NextResponse.json(rows);
  }
}
