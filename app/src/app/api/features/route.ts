import { NextResponse } from "next/server";
import { queryFeatureStore } from "@/lib/snowflake";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const userId = searchParams.get("user_id");

  if (!userId) {
    return NextResponse.json({ error: "user_id required" }, { status: 400 });
  }

  // Real-time read from the Postgres Online Feature Store (10ms),
  // with automatic fallback to a SQL read of the feature DT.
  const row = await queryFeatureStore("USER_FEATURES_ONLINE", "V1", userId);
  return NextResponse.json(row);
}

