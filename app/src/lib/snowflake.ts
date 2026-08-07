import { readFileSync, existsSync } from "fs";
import snowflake from "snowflake-sdk";

let connectionPromise: Promise<snowflake.Connection> | null = null;

function getToken(): string {
  const tokenPath = "/snowflake/session/token";
  if (existsSync(tokenPath)) {
    return readFileSync(tokenPath, "utf-8").trim();
  }
  return "";
}

function getConnection(): Promise<snowflake.Connection> {
  if (connectionPromise) return connectionPromise;

  const token = getToken();
  const host = process.env.SNOWFLAKE_HOST || "";
  const account = process.env.SNOWFLAKE_ACCOUNT || "";

  console.log(`[SF] Connecting: host=${host}, account=${account}, token_len=${token.length}`);
  console.log(`[SF] Env vars: ${Object.keys(process.env).filter(k => k.startsWith('SNOW')).join(', ')}`);

  connectionPromise = new Promise((resolve, reject) => {
    const opts: Record<string, unknown> = {
      authenticator: "OAUTH",
      token: token,
      database: "DEMO_ATAHIR",
      schema: "AD_STREAMS",
      warehouse: "AD_STREAMS_WH",
    };
    if (host) opts.accessUrl = `https://${host}`;
    if (account) opts.account = account;

    const conn = snowflake.createConnection(opts as any);
    conn.connect((err) => {
      if (err) {
        console.error("[SF] Connection failed:", err.message);
        connectionPromise = null;
        reject(err);
      } else {
        console.log("[SF] Connected successfully");
        resolve(conn);
      }
    });
  });

  return connectionPromise;
}

export async function querySnowflake<T = Record<string, unknown>>(
  sql: string
): Promise<T[]> {
  const conn = await getConnection();

  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText: sql,
      complete: (err, _stmt, rows) => {
        if (err) {
          console.error("[SF] Query error:", err.message, "| SQL:", sql.substring(0, 200));
          resolve([] as T[]); // Return empty rather than crash the page
        } else {
          console.log(`[SF] Query OK: ${(rows || []).length} rows | SQL: ${sql.substring(0, 80)}`);
          resolve((rows || []) as T[]);
        }
      },
    });
  });
}

export async function queryInteractive<T = Record<string, unknown>>(
  sql: string
): Promise<T[]> {
  await querySnowflake("USE WAREHOUSE AD_STREAMS_INTERACTIVE_WH");
  const result = await querySnowflake<T>(sql);
  await querySnowflake("USE WAREHOUSE AD_STREAMS_WH");
  return result;
}

/**
 * Real-time feature read from the Postgres-backed Online Feature Store REST
 * query API (~10ms p50). Falls back to a direct SQL read of the feature DT if
 * the online service env vars aren't set or the call fails, so the app never
 * breaks when the Postgres online store is unavailable.
 */
export async function queryFeatureStore(
  featureViewName: string,
  version: string,
  userId: string
): Promise<Record<string, unknown>> {
  const queryUrl = process.env.FEATURE_STORE_QUERY_URL;
  const pat = process.env.SNOWFLAKE_PAT;

  if (queryUrl && pat) {
    try {
      const res = await fetch(`${queryUrl}/api/v1/query`, {
        method: "POST",
        headers: {
          Authorization: `Snowflake Token="${pat}"`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name: featureViewName,
          version,
          object_type: "feature_view",
          request_rows: [{ entity: { USER_ID: userId } }],
        }),
      });
      if (res.ok) {
        const data = await res.json();
        const rows = data?.results ?? data?.rows ?? data;
        if (Array.isArray(rows) && rows.length) return rows[0];
      }
    } catch {
      // fall through to SQL
    }
  }

  // Fallback: direct SQL read of the feature DT
  const safe = userId.replace(/'/g, "''");
  const rows = await querySnowflake(
    `SELECT * FROM DEMO_ATAHIR.AD_STREAMS.dt_user_features WHERE user_id = '${safe}'`
  );
  return (rows[0] as Record<string, unknown>) || {};
}
