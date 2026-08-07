import { NextResponse } from "next/server";
import { querySnowflake } from "@/lib/snowflake";

export async function POST(request: Request) {
  const { message } = await request.json();

  // Gather campaign metrics as context
  const metrics = await querySnowflake(`
    SELECT campaign_name, channel,
      SUM(impressions) as impressions, SUM(clicks) as clicks,
      SUM(conversions) as conversions, SUM(revenue) as revenue,
      DIV0(SUM(clicks), NULLIF(SUM(impressions), 0)) as ctr,
      DIV0(SUM(conversions), NULLIF(SUM(clicks), 0)) as conv_rate
    FROM DEMO_ATAHIR.AD_STREAMS.dt_campaign_performance
    GROUP BY 1, 2 ORDER BY revenue DESC
  `);

  const metricsJson = JSON.stringify(metrics, null, 2);
  const userPrompt = message || "What optimizations do you recommend for my campaigns?";

  const systemPrompt = `You are a marketing intelligence AI for a digital advertising platform. 
You analyze campaign performance data and provide actionable optimization recommendations.
Be specific with numbers and percentages. Format your response with clear sections.
Keep it concise - max 3 recommendations with expected impact.`;

  const escapedSystem = systemPrompt.replace(/'/g, "''");
  const escapedUser = `${userPrompt}\n\nHere are my current campaign metrics:\n${metricsJson}`.replace(/'/g, "''");

  const sql = `SELECT AI_COMPLETE(
    'claude-sonnet-4-6',
    ARRAY_CONSTRUCT(
      OBJECT_CONSTRUCT('role', 'system', 'content', '${escapedSystem}'),
      OBJECT_CONSTRUCT('role', 'user', 'content', '${escapedUser}')
    ),
    OBJECT_CONSTRUCT('max_tokens', 1024)
  ) AS recommendation`;

  const result = await querySnowflake(sql);
  const raw = (result[0] as any)?.RECOMMENDATION || "";

  // Parse AI_COMPLETE response
  let text = raw;
  try {
    const parsed = JSON.parse(raw);
    text = parsed?.choices?.[0]?.messages || parsed?.choices?.[0]?.message?.content || raw;
  } catch {
    // Already a string
  }

  return NextResponse.json({ recommendation: text, metrics });
}
