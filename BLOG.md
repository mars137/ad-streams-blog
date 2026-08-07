# LoCo for Data Engineering with CoCo

*Low-code real-time pipelines, built conversationally.*

![Hero: DeLorean in a data center with streaming cyan data](ad_streams_blog_images/00_hero_delorean_data_center.png)

---

In 2017 I spent a month building a real-time marketing propensity engine. Ad events stream in (impressions, clicks, paid-search hits, conversions), you score every user on conversion likelihood, and you serve those scores to a dashboard fast enough for a campaign manager to act on them.

It took a month because the stack was a month of work: Kafka, Kafka Connect, Kafka Streams, a Python ML service, Cassandra, Flask. Six systems, each with its own failure mode.

Last week I rebuilt the same thing in under an hour using Cortex Code. Here's what happened and how to reproduce it.

---

## The original stack (2017)

```
Kafka → Kafka Connect → Kafka Streams → Python ML svc → Cassandra → Flask
  │          │                │              │              │          │
 ops        ops              ops            ops            ops        ops
```

![Architecture comparison: 2017 six-system stack vs 2026 Snowflake platform](ad_streams_blog_images/01_architecture_comparison_gemini.png)

The rolling-window features (clicks in the last hour, 24 hours, 7 days) meant Kafka Streams with stateful KTables, a RocksDB state store, and a changelog topic for fault tolerance. The model was a hand-tuned Python service on its own box. Cassandra handled sub-second lookups. Flask served the dashboard.

Most of the month went to integration, not logic.

---

## The rebuild (2026)

I opened Cortex Code, pointed it at the old project, and described what I wanted. It worked through it step by step: specify, plan, build, verify.

![Data flow: Datastream → Dynamic Tables → Model Registry → Interactive Table → App Runtime](ad_streams_blog_images/02_data_flow_diagram_gemini.png)

| 2017 | 2026 (Snowflake) |
|---|---|
| Apache Kafka | Datastream (Kafka-compatible, managed) |
| Kafka Connect | Snowpipe Streaming |
| Kafka Streams + KTables | Dynamic Tables, incremental refresh |
| Hand-rolled window state | Custom incrementalization (`MERGE INTO SELF`) |
| Cassandra | Interactive Tables + Interactive Warehouse |
| Python ML service | Notebook → Model Registry → Postgres Online FS |
| Flask | App Runtime (Next.js inside Snowflake) |

---

## What the code looks like

**Incremental features.** The rolling aggregates that took a week of RocksDB topology design became a Dynamic Table with custom incrementalization. It reads only new rows since the last refresh and merges deltas into running counts:

```sql
CREATE OR REPLACE DYNAMIC TABLE dt_user_features (...)
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = AD_STREAMS_WH
  REFRESH USING (
    MERGE INTO SELF AS tgt
    USING (
      SELECT user_id,
             SUM(CASE WHEN event_type='CL'
                      AND event_ts >= DATEADD('hour',-1,CURRENT_TIMESTAMP())
                      THEN 1 ELSE 0 END) AS new_clicks_1h
             -- ... 24h, 7d, impressions, paid search, conversions ...
      FROM raw_marketing_events CHANGES(INFORMATION => APPEND_ONLY)
      GROUP BY user_id
    ) AS src
    ON tgt.user_id = src.user_id
    WHEN MATCHED THEN UPDATE SET tgt.clicks_1h = src.new_clicks_1h
    WHEN NOT MATCHED THEN INSERT ...
  );
```

**Serving.** An Interactive Table with a dedicated Interactive Warehouse handles sub-100ms point lookups. No separate database to keep in sync:

```sql
CREATE OR REPLACE INTERACTIVE TABLE propensity_dashboard
  CLUSTER BY (score_bucket, user_id)
  TARGET_LAG = '1 minute'
  WAREHOUSE = AD_STREAMS_WH
AS SELECT user_id, propensity_score, score_bucket, last_updated
   FROM dt_user_propensity;

CREATE OR REPLACE INTERACTIVE WAREHOUSE AD_STREAMS_INTERACTIVE_WH
  TABLES (propensity_dashboard)
  WAREHOUSE_SIZE = 'XSMALL';
```

**ML.** I raced four model families in a Snowflake ML Experiment, registered the winner, and the Dynamic Table calls it by name:

```python
for name, model in candidates.items():
    with exp.start_run(name):
        model.fit(X_train, y_train)
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        exp.log_metrics({"roc_auc": auc})
```

```sql
AD_PROPENSITY_MODEL!PREDICT_PROBA(clicks_1h, clicks_24h, ... , event_velocity_24h)
```

A Model Monitor watches for drift. A weekly task retrains. Promoting a new version is a one-liner.

**AI recommendations.** One SQL function call, no external API:

```sql
SELECT AI_COMPLETE(
  'claude-sonnet-4-6',
  ARRAY_CONSTRUCT(
    OBJECT_CONSTRUCT('role','system','content',
      'You are a marketing intelligence AI. Analyze campaign
       metrics and give 3 specific, numeric recommendations.'),
    OBJECT_CONSTRUCT('role','user','content',
      'Here are my live campaign metrics: ' || $campaign_json)
  ),
  OBJECT_CONSTRUCT('max_tokens', 1024)
) AS recommendation;
```

**Deploy.** One command:

```bash
snow app deploy
```

---

## What else I added

Because the pipeline was done in under an hour, I kept going:

- Campaign performance page with KPIs and channel-by-hour heatmap
- Multi-touch attribution (first-touch, last-touch, linear, time-decay)
- Audience segment builder with behavioral rules that compile to SQL
- AI campaign optimizer with an intelligence trace panel

In 2017, shipping the propensity dashboard was the entire month. I never got to any of these.

---

## The open-source comparison

To be fair: if you built this today with the best modern OSS (Redpanda, Flink 2.x, Feast, MLflow, Redis Cluster, BentoML, vLLM, Next.js on Kubernetes), it would be better than 2017. The tools are mature, Helm charts exist for everything.

```
Redpanda → Flink → Redis → BentoML → Next.js
   1 pod   2 pods   1 pod    1 pod     1 pod
                      │
              MLflow + Prefect + vLLM
                      │
                    3 pods
```

![The Assembly Gap: Past, Present, and Future](ad_streams_blog_images/06_assembly_gap_gemini.png)

That's about 12 pods, and that count is the floor: one replica each, no high availability. Turn on HA and it roughly triples, because Redpanda and Redis both want quorum. Either way it's 12-16 weeks for a senior engineer, a few thousand lines of config, a pager, and $3-8K/month before the first query.

The Snowflake version: about 200 lines of code. No infrastructure. No pager. An afternoon.

The difference isn't the individual tools. It's that assembly is still the bottleneck, even with better parts.

---

## Why this matters

Most of the month in 2017 wasn't spent writing logic. It was spent in the gaps between systems: serialization formats, sync pipelines, deployment configs, schema coordination. When those gaps go away, the work changes. You spend time on the actual problem instead of the plumbing around it.

If your team is still budgeting weeks for real-time pipelines, it might be worth checking whether the work you're planning still needs to exist.

The code is at [github.com/mars137/ad-streams-blog](https://github.com/mars137/ad-streams-blog). Setup instructions in the README.

*— Atif*

---

![Comparison table: Then vs Now by layer](ad_streams_blog_images/03_comparison_table_gemini.png)

*Built on Snowflake with Datastream, Dynamic Tables, Interactive Tables, the Feature Store, Cortex AI, and App Runtime. Orchestrated in Cortex Code.*

*This is a personal take based on my own data engineering experience, not a Snowflake feature, utility, or product position. See [LEGAL.md](LEGAL.md).*
