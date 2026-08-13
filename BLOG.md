# One Month to One Hour with CoCo

*A 2017 real-time propensity engine, rebuilt conversationally in Cortex Code.*

![Hero: DeLorean in a data center with streaming cyan data](ad_streams_blog_images/00_hero_delorean_data_center.png)

---

Most of the month I spent building a real-time propensity engine in 2017 wasn't spent writing logic. It went into the gaps between systems: serialization formats, sync pipelines, deployment configs, schema coordination between six services that each had their own opinion about state.

Last week I rebuilt the same thing in under an hour. Not because I got better at it, but because the gaps are gone.

That's the claim, and it has a specific consequence: if your team is still budgeting weeks for a real-time pipeline, the thing worth checking first is whether the work you're planning still needs to exist.

---

## The problem, both times

Ad events stream in (impressions, clicks, paid-search hits, conversions). You score every user on conversion likelihood, and you serve those scores to a dashboard fast enough for a campaign manager to act on them.

In 2017 that meant Kafka, Kafka Connect, Kafka Streams, a Python ML service, Cassandra, and Flask. Six systems, six failure modes, one month. The rolling-window features alone needed stateful KTables and a changelog topic for fault tolerance.

![Architecture comparison: 2017 six-system stack vs 2026 Snowflake platform](ad_streams_blog_images/01_architecture_comparison_gemini.png)

---

## The rebuild

I opened Cortex Code, pointed it at the old project, and described what I wanted. It worked through it step by step: specify, plan, build, verify.

![Data flow: Datastream → Dynamic Tables → Model Registry → Interactive Table → App Runtime](ad_streams_blog_images/02_data_flow_diagram_gemini.png)

| 2017 | 2026 (Snowflake) |
|---|---|
| Apache Kafka | Datastream (Kafka-compatible, managed) |
| Kafka Connect | Snowpipe Streaming through a `PIPE` |
| Kafka Streams + KTables | Dynamic Tables, incremental refresh |
| Hand-rolled window state | Custom incrementalization (`MERGE INTO SELF`) |
| Cassandra | Interactive Tables + Interactive Warehouse |
| Python ML service | Snowflake Notebook → Model Registry → Postgres Online FS |
| Flask | App Runtime (Next.js inside Snowflake) |

One caveat on the first row, since I would rather say it than have you find it. Datastream is in private preview, and on my account the broker path is not usable yet: a local broker registers with the management service, heartbeats, and gets back `Action type not implemented yet` for `ACTION_TYPE_BROKER_HEALTHCHECK`, then exits. The topics and consumer group provision fine, so the ingestion in this repo goes through Snowpipe Streaming directly rather than through a Kafka topic. Everything downstream of the bronze tables is unaffected.

---

## What the code looks like

**Ingestion.** Snowpipe Streaming writes rows through a `PIPE`, which is where schema validation and in-flight transforms happen:

```sql
CREATE OR REPLACE PIPE raw_marketing_events_pipe
AS COPY INTO raw_marketing_events
     (event_id, user_id, event_type, campaign_id, event_ts, properties)
  FROM (
    SELECT $1:event_id::VARCHAR, $1:user_id::VARCHAR, $1:event_type::VARCHAR,
           $1:campaign_id::VARCHAR, $1:event_ts::TIMESTAMP_NTZ,
           $1:properties::VARIANT
    FROM TABLE (DATA_SOURCE(TYPE => 'STREAMING'))
  );
```

The producer opens a channel per pipe and appends rows with an offset token. The token is what makes delivery exactly-once: on restart it reads back the last offset Snowflake committed and resumes there instead of replaying rows that are already durable.

```python
channel, status = client.open_channel("ad-streams-marketing-p0")
next_offset = int(status.latest_committed_offset_token) + 1 \
    if status.latest_committed_offset_token is not None else 0
channel.append_row(row, str(next_offset))
```

**Incremental features.** The rolling aggregates that took a week of RocksDB topology design became a Dynamic Table with custom incrementalization. Raw events land in bronze, get unified and enriched by two upstream Dynamic Tables, and this one reads only the new rows since its last refresh and merges deltas into running counts:

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
      FROM dt_events_enriched CHANGES(INFORMATION => APPEND_ONLY)
      GROUP BY user_id
    ) AS src
    ON tgt.user_id = src.user_id
    WHEN MATCHED THEN UPDATE SET
      tgt.clicks_1h = src.new_clicks_1h,               -- window resets
      tgt.clicks_7d = tgt.clicks_7d + src.new_clicks_7d -- delta accumulates
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

**ML.** Model development happened in a Snowflake Notebook, next to the data instead of on a laptop with a sampled extract. I raced four model families in a Snowflake ML Experiment and registered the winner:

```python
for name, model in candidates.items():
    with exp.start_run(name):
        model.fit(X_train, y_train)
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        exp.log_metrics({"roc_auc": auc})
```

Registering it turns the model into a function, which is the part that matters downstream. The Dynamic Table calls it by name and stays incremental, because a registered version is immutable:

```sql
AD_PROPENSITY_MODEL!PREDICT_PROBA(clicks_1h, clicks_24h, ... , event_velocity_24h)
```

A Model Monitor watches for drift. A task retrains weekly on a Monday cron, calling the notebook directly with `EXECUTE NOTEBOOK`. Promoting a new version is one statement, `ALTER MODEL ... SET DEFAULT_VERSION`, and because the Dynamic Table calls the model by name rather than by version, nothing downstream needs to change.

**AI recommendations.** One SQL function call, no external API:

```sql
SELECT AI_COMPLETE(
  'claude-sonnet-4-6',
  ARRAY_CONSTRUCT(
    OBJECT_CONSTRUCT('role','system','content',
      'You are a marketing intelligence AI. Analyze campaign
       performance and give at most 3 recommendations with
       expected impact. Be specific with numbers.'),
    OBJECT_CONSTRUCT('role','user','content',
      'Here are my live campaign metrics: ' || $campaign_json)
  ),
  OBJECT_CONSTRUCT('max_tokens', 1024)
) AS recommendation;
```

The message-array form returns a JSON envelope, so the app pulls the text out of `choices[0]` rather than using the result directly.

**Deploy.** One command:

```bash
snow app deploy
```

---

## Getting it into CI

Building it in an hour only matters if changing it later is also cheap, so the repo has a GitHub Actions workflow with two jobs.

On merge to `main`, it applies the pipeline definitions and redeploys the app with the Snowflake CLI. There's nothing to build and no image to push, so the deploy job is a loop over SQL files and one `snow app deploy`.

On pull requests, it installs the Cortex Code CLI and runs the same agent I built with, headlessly:

```yaml
- name: Install Cortex Code CLI
  run: |
    curl -LsS https://ai.snowflake.com/static/cc-scripts/install.sh | sh
    echo "$HOME/.local/bin" >> "$GITHUB_PATH"

- name: Review changed pipeline code
  run: |
    cortex --print "Review the SQL and app changes in this pull request \
      against the base branch. Focus on Dynamic Table incrementality: flag \
      anything that would force a full refresh, break CHANGES(INFORMATION => \
      APPEND_ONLY) reads, or make a downstream table non-incremental." \
      > review.md
```

That review target is the specific thing I want caught. A small edit to a Dynamic Table can silently drop it from incremental to full refresh, and you find out from the bill rather than from an error. Asking a reviewer to watch for it is more reliable than remembering to check.

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

Comparing against 2017 is a low bar, so here's the fairer version. If you built this today with the best modern OSS (Redpanda, Flink 2.x, MLflow, Redis Cluster, BentoML, vLLM, Next.js on Kubernetes), it would be much better than 2017. The tools are mature and Helm charts exist for everything.

```
Redpanda → Flink → Redis → BentoML → Next.js
   1 pod   2 pods   1 pod    1 pod     1 pod
                      │
              MLflow + Prefect + vLLM
                      │
                    3 pods
```

![The Assembly Gap: Past, Present, and Future](ad_streams_blog_images/06_assembly_gap_gemini.png)

That's about 12 pods, and that count is the floor: one replica each, no high availability. Turn on HA and it roughly triples, because Redpanda and Redis both want quorum. Either way you own a Kubernetes cluster, a few thousand lines of config, and a pager.

The Snowflake version has no infrastructure to maintain, because the equivalent of every pod above is a managed service you don't operate.

The difference isn't the individual tools. It's that assembly is still the bottleneck, even with better parts.

---

## Why this matters

The gaps between systems were never the interesting part of the job, but for a long time they were most of it. When they close, the work changes shape. You spend the time on the problem instead of the plumbing around it.

An hour instead of a month isn't really a statement about speed. It's that the month was mostly overhead, and overhead is the part worth deleting.

The code is at [github.com/mars137/ad-streams-blog](https://github.com/mars137/ad-streams-blog). Setup instructions in the README.

*— Atif*

---

![Comparison table: Then vs Now by layer](ad_streams_blog_images/03_comparison_table_gemini.png)

*Built on Snowflake with Datastream, Dynamic Tables, Interactive Tables, the Feature Store, Cortex AI, and App Runtime. Orchestrated in Cortex Code.*

*This is a personal take based on my own data engineering experience, not a Snowflake feature, utility, or product position. See [LEGAL.md](LEGAL.md).*
