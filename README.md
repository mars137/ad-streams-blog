# Roads? Where We're Going, We Don't Need Roads

*How I rebuilt a month of real-time ad-tech engineering in under an hour — and what that says about where data work is headed*

![Hero: DeLorean in a data center with streaming cyan data](ad_streams_blog_images/00_hero_delorean_data_center.png)

---

> Six distributed systems. A month of my life. I rebuilt all of it on Snowflake in under an hour — and the hard part wasn't the speed. It was realizing the hard part had quietly disappeared.

---

Back in 2017 I spent the better part of a month building a real-time marketing propensity engine.

You know the kind. Ad events stream in — impressions, clicks, paid-search hits, conversions. You score every user on how likely they are to convert, in near real time, and you serve those scores back to a dashboard fast enough that a campaign manager can actually act on them. It's the bread and butter of every DSP, every audience platform, every Quantcast and Trade Desk you've ever heard of.

It took me a month. Not because I'm slow — because the *stack* was a month of work.

Last week I rebuilt the whole thing. Start to finish. In under an hour.

This is the story of that hour. And like any good story about going fast, it starts with a trip to the past.

---

## 1985: The Stack That Ate My Calendar

Let me take you back to the original build. Cue the synth.

To get ad events flowing, I stood up **Apache Kafka**. Topics, partitions, broker config, a ZooKeeper ensemble babysitting the whole thing. To land those events somewhere queryable, I wired up **Kafka Connect** with a sink connector and spent two days arguing with serialization formats.

Then came the *actual* logic. Rolling-window features — clicks in the last hour, last 24 hours, last 7 days — meant **Kafka Streams** with stateful KTables, windowing semantics, and a RocksDB state store that I had to reason about every time I changed an aggregation. The propensity model lived in a separate **Python service** with its own deployment, its own scaling story, its own 3 a.m. pages.

Serving needed sub-second lookups by user ID, so I bolted on **Cassandra**. The dashboard was a **Flask** app held together with optimism and CORS headers.

Six systems. Six failure modes. Six things to secure, monitor, and explain to the next engineer. The diagram looked impressive on a slide. It was miserable to operate.

![Architecture comparison: 2017 six-system stack vs 2026 Snowflake platform](ad_streams_blog_images/01_architecture_comparison_gemini.png)

```
Kafka → Kafka Connect → Kafka Streams → Python ML svc → Cassandra → Flask
  │          │                │              │              │          │
 ops        ops              ops            ops            ops        ops
```

That was the past. Here's where the DeLorean comes in.

---

## The DeLorean Was a Terminal Window

I didn't set out to relive the project. I set out to test a theory.

The theory: everything I built across six distributed systems in 2017 is now a feature *inside the data platform*. Not bolted on. Not "integrated." Native. And if that's true, then the right co-pilot should be able to drive the whole rebuild while I mostly described what I wanted.

So I opened Cortex Code — Snowflake's agentic IDE — pointed it at my old project, and said, in effect: *do this again, but all on Snowflake.*

Great Scott. It did.

Not by generating one giant script and praying. It worked the way a good engineer works — specify, plan, build, verify — and it narrated every step. I watched a month of architecture decisions collapse into a conversation.

Here's what came out the other side.

---

## 88 MPH: The Hour, Layer by Layer

Every box in that miserable 2017 diagram had a one-to-one replacement. The difference is that the new boxes don't need an ops team. They're SQL objects and managed services that live where the data already is.

![Data flow: Datastream → Dynamic Tables → Model Registry → Interactive Table → App Runtime](ad_streams_blog_images/02_data_flow_diagram_gemini.png)

| 2017 (a month) | 2026 (an hour) |
|---|---|
| Apache Kafka | **Snowflake Datastream** (Kafka-compatible, managed) |
| Kafka Connect | **Snowpipe Streaming** |
| Kafka Streams + KTables | **Dynamic Tables**, incremental refresh |
| Hand-rolled window state | **Custom incrementalization** (`MERGE INTO SELF`) |
| Cassandra | **Interactive Tables** on an Interactive Warehouse |
| External Python ML service | **Snowflake Notebook** (experiment) -> **Model Registry & MLOps** (serve) -> **Postgres Online FS** (real-time) |
| Flask | **App Runtime** (Next.js, deployed inside Snowflake) |

Let me show you two moments that made me put the coffee down.

**The streaming features.** In Kafka Streams, a rolling 1h/24h/7d click count was a stateful topology I had to design, test, and operate — a windowed KTable backed by a RocksDB state store, plus a changelog topic to make it fault-tolerant. On Snowflake it became a Dynamic Table with *custom incrementalization*. The engine reads only the new rows since the last refresh — `CHANGES(INFORMATION => APPEND_ONLY)` on the base tables — and merges the deltas straight into a running aggregate:

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

The week I spent on RocksDB topologies became a declarative refresh clause. I described the windows; the platform maintained the state.

**The serving layer.** Cassandra existed in the old design for exactly one reason: sub-second point lookups by user ID under load. Snowflake's new **Interactive Tables** do that natively — off the same data, with no second database to keep in sync:

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

A dedicated Interactive Warehouse keeps the table hot in cache, enforces a 5-second query ceiling so a runaway query can't starve the dashboard, and falls back to a standard warehouse if one does. Scores flow from the gold Dynamic Table into the Interactive Table on a one-minute lag; the dashboard reads them in milliseconds. The old design needed a whole CDC pipeline to keep Cassandra in sync with the source of truth. Here, the source of truth *is* the serving layer.

**The model.** In 2017 the propensity model was a Python service I eyeballed into existence — one algorithm, hand-tuned, deployed on its own box with its own scaling story. This time I opened a **Snowflake Notebook**, pulled the features with a line of SQL, and *raced four model families* — logistic regression, random forest, XGBoost, LightGBM — as tracked runs in a **Snowflake ML Experiment**:

```python
for name, model in candidates.items():
    with exp.start_run(name):
        model.fit(X_train, y_train)
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        exp.log_metrics({"roc_auc": auc})
```

I compared them side by side in Snowsight, logged the winner to the **Model Registry**, and pointed the pipeline at it. The Dynamic Table doesn't hardcode coefficients anymore — it calls the model by name:

```sql
AD_PROPENSITY_MODEL!PREDICT_PROBA(clicks_1h, clicks_24h, ... , event_velocity_24h)
```

Because the registered model is an *immutable function*, the Dynamic Table that consumes it stays incremental — no full rescans. A Model Monitor watches it for drift, a weekly task retrains it, and promoting a new champion is a one-line flip of the registry's default version. No redeploy. No new box.

**Real-time, too.** The same scores serve two ways off one registry entry. Batch, through the incremental Dynamic Table into the Interactive Table for the dashboard. And real-time, through the **Postgres-backed Online Feature Store** — the freshest score per user synced to a managed Postgres serving layer that answers point lookups in ~10 milliseconds, with live features landing in under two seconds. In 2017 that second path would have been *another* system — Redis, maybe, with its own sync job. Here it's a config flag on a feature view.

No connectors. No serialization fights. No 3 a.m. pages waiting to be born.

---

## The Part Where the Future Got Greedy

Here's the thing about time travel: once you're moving, you don't stop at "good enough."

In 2017, "ship the propensity dashboard" was the *entire* month. I never got to the features I actually wanted. With the hard part collapsed, I kept going — and the co-pilot kept up. I looked at what the real marketing-intelligence platforms ship, and I added it:

- **A campaign performance page** — KPIs, a channel-by-hour heatmap, served sub-second from an Interactive Table.
- **Multi-touch attribution** — first-touch, last-touch, linear, and time-decay models, computed live from user journey arrays.
- **An audience segment builder** — point-and-click behavioral rules that compile to SQL and preview the matching cohort instantly.
- **An AI campaign optimizer** — `AI_COMPLETE` reading my live campaign metrics and writing back specific, numeric recommendations, with an "intelligence trace" panel showing its reasoning.

That last one is the kicker. In the old world, "add an AI that explains what's working" would have been *its own quarter* — a model to host, a serving endpoint, a prompt pipeline, a governance review. Here it was one SQL function over data that never left the building:

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

No API key. No model deployment. The same RBAC that governs the underlying tables governs the model call. The "intelligence trace" panel in the UI just renders the steps the request walked through — data pull, analysis, recommendation — so a campaign manager sees the *why*, not just the answer.

All of it deployed to a live, authenticated URL with a single command:

```bash
snow app deploy
```

No Dockerfile to write. No registry to manage. Snowflake builds the image from my `package.json`, versions it as an immutable package, and upgrades the running service in place — the artifact repository and the build pipeline exist, I just don't operate them. And to be honest about it: I skipped CI/CD entirely here because it was a one-shot rebuild. In production you'd want the opposite — version control, automated tests, promotion across dev and prod. The nice part is that this same one-line deploy is exactly what you'd drop into that pipeline. The app builds remotely, runs inside Snowflake's security perimeter, and inherits the governance the data already had.

---

## This Isn't About Speed

It would be easy to read this as "AI made me a fast typist." That misses it.

The month in 2017 wasn't slow because I was writing code slowly. It was slow because the *architecture* was slow — six systems, six integration seams, six things that could be misconfigured between an event and a score. Most of my month was spent in the *gaps between the boxes*, not inside them.

What changed isn't that I type faster. It's that **the gaps are gone.** Streaming, transformation, low-latency serving, machine learning, and the application all sit on one governed copy of the data. The integration work — the genuinely hard, genuinely month-long part — has been absorbed into the platform. The co-pilot just made it conversational.

Roads were the integration layer. Where we're going, we don't need them.

---

## What I Want You to Take From This

## "But Open Source Has Gotten Better Too"

![The Assembly Gap: 2017 vs 2026 OSS vs 2026 Snowflake](ad_streams_blog_images/06_assembly_gap_gemini.png)

Fair point. Let me steelman the counter-argument.

If you built this same pipeline today with the *best* 2026 open-source stack — Redpanda (no ZooKeeper!), Flink 2.x, Feast, MLflow, Redis Cluster, BentoML, vLLM, Next.js, all on Kubernetes — it would genuinely be better than 2017. The tools are more mature, the DX is improved, Helm charts exist for everything.

Here's what that looks like:

```
Redpanda → Flink → Feast/Redis → BentoML → FastAPI → Next.js
    │         │         │            │          │         │
  3 pods    5 pods    6 pods      3 pods     3 pods    2 pods
                         │
                    MLflow + Prefect + Iceberg + vLLM (GPU)
                         │
                      8 more pods
```

**30-35 pieces of infrastructure to maintain.** A senior engineer could wire it together in 12-16 weeks — impressive progress from 2017's timeline, honestly. But you'd still be:

- Writing ~5,000 lines of code and config (Helm, Terraform, Flink SQL, Feast definitions, BentoML services)
- Operating Kubernetes with GPU nodes for the LLM layer
- Coordinating schema evolution across 6 boundaries (one mismatch = silent corruption)
- Carrying a pager for Flink checkpoint recovery and Redis memory pressure
- Paying $3-8K/month in infrastructure before a single query runs

The Snowflake version was **~200 lines of code**, zero infrastructure to maintain — it's a fully managed service — zero pager, built in an afternoon. Not because OSS is bad — it's excellent — but because *assembly* is the wrong abstraction level when the platform already speaks your intent natively.

The gap isn't 2017 vs. 2026. It's **"I assembled a system"** vs. **"I described what I wanted."** That's the real time travel.

---

## So What?

I didn't share this to flex a demo. I shared it because I think a lot of teams are still budgeting 2017 timelines for 2026 problems.

If your mental model of "build a real-time scoring pipeline" still includes standing up Kafka, you're carrying a month of work that no longer exists. The propensity engine, the attribution models, the audience builder, the AI recommendations, the deployed app — that was an afternoon, and most of the afternoon was me getting greedy about scope.

The art of the possible has moved. The interesting question is no longer *"how do we build the plumbing?"* It's *"now that the plumbing is free, what do we actually build?"*

That's a much better question. And you don't need 1.21 gigawatts to start answering it — just the data you already have, and the willingness to describe what you want out loud.

Your DeLorean is already in the garage.

*— Atif*

---

![Comparison table: Then vs Now by layer](ad_streams_blog_images/03_comparison_table_gemini.png)

*Built on Snowflake with Datastream, Dynamic Tables, Interactive Tables, the Feature Store, Cortex AI, and App Runtime — orchestrated end-to-end in Cortex Code. No roads required.*
