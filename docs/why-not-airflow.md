# Why not Airflow

> A short note on why this pipeline runs in Dataform instead of Airflow / Cloud Composer, despite Airflow being the industry default.

## The mismatch

Airflow is excellent for **workflows**: heterogeneous tasks (run a Python job, then call an API, then trigger an ML model, then send an email) that have side effects and need explicit ordering.

This pipeline is not that. It is a **declarative dependency graph of SQL transformations**. Every "task" is a `SELECT … FROM another_table`. The graph is implicit in the SQL itself — every `${ref()}` is an edge.

When the entire DAG is recoverable from `grep -r "ref(" definitions/`, Airflow's imperative DAG (`a >> b >> c`) becomes a duplicate, hand-maintained representation of something Dataform infers automatically.

## What we'd lose with Airflow

- **A Cloud Composer environment.** Composer is ~$300–500/month minimum. Dataform execution is free.
- **A Python runtime to maintain.** Pinned dependencies, Airflow upgrades, provider package incompatibilities, DAG parse failures.
- **Operator boilerplate.** Every transformation becomes a `BigQueryInsertJobOperator(...)` task with hand-maintained `dependencies = [...]`.
- **Two sources of truth for the DAG.** The SQL has its own dependencies via `ref()`; the Python DAG has its own via `>>`. They drift.

## What we'd gain with Airflow

These are real:

- **Cross-system orchestration.** If we needed to trigger a Python ML job, then a BigQuery transformation, then a Slack notification, Airflow handles that natively. Dataform handles SQL only.
- **Custom retries / backoff per task.** Dataform's retry model is coarser.
- **A UI that non-engineers find legible.** Airflow's graph view is famously good. Dataform's UI is more sparse.
- **Sensors and external triggers.** Wait for a file to land in GCS before running. Dataform schedules on cron only.

## When Dataform stops being the right answer

This is honest. The architecture in this repo would not survive these requirements:

1. **The pipeline needs to call non-BigQuery work.** ML training, external API enrichment, conditional branching based on data quality outcomes. Hybrid approach: keep transformations in Dataform, wrap with Airflow for the heterogeneous parts.
2. **The team has > 1 senior data engineer who already runs Airflow.** Tooling overhead amortizes; lone maintenance does not. Adopt what the team already knows.
3. **Multi-cloud or multi-warehouse.** Dataform is BigQuery-only. dbt is the right answer here.
4. **Strict SLA observability requirements.** Airflow's ecosystem of monitoring (Datadog, custom callbacks, SLA misses) is more mature.

## What we adopted from the Airflow mindset

Even though we don't run Airflow:

- **Tags as the unit of scheduling**, not individual table names. Mirrors Airflow's `tags=[...]` convention.
- **Idempotent transformations.** Every run produces the same output for the same inputs. This was the rule before Airflow and remains the rule.
- **Explicit dependencies, no implicit ordering.** `${ref()}` is the DataOps equivalent of `>>`.

The point isn't that Airflow is wrong — it's that **this particular pipeline doesn't have the workload that justifies it.**
