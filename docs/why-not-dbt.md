# Why not dbt

> A short note on why this pipeline uses Dataform rather than dbt, despite dbt being the industry default for "Pipeline as Code" SQL workflows.

## The honest answer first

**dbt is excellent.** If this pipeline ran on Snowflake, Databricks, or a multi-warehouse footprint, dbt would be the right answer with no caveats. The choice of Dataform is specific to a single-warehouse BigQuery shop.

## What Dataform wins on (in this specific context)

- **No runtime to host.** Dataform executes natively in GCP — no Cloud Run service, no scheduled GitHub Action calling `dbt run`, no Airflow. The runtime *is* a Google service.
- **No Python dependency.** dbt-core is Python. That brings: a virtualenv, a pinned `requirements.txt`, periodic compatibility friction with dbt provider packages. Dataform compiles SQLX to BigQuery DDL with zero local runtime.
- **Native IAM model.** Dataform inherits BigQuery permissions. There is no separate "dbt user" with its own credential surface.
- **Dataform Workflows are scheduled by GCP.** No external scheduler needed. Cron expressions live next to the SQLX.
- **Cost is zero.** dbt Cloud is paid; self-hosted dbt requires infrastructure. Dataform's execution is part of BigQuery.

## What dbt wins on

These are real and would justify dbt in a different context:

- **Maturity of ecosystem.** dbt has more packages, more docs, more community patterns, more StackOverflow answers.
- **Testing framework.** `dbt test` with `unique`, `not_null`, `accepted_values`, custom tests, and `dbt-expectations` is more polished than Dataform's `assertions:`.
- **Documentation generation.** `dbt docs generate` produces a queryable lineage UI that the Dataform built-in is catching up to but doesn't quite match.
- **Multi-warehouse portability.** dbt has adapters for Snowflake, Postgres, Databricks, Redshift, etc. Dataform is BigQuery-only.
- **Macros and Jinja.** dbt's templating is more expressive than Dataform's JS includes.

## How the choice would flip

This pipeline migrates to dbt the moment any of the following becomes true:

1. The data lands in a second warehouse (e.g., add Snowflake for sales data)
2. The team grows and has dbt expertise that doesn't transfer to Dataform
3. The need for advanced macros / Jinja templating exceeds what JS includes provide
4. The organization standardizes on dbt across other domains, and the consistency benefit outweighs Dataform's GCP-native advantages

## What dbt and Dataform agree on

The underlying philosophy is identical: **declarative SQL, dependency graph from `ref()`, model materializations as configuration, tests/assertions as code, version control as the source of truth.** Switching between them is a translation, not a rewrite.

This repo's structure (sources/silver/gold, ref-based DAG, A/B/C/D validation pattern) ports to dbt with mechanical changes — not architectural ones.

## Recommended reading

- dbt's [model materializations](https://docs.getdbt.com/docs/build/materializations) — analogous to Dataform's `type: "table" / "incremental" / "view"`
- Dataform's [actions reference](https://cloud.google.com/dataform/docs/reference) — the Dataform side of the same vocabulary
