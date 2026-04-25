# Pipeline as code

> The philosophy behind every other decision in this repo.

## The rule

If a transformation runs in production, it must:

1. Live in **Git**.
2. Be reachable by `${ref()}` from another transformation, or be a documented terminal output.
3. Have passed a **PR review** before being merged.
4. Be **executable from a clean clone** — no external state, no manual setup steps that aren't in the repo.

A query that runs hidden in the BigQuery scheduler UI does not exist. It is a liability waiting to be inherited.

## What this rules out

- **Scheduled queries created in the BigQuery console.** They have no version history, no review, no rollback path. The migration that birthed this repo deleted dozens of them.
- **One-off `bq query` runs that wrote to a "temporary" table that never got cleaned up.** Every table in the warehouse is owned by a SQLX file or it's a candidate for deletion.
- **"I'll just fix it in production real quick."** Production is read-only to humans. The PR is the unit of change.

## What this rules in

- **Diffs as the change unit.** A model change is a diff in a SQLX file. Reviewable, revertable, attributable.
- **CI compilation.** A broken SQL never reaches production because the compile step blocks the merge.
- **Rollback by `git revert`.** No "undo" button needed.
- **Onboarding as `git clone`.** A new analyst reads the repo, not 91 scheduled queries scattered across someone's UI.

## The cultural cost

Pipeline-as-code is a discipline, not a tool. The tools (Dataform, dbt, etc.) make it cheap, but they don't enforce it.

The cultural cost is:
- Quick fixes take longer (PR review > console edit)
- People who used to "own" their scheduled queries lose informal authority
- The team has to agree on what counts as a model worth committing

The cultural benefit is that **the warehouse becomes legible**. Six months in, anyone can answer "where does this number come from?" by reading the repo.

## What changes when this rule is taken seriously

| Before | After |
|---|---|
| "Whoever wrote that query left the company" | `git blame` shows the author and the PR rationale |
| "I think this table is consumed by Dashboard X — not sure" | DAG shows every consumer; dead tables are deleted |
| "We refactored the staging logic and 3 dashboards broke" | CI failed at PR time; the issue never shipped |
| "Costs spiked last month — we're investigating" | Every materialization is reviewed; cost decisions are PR comments |

This is not a tooling story. It is the same shift that software engineering went through 20 years ago when "deploy by FTP" became "deploy via CI". Analytics pipelines lagged it by two decades. This repo is what catching up looks like.
