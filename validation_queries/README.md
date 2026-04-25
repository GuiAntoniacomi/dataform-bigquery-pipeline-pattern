# Validation queries

> Run these in BigQuery — **not** in Dataform. They exist to prove correctness *before* a Dataform change is merged.

## The discipline

The single most expensive bug in analytics is silent data drift: a pipeline change ships, the dashboard keeps rendering, and three weeks later finance notices the number is off by 4%. By then the root-cause is buried under three more deploys.

This folder holds a fixed protocol for catching drift **at PR review time**, before the change hits production.

```
01_baseline_pre_migration.sql    →  capture the world as it is today
02_incremental_simulation.sql    →  prove the proposed change produces identical output
03_pre_post_parity_check.sql     →  re-prove parity after the cutover, with real data
```

The protocol is opinionated. It assumes the change is risky until proven otherwise.

## The A/B/C/D pattern (`02_incremental_simulation.sql`)

Migrating a table from full-refresh to incremental is the most common high-risk migration. The pattern below proves parity without touching production:

| Step | What it represents | Pass condition |
|------|---------------------|---|
| **A** | The current full-refresh result (production baseline) | — |
| **B** | The incremental simulation: "what would the table look like if we had been running incrementally for the last *N* days?" | — |
| **C** | Rows in **A** but not in **B** — rows the incremental would **silently drop** | `COUNT(C) = 0` |
| **D** | Rows in **B** but not in **A** — rows the incremental would **wrongly add** | `COUNT(D) = 0` |

Plus a fifth check, **aggregate parity**: even when row sets match, sums and counts must match. (Numeric precision drift is a separate failure mode.)

If C, D, and aggregate checks all pass on a meaningful window of data, the cutover is safe.

## When to run each query

- **`01_baseline_pre_migration.sql`** — once, before opening the Dataform PR. Snapshot the current state.
- **`02_incremental_simulation.sql`** — at PR review time. Must return zeros across the board.
- **`03_pre_post_parity_check.sql`** — once after merge, against real production data, as the final gate.

## What to do when checks fail

Don't tune the query until it passes. **Investigate.** A non-zero `C` or `D` is a signal that the incremental window, the unique key, or the deduplication logic has a real flaw. The validation query is correct by construction — the migration is what's wrong.

This protocol caught two real-world issues during the original migration:

1. A `MAX(created_at)` filter that dropped a 36-hour late-arriving cohort. Fix: 7-day retroactive window.
2. A `unique_key` collision on a table that allowed duplicate IDs across sources. Fix: composite key with source identifier.
