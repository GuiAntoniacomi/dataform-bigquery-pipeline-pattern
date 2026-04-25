# The A/B/C/D validation pattern

> A four-step protocol for proving that a pipeline change produces correct output **before** it touches production.

## The problem this solves

When migrating a BigQuery table from full-refresh to incremental — the most common high-risk migration in an analytics pipeline — three failure modes are easy to miss:

1. **Silent row loss.** The incremental window misses late-arriving CDC rows that the full refresh would have caught.
2. **Silent duplication.** A weak `unique_key` lets the MERGE insert duplicates instead of upserting.
3. **Aggregate drift.** Row sets match exactly but a `NUMERIC` precision difference shifts a sum by a fraction of a percent — invisible in a row-level diff.

A test that runs *after* the cutover catches the issue once damage is already in the dashboard. This pattern catches it *before*.

## The four steps

```
    A                   B                C                D
┌─────────┐       ┌─────────┐      ┌──────────┐    ┌──────────┐
│ Current │       │Simulated│      │ A but    │    │ B but    │
│ full-   │  vs.  │incremen-│  →   │ NOT in B │ +  │ NOT in A │
│ refresh │       │tal      │      │ (lost)   │    │ (added)  │
└─────────┘       └─────────┘      └──────────┘    └──────────┘
   real             reconstructed     must be 0       must be 0
   baseline         in pure SQL
```

Plus a fifth check that's easy to forget: **aggregate parity.** Even when row sets are identical, sums and counts must match. (Numeric precision can drift across reruns if the simulation isn't deterministic.)

## What "B" actually is

The trick is that **B is reconstructed in pure SQL**, against the source. It's not a real run of the new pipeline. This matters because:

- It runs in seconds, not hours
- It doesn't need a separate environment
- It doesn't write anything anywhere
- It can be diffed against A directly in the same query

The reconstruction logic is: "if I ran the proposed incremental every day for the last *N* days, with the proposed window, against the source as it exists right now, what would the output be?"

For an incremental MERGE with a `K`-day retroactive window, this collapses to: "the latest version (by `source_timestamp`) of each `unique_key`, restricted to source rows where `created_at >= today - N - K days`."

If that reconstruction equals A row-for-row, the migration is safe — by construction.

## Pass criteria

| Check | Pass condition |
|---|---|
| `C` (lost rows) | `COUNT(*) = 0` |
| `D` (added rows) | `COUNT(*) = 0` |
| Total row count parity | `count(A) = count(B)` |
| Distinct key parity | `distinct_keys(A) = distinct_keys(B)` |
| Aggregate parity | `sum_diff = 0` (within rounding tolerance for floats) |

If **all five pass**, the migration is safe for the proposed window. If **any fails**, the failure points to a specific defect:

| Failure | Most likely cause |
|---|---|
| `C > 0` | Window too short — late arrivals being dropped |
| `D > 0` | Source has duplicates not handled by the dedup CTE |
| Row count parity off by 1 | Off-by-one in window boundary (`>` vs `>=`) |
| Aggregate parity off by tiny fraction | Rounding mode or `NUMERIC` precision drift |

## When this pattern caught real bugs

During the original migration, this protocol caught two issues that would have shipped silently otherwise:

1. **A 36-hour late cohort dropped.** First simulation showed `C = 1,847` rows. Investigation revealed CDC was occasionally delivering Saturday's transactions on Monday morning. The proposed `MAX(created_at)` filter dropped all of them. Fix: 7-day retroactive window with MERGE on unique key.

2. **Duplicate IDs across sources.** First simulation showed `D > 0` for a separate table. Investigation revealed two upstream services were writing to the same staging area with overlapping ID ranges. Fix: composite `unique_key = (id, source_system)`.

In both cases, the bug was real, the fix was real, and neither would have been caught by row-count assertions alone.

## What this pattern does NOT replace

- **Schema-level assertions** (Dataform `assertions:` blocks). Those catch null violations, uniqueness violations, and referential integrity. Run them; this pattern is orthogonal.
- **Post-deploy monitoring.** Even after a clean A/B/C/D, the production runtime can drift due to config changes, partition pruning, or upstream schema evolution. Query 03 in `validation_queries/` is the post-deploy companion.
- **Performance testing.** This protocol proves correctness, not cost. Run a separate cost estimate (`bq --dry_run`) before merging.

## Why call it A/B/C/D

The names are deliberately content-free. In the SQL, `A`, `B`, `C`, `D` are temp tables; in code review, "is C zero?" is a three-second question. Most engineers ask "is the diff clean?" and stop there — A/B/C/D forces the reviewer to name *which side* of the diff failed, which forces them to name *which failure mode* they're looking at.

It is, in other words, a vocabulary trick that makes the right discussion the cheap one.
