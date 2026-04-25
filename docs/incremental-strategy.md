# Incremental strategy

## TL;DR

- **`MERGE` on a unique key, with a retroactive window of N days.** Don't filter by `MAX(created_at)`.
- **Combine incremental with a periodic full-refresh.** They are not redundant — they cover different failure modes.
- **`updatePartitionFilter` is your cost lever.** Set it to a window that matches the worst-case late arrival.

## Why not `MAX(created_at)`

The most common pattern in ad-hoc Scheduled Queries is:

```sql
WHERE created_at > (SELECT MAX(created_at) FROM target_table)
```

This breaks the moment the source has late-arriving rows. CDC pipelines routinely deliver records hours — sometimes days — after their nominal `created_at`. A `MAX(created_at)` filter silently drops them.

The retroactive window pattern handles this:

```sql
WHERE created_at >= TIMESTAMP_SUB(
  (SELECT MAX(created_at) FROM target_table),
  INTERVAL 7 DAY
)
```

Combined with a `MERGE` on `unique_key`, late-arriving rows are upserted into the existing partition without producing duplicates.

## How to choose the window

Three inputs:

1. **Worst-case CDC lag.** Measure it. Look at the difference between `datastream_metadata.source_timestamp` and `created_at` over a representative period. Take the 99th percentile.
2. **Cost tolerance.** A 30-day window scans 30 days of partitions every run. A 7-day window scans 7. Multiply by the number of runs per day to get the daily cost differential.
3. **Recovery time after an outage.** If your CDC stops for two days and catches up, the window must be ≥ outage duration to absorb the catch-up.

For the original migration, the chosen window was **7 days** because:

- 99th percentile lag was 36 hours
- Outage tolerance budget was 5 days
- 7 days kept partition scans cheap relative to full-refresh

## Why also keep a monthly full-refresh

The dual-pipeline pattern (incremental MERGE every 2h + monthly full-refresh) looks redundant but is not.

| Failure mode | Caught by incremental? | Caught by monthly refresh? |
|---|:-:|:-:|
| Source row added | ✅ | ✅ |
| Source row updated | ✅ | ✅ |
| Source row late by < 7 days | ✅ | ✅ |
| Source row late by > 7 days | ❌ | ✅ |
| Source row deleted (hard delete) | ❌ | ✅ |
| Schema change in source | ⚠️ partial | ✅ |
| Bug in MERGE logic introduces drift | ❌ | ✅ |

The monthly refresh is the **safety net**. It is intentional duplication. The two SQL files (`fct_orders.sqlx` and `fct_orders_incremental_merge.sqlx`) must be kept in sync — when a business rule changes, both files change in the same PR.

This is documented as an explicit "tech debt accepted" item rather than something to refactor away. The cost of dual maintenance is bounded; the cost of silent drift in financial metrics is not.

## Why `updatePartitionFilter` matters

Without `updatePartitionFilter`, a Dataform incremental MERGE scans the **entire target table** to evaluate the `ON` clause. For a partitioned table with two years of data, this is unacceptable.

```javascript
bigquery: {
  partitionBy: "DATE(created_at)",
  updatePartitionFilter: "created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)"
}
```

The 14-day window is **larger** than the 7-day source window on purpose: it gives BigQuery's query planner enough partitions to safely match `MERGE` keys that may have crossed a partition boundary (e.g. a row whose `created_at` was updated by the source).

Rule of thumb: `updatePartitionFilter` window ≥ 2 × source window.

## What this strategy assumes

- The source has a stable `unique_key` that does not collide across systems
- `created_at` is monotonically derivable from the source (not arbitrary system time)
- The `MERGE` cost (per run) × runs-per-day < full-refresh cost (per run) ÷ 30

If any of these break, revisit the strategy. Don't keep an incremental that no longer earns its keep.
