-- =============================================================================
-- 03_pre_post_parity_check.sql
--
-- Final gate: after the Dataform PR has been merged and the new incremental
-- table has run for at least one cycle in production, compare the live
-- incremental output to the baseline snapshot from 01.
--
-- This is the "trust but verify" step. The simulation in 02 used the source
-- (raw.order_items) to reconstruct B in pure SQL. This query compares the
-- ACTUAL Dataform-managed table against A — catching any divergence
-- introduced by the runtime (config drift, partition pruning, etc.).
--
-- Run once after the first incremental cycle completes.
-- =============================================================================

WITH
A AS (
  SELECT *
  FROM `my-gcp-project.dataform_assertions.baseline_order_items_clean`
),
LIVE AS (
  -- Restrict to the same window as A so the comparison is apples-to-apples.
  SELECT *
  FROM `my-gcp-project.silver.order_items_clean`
  WHERE DATE(created_at) BETWEEN
    (SELECT DATE(MIN(created_at)) FROM A) AND
    (SELECT DATE(MAX(created_at)) FROM A)
)

-- 1. Symmetric difference on the unique key.
SELECT
  'rows_only_in_baseline' AS check_name,
  COUNT(*) AS failing_rows
FROM A
LEFT JOIN LIVE USING (order_item_id)
WHERE LIVE.order_item_id IS NULL

UNION ALL

SELECT
  'rows_only_in_live',
  COUNT(*)
FROM LIVE
LEFT JOIN A USING (order_item_id)
WHERE A.order_item_id IS NULL

UNION ALL

-- 2. Row-level field divergence (same key, different values).
SELECT
  'rows_with_field_divergence',
  COUNT(*)
FROM A
INNER JOIN LIVE USING (order_item_id)
WHERE
     A.order_id            IS DISTINCT FROM LIVE.order_id
  OR A.product_id          IS DISTINCT FROM LIVE.product_id
  OR A.quantity            IS DISTINCT FROM LIVE.quantity
  OR A.net_amount_local    IS DISTINCT FROM LIVE.net_amount_local
  OR A.default_code        IS DISTINCT FROM LIVE.default_code
  OR A.is_recommended      IS DISTINCT FROM LIVE.is_recommended

UNION ALL

-- 3. Aggregate parity.
SELECT
  'sum_net_amount_diff_cents',
  CAST(ROUND(ABS(
    (SELECT SUM(net_amount_local) FROM A) -
    (SELECT SUM(net_amount_local) FROM LIVE)
  ) * 100, 0) AS INT64)

ORDER BY check_name;
