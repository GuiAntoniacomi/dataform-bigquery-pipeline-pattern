-- =============================================================================
-- 01_baseline_pre_migration.sql
-- Purpose: snapshot the current production state of order_items_clean BEFORE
-- the migration to incremental. This snapshot becomes baseline "A" for the
-- A/B/C/D simulation in 02_incremental_simulation.sql.
--
-- Run once. Persist the result as a real table — not a view — so the baseline
-- does not move while you iterate on the migration.
-- =============================================================================

DECLARE baseline_window_days INT64 DEFAULT 90;

CREATE OR REPLACE TABLE `my-gcp-project.dataform_assertions.baseline_order_items_clean`
PARTITION BY DATE(created_at)
CLUSTER BY order_id AS
SELECT
  order_item_id,
  order_id,
  product_id,
  quantity,
  unit_price_local,
  net_amount_local,
  default_code,
  is_recommended,
  created_at,
  updated_at
FROM `my-gcp-project.silver.order_items_clean`
WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL baseline_window_days DAY);

-- Sanity: report the snapshot's shape so the migration author sees what they
-- locked in. If the row count or distinct ID count looks wrong, stop here —
-- do NOT proceed to 02 with a bad baseline.
SELECT
  'baseline_snapshot' AS check_name,
  COUNT(*)                       AS total_rows,
  COUNT(DISTINCT order_item_id)  AS distinct_keys,
  MIN(created_at)                AS window_start,
  MAX(created_at)                AS window_end,
  SUM(net_amount_local)          AS sum_net_amount_local
FROM `my-gcp-project.dataform_assertions.baseline_order_items_clean`;
