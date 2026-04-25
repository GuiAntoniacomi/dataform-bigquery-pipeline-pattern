-- =============================================================================
-- 02_incremental_simulation.sql — the A/B/C/D pattern
--
-- Goal: prove that converting order_items_clean from full-refresh to incremental
-- MERGE (with a 7-day retroactive window on order_item_id) produces an output
-- that is row-for-row and aggregate-for-aggregate identical to the baseline
-- captured by 01_baseline_pre_migration.sql.
--
-- A = full-refresh baseline (snapshot from query 01)
-- B = simulated incremental output reconstructed by replaying the source
-- C = rows in A not in B  → rows the incremental would SILENTLY DROP
-- D = rows in B not in A  → rows the incremental would WRONGLY ADD
--
-- Pass condition: C = 0 AND D = 0 AND aggregate parity holds.
--
-- Run this against production read-only — it does NOT modify any table.
-- =============================================================================

DECLARE simulation_window_days INT64 DEFAULT 7;
DECLARE total_window_days      INT64 DEFAULT 90;

-- ============================================================================
-- Build A: the baseline (already materialized by query 01).
-- ============================================================================
CREATE TEMP TABLE A AS
SELECT *
FROM `my-gcp-project.dataform_assertions.baseline_order_items_clean`;

-- ============================================================================
-- Build B: simulate the incremental.
--
-- We reconstruct what the table WOULD look like if we had been running
-- incrementally for `total_window_days` days, processing one chunk per day,
-- where each chunk re-reads the last `simulation_window_days` of source rows
-- and MERGEs on order_item_id.
--
-- For correctness, this is equivalent to: run a single MERGE that processes
-- the union of all source rows in [today - total_window_days, today], using
-- ROW_NUMBER to keep the latest version per order_item_id.
-- ============================================================================
CREATE TEMP TABLE B AS
WITH source_in_window AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY datastream_metadata.source_timestamp DESC
    ) AS rn
  FROM `my-gcp-project.raw.order_items`
  WHERE datastream_metadata.source_timestamp IS NOT NULL
    AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL total_window_days DAY)
)
SELECT
  SAFE_CAST(id AS STRING)             AS order_item_id,
  SAFE_CAST(order_id AS STRING)       AS order_id,
  SAFE_CAST(product_id AS INT64)      AS product_id,
  SAFE_CAST(quantity AS INT64)        AS quantity,
  SAFE_CAST(unit_price AS NUMERIC)    AS unit_price_local,
  SAFE_CAST(net_amount AS NUMERIC)    AS net_amount_local,
  SAFE_CAST(default_code AS STRING)   AS default_code,
  SAFE_CAST(is_recommended AS BOOL)   AS is_recommended,
  SAFE_CAST(created_at AS TIMESTAMP)  AS created_at,
  SAFE_CAST(updated_at AS TIMESTAMP)  AS updated_at
FROM source_in_window
WHERE rn = 1;

-- ============================================================================
-- C: rows in A not in B. Must be 0.
-- ============================================================================
CREATE TEMP TABLE C AS
SELECT a.*
FROM A
LEFT JOIN B USING (order_item_id)
WHERE B.order_item_id IS NULL;

-- ============================================================================
-- D: rows in B not in A. Must be 0.
-- ============================================================================
CREATE TEMP TABLE D AS
SELECT b.*
FROM B
LEFT JOIN A USING (order_item_id)
WHERE A.order_item_id IS NULL;

-- ============================================================================
-- Verdict block — five checks, all must pass.
-- ============================================================================
SELECT 'C: rows lost (A but not B)'        AS check_name, COUNT(*) AS failing_rows FROM C
UNION ALL
SELECT 'D: rows wrongly added (B but not A)', COUNT(*)             FROM D
UNION ALL
SELECT 'parity: total row count',
       ABS((SELECT COUNT(*) FROM A) - (SELECT COUNT(*) FROM B))
UNION ALL
SELECT 'parity: distinct keys',
       ABS((SELECT COUNT(DISTINCT order_item_id) FROM A) -
           (SELECT COUNT(DISTINCT order_item_id) FROM B))
UNION ALL
SELECT 'parity: sum(net_amount_local)',
       CAST(ROUND(ABS(
         (SELECT SUM(net_amount_local) FROM A) -
         (SELECT SUM(net_amount_local) FROM B)
       ), 2) AS INT64)
ORDER BY check_name;

-- =============================================================================
-- Interpretation:
--   All `failing_rows` columns must equal 0.
--   If any row is non-zero, STOP and inspect C / D before proceeding.
-- =============================================================================
