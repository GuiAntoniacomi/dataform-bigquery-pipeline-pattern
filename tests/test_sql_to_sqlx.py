"""Tests for scripts/sql_to_sqlx.py."""

from __future__ import annotations

from textwrap import dedent

from scripts.sql_to_sqlx import parse_sql, render_sqlx


class TestParseSql:
    def test_extracts_partition_and_cluster(self) -> None:
        raw = dedent("""
            CREATE OR REPLACE TABLE `proj.gold.fct_orders`
            PARTITION BY DATE(created_at)
            CLUSTER BY store_id, customer_id AS
            SELECT * FROM `proj.silver.orders_clean`
        """).strip()

        parsed = parse_sql(raw)

        assert parsed.partition_by == "DATE(created_at)"
        assert parsed.cluster_by == ["store_id", "customer_id"]
        assert parsed.body.startswith("SELECT")
        assert "CREATE" not in parsed.body

    def test_handles_missing_partition_and_cluster(self) -> None:
        raw = "SELECT 1 AS x"
        parsed = parse_sql(raw)

        assert parsed.partition_by is None
        assert parsed.cluster_by is None
        assert parsed.body == "SELECT 1 AS x"

    def test_strips_trailing_semicolon(self) -> None:
        raw = "SELECT 1 AS x;"
        parsed = parse_sql(raw)
        assert parsed.body == "SELECT 1 AS x"


class TestRenderSqlx:
    def test_renders_full_config_block(self) -> None:
        parsed = parse_sql(
            dedent("""
                CREATE OR REPLACE TABLE `proj.gold.fct_orders`
                PARTITION BY DATE(created_at)
                CLUSTER BY store_id AS
                SELECT * FROM `proj.silver.orders_clean`
            """).strip()
        )
        sqlx = render_sqlx(
            parsed,
            schema="gold",
            name="fct_orders",
            description="Orders fact table.",
            tags=["gold", "fct"],
        )

        assert 'type: "table"' in sqlx
        assert 'schema: "gold"' in sqlx
        assert 'name: "fct_orders"' in sqlx
        assert 'partitionBy: "DATE(created_at)"' in sqlx
        assert 'clusterBy: ["store_id"]' in sqlx
        assert 'tags: ["gold", "fct"]' in sqlx
        assert sqlx.endswith("\n")

    def test_omits_bigquery_block_when_no_partition_or_cluster(self) -> None:
        parsed = parse_sql("SELECT 1 AS x")
        sqlx = render_sqlx(
            parsed,
            schema="silver",
            name="trivial",
            description="Trivial table.",
            tags=[],
        )
        assert "bigquery: {" not in sqlx

    def test_no_trailing_comma_on_last_config_entry(self) -> None:
        parsed = parse_sql("SELECT 1")
        sqlx = render_sqlx(
            parsed,
            schema="silver",
            name="trivial",
            description="Trivial.",
            tags=[],
        )
        # The last line of the config block should not be a comma-only line.
        config_block = sqlx.split("config {")[1].split("}")[0]
        assert not any(line.strip().endswith(",,") for line in config_block.splitlines())
