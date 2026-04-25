"""Tests for scripts/add_refs.py."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from scripts.add_refs import (
    apply_replacements,
    build_pattern,
    find_replacements,
    is_already_ref,
    is_inside_config_block,
)


class TestBuildPattern:
    def test_matches_qualified_name(self) -> None:
        pattern = build_pattern(["orders_clean"])
        match = pattern.search("SELECT * FROM `proj.silver.orders_clean`")
        assert match is not None
        assert match.group("table") == "orders_clean"

    def test_matches_unqualified_name(self) -> None:
        pattern = build_pattern(["orders_clean"])
        match = pattern.search("FROM orders_clean")
        assert match is not None
        assert match.group("table") == "orders_clean"

    def test_does_not_match_unknown_table(self) -> None:
        pattern = build_pattern(["orders_clean"])
        assert pattern.search("FROM some_other_table") is None


class TestIsInsideConfigBlock:
    def test_inside_config_block(self) -> None:
        text = 'config {\n  schema: "silver"\n  name: "orders_clean"\n}\nSELECT * FROM x'
        match_start = text.index("orders_clean")
        assert is_inside_config_block(text, match_start) is True

    def test_outside_config_block(self) -> None:
        text = 'config {\n  schema: "silver"\n}\nSELECT * FROM orders_clean'
        match_start = text.index("orders_clean")
        assert is_inside_config_block(text, match_start) is False

    def test_no_config_block(self) -> None:
        text = "SELECT * FROM orders_clean"
        match_start = text.index("orders_clean")
        assert is_inside_config_block(text, match_start) is False


class TestIsAlreadyRef:
    def test_already_wrapped(self) -> None:
        text = 'SELECT * FROM ${ref("orders_clean")}'
        match_start = text.index("orders_clean")
        match_end = match_start + len("orders_clean")
        assert is_already_ref(text, match_start, match_end) is True

    def test_not_wrapped(self) -> None:
        text = "SELECT * FROM orders_clean"
        match_start = text.index("orders_clean")
        match_end = match_start + len("orders_clean")
        assert is_already_ref(text, match_start, match_end) is False


class TestFindAndApplyReplacements:
    def test_replaces_hardcoded_ref(self) -> None:
        content = dedent("""
            config {
              schema: "gold"
              name: "fct_orders"
            }
            SELECT * FROM `proj.silver.orders_clean`
        """).strip()
        pattern = build_pattern(["orders_clean"])

        replacements = find_replacements(content, pattern, Path("fct_orders.sqlx"))
        assert len(replacements) == 1
        assert replacements[0].after == '${ref("orders_clean")}'

        updated = apply_replacements(content, pattern)
        assert '${ref("orders_clean")}' in updated
        assert "`proj.silver.orders_clean`" not in updated

    def test_skips_config_block_references(self) -> None:
        content = dedent("""
            config {
              schema: "silver"
              name: "orders_clean"
            }
            SELECT 1
        """).strip()
        pattern = build_pattern(["orders_clean"])
        replacements = find_replacements(content, pattern, Path("orders_clean.sqlx"))
        assert replacements == []

    def test_skips_already_wrapped(self) -> None:
        content = 'SELECT * FROM ${ref("orders_clean")}'
        pattern = build_pattern(["orders_clean"])
        replacements = find_replacements(content, pattern, Path("x.sqlx"))
        assert replacements == []
