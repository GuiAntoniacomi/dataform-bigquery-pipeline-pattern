"""Migrate hardcoded BigQuery table references in SQLX files to ${ref(...)}.

When converting a Scheduled Queries pipeline to Dataform, the most tedious
step is replacing every hardcoded `dataset.table` (or `project.dataset.table`)
reference inside transformations with `${ref("table")}`. This script
automates that — safely, reversibly, and with a dry-run mode.

Why it matters:
    A `${ref("orders_clean")}` is not just a string substitution. It declares
    a DAG edge. Once every reference uses `ref()`, Dataform can resolve
    execution order automatically, dead-code-detect unused tables, and
    reject PRs that introduce circular dependencies. Hardcoded refs are
    invisible to the DAG.

Usage:
    # Dry run — print what would change without writing.
    python scripts/add_refs.py definitions/ --known-tables orders_clean stores_clean

    # Apply.
    python scripts/add_refs.py definitions/ --known-tables orders_clean stores_clean --apply

The --known-tables list defines which table names are owned by this Dataform
project (i.e. which references should become refs). External tables stay
hardcoded.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Replacement:
    file: Path
    line_no: int
    before: str
    after: str


def build_pattern(known_tables: list[str]) -> re.Pattern[str]:
    """Build a regex matching `[project.][dataset.]<table>` for known tables.

    Matches both backtick-quoted and unquoted forms:
        `myproj.silver.orders_clean`
        myproj.silver.orders_clean
        silver.orders_clean

    Skips matches inside config { ... } blocks (the schema/name fields are
    already structured) and skips matches that are already inside ${ref(...)}.
    """
    table_alt = "|".join(re.escape(t) for t in sorted(known_tables, key=len, reverse=True))
    return re.compile(
        rf"`?(?:[\w-]+\.)?(?:[\w]+\.)?(?P<table>{table_alt})`?",
    )


def is_inside_config_block(text: str, match_start: int) -> bool:
    """True if `match_start` is inside the SQLX config { ... } block."""
    config_start = text.find("config {")
    if config_start == -1 or match_start < config_start:
        return False
    # Find the matching close brace by depth.
    depth = 0
    for idx in range(config_start, len(text)):
        if text[idx] == "{":
            depth += 1
        elif text[idx] == "}":
            depth -= 1
            if depth == 0:
                return match_start < idx
    return False


def is_already_ref(text: str, match_start: int, match_end: int) -> bool:
    """True if the match is already wrapped in ${ref(...)}."""
    window_start = max(0, match_start - 16)
    window_end = min(len(text), match_end + 4)
    window = text[window_start:window_end]
    return "${ref(" in window and ")}" in window[match_end - window_start :]


def find_replacements(content: str, pattern: re.Pattern[str], file: Path) -> list[Replacement]:
    """Return all replacements that should be made in a single SQLX file."""
    replacements: list[Replacement] = []
    for match in pattern.finditer(content):
        if is_inside_config_block(content, match.start()):
            continue
        if is_already_ref(content, match.start(), match.end()):
            continue

        before = match.group(0)
        after = f'${{ref("{match.group("table")}")}}'
        if before == after:
            continue

        line_no = content.count("\n", 0, match.start()) + 1
        replacements.append(Replacement(file=file, line_no=line_no, before=before, after=after))
    return replacements


def apply_replacements(content: str, pattern: re.Pattern[str]) -> str:
    """Apply the same logic as find_replacements but return the new content."""

    def _sub(match: re.Match[str]) -> str:
        text = match.string
        if is_inside_config_block(text, match.start()):
            return match.group(0)
        if is_already_ref(text, match.start(), match.end()):
            return match.group(0)
        return f'${{ref("{match.group("table")}")}}'

    return pattern.sub(_sub, content)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate hardcoded refs to ${ref(...)} across SQLX files.",
    )
    parser.add_argument("path", type=Path, help="Directory containing .sqlx files.")
    parser.add_argument(
        "--known-tables",
        nargs="+",
        required=True,
        help="Table names owned by this Dataform project.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to disk. Without this flag, only prints a dry-run report.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if not args.path.is_dir():
        print(f"error: not a directory: {args.path}", file=sys.stderr)
        return 1

    pattern = build_pattern(args.known_tables)
    sqlx_files = sorted(args.path.rglob("*.sqlx"))
    if not sqlx_files:
        print(f"warning: no .sqlx files found under {args.path}", file=sys.stderr)
        return 0

    total_replacements = 0
    files_changed = 0
    for sqlx_file in sqlx_files:
        original = sqlx_file.read_text(encoding="utf-8")
        replacements = find_replacements(original, pattern, sqlx_file)
        if not replacements:
            continue
        files_changed += 1
        total_replacements += len(replacements)

        relative = sqlx_file.relative_to(args.path)
        for r in replacements:
            print(f"{relative}:{r.line_no}  {r.before!r}  →  {r.after!r}")

        if args.apply:
            updated = apply_replacements(original, pattern)
            sqlx_file.write_text(updated, encoding="utf-8")

    print(
        f"\n{total_replacements} replacement(s) across {files_changed} file(s)."
        f" {'applied' if args.apply else 'dry-run only — pass --apply to write.'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
