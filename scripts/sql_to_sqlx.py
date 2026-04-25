"""Convert plain BigQuery SQL files into Dataform SQLX with a config block.

This utility was built during the original migration to bootstrap the move
from a folder of standalone .sql files (each a Scheduled Query) to a Dataform
project. It is opinionated:

- Detects PARTITION BY and CLUSTER BY clauses in the source SQL and lifts
  them into the SQLX `bigquery: { partitionBy, clusterBy }` config block.
- Strips trailing semicolons (Dataform compiles statements without them).
- Strips CREATE OR REPLACE TABLE / CREATE TABLE wrappers, since Dataform
  manages the materialization.
- Defaults to type:"table" — the user is expected to flip to "incremental"
  manually for high-volume tables (this is a deliberate choice; incremental
  is a design decision, not an automatic conversion).

Usage:
    python scripts/sql_to_sqlx.py path/to/source.sql --schema silver --name orders_clean
    python scripts/sql_to_sqlx.py path/to/source.sql --schema gold --name fct_orders \\
        --output-dir definitions/gold/
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

PARTITION_PATTERN = re.compile(
    r"PARTITION\s+BY\s+(?P<expr>[^\n;)]+?)(?=\s*(?:CLUSTER\s+BY|OPTIONS|AS|;|$))",
    re.IGNORECASE,
)
CLUSTER_PATTERN = re.compile(
    r"CLUSTER\s+BY\s+(?P<cols>[^\n;)]+?)(?=\s*(?:OPTIONS|AS|;|$))",
    re.IGNORECASE,
)
CREATE_TABLE_PATTERN = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+`?[\w.\-]+`?\s*"
    r"(?:\([^)]*\))?\s*"  # optional explicit schema
    r"(?:PARTITION\s+BY\s+[^\n]+)?\s*"
    r"(?:CLUSTER\s+BY\s+[^\n]+)?\s*"
    r"(?:OPTIONS\s*\([^)]*\))?\s*"
    r"AS\s*",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class ParsedSql:
    body: str
    partition_by: str | None
    cluster_by: list[str] | None


def parse_sql(raw_sql: str) -> ParsedSql:
    """Extract partitioning, clustering, and the inner SELECT from raw SQL."""
    partition_by: str | None = None
    cluster_by: list[str] | None = None

    if match := PARTITION_PATTERN.search(raw_sql):
        partition_by = match.group("expr").strip()

    if match := CLUSTER_PATTERN.search(raw_sql):
        cluster_by = [c.strip() for c in match.group("cols").split(",") if c.strip()]

    # Drop the CREATE TABLE wrapper if present, leaving the SELECT.
    body = CREATE_TABLE_PATTERN.sub("", raw_sql, count=1)
    body = body.rstrip().rstrip(";").strip()

    return ParsedSql(body=body, partition_by=partition_by, cluster_by=cluster_by)


def render_sqlx(
    parsed: ParsedSql,
    *,
    schema: str,
    name: str,
    description: str,
    tags: list[str],
) -> str:
    """Compose the final SQLX text from a parsed SQL body and metadata."""
    config_lines: list[str] = [
        '  type: "table",',
        f'  schema: "{schema}",',
        f'  name: "{name}",',
        f'  description: "{description}",',
    ]

    bq_lines: list[str] = []
    if parsed.partition_by:
        bq_lines.append(f'    partitionBy: "{parsed.partition_by}"')
    if parsed.cluster_by:
        cluster_repr = ", ".join(f'"{c}"' for c in parsed.cluster_by)
        bq_lines.append(f"    clusterBy: [{cluster_repr}]")

    if bq_lines:
        config_lines.append("  bigquery: {")
        config_lines.extend(line + "," for line in bq_lines[:-1])
        config_lines.append(bq_lines[-1])
        config_lines.append("  },")

    if tags:
        tag_repr = ", ".join(f'"{t}"' for t in tags)
        config_lines.append(f"  tags: [{tag_repr}]")
    elif config_lines[-1].endswith(","):
        # Strip trailing comma from the last config entry.
        config_lines[-1] = config_lines[-1].rstrip(",")

    config_block = "config {\n" + "\n".join(config_lines) + "\n}"
    return f"{config_block}\n\n{parsed.body}\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a BigQuery .sql file into a Dataform .sqlx file.",
    )
    parser.add_argument("source", type=Path, help="Path to the source .sql file.")
    parser.add_argument("--schema", required=True, help="Target dataset/schema.")
    parser.add_argument("--name", required=True, help="Target table name.")
    parser.add_argument(
        "--description",
        default="",
        help="Free-text description for the SQLX config block.",
    )
    parser.add_argument(
        "--tags",
        nargs="*",
        default=[],
        help="One or more tags (e.g. silver core).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("definitions"),
        help="Directory where the .sqlx file will be written.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if not args.source.is_file():
        print(f"error: source file not found: {args.source}", file=sys.stderr)
        return 1

    raw_sql = args.source.read_text(encoding="utf-8")
    parsed = parse_sql(raw_sql)

    sqlx = render_sqlx(
        parsed,
        schema=args.schema,
        name=args.name,
        description=args.description or f"Auto-generated SQLX for {args.schema}.{args.name}.",
        tags=args.tags,
    )

    output_path = args.output_dir / f"{args.name}.sqlx"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(sqlx, encoding="utf-8")
    print(f"wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
