"""Generate realistic mock data for the Dataform pipeline pattern.

Produces five CSVs that mimic the shape of CDC-style raw tables:
    - raw_orders.csv
    - raw_order_items.csv
    - raw_stores.csv
    - raw_currency_exchange_rates.csv

The data is intentionally messy:
    - duplicate rows (same id, multiple source_timestamps)
    - late-arriving rows (created_at < datastream_metadata.source_timestamp by hours)
    - some null product_ids (so the silver dedup actually has work to do)
    - mixed currency codes per country

This way, running the silver layer on top of these CSVs is a meaningful
exercise — not a SELECT * passthrough.

Usage:
    python scripts/generate_mock_data.py --output-dir mock_data/ --num-orders 10000

Then either load the CSVs to BigQuery via:
    bq load --source_format=CSV --autodetect raw.orders mock_data/raw_orders.csv
"""

from __future__ import annotations

import argparse
import csv
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Reproducibility — the same seed always generates the same dataset.
SEED = 42

COUNTRIES: list[tuple[str, str, str]] = [
    # (country_code, currency_code, timezone)
    ("BR", "BRL", "America/Sao_Paulo"),
    ("US", "USD", "America/New_York"),
    ("GB", "GBP", "Europe/London"),
    ("JP", "JPY", "Asia/Tokyo"),
    ("DE", "EUR", "Europe/Berlin"),
    ("FR", "EUR", "Europe/Paris"),
    ("CZ", "CZK", "Europe/Prague"),
    ("CO", "COP", "America/Bogota"),
    ("KR", "KRW", "Asia/Seoul"),
    ("SG", "SGD", "Asia/Singapore"),
]

CHANNELS = ["app", "pos", "delivery", "kiosk"]
STATUSES = ["paid", "paid", "paid", "paid", "refunded", "cancelled"]
RATE_TO_USD = {
    "USD": 1.0,
    "BRL": 0.20,
    "GBP": 1.27,
    "JPY": 0.0067,
    "EUR": 1.08,
    "CZK": 0.044,
    "COP": 0.00026,
    "KRW": 0.00075,
    "SGD": 0.74,
}


@dataclass(frozen=True)
class Store:
    store_id: int
    name: str
    country_code: str
    currency_code: str
    timezone: str
    is_franchise: bool
    is_dev: bool
    is_excluded_from_dw: bool
    opening_date: str


def generate_stores(num_stores: int, rng: random.Random) -> list[Store]:
    stores: list[Store] = []
    for store_id in range(1, num_stores + 1):
        country_code, currency_code, tz = rng.choice(COUNTRIES)
        opening_offset_days = rng.randint(30, 5 * 365)
        opening_date = (datetime.now(timezone.utc) - timedelta(days=opening_offset_days)).date()
        stores.append(
            Store(
                store_id=store_id,
                name=f"Store-{country_code}-{store_id:04d}",
                country_code=country_code,
                currency_code=currency_code,
                timezone=tz,
                is_franchise=rng.random() < 0.65,
                # 5% dev stores, 2% explicitly excluded — silver must filter these.
                is_dev=rng.random() < 0.05,
                is_excluded_from_dw=rng.random() < 0.02,
                opening_date=opening_date.isoformat(),
            )
        )
    return stores


def write_stores(stores: list[Store], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "id",
                "name",
                "country_code",
                "currency_code",
                "timezone",
                "is_franchise",
                "is_dev",
                "is_excluded_from_dw",
                "opening_date",
                "created_at",
                "updated_at",
                "datastream_metadata.source_timestamp",
            ]
        )
        now = datetime.now(timezone.utc)
        for s in stores:
            writer.writerow(
                [
                    s.store_id,
                    s.name,
                    s.country_code,
                    s.currency_code,
                    s.timezone,
                    s.is_franchise,
                    s.is_dev,
                    s.is_excluded_from_dw,
                    s.opening_date,
                    f"{s.opening_date}T00:00:00Z",
                    now.isoformat(),
                    now.isoformat(),
                ]
            )


def write_orders_and_items(
    stores: list[Store],
    num_orders: int,
    orders_path: Path,
    items_path: Path,
    rng: random.Random,
) -> None:
    now = datetime.now(timezone.utc)
    eligible_stores = [s for s in stores if not s.is_dev and not s.is_excluded_from_dw]

    with orders_path.open("w", newline="", encoding="utf-8") as o_file, \
         items_path.open("w", newline="", encoding="utf-8") as i_file:
        o = csv.writer(o_file)
        i = csv.writer(i_file)

        o.writerow([
            "id", "store_id", "customer_id", "currency_code", "total_price",
            "discount", "net_amount", "channel", "status",
            "created_at", "updated_at", "datastream_metadata.source_timestamp",
        ])
        i.writerow([
            "id", "order_id", "product_id", "quantity", "unit_price",
            "net_amount", "default_code", "is_recommended",
            "created_at", "updated_at", "datastream_metadata.source_timestamp",
        ])

        for order_idx in range(num_orders):
            store = rng.choice(eligible_stores)
            order_id = str(uuid.UUID(int=rng.getrandbits(128)))
            customer_id = rng.randint(1, 50_000)
            offset_days = rng.randint(0, 89)
            offset_seconds = rng.randint(0, 86_399)
            created_at = now - timedelta(days=offset_days, seconds=offset_seconds)

            num_items = rng.randint(1, 5)
            unit_prices = [round(rng.uniform(3.0, 35.0), 2) for _ in range(num_items)]
            quantities = [rng.randint(1, 3) for _ in range(num_items)]
            line_totals = [u * q for u, q in zip(unit_prices, quantities)]
            gross = round(sum(line_totals), 2)
            discount = round(rng.uniform(0, gross * 0.20), 2)
            net = round(gross - discount, 2)

            # Late-arriving simulation: 3% of orders show up in CDC up to 36h
            # after their created_at. Silver must absorb these via the 7-day
            # retroactive window.
            late_offset = timedelta(hours=rng.randint(0, 36)) if rng.random() < 0.03 else timedelta(0)
            source_ts = created_at + late_offset

            order_row = [
                order_id,
                store.store_id,
                customer_id,
                store.currency_code,
                gross,
                discount,
                net,
                rng.choice(CHANNELS),
                rng.choice(STATUSES),
                created_at.isoformat(),
                created_at.isoformat(),
                source_ts.isoformat(),
            ]
            o.writerow(order_row)

            # 8% of orders get a duplicate CDC event with a newer source_timestamp.
            # Silver must keep only the latest per id (ROW_NUMBER dedup).
            if rng.random() < 0.08:
                o.writerow([
                    *order_row[:-1],
                    (source_ts + timedelta(seconds=rng.randint(60, 3600))).isoformat(),
                ])

            for line_idx in range(num_items):
                item_id = str(uuid.UUID(int=rng.getrandbits(128)))
                # 0.5% missing product_id — silver must SAFE_CAST and tolerate.
                product_id = "" if rng.random() < 0.005 else rng.randint(100, 999)
                line_net = round(line_totals[line_idx] * (1 - discount / max(gross, 0.01)), 2)
                i.writerow([
                    item_id,
                    order_id,
                    product_id,
                    quantities[line_idx],
                    unit_prices[line_idx],
                    line_net,
                    f"SKU-{rng.randint(1000, 9999)}",
                    rng.random() < 0.18,  # ~18% of items are model-recommended
                    created_at.isoformat(),
                    created_at.isoformat(),
                    source_ts.isoformat(),
                ])


def write_currencies(path: Path) -> None:
    """Write a 90-day rate snapshot per currency."""
    now = datetime.now(timezone.utc).date()
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["currency_code", "exchange_date", "rate_to_usd"])
        for code, base_rate in RATE_TO_USD.items():
            for offset in range(90):
                date = now - timedelta(days=offset)
                # Tiny daily jitter to look real — within ±0.5%.
                jitter = 1 + ((offset * 7) % 11 - 5) * 0.001
                w.writerow([code, date.isoformat(), round(base_rate * jitter, 6)])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate mock CDC-style data for the Dataform pattern.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("mock_data"),
        help="Directory where CSVs will be written.",
    )
    parser.add_argument("--num-orders", type=int, default=10_000)
    parser.add_argument("--num-stores", type=int, default=80)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(SEED)
    stores = generate_stores(args.num_stores, rng)

    write_stores(stores, args.output_dir / "raw_stores.csv")
    write_currencies(args.output_dir / "raw_currency_exchange_rates.csv")
    write_orders_and_items(
        stores,
        args.num_orders,
        args.output_dir / "raw_orders.csv",
        args.output_dir / "raw_order_items.csv",
        rng,
    )

    print(f"generated mock data in {args.output_dir.resolve()}")
    print(f"  - {args.num_stores} stores")
    print(f"  - {args.num_orders} orders (with ~8% duplicates, ~3% late-arriving)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
