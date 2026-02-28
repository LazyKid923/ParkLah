#!/usr/bin/env python3
"""Match LTA CarParkAvailabilityv2 entries to CarparkRates CSV rows."""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any

from serve_live_map import (
    aggregate_lta_availability_rows,
    build_lta_match_debug_payload,
    fetch_lta_carpark_availability,
    load_cache,
    load_carparks,
    match_lta_availability_to_carparks,
)

ROOT = Path(__file__).resolve().parent
DEFAULT_CSV = ROOT / "parking_rates" / "CarparkRates.csv"
DEFAULT_CACHE = ROOT / "parking_rates" / "carpark_coordinates_cache.json"
DEFAULT_ANNOTATED_CSV = ROOT / "parking_rates" / "CarparkRates_with_lta_match_v2.csv"
DEFAULT_DEBUG_JSON = ROOT / "parking_rates" / "lta_match_debug.json"
API_URL = "https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2"


def annotate_csv(
    input_csv: Path,
    output_csv: Path,
    matches: dict[int, dict[str, Any]],
) -> None:
    with input_csv.open(newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
        if not rows:
            raise RuntimeError(f"CSV has no rows: {input_csv}")
        base_fields = list(rows[0].keys())

    extra_fields = [
        "available_lots",
        "availability_source",
        "availability_source_ref",
        "availability_development",
        "availability_agency",
        "availability_match_method",
        "availability_lot_types",
    ]
    fieldnames = base_fields + [field for field in extra_fields if field not in base_fields]

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for idx, row in enumerate(rows, start=1):
            match = matches.get(idx, {})
            out = dict(row)
            for field in extra_fields:
                value = match.get(field)
                out[field] = "" if value is None else str(value)
            writer.writerow(out)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch LTA availability and write an annotated CarparkRates CSV."
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Input CarparkRates CSV")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE, help="Coordinate cache JSON")
    parser.add_argument("--output", type=Path, default=DEFAULT_ANNOTATED_CSV, help="Output annotated CSV")
    parser.add_argument(
        "--account-key",
        default=(os.environ.get("LTA_DATAMALL_ACCOUNT_KEY") or os.environ.get("LTA_ACCOUNT_KEY") or ""),
        help="LTA DataMall AccountKey (or env LTA_DATAMALL_ACCOUNT_KEY / LTA_ACCOUNT_KEY)",
    )
    parser.add_argument("--debug-json", type=Path, default=DEFAULT_DEBUG_JSON, help="Debug JSON output path")
    parser.add_argument(
        "--debug-max-rows",
        type=int,
        default=2000,
        help="Max unmatched rows per debug section",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.csv.exists():
        raise SystemExit(f"CSV not found: {args.csv}")
    if not args.account_key:
        raise SystemExit(
            "Missing LTA account key. Set --account-key or LTA_DATAMALL_ACCOUNT_KEY / LTA_ACCOUNT_KEY."
        )

    carparks = load_carparks(args.csv)
    cache = load_cache(args.cache)
    for cp in carparks:
        point = cache.get(cp.postal_code or "")
        if not point:
            continue
        cp.lat = point.get("lat")
        cp.lon = point.get("lon")

    rows = fetch_lta_carpark_availability(args.account_key)
    entries = aggregate_lta_availability_rows(rows)
    matches, stats, debug = match_lta_availability_to_carparks(carparks, entries)
    annotate_csv(args.csv, args.output, matches)

    debug_payload = build_lta_match_debug_payload(
        carparks_snapshot=carparks,
        entries=entries,
        match_stats=stats,
        match_debug=debug,
        max_rows=args.debug_max_rows,
    )
    debug_payload["endpoint"] = API_URL
    args.debug_json.parent.mkdir(parents=True, exist_ok=True)
    with args.debug_json.open("w", encoding="utf-8") as f:
        json.dump(debug_payload, f, indent=2, ensure_ascii=False)

    print(f"[done] matched {stats.get('matched_total', 0)} / {len(carparks)} carparks")
    print(f"[done] annotated csv: {args.output}")
    print(f"[done] debug json: {args.debug_json}")


if __name__ == "__main__":
    main()
