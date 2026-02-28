#!/usr/bin/env python3
"""Serve a localhost map with live user location and CSV carpark markers."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

from calc_parking_cost import (
    SG_TZ as CALC_SG_TZ,
    day_type_for_dt as calc_day_type_for_dt,
    estimate_cost as calc_estimate_cost,
    find_active_period as calc_find_active_period,
    mins_until_period_end as calc_mins_until_period_end,
    parse_periods as calc_parse_periods,
    pick_day_text as calc_pick_day_text,
)

ROOT = Path(__file__).resolve().parent
WEB_DIR = ROOT / "web"
DEFAULT_CSV = ROOT / "parking_rates" / "CarparkRates.csv"
DEFAULT_CACHE = ROOT / "parking_rates" / "carpark_coordinates_cache.json"
USER_AGENT = "ParkLahLiveMap/0.1 (+localhost)"
PRICE_ESTIMATE_MINUTES = 60
MAX_STAY_MINUTES = 24 * 60
LTA_CARPARK_AVAILABILITY_URL = "https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2"
LTA_FETCH_PAGE_SIZE = 500
DEFAULT_LTA_REFRESH_SEC = 60.0
DEFAULT_URA_REFRESH_SEC = 60.0
LTA_COORD_MATCH_MAX_KM = 0.22
LTA_COORD_AMBIGUOUS_GAP_KM = 0.02
LTA_COORD_STRICT_DISTANCE_KM = 0.03
LTA_POSTAL_COORD_MATCH_MAX_KM = 0.15
LTA_NAME_MATCH_MIN_SCORE = 0.62
DEFAULT_LTA_MATCH_DEBUG_LOG = ROOT / "parking_rates" / "lta_match_debug.json"
DEFAULT_LTA_MATCH_DEBUG_MAX_ROWS = 2000
PLACE_SEARCH_MAX_RESULTS = 10
URA_TOKEN_URL = "https://www.ura.gov.sg/uraDataService/insertNewToken.action"
URA_INVOKE_URL = "https://www.ura.gov.sg/uraDataService/invokeUraDS"

MATCH_TOKEN_ALIASES = {
    "RD": "ROAD",
    "ST": "STREET",
    "AVE": "AVENUE",
    "BLVD": "BOULEVARD",
    "CTR": "CENTRE",
    "CTRE": "CENTRE",
    "PK": "PARK",
    "INTL": "INTERNATIONAL",
    "BLDG": "BUILDING",
    "MT": "MOUNT",
    "NTH": "NORTH",
    "STH": "SOUTH",
}

MATCH_STOPWORDS = {
    "SINGAPORE",
    "THE",
    "AND",
    "AT",
    "CARPARK",
    "CARPARKS",
    "PARKING",
    "LOT",
    "LOTS",
    "CAR",
    "BLOCK",
    "BLK",
    "LEVEL",
    "CENTRE",
    "CENTER",
    "BUILDING",
    "MALL",
    "PLAZA",
    "SQUARE",
    "COMPLEX",
    "TOWER",
    "HUB",
    "OFF",
    "ROAD",
    "STREET",
    "AVENUE",
    "DRIVE",
    "LANE",
    "CRESCENT",
    "BOULEVARD",
    "JALAN",
    "LORONG",
    "CENTRAL",
    "UPPER",
    "LOWER",
    "EAST",
    "WEST",
    "NORTH",
    "SOUTH",
}

BASE_RATE_KEYS = {
    "weekdays_rate_1",
    "weekdays_rate_2",
    "saturday_rate",
    "sunday_publicholiday_rate",
}

AVAILABILITY_SOURCE_PRIORITY = {
    "ura_direct": 2,
    "lta_datamall": 1,
}


@dataclass
class Carpark:
    id: int
    carpark: str
    address: str
    postal_code: str
    weekdays_rate_1: str
    weekdays_rate_2: str
    saturday_rate: str
    sunday_publicholiday_rate: str
    extra_rate_fields: dict[str, str] = field(default_factory=dict)
    lat: float | None = None
    lon: float | None = None

    def to_dict(self) -> dict[str, Any]:
        data = {
            "id": self.id,
            "carpark": self.carpark,
            "address": self.address,
            "postal_code": self.postal_code,
            "weekdays_rate_1": self.weekdays_rate_1,
            "weekdays_rate_2": self.weekdays_rate_2,
            "saturday_rate": self.saturday_rate,
            "sunday_publicholiday_rate": self.sunday_publicholiday_rate,
            "lat": self.lat,
            "lon": self.lon,
        }
        data.update(self.extra_rate_fields)
        return data


@dataclass(frozen=True)
class LtaAvailabilityEntry:
    entry_id: str
    source: str
    source_ref: str
    key: str
    development: str
    available_lots: int | None
    lat: float | None
    lon: float | None
    area: str
    agency: str
    lot_types: tuple[str, ...]
    tokens: tuple[str, ...]


class Svy21Converter:
    """SVY21 (EPSG:3414) to WGS84 converter."""

    def __init__(self) -> None:
        from math import pi, radians

        self.a = 6378137.0
        self.f = 1 / 298.257223563
        self.b = self.a * (1 - self.f)
        self.o_lat = radians(1.366666)
        self.o_lon = radians(103.833333)
        self.o_n = 38744.572
        self.o_e = 28001.642
        self.k = 1.0

        self.e2 = (2 * self.f) - (self.f * self.f)
        self.e4 = self.e2 * self.e2
        self.e6 = self.e4 * self.e2
        self.a0 = 1 - (self.e2 / 4) - (3 * self.e4 / 64) - (5 * self.e6 / 256)
        self.a2 = (3 / 8) * (self.e2 + (self.e4 / 4) + (15 * self.e6 / 128))
        self.a4 = (15 / 256) * (self.e4 + (3 * self.e6 / 4))
        self.a6 = 35 * self.e6 / 3072
        self.n = (self.a - self.b) / (self.a + self.b)
        self.m0 = self._calc_m(self.o_lat)
        self._pi = pi

    def _calc_m(self, lat: float) -> float:
        from math import sin

        return self.a * (
            self.a0 * lat
            - self.a2 * sin(2 * lat)
            + self.a4 * sin(4 * lat)
            - self.a6 * sin(6 * lat)
        )

    def to_latlon(self, easting: float, northing: float) -> tuple[float, float]:
        from math import atan2, cos, degrees, pi, radians, sin, sqrt, tan

        n_prime = northing - self.o_n
        m_prime = self.m0 + (n_prime / self.k)

        n = self.n
        g = (
            self.a
            * (1 - n)
            * (1 - n * n)
            * (1 + (9 * n * n / 4) + (225 * n**4 / 64))
            * (pi / 180)
        )
        sigma = (m_prime * pi) / (180 * g)
        lat_prime = (
            sigma
            + ((3 * n / 2) - (27 * n**3 / 32)) * sin(2 * sigma)
            + ((21 * n * n / 16) - (55 * n**4 / 32)) * sin(4 * sigma)
            + (151 * n**3 / 96) * sin(6 * sigma)
            + (1097 * n**4 / 512) * sin(8 * sigma)
        )

        sin_lat = sin(lat_prime)
        sin2 = sin_lat * sin_lat
        rho = self.a * (1 - self.e2) / ((1 - self.e2 * sin2) ** 1.5)
        v = self.a / sqrt(1 - self.e2 * sin2)
        psi = v / rho
        t = tan(lat_prime)
        e_prime = easting - self.o_e

        x = e_prime / (self.k * v)
        x2 = x * x
        x3 = x2 * x
        x5 = x3 * x2
        x7 = x5 * x2

        lat_factor = t / (self.k * rho)
        term1 = (e_prime * x) / 2
        term2 = (e_prime * x3 / 24) * (
            (-4 * psi**2) + (9 * psi * (1 - t**2)) + (12 * t**2)
        )
        term3 = (e_prime * x5 / 720) * (
            (8 * psi**4 * (11 - 24 * t**2))
            - (12 * psi**3 * (21 - 71 * t**2))
            + (15 * psi**2 * (15 - 98 * t**2 + 15 * t**4))
            + (180 * psi * (5 * t**2 - 3 * t**4))
            + (360 * t**4)
        )
        term4 = (e_prime * x7 / 40320) * (
            1385 + (3633 * t**2) + (4095 * t**4) + (1575 * t**6)
        )
        lat = lat_prime - lat_factor * (term1 - term2 + term3 - term4)

        sec_lat = 1 / cos(lat_prime)
        lon_term1 = x
        lon_term2 = (x3 / 6) * (psi + 2 * t**2)
        lon_term3 = (x5 / 120) * (
            (-4 * psi**3 * (1 - 6 * t**2))
            + (psi**2 * (9 - 68 * t**2))
            + (72 * psi * t**2)
            + (24 * t**4)
        )
        lon_term4 = (x7 / 5040) * (
            61 + (662 * t**2) + (1320 * t**4) + (720 * t**6)
        )
        lon = self.o_lon + sec_lat * (lon_term1 - lon_term2 + lon_term3 - lon_term4)
        return (degrees(lat), degrees(lon))


class AppState:
    def __init__(
        self,
        carparks: list[Carpark],
        cache: dict[str, dict[str, float]],
        lta_account_key: str = "",
        lta_refresh_sec: float = DEFAULT_LTA_REFRESH_SEC,
        ura_access_key: str = "",
        ura_refresh_sec: float = DEFAULT_URA_REFRESH_SEC,
        lta_match_debug_log: Path | None = DEFAULT_LTA_MATCH_DEBUG_LOG,
        lta_match_debug_max_rows: int = DEFAULT_LTA_MATCH_DEBUG_MAX_ROWS,
    ) -> None:
        self.carparks = carparks
        self.cache = cache
        self.lock = threading.Lock()
        self.geocode_total = 0
        self.geocode_done = 0
        self.geocode_failed = 0
        self.geocode_running = False
        self.geocode_started_at: float | None = None
        self.price_snapshot_minute: str | None = None
        self.price_snapshot_by_key: dict[str, dict[int, dict[str, Any]]] = {}
        self.lta_account_key = normalize_account_key(lta_account_key)
        self.lta_enabled = bool(self.lta_account_key)
        self.lta_refresh_sec = max(15.0, float(lta_refresh_sec))
        self.ura_access_key = normalize_account_key(ura_access_key)
        self.ura_enabled = bool(self.ura_access_key)
        self.ura_refresh_sec = max(15.0, float(ura_refresh_sec))
        self.lta_match_debug_log = lta_match_debug_log
        self.lta_match_debug_max_rows = max(1, int(lta_match_debug_max_rows))
        self.availability_snapshot_by_carpark_id: dict[int, dict[str, Any]] = {}
        self.availability_snapshot_at: str | None = None
        self.availability_last_fetch_ts: float | None = None
        self.availability_last_error: str | None = None
        self.availability_match_stats: dict[str, Any] = {}
        self.availability_match_debug_summary: dict[str, Any] = {}

    def refresh_pricing_snapshot(
        self,
        estimate_minutes: int = PRICE_ESTIMATE_MINUTES,
        start_minute: int | None = None,
    ) -> None:
        estimate_minutes = max(1, min(MAX_STAY_MINUTES, estimate_minutes))
        now = datetime.now(CALC_SG_TZ) if CALC_SG_TZ else datetime.now()
        now = now.replace(second=0, microsecond=0)
        minute_key = now.strftime("%Y-%m-%d %H:%M")
        snapshot_key = make_snapshot_key(estimate_minutes, start_minute)

        with self.lock:
            if self.price_snapshot_minute != minute_key:
                self.price_snapshot_minute = minute_key
                self.price_snapshot_by_key = {}
            if snapshot_key in self.price_snapshot_by_key:
                return
            carparks_snapshot = list(self.carparks)

        estimate_start = resolve_estimate_start(now, start_minute)
        day_type = calc_day_type_for_dt(estimate_start, None)
        current_min = estimate_start.hour * 60 + estimate_start.minute
        snapshot: dict[int, dict[str, Any]] = {}

        for cp in carparks_snapshot:
            row = {
                "carpark": cp.carpark,
                "address": cp.address,
                "postal_code": cp.postal_code,
                "weekdays_rate_1": cp.weekdays_rate_1,
                "weekdays_rate_2": cp.weekdays_rate_2,
                "saturday_rate": cp.saturday_rate,
                "sunday_publicholiday_rate": cp.sunday_publicholiday_rate,
                **cp.extra_rate_fields,
            }

            rate_now_rule = "-"
            try:
                rate_text = calc_pick_day_text(row, day_type)
                periods = calc_parse_periods(rate_text)
                active_period = calc_find_active_period(periods, current_min) if periods else None
                if active_period and active_period.rule and active_period.rule.raw:
                    rate_now_rule = active_period.rule.raw
            except Exception:
                pass

            price_now_estimate: float | None = None
            price_now_label = "Price unavailable"
            breakdown_segments: list[dict[str, Any]] = []
            relevant_rate_segments: list[dict[str, Any]] = []
            try:
                relevant_rate_segments = build_relevant_rate_segments(row, estimate_start, estimate_minutes)
            except Exception:
                pass
            try:
                estimate_total, breakdown = calc_estimate_cost(row, estimate_start, estimate_minutes, None)
                price_now_estimate = estimate_total
                price_now_label = f"${estimate_total:.2f} / {estimate_minutes} mins"
                breakdown_segments = build_breakdown_segments(estimate_start, breakdown)
            except Exception as exc:
                msg = str(exc).lower()
                if "no parking" in msg or "closed" in msg:
                    price_now_label = "Closed now"
                elif "no usable rate text" in msg or "could not locate active pricing period" in msg:
                    price_now_label = "No active rate"
                elif "unsupported pricing format" in msg:
                    price_now_label = "Unsupported format"

            snapshot[cp.id] = {
                "price_now_label": price_now_label,
                "price_now_estimate": price_now_estimate,
                "price_now_estimate_minutes": estimate_minutes,
                "price_now_estimate_60min": price_now_estimate if estimate_minutes == PRICE_ESTIMATE_MINUTES else None,
                "rate_now_rule": rate_now_rule,
                "price_day_type": day_type,
                "price_evaluated_at": estimate_start.isoformat(),
                "price_breakdown_segments": breakdown_segments,
                "price_relevant_rate_segments": relevant_rate_segments,
            }

        with self.lock:
            if self.price_snapshot_minute != minute_key:
                self.price_snapshot_minute = minute_key
                self.price_snapshot_by_key = {}
            self.price_snapshot_by_key[snapshot_key] = snapshot

    def _availability_refresh_interval(self) -> float:
        intervals: list[float] = []
        if self.lta_enabled:
            intervals.append(self.lta_refresh_sec)
        if self.ura_enabled:
            intervals.append(self.ura_refresh_sec)
        if intervals:
            return min(intervals)
        return DEFAULT_LTA_REFRESH_SEC

    def refresh_availability_snapshot(self, force: bool = False) -> None:
        if not (self.lta_enabled or self.ura_enabled):
            return

        now_ts = time.time()
        refresh_interval_sec = self._availability_refresh_interval()
        with self.lock:
            should_refresh = force
            if self.availability_last_fetch_ts is None:
                should_refresh = True
            elif (now_ts - self.availability_last_fetch_ts) >= refresh_interval_sec:
                should_refresh = True
            if not should_refresh:
                return
            carparks_snapshot = list(self.carparks)

        source_stats: dict[str, Any] = {}
        source_errors: list[str] = []
        entries: list[LtaAvailabilityEntry] = []
        lta_rows: list[dict[str, Any]] = []

        if self.lta_enabled:
            try:
                lta_rows = fetch_lta_carpark_availability(self.lta_account_key)
                lta_entries = aggregate_lta_availability_rows(lta_rows)
                entries.extend(lta_entries)
                source_stats["lta_source_rows"] = len(lta_rows)
                source_stats["lta_entries"] = len(lta_entries)
            except Exception as exc:
                source_errors.append(f"lta: {exc}")

        if self.ura_enabled:
            try:
                ura_entries, ura_stats = fetch_ura_carpark_availability(self.ura_access_key)
                entries.extend(ura_entries)
                source_stats["ura_entries"] = len(ura_entries)
                source_stats["ura_token_ok"] = bool(ura_stats.get("token_ok"))
                source_stats["ura_availability_rows"] = int(ura_stats.get("availability_rows") or 0)
                source_stats["ura_detail_rows"] = int(ura_stats.get("detail_rows") or 0)
                if ura_stats.get("errors"):
                    source_errors.extend(f"ura: {msg}" for msg in ura_stats["errors"])
            except Exception as exc:
                source_errors.append(f"ura: {exc}")

        if source_errors and not entries:
            with self.lock:
                self.availability_last_fetch_ts = now_ts
                self.availability_last_error = " ; ".join(source_errors)
            return

        mapping, match_stats, match_debug = match_lta_availability_to_carparks(carparks_snapshot, entries)
        debug_payload = build_lta_match_debug_payload(
            carparks_snapshot=carparks_snapshot,
            entries=entries,
            match_stats=match_stats,
            match_debug=match_debug,
            max_rows=self.lta_match_debug_max_rows,
        )
        with self.lock:
            self.availability_snapshot_by_carpark_id = mapping
            self.availability_snapshot_at = utc_now_iso()
            self.availability_last_fetch_ts = now_ts
            self.availability_last_error = " ; ".join(source_errors) if source_errors else None
            self.availability_match_stats = {
                **source_stats,
                "aggregated_entries": len(entries),
                **match_stats,
            }
            self.availability_match_debug_summary = {
                "csv_unmatched_total": int(debug_payload["csv_unmatched_total"]),
                "api_unmatched_total": int(debug_payload["api_unmatched_total"]),
                "debug_log_path": str(self.lta_match_debug_log) if self.lta_match_debug_log else None,
                "csv_unmatched_logged": int(debug_payload["csv_unmatched_logged"]),
                "api_unmatched_logged": int(debug_payload["api_unmatched_logged"]),
            }
        if self.lta_match_debug_log:
            try:
                save_lta_match_debug_log(self.lta_match_debug_log, debug_payload)
            except Exception as exc:
                print(f"[availability-match] Failed to write debug log: {exc}")
        print(
            "[availability-match] "
            f"matched={match_stats['matched_total']}/{len(carparks_snapshot)}, "
            f"csv_unmatched={debug_payload['csv_unmatched_total']}, "
            f"api_unmatched={debug_payload['api_unmatched_total']}"
        )

    def get_carparks(
        self,
        include_unlocated: bool,
        estimate_minutes: int = PRICE_ESTIMATE_MINUTES,
        start_minute: int | None = None,
    ) -> list[dict[str, Any]]:
        snapshot_key = make_snapshot_key(estimate_minutes, start_minute)
        with self.lock:
            carparks = list(self.carparks)
            pricing = dict(self.price_snapshot_by_key.get(snapshot_key, {}))
            availability = dict(self.availability_snapshot_by_carpark_id)
            availability_snapshot_at = self.availability_snapshot_at

        rows: list[dict[str, Any]] = []
        for cp in carparks:
            if not include_unlocated and (cp.lat is None or cp.lon is None):
                continue
            row = cp.to_dict()
            row.update(pricing.get(cp.id, {}))
            row["available_lots"] = None
            row["availability_source"] = None
            row["availability_source_ref"] = None
            row["availability_development"] = None
            row["availability_agency"] = None
            row["availability_match_method"] = None
            row["availability_lot_types"] = None
            if availability_snapshot_at:
                row["availability_updated_at"] = availability_snapshot_at
            row.update(availability.get(cp.id, {}))
            rows.append(row)
        return rows

    def get_status(self) -> dict[str, Any]:
        with self.lock:
            total = len(self.carparks)
            with_coords = sum(1 for cp in self.carparks if cp.lat is not None and cp.lon is not None)
            return {
                "timestamp": utc_now_iso(),
                "total_carparks": total,
                "carparks_with_coordinates": with_coords,
                "geocode_running": self.geocode_running,
                "geocode_total": self.geocode_total,
                "geocode_done": self.geocode_done,
                "geocode_failed": self.geocode_failed,
                "geocode_started_at": self.geocode_started_at,
                "price_snapshot_minute": self.price_snapshot_minute,
                "price_snapshot_keys_cached": sorted(self.price_snapshot_by_key.keys()),
                "availability_enabled": self.lta_enabled or self.ura_enabled,
                "availability_last_error": self.availability_last_error,
                "availability_match_stats": self.availability_match_stats,
                "availability_match_debug_summary": self.availability_match_debug_summary,
                "lta_enabled": self.lta_enabled,
                "lta_refresh_sec": self.lta_refresh_sec,
                "ura_enabled": self.ura_enabled,
                "ura_refresh_sec": self.ura_refresh_sec,
                "lta_availability_updated_at": self.availability_snapshot_at,
                "lta_availability_matched_carparks": len(self.availability_snapshot_by_carpark_id),
                "lta_availability_last_error": self.availability_last_error,
                "lta_availability_match_stats": self.availability_match_stats,
                "lta_availability_match_debug_summary": self.availability_match_debug_summary,
            }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_snapshot_key(estimate_minutes: int, start_minute: int | None) -> str:
    start_key = "now" if start_minute is None else str(start_minute)
    return f"{estimate_minutes}:{start_key}"


def resolve_estimate_start(now: datetime, start_minute: int | None) -> datetime:
    if start_minute is None:
        return now
    candidate = now.replace(
        hour=start_minute // 60,
        minute=start_minute % 60,
        second=0,
        microsecond=0,
    )
    if candidate < now:
        candidate += timedelta(days=1)
    return candidate


def format_time_label(dt: datetime) -> str:
    hour24 = dt.hour
    minute = dt.minute
    suffix = "am" if hour24 < 12 else "pm"
    hour12 = hour24 % 12
    if hour12 == 0:
        hour12 = 12
    if minute == 0:
        return f"{hour12}{suffix}"
    return f"{hour12}:{minute:02d}{suffix}"


def build_breakdown_segments(start_dt: datetime, breakdown: list[dict[str, Any]]) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    cursor = start_dt
    for item in breakdown:
        seg_mins = int(item.get("mins") or 0)
        if seg_mins <= 0:
            continue
        seg_end = cursor + timedelta(minutes=seg_mins)
        segments.append(
            {
                "from_iso": cursor.isoformat(),
                "to_iso": seg_end.isoformat(),
                "from_label": format_time_label(cursor),
                "to_label": format_time_label(seg_end),
                "mins": seg_mins,
                "cost": float(item.get("cost") or 0.0),
                "rule": item.get("rule") or "",
                "mode": item.get("mode") or "",
                "day_type": item.get("day_type") or "",
                "grace_used_min": int(item.get("grace_used_min") or 0),
            }
        )
        cursor = seg_end
    return segments


def extract_period_display_clauses(rate_text: str) -> list[str]:
    text = " ".join((rate_text or "").strip().split())
    if not text or text == "-":
        return []

    patt = re.compile(
        r"(\d{1,2}[:.]\d{2}\s*[APap][Mm])\s*(?:-\s*(\d{1,2}[:.]\d{2}\s*[APap][Mm])|onwards)\s*:?",
        re.IGNORECASE,
    )
    matches = list(patt.finditer(text))
    if not matches:
        return [text]

    clauses: list[str] = []
    for i, match in enumerate(matches):
        start = 0 if i == 0 else match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        clause = text[start:end].strip(" ;,")
        if not clause:
            clause = text[match.start():end].strip(" ;,")
        clauses.append(clause)
    return clauses


def build_relevant_rate_segments(row: dict[str, str], start_dt: datetime, duration_min: int) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    current = start_dt
    remaining = max(1, int(duration_min))

    while remaining > 0:
        day_type = calc_day_type_for_dt(current, None)
        rate_text = calc_pick_day_text(row, day_type)
        periods = calc_parse_periods(rate_text)
        display_clauses = extract_period_display_clauses(rate_text)
        if not periods:
            break

        current_min = current.hour * 60 + current.minute
        period = calc_find_active_period(periods, current_min)
        if not period:
            break

        seg_mins = min(remaining, calc_mins_until_period_end(current_min, period))
        if seg_mins <= 0:
            break

        seg_end = current + timedelta(minutes=seg_mins)
        rule_text = (period.rule.raw if period.rule and period.rule.raw else period.raw).strip()
        mode = period.rule.kind if period.rule else ""
        period_index = next((idx for idx, candidate in enumerate(periods) if candidate is period), -1)
        display_rate = (
            display_clauses[period_index]
            if period_index >= 0 and period_index < len(display_clauses)
            else rule_text
        )

        if (
            segments
            and segments[-1]["rule"] == rule_text
            and segments[-1]["mode"] == mode
            and segments[-1]["display_rate"] == display_rate
        ):
            segments[-1]["to_iso"] = seg_end.isoformat()
            segments[-1]["to_label"] = format_time_label(seg_end)
            segments[-1]["mins"] = int(segments[-1]["mins"]) + seg_mins
        else:
            segments.append(
                {
                    "from_iso": current.isoformat(),
                    "to_iso": seg_end.isoformat(),
                    "from_label": format_time_label(current),
                    "to_label": format_time_label(seg_end),
                    "mins": seg_mins,
                    "rule": rule_text,
                    "mode": mode,
                    "day_type": day_type,
                    "display_rate": display_rate,
                }
            )

        current = seg_end
        remaining -= seg_mins

    return segments


def parse_stay_minutes(raw: str | None) -> int:
    if raw is None:
        return PRICE_ESTIMATE_MINUTES
    try:
        minutes = int(raw)
    except (TypeError, ValueError):
        return PRICE_ESTIMATE_MINUTES
    return max(1, min(MAX_STAY_MINUTES, minutes))


def parse_time_of_day_minutes(raw: str | None) -> int | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    if ":" in text:
        parts = text.split(":", maxsplit=1)
        if len(parts) != 2:
            return None
        try:
            hh = int(parts[0])
            mm = int(parts[1])
        except ValueError:
            return None
        if hh < 0 or hh > 23 or mm < 0 or mm > 59:
            return None
        return hh * 60 + mm
    try:
        mins = int(text)
    except ValueError:
        return None
    if mins < 0 or mins >= 24 * 60:
        return None
    return mins


def compute_window_minutes(start_minute: int, end_minute: int) -> int:
    diff = end_minute - start_minute
    if diff <= 0:
        diff += 24 * 60
    return max(1, min(MAX_STAY_MINUTES, diff))


def parse_int_like(raw: Any) -> int | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def normalize_account_key(raw: str | None) -> str:
    text = (raw or "").strip()
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        text = text[1:-1].strip()
    return text


def parse_lta_location(raw: Any) -> tuple[float | None, float | None]:
    text = str(raw or "").strip()
    if not text:
        return (None, None)
    parts = [part for part in re.split(r"[,\s]+", text) if part]
    if len(parts) < 2:
        return (None, None)
    try:
        a = float(parts[0])
        b = float(parts[1])
    except ValueError:
        return (None, None)

    # SG coordinates should be roughly lat=1.x, lon=103.x.
    if abs(a) > 20 and abs(b) <= 20:
        a, b = b, a
    if not (-90 <= a <= 90 and -180 <= b <= 180):
        return (None, None)
    return (a, b)


def parse_xy_to_latlon(x: Any, y: Any, svy: Svy21Converter) -> tuple[float | None, float | None]:
    try:
        xv = float(x)
        yv = float(y)
    except (TypeError, ValueError):
        return (None, None)

    # Already WGS84-like.
    if -90 <= xv <= 90 and -180 <= yv <= 180:
        return (xv, yv)
    if -90 <= yv <= 90 and -180 <= xv <= 180:
        return (yv, xv)

    # Assume SVY21 X/Easting and Y/Northing.
    if abs(xv) > 1000 and abs(yv) > 1000:
        lat, lon = svy.to_latlon(xv, yv)
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return (lat, lon)

    return (None, None)


def parse_ura_geometry_point(raw: Any, svy: Svy21Converter) -> tuple[float | None, float | None]:
    text = str(raw or "").strip()
    if not text:
        return (None, None)
    parts = [x for x in re.split(r"[,\s]+", text) if x]
    if len(parts) < 2:
        return (None, None)
    return parse_xy_to_latlon(parts[0], parts[1], svy)


def parse_ura_result(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    result = payload.get("Result")
    if isinstance(result, list):
        return [x for x in result if isinstance(x, dict)]
    return []


def normalize_match_text(raw: str) -> str:
    text = str(raw or "").upper()
    if not text:
        return ""
    text = text.replace("&", " AND ").replace("@", " AT ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    tokens: list[str] = []
    for token in text.split():
        token = MATCH_TOKEN_ALIASES.get(token, token)
        if len(token) <= 1 or token in MATCH_STOPWORDS:
            continue
        tokens.append(token)
    return " ".join(tokens)


def tokenize_match_text(raw: str) -> tuple[str, ...]:
    norm = normalize_match_text(raw)
    if not norm:
        return ()
    return tuple(norm.split())


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    from math import atan2, cos, radians, sin, sqrt

    r_km = 6371.0
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    a = (
        sin(d_lat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lon / 2) ** 2
    )
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return r_km * c


def name_match_score(
    carpark_key: str,
    carpark_tokens: set[str],
    entry_key: str,
    entry_tokens: set[str],
) -> float:
    if not carpark_key or not entry_key:
        return 0.0
    if carpark_key == entry_key:
        return 1.0

    shorter = min(len(carpark_key), len(entry_key))
    if shorter >= 8 and (carpark_key in entry_key or entry_key in carpark_key):
        return 0.9

    if not carpark_tokens or not entry_tokens:
        return 0.0
    shared = len(carpark_tokens & entry_tokens)
    if shared == 0:
        return 0.0
    union = len(carpark_tokens | entry_tokens)
    jaccard = shared / union if union else 0.0
    coverage = shared / max(1, min(len(carpark_tokens), len(entry_tokens)))
    return max(jaccard, coverage * 0.85)


def choose_best_entry_for_carpark(
    cp: Carpark,
    candidates: list[LtaAvailabilityEntry],
    max_distance_km: float | None = None,
) -> LtaAvailabilityEntry | None:
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    if cp.lat is not None and cp.lon is not None:
        best: tuple[float, LtaAvailabilityEntry] | None = None
        for entry in candidates:
            if entry.lat is None or entry.lon is None:
                continue
            distance = haversine_km(cp.lat, cp.lon, entry.lat, entry.lon)
            if best is None or distance < best[0]:
                best = (distance, entry)
        if best is not None:
            if max_distance_km is None or best[0] <= max_distance_km:
                return best[1]

    return max(
        candidates,
        key=lambda entry: (
            entry.available_lots is not None,
            entry.available_lots or -1,
            AVAILABILITY_SOURCE_PRIORITY.get(entry.source, 0),
            len(entry.key),
            entry.entry_id,
        ),
    )


def find_best_name_entry(
    cp: Carpark,
    cp_key: str,
    cp_tokens: set[str],
    entries: list[LtaAvailabilityEntry],
) -> LtaAvailabilityEntry | None:
    best: LtaAvailabilityEntry | None = None
    best_score = 0.0
    best_distance = float("inf")

    for entry in entries:
        entry_tokens = set(entry.tokens)
        score = name_match_score(cp_key, cp_tokens, entry.key, entry_tokens)
        if score < LTA_NAME_MATCH_MIN_SCORE:
            continue

        distance = float("inf")
        if cp.lat is not None and cp.lon is not None and entry.lat is not None and entry.lon is not None:
            distance = haversine_km(cp.lat, cp.lon, entry.lat, entry.lon)

        if score > best_score or (score == best_score and distance < best_distance):
            best = entry
            best_score = score
            best_distance = distance

    return best


def find_nearest_entries(
    cp: Carpark,
    entries: list[LtaAvailabilityEntry],
    max_distance_km: float,
    limit: int = 2,
    excluded_entry_ids: set[str] | None = None,
) -> list[tuple[float, LtaAvailabilityEntry]]:
    if cp.lat is None or cp.lon is None:
        return []

    excluded = excluded_entry_ids or set()
    nearest: list[tuple[float, LtaAvailabilityEntry]] = []
    for entry in entries:
        if entry.entry_id in excluded:
            continue
        if entry.lat is None or entry.lon is None:
            continue
        distance = haversine_km(cp.lat, cp.lon, entry.lat, entry.lon)
        if distance > max_distance_km:
            continue
        nearest.append((distance, entry))

    nearest.sort(key=lambda pair: pair[0])
    if limit > 0:
        return nearest[:limit]
    return nearest


def aggregate_lta_availability_rows(rows: list[dict[str, Any]]) -> list[LtaAvailabilityEntry]:
    grouped: dict[str, dict[str, Any]] = {}

    for row in rows:
        development = str(
            row.get("Development")
            or row.get("development")
            or row.get("CarParkID")
            or row.get("carpark_id")
            or ""
        ).strip()
        area = str(row.get("Area") or row.get("area") or "").strip()
        agency = str(row.get("Agency") or row.get("agency") or "").strip()
        lot_type = str(row.get("LotType") or row.get("lot_type") or "").strip().upper()
        available = parse_int_like(row.get("AvailableLots") or row.get("available_lots"))
        lat, lon = parse_lta_location(row.get("Location") or row.get("location"))

        key = normalize_match_text(development)
        if not key and lat is not None and lon is not None:
            key = f"LOC-{lat:.5f}-{lon:.5f}"
        if not key:
            continue

        group = grouped.setdefault(
            key,
            {
                "key": key,
                "development": development,
                "area": area,
                "agency_set": set(),
                "lot_type_set": set(),
                "lat": lat,
                "lon": lon,
                "car_lots_total": 0,
                "all_lots_total": 0,
                "has_car_lots": False,
                "has_any_lots": False,
                "tokens": set(tokenize_match_text(development)),
            },
        )

        if development and not group["development"]:
            group["development"] = development
        if area and not group["area"]:
            group["area"] = area
        if agency:
            group["agency_set"].add(agency)
        if lot_type:
            group["lot_type_set"].add(lot_type)
        if group["lat"] is None and lat is not None:
            group["lat"] = lat
        if group["lon"] is None and lon is not None:
            group["lon"] = lon

        if available is not None:
            group["all_lots_total"] += available
            group["has_any_lots"] = True
            if lot_type in {"C", "CAR"}:
                group["car_lots_total"] += available
                group["has_car_lots"] = True

    entries: list[LtaAvailabilityEntry] = []
    for group in grouped.values():
        if group["has_car_lots"]:
            available_lots = int(group["car_lots_total"])
        elif group["has_any_lots"]:
            available_lots = int(group["all_lots_total"])
        else:
            available_lots = None

        entries.append(
            LtaAvailabilityEntry(
                entry_id=f"lta:{group['key']}",
                source="lta_datamall",
                source_ref=group["development"] or group["key"],
                key=group["key"],
                development=group["development"] or group["key"],
                available_lots=available_lots,
                lat=group["lat"],
                lon=group["lon"],
                area=group["area"],
                agency=", ".join(sorted(group["agency_set"])) if group["agency_set"] else "",
                lot_types=tuple(sorted(group["lot_type_set"])),
                tokens=tuple(sorted(group["tokens"])),
            )
        )

    return entries


def aggregate_ura_availability_rows(
    availability_rows: list[dict[str, Any]],
    detail_rows: list[dict[str, Any]],
) -> list[LtaAvailabilityEntry]:
    svy = Svy21Converter()
    details_by_code: dict[str, dict[str, Any]] = {}

    for row in detail_rows:
        code = str(row.get("ppCode") or row.get("carparkNo") or "").strip().upper()
        if not code:
            continue

        name = str(row.get("ppName") or "").strip() or code
        lat = lon = None
        geometries = row.get("geometries")
        if isinstance(geometries, list):
            for geometry in geometries:
                if not isinstance(geometry, dict):
                    continue
                lat, lon = parse_ura_geometry_point(geometry.get("startingPoint"), svy)
                if lat is not None and lon is not None:
                    break

        details_by_code[code] = {
            "name": name,
            "lat": lat,
            "lon": lon,
        }

    grouped: dict[str, dict[str, Any]] = {}
    for row in availability_rows:
        code = str(row.get("carparkNo") or row.get("ppCode") or "").strip().upper()
        if not code:
            continue
        lot_type = str(row.get("lotType") or "").strip().upper()
        lots = parse_int_like(row.get("lotsAvailable"))
        timestamp = str(row.get("datetime") or "").strip() or None
        group = grouped.setdefault(
            code,
            {
                "has_any": False,
                "has_car": False,
                "all_total": 0,
                "car_total": 0,
                "lot_types": set(),
                "timestamp": timestamp,
            },
        )

        if lot_type:
            group["lot_types"].add(lot_type)
        if group["timestamp"] is None and timestamp:
            group["timestamp"] = timestamp
        if lots is not None:
            group["has_any"] = True
            group["all_total"] += lots
            if lot_type in {"C", "CAR"}:
                group["has_car"] = True
                group["car_total"] += lots

    entries: list[LtaAvailabilityEntry] = []
    for code, group in grouped.items():
        if group["has_car"]:
            available = int(group["car_total"])
        elif group["has_any"]:
            available = int(group["all_total"])
        else:
            available = None

        detail = details_by_code.get(code, {})
        name = str(detail.get("name") or code)
        lat = detail.get("lat")
        lon = detail.get("lon")
        key = normalize_match_text(name) or normalize_match_text(code) or f"URA-{code}"
        if not key:
            continue
        entries.append(
            LtaAvailabilityEntry(
                entry_id=f"ura:{code}",
                source="ura_direct",
                source_ref=code,
                key=key,
                development=name,
                available_lots=available,
                lat=lat,
                lon=lon,
                area="",
                agency="URA",
                lot_types=tuple(sorted(group["lot_types"])),
                tokens=tuple(sorted(tokenize_match_text(name))),
            )
        )
    return entries


def match_lta_availability_to_carparks(
    carparks: list[Carpark],
    entries: list[LtaAvailabilityEntry],
) -> tuple[dict[int, dict[str, Any]], dict[str, int], dict[str, Any]]:
    by_key: dict[str, list[LtaAvailabilityEntry]] = {}
    for entry in entries:
        if not entry.key.startswith("LOC-"):
            by_key.setdefault(entry.key, []).append(entry)

    result: dict[int, dict[str, Any]] = {}
    matched_entry_ids: set[str] = set()
    csv_unmatched: list[dict[str, Any]] = []
    match_reason_by_cp_id: dict[int, str] = {}
    pending_coordinate_candidates: list[Carpark] = []
    stats = {
        "matched_total": 0,
        "matched_name_exact": 0,
        "matched_name_fuzzy": 0,
        "matched_postal_coordinates": 0,
        "matched_coordinates": 0,
        "unmatched_total": 0,
    }

    def assign_match(cp: Carpark, entry: LtaAvailabilityEntry, match_method: str) -> None:
        matched_entry_ids.add(entry.entry_id)
        result[cp.id] = {
            "available_lots": entry.available_lots,
            "availability_source": entry.source,
            "availability_source_ref": entry.source_ref,
            "availability_development": entry.development,
            "availability_agency": entry.agency or None,
            "availability_match_method": match_method,
            "availability_lot_types": ", ".join(entry.lot_types) if entry.lot_types else None,
        }

        stats["matched_total"] += 1
        if match_method == "name_exact":
            stats["matched_name_exact"] += 1
        elif match_method == "name_fuzzy":
            stats["matched_name_fuzzy"] += 1
        elif match_method == "postal_coordinates":
            stats["matched_postal_coordinates"] += 1
        elif match_method == "coordinates":
            stats["matched_coordinates"] += 1

    for cp in carparks:
        cp_name_key = normalize_match_text(cp.carpark)
        cp_name_tokens = set(tokenize_match_text(cp.carpark))
        entry: LtaAvailabilityEntry | None = None
        match_method: str | None = None

        if cp_name_key:
            candidates = by_key.get(cp_name_key, [])
            if candidates:
                entry = choose_best_entry_for_carpark(cp, candidates, max_distance_km=1.0)
                match_method = "name_exact"

        if entry is None and cp_name_key and cp_name_tokens:
            entry = find_best_name_entry(cp, cp_name_key, cp_name_tokens, entries)
            if entry is not None:
                match_method = "name_fuzzy"

        if entry is not None and match_method is not None:
            assign_match(cp, entry, match_method)
            continue

        pending_coordinate_candidates.append(cp)

    coord_candidates: list[tuple[float, Carpark, LtaAvailabilityEntry]] = []
    for cp in pending_coordinate_candidates:
        if cp.lat is None or cp.lon is None:
            match_reason_by_cp_id[cp.id] = "missing_coordinates"
            continue

        nearest = find_nearest_entries(
            cp,
            entries,
            max_distance_km=LTA_COORD_MATCH_MAX_KM,
            limit=2,
        )
        if not nearest:
            match_reason_by_cp_id[cp.id] = "no_nearby_api_coordinate"
            continue

        if len(nearest) > 1 and (nearest[1][0] - nearest[0][0]) < LTA_COORD_AMBIGUOUS_GAP_KM:
            match_reason_by_cp_id[cp.id] = (
                f"coordinate_ambiguous({nearest[0][0]:.3f}km,{nearest[1][0]:.3f}km)"
            )
            continue

        distance_km, entry = nearest[0]
        cp_context_tokens = set(tokenize_match_text(f"{cp.carpark} {cp.address}"))
        entry_tokens = set(entry.tokens)
        token_overlap = len(cp_context_tokens & entry_tokens)
        if token_overlap <= 0 and distance_km > LTA_COORD_STRICT_DISTANCE_KM:
            match_reason_by_cp_id[cp.id] = (
                f"coordinate_no_token_overlap({distance_km:.3f}km)"
            )
            continue

        coord_candidates.append((distance_km, cp, entry))

    for _, cp, entry in sorted(coord_candidates, key=lambda item: item[0]):
        if cp.id in result:
            continue
        assign_match(cp, entry, "coordinates")

    for cp in carparks:
        if cp.id in result:
            continue
        stats["unmatched_total"] += 1
        cp_name_key = normalize_match_text(cp.carpark)
        reason_parts: list[str] = []
        if not cp.postal_code:
            reason_parts.append("missing_postal_code")
        if cp.lat is None or cp.lon is None:
            reason_parts.append("missing_coordinates")
        if not cp_name_key:
            reason_parts.append("empty_name_key")
        fallback_reason = match_reason_by_cp_id.get(cp.id)
        if fallback_reason:
            reason_parts.append(fallback_reason)
        reason = ",".join(dict.fromkeys(reason_parts)) if reason_parts else "no_match_found"
        csv_unmatched.append(
            {
                "id": cp.id,
                "carpark": cp.carpark,
                "address": cp.address,
                "postal_code": cp.postal_code,
                "lat": cp.lat,
                "lon": cp.lon,
                "reason": reason,
                "best_name_candidates": build_csv_name_candidate_summary(cp.carpark, entries, top_n=3),
                "nearest_candidates": build_csv_nearest_candidate_summary(
                    cp,
                    entries,
                    top_n=3,
                    max_distance_km=1.0,
                ),
            }
        )

    api_unmatched = [
        {
            "entry_id": entry.entry_id,
            "source": entry.source,
            "source_ref": entry.source_ref,
            "key": entry.key,
            "development": entry.development,
            "available_lots": entry.available_lots,
            "area": entry.area,
            "agency": entry.agency,
            "lot_types": list(entry.lot_types),
            "lat": entry.lat,
            "lon": entry.lon,
            "best_csv_name_candidates": build_api_name_candidate_summary(entry, carparks, top_n=3),
            "nearest_csv_candidates": build_api_nearest_candidate_summary(
                entry,
                carparks,
                top_n=3,
                max_distance_km=1.0,
            ),
        }
        for entry in entries
        if entry.entry_id not in matched_entry_ids
    ]

    debug = {
        "matched_entry_ids": sorted(matched_entry_ids),
        "csv_unmatched": csv_unmatched,
        "api_unmatched": api_unmatched,
    }
    return result, stats, debug


def _slice_debug_rows(rows: list[dict[str, Any]], max_rows: int) -> tuple[list[dict[str, Any]], int]:
    if max_rows <= 0 or len(rows) <= max_rows:
        return rows, 0
    return rows[:max_rows], len(rows) - max_rows


def build_lta_match_debug_payload(
    carparks_snapshot: list[Carpark],
    entries: list[LtaAvailabilityEntry],
    match_stats: dict[str, int],
    match_debug: dict[str, Any],
    max_rows: int,
) -> dict[str, Any]:
    csv_unmatched_rows = list(match_debug.get("csv_unmatched", []))
    api_unmatched_rows = list(match_debug.get("api_unmatched", []))
    csv_logged, csv_omitted = _slice_debug_rows(csv_unmatched_rows, max_rows)
    api_logged, api_omitted = _slice_debug_rows(api_unmatched_rows, max_rows)

    return {
        "timestamp": utc_now_iso(),
        "config": {
            "sources": sorted({entry.source for entry in entries}),
            "matching_order": ["name_exact", "name_fuzzy", "coordinates"],
            "postal_matching_enabled": False,
            "postal_coord_match_max_km": LTA_POSTAL_COORD_MATCH_MAX_KM,
            "coord_fallback_max_km": LTA_COORD_MATCH_MAX_KM,
            "coord_fallback_ambiguous_gap_km": LTA_COORD_AMBIGUOUS_GAP_KM,
            "coord_fallback_strict_distance_km": LTA_COORD_STRICT_DISTANCE_KM,
            "coord_fallback_token_overlap_required_outside_strict_distance": True,
            "coord_fallback_unique_entry_assignment": False,
            "name_match_min_score": LTA_NAME_MATCH_MIN_SCORE,
            "max_rows_per_section": max_rows,
        },
        "counts": {
            "csv_total": len(carparks_snapshot),
            "api_total": len(entries),
            "csv_matched": int(match_stats.get("matched_total") or 0),
            "csv_unmatched": len(csv_unmatched_rows),
            "api_unmatched": len(api_unmatched_rows),
        },
        "match_stats": match_stats,
        "csv_unmatched_total": len(csv_unmatched_rows),
        "api_unmatched_total": len(api_unmatched_rows),
        "csv_unmatched_logged": len(csv_logged),
        "api_unmatched_logged": len(api_logged),
        "csv_unmatched_omitted": csv_omitted,
        "api_unmatched_omitted": api_omitted,
        "csv_unmatched": csv_logged,
        "api_unmatched": api_logged,
    }


def build_csv_name_candidate_summary(
    cp_name: str,
    entries: list[LtaAvailabilityEntry],
    top_n: int = 3,
) -> list[dict[str, Any]]:
    cp_key = normalize_match_text(cp_name)
    cp_tokens = set(tokenize_match_text(cp_name))
    scored: list[tuple[float, LtaAvailabilityEntry]] = []
    for entry in entries:
        score = name_match_score(cp_key, cp_tokens, entry.key, set(entry.tokens))
        if score <= 0:
            continue
        scored.append((score, entry))
    scored.sort(key=lambda item: item[0], reverse=True)
    out: list[dict[str, Any]] = []
    for score, entry in scored[:top_n]:
        out.append(
            {
                "development": entry.development,
                "score": round(score, 4),
                "agency": entry.agency,
                "available_lots": entry.available_lots,
            }
        )
    return out


def build_csv_nearest_candidate_summary(
    cp: Carpark,
    entries: list[LtaAvailabilityEntry],
    top_n: int = 3,
    max_distance_km: float = 1.0,
) -> list[dict[str, Any]]:
    nearest = find_nearest_entries(cp, entries, max_distance_km=max_distance_km, limit=top_n)
    out: list[dict[str, Any]] = []
    for distance_km, entry in nearest:
        out.append(
            {
                "development": entry.development,
                "distance_km": round(distance_km, 4),
                "agency": entry.agency,
                "available_lots": entry.available_lots,
            }
        )
    return out


def build_api_name_candidate_summary(
    entry: LtaAvailabilityEntry,
    carparks: list[Carpark],
    top_n: int = 3,
) -> list[dict[str, Any]]:
    scored: list[tuple[float, Carpark]] = []
    entry_tokens = set(entry.tokens)
    for cp in carparks:
        cp_key = normalize_match_text(cp.carpark)
        cp_tokens = set(tokenize_match_text(cp.carpark))
        score = name_match_score(cp_key, cp_tokens, entry.key, entry_tokens)
        if score <= 0:
            continue
        scored.append((score, cp))
    scored.sort(key=lambda item: item[0], reverse=True)
    out: list[dict[str, Any]] = []
    for score, cp in scored[:top_n]:
        out.append(
            {
                "carpark": cp.carpark,
                "postal_code": cp.postal_code,
                "score": round(score, 4),
            }
        )
    return out


def build_api_nearest_candidate_summary(
    entry: LtaAvailabilityEntry,
    carparks: list[Carpark],
    top_n: int = 3,
    max_distance_km: float = 1.0,
) -> list[dict[str, Any]]:
    if entry.lat is None or entry.lon is None:
        return []
    nearest: list[tuple[float, Carpark]] = []
    for cp in carparks:
        if cp.lat is None or cp.lon is None:
            continue
        distance_km = haversine_km(entry.lat, entry.lon, cp.lat, cp.lon)
        if distance_km > max_distance_km:
            continue
        nearest.append((distance_km, cp))
    nearest.sort(key=lambda item: item[0])
    out: list[dict[str, Any]] = []
    for distance_km, cp in nearest[:top_n]:
        out.append(
            {
                "carpark": cp.carpark,
                "postal_code": cp.postal_code,
                "distance_km": round(distance_km, 4),
            }
        )
    return out


def save_lta_match_debug_log(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def load_carparks(csv_path: Path) -> list[Carpark]:
    carparks: list[Carpark] = []
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            carparks.append(
                Carpark(
                    id=idx,
                    carpark=(row.get("carpark") or "").strip(),
                    address=(row.get("address") or "").strip(),
                    postal_code=normalize_postal_code(row.get("postal_code") or ""),
                    weekdays_rate_1=(row.get("weekdays_rate_1") or "").strip(),
                    weekdays_rate_2=(row.get("weekdays_rate_2") or "").strip(),
                    saturday_rate=(row.get("saturday_rate") or "").strip(),
                    sunday_publicholiday_rate=(row.get("sunday_publicholiday_rate") or "").strip(),
                    extra_rate_fields=extract_extra_rate_fields(row),
                )
            )
    return carparks


def extract_extra_rate_fields(raw_row: dict[str, str]) -> dict[str, str]:
    extras: dict[str, str] = {}
    for key, value in raw_row.items():
        if key in BASE_RATE_KEYS:
            continue
        if not key.startswith(("weekdays_rate_", "saturday_rate_", "sunday_publicholiday_rate_")):
            continue
        text = (value or "").strip()
        if not text:
            continue
        extras[key] = text
    return extras


def normalize_postal_code(raw: str) -> str:
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits if len(digits) == 6 else ""


def load_cache(cache_path: Path) -> dict[str, dict[str, float]]:
    if not cache_path.exists():
        return {}
    with cache_path.open(encoding="utf-8") as f:
        raw = json.load(f)
    cache: dict[str, dict[str, float]] = {}
    if not isinstance(raw, dict):
        return cache
    for postal, info in raw.items():
        if not isinstance(info, dict):
            continue
        try:
            lat = float(info.get("lat"))
            lon = float(info.get("lon"))
        except (TypeError, ValueError):
            continue
        cache[postal] = {"lat": lat, "lon": lon}
    return cache


def save_cache(cache_path: Path, cache: dict[str, dict[str, float]]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, sort_keys=True)


def apply_cache(state: AppState) -> None:
    with state.lock:
        for cp in state.carparks:
            if not cp.postal_code:
                continue
            point = state.cache.get(cp.postal_code)
            if not point:
                continue
            cp.lat = point["lat"]
            cp.lon = point["lon"]


def fetch_json(url: str, timeout: float = 8.0, headers: dict[str, str] | None = None) -> Any:
    req_headers = {"User-Agent": USER_AGENT}
    if headers:
        req_headers.update(headers)
    req = Request(url, headers=req_headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        body_preview = " ".join(body.split())[:300]
        detail = f"HTTP {exc.code}: {exc.reason}"
        if body_preview:
            detail += f" | {body_preview}"
        raise RuntimeError(detail) from exc
    except URLError as exc:
        raise RuntimeError(f"Network error: {exc.reason}") from exc
    return json.loads(raw)


def fetch_lta_carpark_availability(account_key: str, timeout: float = 10.0) -> list[dict[str, Any]]:
    if not account_key:
        return []

    rows: list[dict[str, Any]] = []
    skip = 0

    while True:
        url = LTA_CARPARK_AVAILABILITY_URL if skip == 0 else f"{LTA_CARPARK_AVAILABILITY_URL}?$skip={skip}"
        payload = fetch_json(
            url,
            timeout=timeout,
            headers={
                "AccountKey": account_key,
                "Accept": "application/json",
            },
        )
        page = payload.get("value") if isinstance(payload, dict) else None
        if not isinstance(page, list):
            raise RuntimeError("Unexpected LTA DataMall response shape for CarParkAvailabilityv2")
        rows.extend(item for item in page if isinstance(item, dict))
        if len(page) < LTA_FETCH_PAGE_SIZE:
            break
        skip += len(page)

    return rows


def fetch_ura_payload(
    access_key: str,
    service: str,
    token: str | None,
    timeout: float,
) -> Any:
    headers = {
        "AccessKey": access_key,
        "Accept": "application/json",
    }
    if token:
        headers["Token"] = token
    return fetch_json(f"{URA_INVOKE_URL}?service={service}", headers=headers, timeout=timeout)


def fetch_ura_carpark_availability(
    access_key: str,
    timeout: float = 10.0,
) -> tuple[list[LtaAvailabilityEntry], dict[str, Any]]:
    stats: dict[str, Any] = {
        "errors": [],
        "token_ok": False,
        "availability_rows": 0,
        "detail_rows": 0,
    }
    if not access_key:
        stats["errors"].append("URA access key missing")
        return ([], stats)

    try:
        token_payload = fetch_json(
            URA_TOKEN_URL,
            timeout=timeout,
            headers={
                "AccessKey": access_key,
                "Accept": "application/json",
            },
        )
        token = str(token_payload.get("Result") or "").strip()
        if not token:
            raise RuntimeError("Missing token in URA token response")
        stats["token_ok"] = True
    except Exception as exc:
        stats["errors"].append(f"token: {exc}")
        return ([], stats)

    availability_rows: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []

    try:
        availability_payload = fetch_ura_payload(access_key, "Car_Park_Availability", token, timeout)
        availability_rows = parse_ura_result(availability_payload)
        stats["availability_rows"] = len(availability_rows)
    except Exception as exc:
        stats["errors"].append(f"Car_Park_Availability: {exc}")

    try:
        details_payload = fetch_ura_payload(access_key, "Car_Park_Details", token, timeout)
        detail_rows = parse_ura_result(details_payload)
        stats["detail_rows"] = len(detail_rows)
    except Exception as exc:
        stats["errors"].append(f"Car_Park_Details: {exc}")

    entries = aggregate_ura_availability_rows(availability_rows, detail_rows)
    return (entries, stats)


def geocode_via_onemap(postal_code: str) -> tuple[float, float] | None:
    params = {
        "searchVal": postal_code,
        "returnGeom": "Y",
        "getAddrDetails": "Y",
        "pageNum": "1",
    }
    url = f"https://www.onemap.gov.sg/api/common/elastic/search?{urlencode(params)}"
    payload = fetch_json(url)
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return None

    best = results[0]
    try:
        lat = float(best.get("LATITUDE"))
        lon = float(best.get("LONGITUDE"))
        return (lat, lon)
    except (TypeError, ValueError):
        return None


def _to_float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not number == number or number in {float("inf"), float("-inf")}:
        return None
    return number


def _onemap_search_result_to_place(result: dict[str, Any]) -> dict[str, Any] | None:
    lat = _to_float_or_none(result.get("LATITUDE"))
    lon = _to_float_or_none(result.get("LONGITUDE"))
    if lat is None or lon is None:
        return None

    building = str(result.get("BUILDING") or "").strip()
    block = str(result.get("BLK_NO") or "").strip()
    road = str(result.get("ROAD_NAME") or "").strip()
    postal = str(result.get("POSTAL") or "").strip()
    fallback = str(result.get("SEARCHVAL") or "").strip()

    if building and building.upper() not in {"NIL", "NA", "N/A"}:
        label = building
    else:
        label = " ".join(part for part in (block, road) if part).strip() or fallback

    address_parts = []
    street = " ".join(part for part in (block, road) if part).strip()
    if street:
        address_parts.append(street)
    if postal:
        address_parts.append(f"S{postal}")
    address = ", ".join(address_parts) or str(result.get("ADDRESS") or "").strip()

    return {
        "label": label or fallback or "Unknown place",
        "address": address,
        "lat": lat,
        "lon": lon,
        "source": "onemap",
    }


def search_place_via_onemap(query: str, limit: int = 5) -> list[dict[str, Any]]:
    params = {
        "searchVal": query,
        "returnGeom": "Y",
        "getAddrDetails": "Y",
        "pageNum": "1",
    }
    url = f"https://www.onemap.gov.sg/api/common/elastic/search?{urlencode(params)}"
    payload = fetch_json(url)
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return []

    places: list[dict[str, Any]] = []
    seen: set[tuple[float, float]] = set()
    for result in results:
        if not isinstance(result, dict):
            continue
        place = _onemap_search_result_to_place(result)
        if place is None:
            continue
        dedupe_key = (round(float(place["lat"]), 6), round(float(place["lon"]), 6))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        places.append(place)
        if len(places) >= limit:
            break
    return places


def geocode_via_nominatim(postal_code: str) -> tuple[float, float] | None:
    params = {
        "postalcode": postal_code,
        "country": "Singapore",
        "format": "jsonv2",
        "limit": "1",
    }
    url = f"https://nominatim.openstreetmap.org/search?{urlencode(params)}"
    payload = fetch_json(url)
    if not isinstance(payload, list) or not payload:
        return None
    best = payload[0]
    try:
        return (float(best.get("lat")), float(best.get("lon")))
    except (TypeError, ValueError):
        return None


def search_place_via_nominatim(query: str, limit: int = 5) -> list[dict[str, Any]]:
    params = {
        "q": query,
        "format": "jsonv2",
        "addressdetails": "1",
        "limit": str(max(1, min(limit, PLACE_SEARCH_MAX_RESULTS))),
    }
    url = f"https://nominatim.openstreetmap.org/search?{urlencode(params)}"
    payload = fetch_json(url)
    if not isinstance(payload, list) or not payload:
        return []

    places: list[dict[str, Any]] = []
    seen: set[tuple[float, float]] = set()
    for result in payload:
        if not isinstance(result, dict):
            continue
        lat = _to_float_or_none(result.get("lat"))
        lon = _to_float_or_none(result.get("lon"))
        if lat is None or lon is None:
            continue
        dedupe_key = (round(lat, 6), round(lon, 6))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        display_name = str(result.get("display_name") or "").strip()
        label = str(result.get("name") or "").strip()
        if not label and display_name:
            label = display_name.split(",")[0].strip()
        places.append(
            {
                "label": label or display_name or "Unknown place",
                "address": display_name,
                "lat": lat,
                "lon": lon,
                "source": "nominatim",
            }
        )
        if len(places) >= limit:
            break
    return places


def search_place(query: str, limit: int = 5) -> list[dict[str, Any]]:
    clean_query = " ".join((query or "").split()).strip()
    if len(clean_query) < 2:
        return []

    limit = max(1, min(limit, PLACE_SEARCH_MAX_RESULTS))
    errors: list[str] = []

    for provider in (search_place_via_onemap, search_place_via_nominatim):
        try:
            places = provider(clean_query, limit=limit)
            if places:
                return places
        except Exception as exc:
            errors.append(str(exc))

    if errors:
        raise RuntimeError(" ; ".join(errors))
    return []


def geocode_postal_code(postal_code: str) -> tuple[float, float] | None:
    try:
        point = geocode_via_onemap(postal_code)
        if point:
            return point
    except Exception:
        pass

    try:
        point = geocode_via_nominatim(postal_code)
        if point:
            return point
    except Exception:
        pass

    return None


def geocode_worker(state: AppState, cache_path: Path, interval_sec: float) -> None:
    postal_to_indexes: dict[str, list[int]] = {}
    with state.lock:
        for idx, cp in enumerate(state.carparks):
            if not cp.postal_code:
                continue
            if cp.postal_code in state.cache:
                continue
            postal_to_indexes.setdefault(cp.postal_code, []).append(idx)
        missing_postals = sorted(postal_to_indexes.keys())
        state.geocode_total = len(missing_postals)
        state.geocode_done = 0
        state.geocode_failed = 0
        state.geocode_running = True
        state.geocode_started_at = time.time()

    if not missing_postals:
        with state.lock:
            state.geocode_running = False
        return

    print(f"[geocode] Resolving {len(missing_postals)} unique postal codes...")

    updates_since_save = 0
    for idx, postal_code in enumerate(missing_postals, start=1):
        point = geocode_postal_code(postal_code)

        with state.lock:
            state.geocode_done += 1
            if point is None:
                state.geocode_failed += 1
            else:
                state.cache[postal_code] = {"lat": point[0], "lon": point[1]}
                for cp_index in postal_to_indexes.get(postal_code, []):
                    cp = state.carparks[cp_index]
                    cp.lat = point[0]
                    cp.lon = point[1]
                updates_since_save += 1

        if idx % 20 == 0 or idx == len(missing_postals):
            print(f"[geocode] {idx}/{len(missing_postals)} completed")

        if updates_since_save >= 25:
            with state.lock:
                cache_copy = dict(state.cache)
            save_cache(cache_path, cache_copy)
            updates_since_save = 0

        if interval_sec > 0:
            time.sleep(interval_sec)

    with state.lock:
        state.geocode_running = False
        cache_copy = dict(state.cache)

    save_cache(cache_path, cache_copy)
    print(
        f"[geocode] Done. Success={len(missing_postals) - state.geocode_failed}, "
        f"Failed={state.geocode_failed}"
    )


def make_handler(state: AppState):
    class MapHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(WEB_DIR), **kwargs)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/carparks":
                query = parse_qs(parsed.query)
                include_unlocated = query.get("include_unlocated", ["0"])[0] == "1"
                stay_minutes = parse_stay_minutes(query.get("stay_min", [None])[0])
                stay_from_minute = parse_time_of_day_minutes(query.get("stay_from", [None])[0])
                stay_to_minute = parse_time_of_day_minutes(query.get("stay_to", [None])[0])
                if stay_from_minute is not None and stay_to_minute is not None:
                    stay_minutes = compute_window_minutes(stay_from_minute, stay_to_minute)

                state.refresh_availability_snapshot()
                state.refresh_pricing_snapshot(
                    estimate_minutes=stay_minutes,
                    start_minute=stay_from_minute,
                )
                payload = {
                    "timestamp": utc_now_iso(),
                    "estimate_minutes": stay_minutes,
                    "stay_from_minute": stay_from_minute,
                    "stay_to_minute": stay_to_minute,
                    "carparks": state.get_carparks(
                        include_unlocated=include_unlocated,
                        estimate_minutes=stay_minutes,
                        start_minute=stay_from_minute,
                    ),
                }
                self._send_json(payload)
                return

            if parsed.path == "/api/place-search":
                query = parse_qs(parsed.query)
                raw_text = query.get("q", [""])[0]
                raw_limit = query.get("limit", ["5"])[0]
                text = " ".join(str(raw_text).split()).strip()
                if len(text) < 2:
                    self._send_json(
                        {
                            "timestamp": utc_now_iso(),
                            "query": text,
                            "results": [],
                            "error": "Query must be at least 2 characters.",
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    limit = int(raw_limit)
                except (TypeError, ValueError):
                    limit = 5
                limit = max(1, min(limit, PLACE_SEARCH_MAX_RESULTS))

                try:
                    results = search_place(text, limit=limit)
                except Exception as exc:
                    self._send_json(
                        {
                            "timestamp": utc_now_iso(),
                            "query": text,
                            "results": [],
                            "error": f"Place search failed: {exc}",
                        },
                        status=HTTPStatus.BAD_GATEWAY,
                    )
                    return
                self._send_json(
                    {
                        "timestamp": utc_now_iso(),
                        "query": text,
                        "results": results,
                    }
                )
                return

            if parsed.path == "/api/status":
                self._send_json(state.get_status())
                return

            if parsed.path == "/":
                self.path = "/index.html"

            super().do_GET()

        def _send_json(self, payload: Any, status: int = HTTPStatus.OK) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: Any) -> None:
            print(f"[http] {self.client_address[0]} - {fmt % args}")

    return MapHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ParkLah live map localhost server.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Path to rates CSV")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE, help="Path to coordinate cache JSON")
    parser.add_argument(
        "--lta-account-key",
        default=(os.environ.get("LTA_DATAMALL_ACCOUNT_KEY") or os.environ.get("LTA_ACCOUNT_KEY") or ""),
        help="LTA DataMall AccountKey (or env LTA_DATAMALL_ACCOUNT_KEY)",
    )
    parser.add_argument(
        "--lta-refresh-sec",
        type=float,
        default=DEFAULT_LTA_REFRESH_SEC,
        help=f"Seconds between LTA availability refreshes (default: {int(DEFAULT_LTA_REFRESH_SEC)})",
    )
    parser.add_argument(
        "--ura-access-key",
        default=os.environ.get("URA_ACCESS_KEY") or "",
        help="URA access key for uraDataService (or env URA_ACCESS_KEY)",
    )
    parser.add_argument(
        "--ura-refresh-sec",
        type=float,
        default=DEFAULT_URA_REFRESH_SEC,
        help=f"Seconds between URA availability refreshes (default: {int(DEFAULT_URA_REFRESH_SEC)})",
    )
    parser.add_argument(
        "--lta-match-debug-log",
        type=Path,
        default=DEFAULT_LTA_MATCH_DEBUG_LOG,
        help=f"Path to JSON debug report for LTA matching (default: {DEFAULT_LTA_MATCH_DEBUG_LOG})",
    )
    parser.add_argument(
        "--lta-match-debug-max-rows",
        type=int,
        default=DEFAULT_LTA_MATCH_DEBUG_MAX_ROWS,
        help=f"Max unmatched rows per section in debug report (default: {DEFAULT_LTA_MATCH_DEBUG_MAX_ROWS})",
    )
    parser.add_argument(
        "--no-lta-match-debug-log",
        action="store_true",
        help="Disable writing LTA matching debug report",
    )
    parser.add_argument("--no-geocode", action="store_true", help="Disable background geocoding")
    parser.add_argument(
        "--geocode-interval",
        type=float,
        default=0.25,
        help="Delay (seconds) between geocoding requests (default: 0.25)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.csv.exists():
        raise SystemExit(f"CSV not found: {args.csv}")
    if not WEB_DIR.exists():
        raise SystemExit(f"Web directory not found: {WEB_DIR}")

    carparks = load_carparks(args.csv)
    cache = load_cache(args.cache)

    state = AppState(
        carparks,
        cache,
        lta_account_key=args.lta_account_key,
        lta_refresh_sec=args.lta_refresh_sec,
        ura_access_key=args.ura_access_key,
        ura_refresh_sec=args.ura_refresh_sec,
        lta_match_debug_log=None if args.no_lta_match_debug_log else args.lta_match_debug_log,
        lta_match_debug_max_rows=args.lta_match_debug_max_rows,
    )
    apply_cache(state)

    if state.lta_enabled or state.ura_enabled:
        state.refresh_availability_snapshot(force=True)

    if not args.no_geocode:
        thread = threading.Thread(
            target=geocode_worker,
            args=(state, args.cache, args.geocode_interval),
            name="postal-geocoder",
            daemon=True,
        )
        thread.start()

    handler_cls = make_handler(state)
    server = ThreadingHTTPServer((args.host, args.port), handler_cls)

    status = state.get_status()
    print(f"Serving ParkLah map at http://{args.host}:{args.port}")
    print(
        f"Loaded {status['total_carparks']} carparks, "
        f"{status['carparks_with_coordinates']} already have coordinates"
    )
    if status["lta_enabled"] or status.get("ura_enabled"):
        enabled_sources: list[str] = []
        if status["lta_enabled"]:
            enabled_sources.append("LTA")
        if status.get("ura_enabled"):
            enabled_sources.append("URA")
        print(
            f"Availability enabled via {', '.join(enabled_sources)} "
            f"(matched {status['lta_availability_matched_carparks']} carparks)"
        )
        debug_path = (
            status.get("lta_availability_match_debug_summary", {}).get("debug_log_path")
            if isinstance(status.get("lta_availability_match_debug_summary"), dict)
            else None
        )
        if debug_path:
            print(f"Availability match debug log: {debug_path}")
    else:
        print(
            "Live availability disabled "
            "(set --lta-account-key / LTA_DATAMALL_ACCOUNT_KEY or --ura-access-key / URA_ACCESS_KEY)"
        )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\\nShutting down server...")
    finally:
        server.server_close()
        with state.lock:
            cache_copy = dict(state.cache)
        save_cache(args.cache, cache_copy)


if __name__ == "__main__":
    main()
