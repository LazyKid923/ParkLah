#!/usr/bin/env python3
"""Fetch parking rates from Motorist, optionally merge OneMotoring rates, and export CSV."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

INDEX_URL = "https://www.motorist.sg/parking-rates"
BASE_URL = "https://www.motorist.sg"
ONEMOTORING_URL = (
    "https://onemotoring.lta.gov.sg/content/onemotoring/home/owning/ongoing-car-costs/"
    "parking/parking_rates.8.html"
)

DEFAULT_OUTPUT = Path(__file__).resolve().parent / "parking_rates" / "CarparkRates.csv"

LOCATION_FIELDS = ["carpark", "address", "postal_code"]
DAY_PREFIXES = ["weekdays_rate", "saturday_rate", "sunday_publicholiday_rate"]
MATCH_STOPWORDS = {
    "singapore",
    "the",
    "and",
    "of",
    "at",
    "park",
    "parks",
    "carpark",
    "car",
    "parking",
    "rates",
    "museum",
    "centre",
    "center",
    "gallery",
    "city",
}

# User-approved one-off correction where Motorist page omits the postal code.
HARDCODED_POSTAL_BY_NAME_KEY = {
    "51cuppageroad": "229469",
}


def fetch_text(url: str, timeout: float = 25.0) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ParkLahMotoristScraper/3.0)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def clean_text(text: str) -> str:
    out = unescape(text).replace("\xa0", " ").replace("\u200b", "")
    out = re.sub(r"\s+", " ", out)
    return out.strip(" \t\r\n;,")


def normalize_name(text: str) -> str:
    lowered = clean_text(text).lower()
    lowered = re.sub(r"\(.*?\)", " ", lowered)
    lowered = re.sub(r"\b(car\s*park|carpark|parking|rates?)\b", " ", lowered)
    lowered = re.sub(r"[^a-z0-9]+", "", lowered)
    return lowered


def normalize_space_name(text: str) -> str:
    lowered = clean_text(text).lower()
    lowered = re.sub(r"\(.*?\)", " ", lowered)
    lowered = re.sub(r"\b(car\s*park|carpark|parking|rates?)\b", " ", lowered)
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def is_time_range(text: str) -> bool:
    t = clean_text(text)
    # also accepts "7.00am" style used on some pages
    return bool(
        re.search(
            r"\b\d{1,2}[:.]\d{2}\s*(?:AM|PM|am|pm)\b\s*-\s*\d{1,2}[:.]\d{2}\s*(?:AM|PM|am|pm)\b",
            t,
        )
    )


def is_rate_line(text: str) -> bool:
    low = clean_text(text).lower()
    if not low:
        return False
    if "$" in low:
        return True
    tokens = [
        "free parking",
        "no parking",
        "first hour",
        "subsequent",
        "grace",
        "per entry",
        "per hour",
        "per 30",
        "per 15",
        "capped",
        "same as",
    ]
    return any(tok in low for tok in tokens)


def looks_like_day_label(text: str) -> bool:
    low = clean_text(text).lower()
    if not low:
        return False
    if is_time_range(low) or is_rate_line(low):
        return False
    day_tokens = ["daily", "mon", "tue", "wed", "thu", "fri", "sat", "sun", "ph", "public holiday", "weekday"]
    return any(tok in low for tok in day_tokens)


def _extract_postal(text: str) -> str:
    t = clean_text(text)
    m = re.search(r"\bS\s*\(?\s*(\d{6})\s*\)?\b", t, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\bSingapore\s*(\d{6})\b", t, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\b(\d{6})\b", t)
    return m.group(1) if m else ""


def _strip_map_suffix(text: str) -> str:
    t = clean_text(text)
    t = re.sub(r"\(\s*Map\s*\)$", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\bMap\s*$", "", t, flags=re.IGNORECASE)
    return clean_text(t)


def _address_up_to_postal(text: str) -> str:
    clean = _strip_map_suffix(text)
    if not clean:
        return ""
    m = re.search(r"^(.*?\b(?:Singapore\s*)?(?:S\s*\(?\s*)?\d{6}\)?)\b", clean, flags=re.IGNORECASE)
    if not m:
        return clean
    return clean_text(m.group(1))


def looks_like_address_line(text: str) -> bool:
    value = clean_text(text)
    if not value:
        return False
    low = value.lower()
    if low in {
        "road",
        "street",
        "avenue",
        "drive",
        "lane",
        "way",
        "place",
        "close",
        "boulevard",
        "quay",
        "park",
    }:
        return False
    if len(value.split()) < 3:
        return False
    if "motorist singapore" in low or "parking rates |" in low:
        return False
    road_tokens = [
        "road",
        "avenue",
        "street",
        "drive",
        "lane",
        "crescent",
        "way",
        "link",
        "place",
        "close",
        "boulevard",
        "quay",
        "park",
        "lorong",
        "jalan",
    ]
    has_street_token = any(token in low for token in road_tokens)
    has_location_hint = "singapore" in low or bool(re.search(r"\b\d{6}\b", low))
    has_number = bool(re.search(r"\d", low))
    return has_street_token and (has_location_hint or has_number)


def is_noise_line(text: str) -> bool:
    low = clean_text(text).lower()
    if not low:
        return True
    noise_phrases = {
        "sell vehicle",
        "buy vehicle",
        "download the motorist app",
        "download motorist app",
        "motorist app",
        "sign up",
        "sign-up",
        "signup",
        "motor directory",
        "car reviews",
        "car forum",
        "news",
        "promotions",
        "insurance",
        "financing",
        "used cars",
        "new cars",
    }
    if low in noise_phrases:
        return True
    return any(
        phrase in low
        for phrase in ["sign in", "sign up", "register", "log in", "contact us", "join now", "subscribe"]
    )


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = None
        for k, v in attrs:
            if k.lower() == "href":
                href = v
                break
        if href:
            self.links.append(href)


class ScriptJsonParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture = False
        self._current: list[str] = []
        self.json_blobs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "script":
            return
        attrs_map = {k.lower(): (v or "") for k, v in attrs}
        script_type = attrs_map.get("type", "").lower()
        script_id = attrs_map.get("id", "").lower()
        if "json" in script_type or script_id == "__next_data__":
            self._capture = True
            self._current = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "script" or not self._capture:
            return
        blob = "".join(self._current).strip()
        if blob:
            self.json_blobs.append(blob)
        self._capture = False
        self._current = []

    def handle_data(self, data: str) -> None:
        if self._capture:
            self._current.append(data)


class TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t in {"script", "style", "noscript"}:
            self._skip += 1
            return
        if self._skip:
            return
        if t in {"br", "p", "li", "div", "section", "article", "tr", "td", "th", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in {"script", "style", "noscript"}:
            self._skip = max(0, self._skip - 1)
            return
        if self._skip:
            return
        if t in {"p", "li", "div", "section", "article", "tr", "td", "th", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self.parts.append(data)

    def lines(self) -> list[str]:
        lines: list[str] = []
        for line in "".join(self.parts).splitlines():
            text = clean_text(line)
            if text:
                lines.append(text)
        return lines


@dataclass
class RateRow:
    carpark: str
    address: str
    postal_code: str
    weekday_rates: list[str]
    saturday_rates: list[str]
    sunday_ph_rates: list[str]

    def to_dict(self) -> dict[str, str]:
        out: dict[str, str] = {
            "carpark": self.carpark,
            "address": self.address,
            "postal_code": self.postal_code,
        }
        _apply_rates(out, "weekdays_rate", self.weekday_rates)
        _apply_rates(out, "saturday_rate", self.saturday_rates)
        _apply_rates(out, "sunday_publicholiday_rate", self.sunday_ph_rates)
        return out


def _apply_rates(row: dict[str, str], prefix: str, rates: list[str]) -> None:
    if not rates:
        row[prefix] = ""
        return
    row[prefix] = rates[0]
    for i, rate in enumerate(rates[1:], start=2):
        row[f"{prefix}_{i}"] = rate


def canonical_parking_rates_url(raw_url: str) -> str | None:
    url = (
        raw_url.replace("\\u002F", "/")
        .replace("\\u002f", "/")
        .replace("%2F", "/")
        .replace("%2f", "/")
        .replace("\\/", "/")
    )
    url = urljoin(BASE_URL, url)
    path = urlparse(url).path.lower().rstrip("/")
    is_old_style = "/parking-rates/" in path
    is_current_style = path.startswith("/parking/") and path.endswith("-parking-rates")
    if not (is_old_style or is_current_style):
        return None
    if url.rstrip("/") == INDEX_URL:
        return None
    return url.split("#", 1)[0].split("?", 1)[0].rstrip("/")


def flatten_json_strings(node: object, out: list[str]) -> None:
    if isinstance(node, dict):
        for _key, value in node.items():
            flatten_json_strings(value, out)
        return
    if isinstance(node, list):
        for item in node:
            flatten_json_strings(item, out)
        return
    if isinstance(node, (str, int, float)):
        out.append(str(node))


def discover_pages(index_html: str) -> list[str]:
    seen: set[str] = set()
    pages: list[str] = []

    parser = LinkParser()
    parser.feed(index_html)
    for href in parser.links:
        key = canonical_parking_rates_url(href)
        if not key or key in seen:
            continue
        seen.add(key)
        pages.append(key)

    for matched in re.findall(r"""["']((?:\\?/)?parking-rates/[^"'>\s]+)["']""", index_html):
        key = canonical_parking_rates_url(matched)
        if not key or key in seen:
            continue
        seen.add(key)
        pages.append(key)

    escaped_matches = re.findall(
        r"""(https?:\\u002F\\u002F[^"'\s]*parking-rates(?:\\u002F|/|\\/) [^"'\s]*)""".replace(" ", ""),
        index_html,
    )
    escaped_matches += re.findall(
        r"""((?:\\u002F|\\/|/)parking-rates(?:\\u002F|/|\\/)[^"'\s<]+)""",
        index_html,
    )
    for matched in escaped_matches:
        key = canonical_parking_rates_url(matched)
        if not key or key in seen:
            continue
        seen.add(key)
        pages.append(key)

    for matched in re.findall(
        r'"(?:url|uri|href|link|permalink|canonical)"\s*:\s*"([^"]*parking-rates[^"]*)"',
        index_html,
        flags=re.IGNORECASE,
    ):
        key = canonical_parking_rates_url(matched)
        if not key or key in seen:
            continue
        seen.add(key)
        pages.append(key)

    for slug in re.findall(r'"slug"\s*:\s*"([a-z0-9-]{3,})"', index_html, flags=re.IGNORECASE):
        key = canonical_parking_rates_url(f"/parking-rates/{slug}")
        if not key or key in seen:
            continue
        seen.add(key)
        pages.append(key)

    json_parser = ScriptJsonParser()
    json_parser.feed(index_html)
    for blob in json_parser.json_blobs:
        try:
            parsed = json.loads(blob)
        except Exception:
            continue
        values: list[str] = []
        flatten_json_strings(parsed, values)
        for value in values:
            if "parking-rates/" not in value:
                continue
            key = canonical_parking_rates_url(value)
            if not key or key in seen:
                continue
            seen.add(key)
            pages.append(key)

    return pages


def discover_pages_from_sitemap() -> list[str]:
    sitemap_url = f"{BASE_URL}/sitemap.xml"
    xml = fetch_text(sitemap_url)
    seen: set[str] = set()
    pages: list[str] = []
    for matched in re.findall(r"<loc>([^<]+)</loc>", xml, flags=re.IGNORECASE):
        key = canonical_parking_rates_url(matched)
        if not key or key in seen:
            continue
        seen.add(key)
        pages.append(key)
    return pages


def _find_day_rates_index(lines: list[str]) -> int:
    for i in range(len(lines) - 1):
        if clean_text(lines[i]).lower() == "day" and clean_text(lines[i + 1]).lower() == "rates":
            return i
    for i in range(len(lines)):
        if looks_like_day_label(lines[i]):
            if i + 1 < len(lines) and (is_time_range(lines[i + 1]) or is_rate_line(lines[i + 1])):
                return max(0, i - 2)
    return -1


def _is_vehicle_section_header(text: str) -> bool:
    low = clean_text(text).lower()
    if not low:
        return False
    normalized = re.sub(r"[^a-z]+", " ", low)
    tokens = {token for token in normalized.split() if token}
    if "car" not in tokens:
        return False
    allowed = {"car", "commercial", "motorcycle"}
    if not tokens.issubset(allowed):
        return False
    return True


def _find_vehicle_section_index(lines: list[str]) -> int:
    for idx, line in enumerate(lines):
        if _is_vehicle_section_header(line):
            return idx
    return -1


def _clean_carpark_name(text: str) -> str:
    value = clean_text(text)
    value = re.sub(r"\s*\|\s*carpark\s*\|\s*motorist\s*singapore.*$", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s*parking rates?.*$", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^[^A-Za-z0-9]+", "", value)
    return clean_text(value)


def _extract_name(page_url: str, lines: list[str]) -> str:
    for line in lines[:30]:
        low = line.lower()
        if low in {"day", "rates", "car", "commercial", "motorcycle"}:
            continue
        if looks_like_day_label(line) or is_time_range(line) or is_rate_line(line):
            continue
        candidate = _clean_carpark_name(line)
        if "motorist singapore" in low and candidate:
            return candidate
        if len(candidate) > 2:
            return candidate
    slug = page_url.rstrip("/").rsplit("/", 1)[-1].replace("-", " ")
    return _clean_carpark_name(slug).title()


def _extract_address_and_postal(lines: list[str], name: str) -> tuple[str, str, str, str]:
    """Returns (address, postal_code, reason_address, reason_postal)."""
    day_idx = _find_day_rates_index(lines)
    anchor_end = day_idx if day_idx >= 0 else min(len(lines), 30)
    head = lines[:anchor_end]

    address = ""
    postal = ""
    reason_address = ""
    reason_postal = ""
    vehicle_idx = _find_vehicle_section_index(lines)

    # Postal-first extraction: use the line that contains postal code as the address source.
    postal_candidates: list[str] = []
    for line in head + lines:
        clean = _strip_map_suffix(line)
        if not clean:
            continue
        low = clean.lower()
        if low == name.lower():
            continue
        if is_noise_line(clean):
            continue
        if looks_like_day_label(clean) or is_time_range(clean) or is_rate_line(clean):
            continue
        if _is_vehicle_section_header(clean):
            continue
        detected_postal = _extract_postal(clean)
        if not detected_postal:
            continue
        address_candidate = _address_up_to_postal(clean)
        if not address_candidate:
            continue
        # Prefer lines that look like real addresses first.
        if looks_like_address_line(address_candidate) or "singapore" in address_candidate.lower():
            address = address_candidate
            postal = detected_postal
            break
        postal_candidates.append(address_candidate)

    if not address and postal_candidates:
        address = postal_candidates[0]
        postal = _extract_postal(address)
        reason_address = "postal_line_fallback"

    # Position-based extraction: nearest meaningful line above the vehicle section header.
    if not address and vehicle_idx > 0:
        scan_start = max(0, vehicle_idx - 8)
        for i in range(vehicle_idx - 1, scan_start - 1, -1):
            clean = _strip_map_suffix(lines[i])
            if not clean:
                continue
            low = clean.lower()
            if low == name.lower():
                continue
            if is_noise_line(clean):
                continue
            if looks_like_day_label(clean) or is_time_range(clean) or is_rate_line(clean):
                continue
            if _is_vehicle_section_header(clean):
                continue
            if len(clean.split()) < 3 and not _extract_postal(clean):
                continue
            address = clean
            postal = _extract_postal(clean)
            if not postal:
                reason_postal = "postal_not_present_on_address_line"
            break

    # 1) Best case: find a line that contains a postal code and resembles a real address.
    if not address:
        for line in head:
            clean = _strip_map_suffix(line)
            if not clean:
                continue
            if looks_like_day_label(clean) or is_time_range(clean) or is_rate_line(clean):
                continue
            if looks_like_address_line(clean) and _extract_postal(clean):
                address = clean
                postal = _extract_postal(clean)
                break

    # 2) Next best: address-like line without postal code.
    if not address:
        for line in head:
            clean = _strip_map_suffix(line)
            if not clean:
                continue
            if is_noise_line(clean):
                continue
            if looks_like_day_label(clean) or is_time_range(clean) or is_rate_line(clean):
                continue
            if looks_like_address_line(clean):
                address = clean
                break

    # 3) Try all lines for postal-bearing address strings.
    if not address:
        for line in lines:
            clean = _strip_map_suffix(line)
            if not clean:
                continue
            if is_noise_line(clean):
                continue
            if looks_like_day_label(clean) or is_time_range(clean) or is_rate_line(clean):
                continue
            if _extract_postal(clean) and looks_like_address_line(clean):
                address = clean
                postal = _extract_postal(clean)
                break

    if not address:
        for line in head:
            clean = _strip_map_suffix(line)
            if not clean:
                continue
            if is_noise_line(clean):
                continue
            if looks_like_day_label(clean) or is_time_range(clean) or is_rate_line(clean):
                continue
            if clean.lower() in {"car", "commercial", "motorcycle", "car, commercial, motorcycle", "day", "rates"}:
                continue
            if "motorist singapore" in clean.lower() or "parking rates |" in clean.lower():
                continue
            if clean.lower() != name.lower():
                address = clean
                reason_address = "fallback_first_non_header_line"
                break

    if not address:
        reason_address = "address_not_found_before_day_rates"

    if address and not postal:
        postal = _extract_postal(address)

    if not postal:
        for line in head:
            postal = _extract_postal(line)
            if postal:
                reason_postal = "postal_from_nearby_line"
                break

    if not postal:
        name_key = normalize_name(name)
        if name_key in HARDCODED_POSTAL_BY_NAME_KEY:
            postal = HARDCODED_POSTAL_BY_NAME_KEY[name_key]
            reason_postal = "hardcoded_override"

    if not postal:
        reason_postal = reason_postal or "postal_not_present_on_page"

    return address, postal, reason_address, reason_postal


def _classify_day_bucket(day_label: str) -> set[str]:
    low = clean_text(day_label).lower()
    if not low:
        return set()

    has_weekday = any(tok in low for tok in ["mon", "tue", "wed", "thu", "fri", "weekday"])
    has_sat = "sat" in low or "saturday" in low
    has_sun = "sun" in low or "sunday" in low
    has_ph = "ph" in low or "public holiday" in low

    buckets: set[str] = set()
    if has_weekday:
        buckets.add("weekday")
    if has_sat:
        buckets.add("saturday")
    if has_sun or has_ph:
        buckets.add("sunday_ph")

    if "daily" in low or "mon - sun" in low or "mon-sun" in low:
        buckets.update({"weekday", "saturday", "sunday_ph"})

    if not buckets:
        if "same as weekday" in low:
            buckets.add("saturday")
            buckets.add("sunday_ph")

    return buckets


def _normalize_rate_segment(time_text: str, rate_text: str) -> str:
    t = clean_text(time_text)
    r = clean_text(rate_text)
    if not t:
        return r
    return f"{t}: {r}"


def _append_unique(target: list[str], values: Iterable[str]) -> None:
    existing = {clean_text(v).lower() for v in target}
    for value in values:
        v = clean_text(value)
        if not v:
            continue
        key = v.lower()
        if key in existing:
            continue
        target.append(v)
        existing.add(key)


def _extract_rates(lines: list[str]) -> tuple[list[str], list[str], list[str], str]:
    vehicle_idx = _find_vehicle_section_index(lines)
    has_vehicle_section = vehicle_idx >= 0

    day_idx = _find_day_rates_index(lines)
    if day_idx < 0:
        if not has_vehicle_section:
            return [], [], [], "car_section_not_found"
        return [], [], [], "day_rates_table_not_found"

    weekday: list[str] = []
    saturday: list[str] = []
    sunday_ph: list[str] = []

    current_day = ""
    current_buckets: set[str] = set()
    pending_time = ""

    for raw in lines[day_idx + 2 :]:
        line = clean_text(raw)
        if not line:
            continue
        low = line.lower()
        if re.fullmatch(r"[a-z_]{3,}", low):
            continue

        if low in {"day", "rates"}:
            continue

        inline_buckets = _classify_day_bucket(line)
        if inline_buckets and low.startswith("daily"):
            current_buckets = inline_buckets
            current_day = line
            pending_time = ""
            if is_rate_line(line):
                segment = line.split(":", 1)[1].strip() if ":" in line else line
                if "weekday" in current_buckets:
                    _append_unique(weekday, [segment])
                if "saturday" in current_buckets:
                    _append_unique(saturday, [segment])
                if "sunday_ph" in current_buckets:
                    _append_unique(sunday_ph, [segment])
                continue

        if looks_like_day_label(line):
            current_day = line
            current_buckets = _classify_day_bucket(line)
            pending_time = ""
            if current_buckets and is_rate_line(line):
                segment = line.split(":", 1)[1].strip() if ":" in line else line
                if "weekday" in current_buckets:
                    _append_unique(weekday, [segment])
                if "saturday" in current_buckets:
                    _append_unique(saturday, [segment])
                if "sunday_ph" in current_buckets:
                    _append_unique(sunday_ph, [segment])
            continue

        if is_time_range(line):
            pending_time = line
            continue

        if not is_rate_line(line):
            continue

        segment = _normalize_rate_segment(pending_time, line)
        pending_time = ""
        if not current_buckets:
            continue
        if "weekday" in current_buckets:
            _append_unique(weekday, [segment])
        if "saturday" in current_buckets:
            _append_unique(saturday, [segment])
        if "sunday_ph" in current_buckets:
            _append_unique(sunday_ph, [segment])

    if not weekday and not saturday and not sunday_ph:
        for raw in lines:
            line = clean_text(raw)
            if not line or not is_rate_line(line):
                continue
            buckets = _classify_day_bucket(line)
            if not buckets:
                continue
            segment = line.split(":", 1)[1].strip() if ":" in line else line
            if "weekday" in buckets:
                _append_unique(weekday, [segment])
            if "saturday" in buckets:
                _append_unique(saturday, [segment])
            if "sunday_ph" in buckets:
                _append_unique(sunday_ph, [segment])

    if not weekday and not saturday and not sunday_ph:
        if not has_vehicle_section:
            return [], [], [], "car_section_not_found"
        return [], [], [], "no_rate_segments_detected"

    if not saturday and weekday:
        saturday = ["Same as weekdays"]
    if not sunday_ph and weekday:
        sunday_ph = ["Same as weekdays"]

    return weekday, saturday, sunday_ph, ""


def scrape_motorist_page(page_url: str) -> tuple[RateRow | None, dict[str, str]]:
    html = fetch_text(page_url)
    parser = TextParser()
    parser.feed(html)
    lines = parser.lines()

    json_parser = ScriptJsonParser()
    json_parser.feed(html)
    if json_parser.json_blobs:
        extra_lines: list[str] = []
        for blob in json_parser.json_blobs:
            try:
                parsed = json.loads(blob)
            except Exception:
                continue
            flatten_json_strings(parsed, extra_lines)
        if extra_lines:
            lines.extend(clean_text(item) for item in extra_lines if clean_text(item))

    if not lines:
        return None, {"reason": "empty_page"}

    name = _extract_name(page_url, lines)
    address, postal, reason_address, reason_postal = _extract_address_and_postal(lines, name)
    weekday, saturday, sunday_ph, reason_rates = _extract_rates(lines)
    day_idx = _find_day_rates_index(lines)
    rate_like_lines = [clean_text(line) for line in lines if is_rate_line(line)]

    if not (weekday or saturday or sunday_ph):
        return None, {
            "carpark": name,
            "reason": reason_rates or "no_rates",
            "page": page_url,
            "day_idx": str(day_idx),
            "rate_like_count": str(len(rate_like_lines)),
            "sample_rate_like_lines": " || ".join(rate_like_lines[:10]),
            "sample_lines": " || ".join(lines[:20]),
        }

    row = RateRow(
        carpark=name,
        address=address,
        postal_code=postal,
        weekday_rates=weekday,
        saturday_rates=saturday,
        sunday_ph_rates=sunday_ph,
    )

    return row, {
        "carpark": name,
        "page": page_url,
        "reason_address": reason_address,
        "reason_postal": reason_postal,
        "reason_rates": reason_rates,
    }


def _day_group_candidates(day_text: str) -> set[str]:
    low = clean_text(day_text).lower()
    out: set[str] = set()
    if not low:
        return out
    if any(tok in low for tok in ["mon", "tue", "wed", "thu", "fri", "weekday"]):
        out.add("weekday")
    if "sat" in low:
        out.add("saturday")
    if "sun" in low or "ph" in low or "public holiday" in low:
        out.add("sunday_ph")
    if "daily" in low or "mon - sun" in low or "mon-sun" in low:
        out.update({"weekday", "saturday", "sunday_ph"})
    return out


def scrape_onemotoring_rows(url: str) -> list[dict[str, str]]:
    html = fetch_text(url)
    rows: list[dict[str, str]] = []
    header_groups: list[set[str]] = []
    header_labels: list[str] = []

    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE | re.DOTALL):
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.IGNORECASE | re.DOTALL)
        if len(cells) < 2:
            continue
        clean_cells = [clean_text(re.sub(r"<[^>]+>", " ", c)) for c in cells]
        if not clean_cells:
            continue

        first_cell = clean_cells[0].lower()
        if first_cell in {"carpark", "car park", "name"}:
            header_groups = [_day_group_candidates(text) for text in clean_cells]
            header_labels = clean_cells
            continue

        name = clean_cells[0]
        if not name:
            continue

        weekday: list[str] = []
        saturday: list[str] = []
        sunday_ph: list[str] = []

        if header_groups and len(clean_cells) == len(header_groups):
            for idx in range(1, len(clean_cells)):
                value = clean_cells[idx]
                if not value:
                    continue
                groups = header_groups[idx]
                if not groups:
                    continue
                header_label = header_labels[idx] if idx < len(header_labels) else ""
                segment = f"{header_label}: {value}" if header_label else value
                if "weekday" in groups:
                    _append_unique(weekday, [segment])
                if "saturday" in groups:
                    _append_unique(saturday, [segment])
                if "sunday_ph" in groups:
                    _append_unique(sunday_ph, [segment])
        else:
            pending_day = ""
            for c in clean_cells[1:]:
                if looks_like_day_label(c):
                    pending_day = c
                    if is_rate_line(c):
                        value = c.split(":", 1)[1].strip() if ":" in c else c
                        groups = _day_group_candidates(pending_day)
                        if "weekday" in groups:
                            _append_unique(weekday, [value])
                        if "saturday" in groups:
                            _append_unique(saturday, [value])
                        if "sunday_ph" in groups:
                            _append_unique(sunday_ph, [value])
                    continue
                if not c:
                    continue
                groups = _day_group_candidates(pending_day)
                if not groups:
                    continue
                if "weekday" in groups:
                    _append_unique(weekday, [c])
                if "saturday" in groups:
                    _append_unique(saturday, [c])
                if "sunday_ph" in groups:
                    _append_unique(sunday_ph, [c])

        if not (weekday or saturday or sunday_ph):
            continue

        rows.append(
            {
                "carpark": name,
                "weekdays_rate": " ; ".join(weekday),
                "saturday_rate": " ; ".join(saturday),
                "sunday_publicholiday_rate": " ; ".join(sunday_ph),
            }
        )

    return rows


def _name_candidates(name: str) -> set[str]:
    src = clean_text(name)
    out = {normalize_name(src)}
    no_paren = clean_text(re.sub(r"\(.*?\)", " ", src))
    if no_paren:
        out.add(normalize_name(no_paren))

    for part in re.split(r"\s*/\s*|\s*-\s*|\s+formerly\s+|\s+\|\s+", src, flags=re.IGNORECASE):
        p = clean_text(re.sub(r"\(.*?\)", " ", part))
        if len(p) >= 3:
            out.add(normalize_name(p))

    return {c for c in out if c and c not in MATCH_STOPWORDS and len(c) >= 5}


def _name_phrase_candidates(name: str) -> set[str]:
    src = clean_text(name)
    out = {normalize_space_name(src)}
    no_paren = clean_text(re.sub(r"\(.*?\)", " ", src))
    if no_paren:
        out.add(normalize_space_name(no_paren))
    for part in re.split(r"\s*/\s*|\s*-\s*|\s+formerly\s+|\s+\|\s+", src, flags=re.IGNORECASE):
        p = clean_text(re.sub(r"\(.*?\)", " ", part))
        if len(p) >= 3:
            out.add(normalize_space_name(p))
    cleaned: set[str] = set()
    for phrase in out:
        if not phrase:
            continue
        tokens = [t for t in phrase.split() if t and t not in MATCH_STOPWORDS]
        if len(tokens) >= 2:
            cleaned.add(" ".join(tokens))
        elif len("".join(tokens)) >= 8:
            cleaned.add(" ".join(tokens))
    return cleaned


def _phrase_tokens(text: str) -> set[str]:
    return {
        token
        for token in clean_text(text).lower().split()
        if len(token) >= 3 and token not in MATCH_STOPWORDS
    }


def _shared_distinctive_token_count(a: str, b: str) -> int:
    ta = _phrase_tokens(a)
    tb = _phrase_tokens(b)
    return len(ta & tb)


def _name_match_score(a: str, b: str) -> float:
    a_keys = _name_candidates(a)
    b_keys = _name_candidates(b)
    if a_keys & b_keys:
        return 1.0

    a_phrases = _name_phrase_candidates(a)
    b_phrases = _name_phrase_candidates(b)
    best = 0.0
    for pa in a_phrases:
        for pb in b_phrases:
            if pa == pb:
                return max(best, 0.98)
            if pa in pb or pb in pa:
                shorter = min(len(pa), len(pb))
                longer = max(len(pa), len(pb))
                if longer > 0:
                    best = max(best, 0.72 + 0.24 * (shorter / longer))
            ta = _phrase_tokens(pa)
            tb = _phrase_tokens(pb)
            if ta and tb:
                inter = len(ta & tb)
                if inter:
                    jaccard = inter / len(ta | tb)
                    subset = inter / min(len(ta), len(tb))
                    best = max(best, 0.55 * jaccard + 0.35 * subset)

    return min(best, 0.99)


def _rate_field_names(rows: list[dict[str, str]]) -> list[str]:
    names: set[str] = set()
    for row in rows:
        for key in row.keys():
            if any(key == p or key.startswith(f"{p}_") for p in DAY_PREFIXES):
                names.add(key)
    ordered = []
    for p in DAY_PREFIXES:
        if p in names:
            ordered.append(p)
        suffixes = sorted(
            (k for k in names if k.startswith(f"{p}_")),
            key=lambda x: int(x.split("_")[-1]) if x.split("_")[-1].isdigit() else 999,
        )
        ordered.extend(suffixes)
    return ordered


def _split_rate_text(value: str) -> list[str]:
    text = clean_text(value)
    if not text:
        return []
    return [clean_text(p) for p in text.split(" ; ") if clean_text(p)]


def _set_rate_text(row: dict[str, str], prefix: str, values: list[str]) -> None:
    row[prefix] = values[0] if values else ""
    idx = 2
    for value in values[1:]:
        row[f"{prefix}_{idx}"] = value
        idx += 1


def merge_onemotoring_rows(
    motorist_rows: list[dict[str, str]], onemotoring_rows: list[dict[str, str]]
) -> tuple[list[dict[str, str]], list[str], list[str], list[str]]:
    """
    Replace only rate fields on matched rows.

    Returns (rows, replaced_logs, unmatched_logs, ambiguous_logs)
    """
    if not onemotoring_rows:
        return motorist_rows, [], [], []

    rate_fields = _rate_field_names(motorist_rows)
    replaced_logs: list[str] = []
    unmatched_logs: list[str] = []
    ambiguous_logs: list[str] = []
    used_motorist_indexes: set[int] = set()

    for src in onemotoring_rows:
        src_name = src.get("carpark", "")
        scored_matches = []
        for idx, dst in enumerate(motorist_rows):
            score = _name_match_score(src_name, dst.get("carpark", ""))
            if score >= 0.60:
                scored_matches.append((score, idx))
        scored_matches.sort(reverse=True, key=lambda item: item[0])

        if not scored_matches:
            unmatched_logs.append(src_name)
            continue

        best_score, best_idx = scored_matches[0]
        if len(scored_matches) > 1:
            second_score = scored_matches[1][0]
            margin = best_score - second_score
            if best_score < 0.90 and margin < 0.10:
                ambiguous_logs.append(
                    f"{src_name} (ambiguous_match:{len(scored_matches)} best={best_score:.2f} second={second_score:.2f})"
                )
                continue
            if best_score < 0.72:
                ambiguous_logs.append(
                    f"{src_name} (low_confidence best={best_score:.2f} second={second_score:.2f})"
                )
                continue
        elif best_score < 0.68:
            ambiguous_logs.append(f"{src_name} (low_confidence best={best_score:.2f})")
            continue

        shared_tokens = _shared_distinctive_token_count(src_name, motorist_rows[best_idx].get("carpark", ""))
        if best_score < 0.90 and shared_tokens < 2:
            ambiguous_logs.append(
                f"{src_name} (insufficient_distinctive_overlap best={best_score:.2f} shared_tokens={shared_tokens})"
            )
            continue
        if best_idx in used_motorist_indexes:
            ambiguous_logs.append(
                f"{src_name} (target_already_replaced:{motorist_rows[best_idx].get('carpark', '')})"
            )
            continue

        dst = motorist_rows[best_idx]
        keep_name = dst.get("carpark", "")
        keep_location = {k: dst.get(k, "") for k in LOCATION_FIELDS}

        wd = _split_rate_text(src.get("weekdays_rate", ""))
        sat = _split_rate_text(src.get("saturday_rate", ""))
        sun = _split_rate_text(src.get("sunday_publicholiday_rate", ""))
        if not wd and not sat and not sun:
            unmatched_logs.append(f"{src_name} (empty_onemotoring_rates)")
            continue

        # Clear existing rate fields first so stale suffix columns are removed.
        for key in list(dst.keys()):
            if key in rate_fields or any(key == p or key.startswith(f"{p}_") for p in DAY_PREFIXES):
                dst[key] = ""

        if wd:
            _set_rate_text(dst, "weekdays_rate", wd)
        if sat:
            _set_rate_text(dst, "saturday_rate", sat)
        if sun:
            _set_rate_text(dst, "sunday_publicholiday_rate", sun)

        for k, v in keep_location.items():
            dst[k] = v
        dst["carpark"] = keep_name
        used_motorist_indexes.add(best_idx)

        replaced_logs.append(f"{src_name} -> {dst.get('carpark', '')} (score={best_score:.2f})")

    return motorist_rows, replaced_logs, unmatched_logs, ambiguous_logs


def dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        key = normalize_name(row.get("carpark", ""))
        if not key:
            key = f"postal:{row.get('postal_code','')}"
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _build_missing_location_logs(meta_rows: list[dict[str, str]]) -> list[str]:
    logs: list[str] = []
    for item in meta_rows:
        name = item.get("carpark", "")
        addr_reason = item.get("reason_address", "")
        post_reason = item.get("reason_postal", "")
        if addr_reason or post_reason:
            logs.append(
                f"{name} | reason_address={addr_reason or '-'} | reason_postal={post_reason or '-'}"
            )
    return logs


def save_rows(rows: list[dict[str, str]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    field_order: list[str] = ["carpark", "address", "postal_code"]
    for prefix in DAY_PREFIXES:
        keys = sorted(
            [k for row in rows for k in row.keys() if k == prefix or k.startswith(f"{prefix}_")],
            key=lambda x: (0 if x == prefix else 1, int(x.split("_")[-1]) if x.split("_")[-1].isdigit() else 999),
        )
        for k in keys:
            if k not in field_order:
                field_order.append(k)

    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=field_order)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in field_order})


def save_merge_log(
    log_path: Path,
    scraped_pages: int,
    rows_before_dedupe: int,
    rows_after_dedupe: int,
    replaced_logs: list[str],
    unmatched_logs: list[str],
    ambiguous_logs: list[str],
    missing_location_logs: list[str],
    skipped_pages: list[str],
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as f:
        f.write("Motorist scrape + merge log\n")
        f.write(f"scraped_pages={scraped_pages}\n")
        f.write(f"rows_before_dedupe={rows_before_dedupe}\n")
        f.write(f"rows_after_dedupe={rows_after_dedupe}\n")
        f.write("\n")

        f.write("OneMotoring replacements applied:\n")
        if replaced_logs:
            for line in replaced_logs:
                f.write(f"  - {line}\n")
        else:
            f.write("  - (none)\n")
        f.write("\n")

        f.write("OneMotoring unmatched rows skipped (no append):\n")
        if unmatched_logs:
            for line in unmatched_logs:
                f.write(f"  - {line}\n")
        else:
            f.write("  - (none)\n")
        f.write("\n")

        f.write("OneMotoring ambiguous matches skipped:\n")
        if ambiguous_logs:
            for line in ambiguous_logs:
                f.write(f"  - {line}\n")
        else:
            f.write("  - (none)\n")
        f.write("\n")

        f.write("Missing or partial location data:\n")
        if missing_location_logs:
            for line in missing_location_logs:
                f.write(f"  - {line}\n")
        else:
            f.write("  - (none)\n")
        f.write("\n")

        f.write("Scrape skips:\n")
        if skipped_pages:
            for line in skipped_pages:
                f.write(f"  - {line}\n")
        else:
            f.write("  - (none)\n")


def save_debug_report(
    debug_path: Path,
    index_url: str,
    index_html_len: int,
    discovered_pages: list[str],
    meta_rows: list[dict[str, str]],
    skipped_pages: list[str],
) -> None:
    reason_counter: Counter[str] = Counter()
    no_rate_samples: list[dict[str, object]] = []
    for item in meta_rows:
        reason = str(item.get("reason") or item.get("reason_rates") or "").strip()
        if reason:
            reason_counter[reason] += 1
        if reason in {"no_rate_segments_detected", "day_rates_table_not_found", "empty_page"} and len(no_rate_samples) < 15:
            no_rate_samples.append(
                {
                    "carpark": item.get("carpark", ""),
                    "page": item.get("page", ""),
                    "reason": reason,
                    "day_idx": item.get("day_idx", ""),
                    "rate_like_count": item.get("rate_like_count", ""),
                    "sample_rate_like_lines": item.get("sample_rate_like_lines", ""),
                    "sample_lines": item.get("sample_lines", ""),
                }
            )

    payload = {
        "index_url": index_url,
        "index_html_length": index_html_len,
        "discovered_pages_count": len(discovered_pages),
        "discovered_pages_sample": discovered_pages[:30],
        "meta_rows_count": len(meta_rows),
        "skipped_pages_count": len(skipped_pages),
        "skipped_pages_sample": skipped_pages[:30],
        "reason_counts": dict(reason_counter),
        "no_rate_samples": no_rate_samples,
    }
    debug_path.parent.mkdir(parents=True, exist_ok=True)
    debug_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Motorist parking rates into ParkLah CSV format.")
    parser.add_argument("--index-url", default=INDEX_URL, help="Motorist parking-rates index URL")
    parser.add_argument("--onemotoring-url", default=ONEMOTORING_URL, help="OneMotoring parking rates URL")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output CSV path")
    parser.add_argument("--log", type=Path, default=None, help="Log file path (default: <output_stem>_merge.log)")
    parser.add_argument("--limit", type=int, default=0, help="Only scrape first N pages (0=all)")
    parser.add_argument(
        "--skip-onemotoring-merge",
        action="store_true",
        help="Skip OneMotoring replacement phase",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Write detailed debug report JSON beside output",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    output = args.output
    if output.parent == Path("."):
        output = Path("parking_rates") / output.name

    log_path = args.log or output.with_name(f"{output.stem}_merge.log")
    debug_path = output.with_name(f"{output.stem}_debug.json")

    index_html = fetch_text(args.index_url)
    pages = discover_pages(index_html)
    if not pages:
        try:
            pages = discover_pages_from_sitemap()
            print(f"[info] index page had no links; discovered {len(pages)} pages from sitemap")
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] failed sitemap discovery: {exc}")
    if args.limit > 0:
        pages = pages[: args.limit]

    scraped_rows: list[dict[str, str]] = []
    meta_rows: list[dict[str, str]] = []
    skipped_pages: list[str] = []

    for idx, page in enumerate(pages, start=1):
        try:
            row, meta = scrape_motorist_page(page)
        except Exception as exc:  # noqa: BLE001
            skipped_pages.append(f"{page} ({exc})")
            continue

        if meta:
            meta_rows.append(meta)
        if row is not None:
            scraped_rows.append(row.to_dict())

        if idx % 25 == 0 or idx == len(pages):
            print(f"[info] scraped {idx}/{len(pages)} pages")

    rows = dedupe_rows(scraped_rows)

    replaced_logs: list[str] = []
    unmatched_logs: list[str] = []
    ambiguous_logs: list[str] = []

    if not args.skip_onemotoring_merge:
        try:
            om_rows = scrape_onemotoring_rows(args.onemotoring_url)
            rows, replaced_logs, unmatched_logs, ambiguous_logs = merge_onemotoring_rows(rows, om_rows)
        except Exception as exc:  # noqa: BLE001
            skipped_pages.append(f"onemotoring_merge_failed ({exc})")

    missing_location_logs = _build_missing_location_logs(meta_rows)

    save_rows(rows, output)
    save_merge_log(
        log_path=log_path,
        scraped_pages=len(pages),
        rows_before_dedupe=len(scraped_rows),
        rows_after_dedupe=len(rows),
        replaced_logs=replaced_logs,
        unmatched_logs=unmatched_logs,
        ambiguous_logs=ambiguous_logs,
        missing_location_logs=missing_location_logs,
        skipped_pages=skipped_pages,
    )
    if args.debug:
        save_debug_report(
            debug_path=debug_path,
            index_url=args.index_url,
            index_html_len=len(index_html),
            discovered_pages=pages,
            meta_rows=meta_rows,
            skipped_pages=skipped_pages,
        )

    print(f"[done] wrote {len(rows)} rows to {output}")
    print(f"[done] wrote merge log to {log_path}")
    if args.debug:
        print(f"[done] wrote debug report to {debug_path}")
    if len(rows) == 0:
        reason_counter = Counter(str(item.get("reason") or item.get("reason_rates") or "") for item in meta_rows)
        top_reasons = ", ".join(f"{reason}:{count}" for reason, count in reason_counter.most_common(5) if reason)
        if top_reasons:
            print(f"[warn] top empty-row reasons: {top_reasons}")


if __name__ == "__main__":
    main()
