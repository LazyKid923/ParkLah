#!/usr/bin/env python3
"""Fetch parking rates from Motorist and export to CarparkRates CSV format."""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, urlopen

INDEX_URL = "https://www.motorist.sg/parking-rates"
BASE_URL = "https://www.motorist.sg"
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "parking_rates" / "CarparkRates.csv"


def fetch_text(url: str, timeout: float = 25.0) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ParkLahMotoristScraper/1.0)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def clean_text(text: str) -> str:
    out = unescape(text).replace("\xa0", " ").replace("\u200b", "")
    out = re.sub(r"\s+", " ", out)
    return out.strip()


def normalize_name(text: str) -> str:
    lowered = clean_text(text).lower()
    lowered = re.sub(r"\bcarpark\b|\bparking\b|\brates?\b", "", lowered)
    lowered = re.sub(r"[^a-z0-9]+", "", lowered)
    return lowered


def normalize_rate(text: str) -> str:
    raw = clean_text(text)
    if not raw:
        return ""
    raw = raw.replace("S$", "$").replace("sgd", "$")
    raw = re.sub(r"\s*/\s*", "/", raw)
    raw = re.sub(r"\bmins\b", "min", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\bminutes\b", "min", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\bhours?\b", "hr", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\bper\b", "/", raw, flags=re.IGNORECASE)
    return raw.strip()


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
        if t in {"br", "p", "li", "div", "section", "article", "tr", "td", "th"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in {"script", "style", "noscript"}:
            self._skip = max(0, self._skip - 1)
            return
        if self._skip:
            return
        if t in {"p", "li", "div", "section", "article", "tr", "td", "th"}:
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
    weekdays_rate_1: str
    weekdays_rate_2: str
    saturday_rate: str
    sunday_publicholiday_rate: str


def discover_pages(index_html: str) -> list[str]:
    parser = LinkParser()
    parser.feed(index_html)
    pages: list[str] = []
    seen: set[str] = set()
    for href in parser.links:
        url = urljoin(BASE_URL, href)
        if "/parking-rates/" not in url:
            continue
        if url.rstrip("/") == INDEX_URL:
            continue
        key = url.split("#", 1)[0].split("?", 1)[0].rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        pages.append(key)
    return pages


def parse_postal_and_address(lines: list[str]) -> tuple[str, str]:
    postal_code = ""
    address = ""
    for line in lines:
        if not postal_code:
            m = re.search(r"\b(\d{6})\b", line)
            if m:
                postal_code = m.group(1)
        if "singapore" in line.lower() and any(ch.isdigit() for ch in line):
            address = line
            break
    return postal_code, address


def parse_rates(lines: list[str]) -> tuple[str, str, str, str]:
    weekday_rates: list[str] = []
    saturday = ""
    sunday_ph = ""

    section = ""
    for line in lines:
        lower = line.lower()
        if "weekday" in lower:
            section = "weekday"
            continue
        if "saturday" in lower:
            section = "saturday"
            continue
        if "sunday" in lower or "public holiday" in lower:
            section = "sunday_ph"
            continue
        if "$" not in line and "free" not in lower:
            continue
        rate = normalize_rate(line)
        if not rate:
            continue
        if section == "weekday":
            weekday_rates.append(rate)
        elif section == "saturday" and not saturday:
            saturday = rate
        elif section == "sunday_ph" and not sunday_ph:
            sunday_ph = rate

    weekday_1 = weekday_rates[0] if len(weekday_rates) > 0 else ""
    weekday_2 = weekday_rates[1] if len(weekday_rates) > 1 else ""
    return weekday_1, weekday_2, saturday, sunday_ph


def extract_name(page_url: str, lines: list[str]) -> str:
    slug = page_url.rstrip("/").rsplit("/", 1)[-1].replace("-", " ")
    slug_name = clean_text(slug).title()
    for line in lines[:12]:
        lower = line.lower()
        if "parking rate" in lower or "carpark" in lower:
            candidate = re.sub(r"\s*parking rates?.*$", "", line, flags=re.IGNORECASE)
            candidate = clean_text(candidate)
            if len(candidate) > 2:
                return candidate
    return slug_name


def scrape_page(page_url: str) -> RateRow | None:
    html = fetch_text(page_url)
    parser = TextParser()
    parser.feed(html)
    lines = parser.lines()
    if not lines:
        return None

    name = extract_name(page_url, lines)
    postal_code, address = parse_postal_and_address(lines)
    weekday_1, weekday_2, saturday, sunday_ph = parse_rates(lines)
    if not any([weekday_1, weekday_2, saturday, sunday_ph]):
        return None

    return RateRow(
        carpark=name,
        address=address,
        postal_code=postal_code,
        weekdays_rate_1=weekday_1,
        weekdays_rate_2=weekday_2,
        saturday_rate=saturday,
        sunday_publicholiday_rate=sunday_ph,
    )


def dedupe_rows(rows: list[RateRow]) -> list[RateRow]:
    out: list[RateRow] = []
    seen: set[str] = set()
    for row in rows:
        key = normalize_name(row.carpark) or row.postal_code
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def save_rows(rows: list[RateRow], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "carpark",
                "address",
                "postal_code",
                "weekdays_rate_1",
                "weekdays_rate_2",
                "saturday_rate",
                "sunday_publicholiday_rate",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Motorist parking rates into CarparkRates CSV format.")
    parser.add_argument("--index-url", default=INDEX_URL, help="Motorist parking-rates index URL")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output CSV path")
    parser.add_argument("--limit", type=int, default=0, help="Only scrape first N pages (0 = all)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    index_html = fetch_text(args.index_url)
    pages = discover_pages(index_html)
    if args.limit > 0:
        pages = pages[: args.limit]

    rows: list[RateRow] = []
    for idx, page in enumerate(pages, start=1):
        try:
            row = scrape_page(page)
        except Exception as exc:
            print(f"[warn] {idx}/{len(pages)} failed: {page} ({exc})")
            continue
        if row is not None:
            rows.append(row)
        if idx % 25 == 0 or idx == len(pages):
            print(f"[info] scraped {idx}/{len(pages)} pages")

    deduped = dedupe_rows(rows)
    save_rows(deduped, args.output)
    print(f"[done] wrote {len(deduped)} rows to {args.output}")


if __name__ == "__main__":
    main()
