#!/usr/bin/env python3
"""Estimate parking cost from CarparkRates-style CSV rows."""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore


SG_TZ = ZoneInfo("Asia/Singapore") if ZoneInfo else None


@dataclass
class PricingRule:
    kind: str  # entry | interval | tiered | free | closed | unknown
    amount: float = 0.0
    interval_min: int = 0
    first_amount: float = 0.0
    first_block_min: int = 0
    subsequent_amount: float = 0.0
    subsequent_interval_min: int = 0
    grace_min: int = 0
    cap: Optional[float] = None
    raw: str = ""


@dataclass
class Period:
    start_min: int
    end_min: int
    rule: PricingRule
    raw: str


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Estimate parking cost for a carpark and stay duration.")
    ap.add_argument("--csv", default=None, help="Rates CSV path (default: auto-pick project CSV)")
    ap.add_argument("--carpark", required=True, help="Carpark name (exact or partial, case-insensitive)")
    ap.add_argument("--duration-min", type=int, required=True, help="Parking duration in minutes")
    ap.add_argument(
        "--datetime",
        default=None,
        help='Manual start datetime in Singapore time, format "YYYY-MM-DD HH:MM"',
    )
    ap.add_argument(
        "--start-time",
        default=None,
        help='Manual start time only, format "HH:MM" (24-hour), used with current/--day-type date context',
    )
    ap.add_argument(
        "--day-type",
        choices=["weekday", "saturday", "sunday_publicholiday"],
        default=None,
        help="Override day type bucket for calculation (manual testing mode)",
    )
    ap.add_argument("--show-breakdown", action="store_true", help="Print per-segment cost breakdown")
    return ap.parse_args()


def pick_default_csv() -> Path:
    cwd = Path.cwd()
    preferred = [
        cwd / "parking_rates" / "CarparkRates_from_motorist.csv",
        cwd / "parking_rates" / "CarparkRates_from_onemotoring.csv",
        cwd / "parking_rates" / "CarparkRates.csv",
        cwd / "CarparkRates_from_motorist.csv",
        cwd / "CarparkRates_from_onemotoring.csv",
        cwd / "CarparkRates.csv",
    ]
    for p in preferred:
        if p.exists():
            return p
    return cwd / "CarparkRates.csv"


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def find_carpark(rows: list[dict[str, str]], query: str) -> dict[str, str]:
    q = query.strip().lower()

    exact = [r for r in rows if (r.get("carpark", "").strip().lower() == q)]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        return exact[0]

    partial = [r for r in rows if q in r.get("carpark", "").strip().lower()]
    if not partial:
        raise ValueError(f'No carpark matched query: "{query}"')
    if len(partial) > 1:
        names = ", ".join(r.get("carpark", "") for r in partial[:8])
        raise ValueError(
            f'Ambiguous carpark query: "{query}" matched {len(partial)} rows. '
            f"Try a more specific name. Examples: {names}"
        )
    return partial[0]


def parse_now_or_manual(args: argparse.Namespace) -> datetime:
    now = datetime.now(SG_TZ) if SG_TZ else datetime.now()

    if args.datetime:
        dt = datetime.strptime(args.datetime, "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=SG_TZ) if SG_TZ else dt

    if args.start_time:
        hh, mm = args.start_time.split(":")
        dt = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        return dt

    return now.replace(second=0, microsecond=0)


def day_type_for_dt(dt: datetime, override: Optional[str]) -> str:
    if override:
        return override
    wd = dt.weekday()
    if wd == 5:
        return "saturday"
    if wd == 6:
        return "sunday_publicholiday"
    return "weekday"


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def is_usable_rate_text(s: str) -> bool:
    t = normalize_text(s)
    if not t or t == "-":
        return False
    return True


def collect_numbered_rate_text(row: dict[str, str], prefix: str, skip_key: Optional[str] = None) -> list[str]:
    patt = re.compile(rf"^{re.escape(prefix)}_(\d+)$")
    numbered: list[tuple[int, str]] = []
    for key, value in row.items():
        if skip_key and key == skip_key:
            continue
        m = patt.match(key)
        if not m:
            continue
        text = normalize_text(value)
        if not text or text == "-":
            continue
        numbered.append((int(m.group(1)), text))
    numbered.sort(key=lambda x: x[0])
    return [text for _, text in numbered]


def compose_rate_text(
    row: dict[str, str], base_key: str, numbered_prefix: str, fallback_base_keys: Optional[list[str]] = None
) -> str:
    values: list[str] = []
    base_keys = [base_key]
    if fallback_base_keys:
        base_keys.extend(fallback_base_keys)
    for key in base_keys:
        base = normalize_text(row.get(key, "-"))
        if base and base != "-":
            values.append(base)
            break
    values.extend(collect_numbered_rate_text(row, numbered_prefix, skip_key=base_key))
    if not values:
        return "-"
    return " ; ".join(values)


def pick_day_text(row: dict[str, str], day_type: str) -> str:
    weekday_text = compose_rate_text(
        row, "weekdays_rate_1", "weekdays_rate", fallback_base_keys=["weekdays_rate"]
    )
    saturday = compose_rate_text(row, "saturday_rate", "saturday_rate")
    sunday = compose_rate_text(row, "sunday_publicholiday_rate", "sunday_publicholiday_rate")

    def resolve_alias(text: str) -> str:
        tl = text.lower()
        if text == "-" or not text:
            return "-"
        if "same as weekday" in tl or "same as wkday" in tl:
            return weekday_text
        if "same as saturday" in tl:
            return saturday if saturday != text else "-"
        if "same as sunday" in tl:
            return sunday if sunday != text else "-"
        return text

    saturday = resolve_alias(saturday)
    sunday = resolve_alias(sunday)

    if day_type == "weekday":
        for candidate in [weekday_text, saturday, sunday]:
            if is_usable_rate_text(candidate):
                return candidate
        return "-"

    if day_type == "saturday":
        for candidate in [saturday, weekday_text, sunday]:
            if is_usable_rate_text(candidate):
                return candidate
        return "-"

    for candidate in [sunday, saturday, weekday_text]:
        if is_usable_rate_text(candidate):
            return candidate
    return "-"


def parse_money(s: str) -> Optional[float]:
    m = re.search(r"\$\s*(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    return float(m.group(1))


def parse_minutes(num_str: Optional[str], unit: str) -> int:
    n = 1.0
    if num_str:
        t = num_str.strip().lower().replace("½", "0.5")
        n = float(t)
    unit = unit.lower()
    if unit.startswith("day"):
        return max(1, int(round(n * 24 * 60)))
    if unit.startswith("hr") or unit.startswith("hour"):
        return max(1, int(round(n * 60)))
    return max(1, int(round(n)))


def extract_grace_minutes(low: str) -> int:
    grace = 0
    patterns = [
        r"(\d+(?:\.\d+)?)\s*(min|mins|minute|minutes|hr|hrs|hour|hours)\s*grace\s*period",
        r"first\s*(\d+(?:\.\d+)?)\s*(min|mins|minute|minutes|hr|hrs|hour|hours)\s*free",
    ]
    for patt in patterns:
        for m in re.finditer(patt, low):
            grace = max(grace, parse_minutes(m.group(1), m.group(2)))
    return grace


def parse_rule(desc: str) -> PricingRule:
    raw = normalize_text(desc)
    low = raw.lower()
    low = low.replace("½", "0.5")

    grace_min = extract_grace_minutes(low)

    cap = None
    cap_m = re.search(r"cap(?:ped)?\s*(?:at)?\s*\$\s*(\d+(?:\.\d+)?)", low)
    if cap_m:
        cap = float(cap_m.group(1))

    if "no parking" in low:
        return PricingRule(kind="closed", cap=cap, grace_min=grace_min, raw=raw)

    if "free parking" in low and "$" not in low:
        return PricingRule(kind="free", cap=cap, grace_min=grace_min, raw=raw)

    # Entry pricing: "$2.50 per entry" or "Per entry $2.50"
    m_entry = re.search(r"\$\s*(\d+(?:\.\d+)?)\s*per\s*entry", low)
    if not m_entry:
        m_entry = re.search(r"per\s*entry\s*\$\s*(\d+(?:\.\d+)?)", low)
    if m_entry:
        return PricingRule(
            kind="entry",
            amount=float(m_entry.group(1)),
            cap=cap,
            grace_min=grace_min,
            raw=raw,
        )

    # Tiered pricing first block.
    first_amount = None
    first_block = None
    m_first_a = re.search(
        r"\$\s*(\d+(?:\.\d+)?)\s*for\s*(?:1st|first)\s*(\d+(?:\.\d+)?)?\s*"
        r"(day|days|hr|hrs|hour|hours|min|mins|minute|minutes)",
        low,
    )
    m_first_b = re.search(
        r"(?:1st|first)\s*(\d+(?:\.\d+)?)?\s*(day|days|hr|hrs|hour|hours|min|mins|minute|minutes)\s*\$\s*(\d+(?:\.\d+)?)",
        low,
    )
    if m_first_a:
        first_amount = float(m_first_a.group(1))
        first_block = parse_minutes(m_first_a.group(2), m_first_a.group(3))
    elif m_first_b:
        first_amount = float(m_first_b.group(3))
        first_block = parse_minutes(m_first_b.group(1), m_first_b.group(2))

    # Subsequent block.
    sub_amount = None
    sub_block = None
    m_sub_a = re.search(
        r"\$\s*(\d+(?:\.\d+)?)\s*(?:for)?\s*sub(?:sequent|\.)?\s*(\d+(?:\.\d+)?)?\s*"
        r"(day|days|hr|hrs|hour|hours|min|mins|minute|minutes)",
        low,
    )
    m_sub_b = re.search(
        r"sub(?:sequent|\.)?\s*(\d+(?:\.\d+)?)?\s*(day|days|hr|hrs|hour|hours|min|mins|minute|minutes)\s*\$\s*(\d+(?:\.\d+)?)",
        low,
    )
    if m_sub_a:
        sub_amount = float(m_sub_a.group(1))
        sub_block = parse_minutes(m_sub_a.group(2), m_sub_a.group(3))
    elif m_sub_b:
        sub_amount = float(m_sub_b.group(3))
        sub_block = parse_minutes(m_sub_b.group(1), m_sub_b.group(2))

    if first_amount is not None and sub_amount is not None and first_block is not None and sub_block is not None:
        return PricingRule(
            kind="tiered",
            first_amount=first_amount,
            first_block_min=first_block,
            subsequent_amount=sub_amount,
            subsequent_interval_min=sub_block,
            cap=cap,
            grace_min=grace_min,
            raw=raw,
        )

    # "First X free ; subsequent Y mins $Z"
    m_sub_only = re.search(
        r"sub(?:sequent|\.)?\s*(\d+(?:\.\d+)?)?\s*(day|days|hr|hrs|hour|hours|min|mins|minute|minutes)\s*\$\s*(\d+(?:\.\d+)?)",
        low,
    )
    if m_sub_only and ("first" in low and "free" in low):
        interval = parse_minutes(m_sub_only.group(1), m_sub_only.group(2))
        return PricingRule(
            kind="interval",
            amount=float(m_sub_only.group(3)),
            interval_min=interval,
            cap=cap,
            grace_min=grace_min,
            raw=raw,
        )

    # Interval pricing: "$0.60 per 30 mins", "$0.02 /min", "Per hour $1.00"
    m_per_a = re.search(
        r"\$\s*(\d+(?:\.\d+)?)\s*(?:per|/)\s*(\d+(?:\.\d+)?)?\s*"
        r"(day|days|min|mins|minute|minutes|hr|hrs|hour|hours)",
        low,
    )
    m_per_b = re.search(
        r"(?:per|/)\s*(\d+(?:\.\d+)?)?\s*(day|days|min|mins|minute|minutes|hr|hrs|hour|hours)\s*\$\s*(\d+(?:\.\d+)?)",
        low,
    )
    if m_per_a:
        amount = float(m_per_a.group(1))
        interval = parse_minutes(m_per_a.group(2), m_per_a.group(3))
        return PricingRule(kind="interval", amount=amount, interval_min=interval, cap=cap, grace_min=grace_min, raw=raw)
    if m_per_b:
        amount = float(m_per_b.group(3))
        interval = parse_minutes(m_per_b.group(1), m_per_b.group(2))
        return PricingRule(kind="interval", amount=amount, interval_min=interval, cap=cap, grace_min=grace_min, raw=raw)

    # If "free" appears with other terms but no parseable paid component, treat as free.
    if "free" in low and "$" not in low:
        return PricingRule(kind="free", cap=cap, grace_min=grace_min, raw=raw)

    return PricingRule(kind="unknown", cap=cap, grace_min=grace_min, raw=raw)


def extract_caps_from_rate_text(rate_text: str) -> list[float]:
    txt = normalize_text(rate_text).lower()
    if not txt or txt == "-":
        return []
    return [float(m) for m in re.findall(r"cap(?:ped)?\s*(?:at)?\s*\$\s*(\d+(?:\.\d+)?)", txt)]


def normalize_time_token(t: str) -> str:
    s = t.strip().lower().replace(".", ":")
    s = re.sub(r"\s+", "", s)
    m = re.match(r"^(\d{1,2}):(\d{2})(am|pm)$", s)
    if not m:
        raise ValueError(f"Invalid time token: {t}")
    hh = int(m.group(1))
    mm = int(m.group(2))
    ap = m.group(3).upper()
    return f"{hh:02d}:{mm:02d} {ap}"


def parse_time_ampm(t: str) -> int:
    d = datetime.strptime(normalize_time_token(t), "%I:%M %p")
    return d.hour * 60 + d.minute


def inclusive_end_to_exclusive(minute: int) -> int:
    return (minute + 1) % (24 * 60)


def parse_periods(rate_text: str) -> list[Period]:
    txt = normalize_text(rate_text)
    if not txt or txt == "-":
        return []

    txt = re.sub(r"(\d{1,2})\.(\d{2})\s*([ap]m)\b", r"\1:\2\3", txt, flags=re.IGNORECASE)
    # Normalize "6:00pm onwards: ..." into an explicit range so it becomes a separate period.
    txt = re.sub(
        r"(\d{1,2}:\d{2}\s*[APap][Mm])\s*onwards\s*:",
        r"\1 - 11:59PM:",
        txt,
        flags=re.IGNORECASE,
    )
    patt = re.compile(
        r"(\d{1,2}[:.]\d{2}\s*[APap][Mm])\s*-\s*(\d{1,2}[:.]\d{2}\s*[APap][Mm])\s*:?",
        re.IGNORECASE,
    )
    matches = list(patt.finditer(txt))
    periods: list[Period] = []

    if not matches:
        periods.append(Period(0, 0, parse_rule(txt), txt))
        return periods

    for i, m in enumerate(matches):
        start_raw = parse_time_ampm(m.group(1))
        end_raw = parse_time_ampm(m.group(2))
        start = start_raw
        # Identical start/end times should be treated as a full-day window.
        end = start if start_raw == end_raw else inclusive_end_to_exclusive(end_raw)
        seg_start = m.end()
        seg_end = matches[i + 1].start() if i + 1 < len(matches) else len(txt)
        desc = txt[seg_start:seg_end].strip(" ;,")
        # Some provider strings wrap ranges like "Daily (6.00am-5.59pm): ...".
        # Remove leftover leading/trailing punctuation from the captured segment.
        desc = re.sub(r"^[\s\)\]\}\:\-\.,;]+", "", desc)
        desc = re.sub(r"[\s\(\[\{\:\-\.,;]+$", "", desc)
        rule = parse_rule(desc)
        periods.append(Period(start, end, rule, desc))

    return periods


def in_period(minute: int, p: Period) -> bool:
    if p.start_min == p.end_min:
        return True
    if p.start_min < p.end_min:
        return p.start_min <= minute < p.end_min
    return minute >= p.start_min or minute < p.end_min


def mins_until_period_end(current_min: int, p: Period) -> int:
    if p.start_min == p.end_min:
        return 24 * 60 - current_min
    if p.start_min < p.end_min:
        return max(1, p.end_min - current_min)
    if current_min >= p.start_min:
        return (24 * 60 - current_min) + p.end_min
    return max(1, p.end_min - current_min)


def compute_rule_cost(rule: PricingRule, minutes: int) -> tuple[Optional[float], str]:
    if minutes <= 0:
        return 0.0, "empty"

    if rule.kind == "closed":
        return None, "closed"
    if rule.kind == "free":
        return 0.0, "free"
    if rule.kind == "entry":
        cost = rule.amount
    elif rule.kind == "interval":
        if rule.interval_min <= 0:
            return None, "invalid interval"
        blocks = math.ceil(minutes / rule.interval_min)
        cost = blocks * rule.amount
    elif rule.kind == "tiered":
        if minutes <= rule.first_block_min:
            cost = rule.first_amount
        else:
            rem = minutes - rule.first_block_min
            if rule.subsequent_interval_min <= 0:
                return None, "invalid tier interval"
            cost = rule.first_amount + math.ceil(rem / rule.subsequent_interval_min) * rule.subsequent_amount
    else:
        return None, "unsupported rate format"

    if rule.cap is not None:
        cost = min(cost, rule.cap)

    return round(cost, 2), rule.kind


def find_active_period(periods: list[Period], current_min: int) -> Optional[Period]:
    for p in periods:
        if in_period(current_min, p):
            return p
    return None


def estimate_cost(row: dict[str, str], start_dt: datetime, duration_min: int, day_override: Optional[str]):
    current = start_dt
    remaining = duration_min
    total = 0.0
    breakdown = []
    grace_applied = False
    stay_cap: Optional[float] = None

    while remaining > 0:
        dt_day_type = day_type_for_dt(current, day_override)
        rate_text = pick_day_text(row, dt_day_type)
        caps = extract_caps_from_rate_text(rate_text)
        if caps:
            parsed_min_cap = min(caps)
            stay_cap = parsed_min_cap if stay_cap is None else min(stay_cap, parsed_min_cap)
        periods = parse_periods(rate_text)
        if not periods:
            raise ValueError(f"No usable rate text for day type: {dt_day_type}")

        current_min = current.hour * 60 + current.minute
        period = find_active_period(periods, current_min)
        if not period:
            raise ValueError("Could not locate active pricing period")

        seg_mins = min(remaining, mins_until_period_end(current_min, period))
        billable_mins = seg_mins
        grace_used = 0
        if not grace_applied and period.rule.grace_min > 0:
            grace_used = min(seg_mins, period.rule.grace_min)
            billable_mins = max(0, seg_mins - grace_used)
            grace_applied = True

        seg_cost, mode = compute_rule_cost(period.rule, billable_mins)
        if seg_cost is None:
            if mode == "closed":
                raise ValueError(
                    f"No parking in selected period for carpark '{row.get('carpark')}', "
                    f"day={dt_day_type}, time={current.strftime('%H:%M')}, text='{period.raw}'"
                )
            raise ValueError(
                f"Unsupported pricing format for carpark '{row.get('carpark')}', "
                f"day={dt_day_type}, text='{period.raw}'"
            )

        # Safety clamp for providers that express caps at the day-rate level.
        if stay_cap is not None:
            seg_cost = min(seg_cost, max(0.0, stay_cap - total))

        total += seg_cost
        breakdown.append(
            {
                "from": current.strftime("%Y-%m-%d %H:%M"),
                "mins": seg_mins,
                "day_type": dt_day_type,
                "rule": period.rule.raw,
                "mode": mode,
                "grace_used_min": grace_used,
                "cost": round(seg_cost, 2),
            }
        )

        current += timedelta(minutes=seg_mins)
        remaining -= seg_mins

    return round(total, 2), breakdown


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv) if args.csv else pick_default_csv()

    if args.duration_min <= 0:
        print("Error: --duration-min must be > 0", file=sys.stderr)
        return 2

    if not csv_path.exists():
        print(f"Error: CSV not found: {csv_path}", file=sys.stderr)
        return 2

    try:
        rows = load_rows(csv_path)
        row = find_carpark(rows, args.carpark)
        start_dt = parse_now_or_manual(args)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    try:
        total, breakdown = estimate_cost(row, start_dt, args.duration_min, args.day_type)
    except Exception as exc:
        msg = str(exc)
        if msg.startswith("No parking in selected period") or msg.startswith("No usable rate text for day type"):
            mode = "manual" if (args.datetime or args.start_time or args.day_type) else "auto"
            print(f"Carpark: {row.get('carpark', '-')}")
            if row.get("address") is not None or row.get("postal_code") is not None:
                print(f"Address: {row.get('address', '-')}")
                print(f"Postal Code: {row.get('postal_code', '-')}")
            else:
                print(f"Category: {row.get('category', '-')}")
            print(f"CSV: {csv_path}")
            print(f"Mode: {mode}")
            print(f"Start (SG): {start_dt.strftime('%Y-%m-%d %H:%M')}")
            print(f"Duration: {args.duration_min} mins")
            print("Estimated total: N/A")
            print(f"Reason: {msg}")
            return 0
        print(f"Error: {msg}", file=sys.stderr)
        return 1

    mode = "manual" if (args.datetime or args.start_time or args.day_type) else "auto"
    print(f"Carpark: {row.get('carpark', '-')}")
    if row.get("address") is not None or row.get("postal_code") is not None:
        print(f"Address: {row.get('address', '-')}")
        print(f"Postal Code: {row.get('postal_code', '-')}")
    else:
        print(f"Category: {row.get('category', '-')}")
    print(f"CSV: {csv_path}")
    print(f"Mode: {mode}")
    print(f"Start (SG): {start_dt.strftime('%Y-%m-%d %H:%M')}")
    print(f"Duration: {args.duration_min} mins")
    print(f"Estimated total: ${total:.2f}")

    if args.show_breakdown:
        print("Breakdown:")
        for b in breakdown:
            grace_suffix = f" | grace={b['grace_used_min']}m" if b.get("grace_used_min", 0) else ""
            print(
                f"- {b['from']} | {b['mins']} mins | {b['day_type']} | "
                f"{b['mode']} | ${b['cost']:.2f}{grace_suffix} | {b['rule']}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
