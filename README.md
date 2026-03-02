# ParkLah

ParkLah is a web app for exploring **Singapore carparks** with:

- **Parking price estimates** for a chosen time window (e.g. “9:30am to 11:30am”)
- **Live available-lots data** (optional) from LTA DataMall and/or URA
- A map UI with **your live location**, **place search**, **filters**, and **“best places”** sorting

It uses a CSV dataset of carpark names/addresses/postal codes and human-readable rate strings.

---

## What you get (features)

### Map UI (frontend)
The React app in `mapcn_web/` shows:

- A **MapLibre** map of Singapore with carpark markers
- Your **live GPS location** (browser geolocation) plus an **accuracy circle**
- A **time window picker** (“from” / “to”) so the backend can estimate cost for that specific window
- **Filters** to show/hide groups:
  - URA carparks (when availability is enabled and matched)
  - Non‑URA carparks with *free* price labels
  - Non‑URA carparks with *paid* price labels
- “Nearby” mode: choose a **center** (your location or a searched place) and a **radius**
- A “**Best places**” panel that ranks carparks by **estimated price**, then **distance**
- Navigation links to **Google Maps**, **Waze**, and **Apple Maps** for the selected carpark

### Pricing engine (backend)
ParkLah can estimate price from the rate text in the CSV, including:

- **Time-of-day periods** like `6:00AM - 5:59PM: ... ; 6:00PM - 11:59PM: ...`
- **Per-interval** pricing like `$0.60 per 30 mins`, `$1 per hour`, `$0.02/min`
- **Per-entry** pricing like `$2.50 per entry`
- **Tiered** pricing like `$2 for first 1 hr, $1 subsequent 30 mins`
- “**First X free**” / “**grace period**” wording (applied once per estimate window)
- **Caps** like `cap at $12` (applied within the estimator for the stay window)
- “**No parking**” / closed periods (returns “Closed now” / unavailable for that window)

When a stay crosses rate boundaries, ParkLah computes a **segment breakdown** and returns it to the UI so the popup can show *why* the total is what it is.

### Live lot availability (optional)
If you provide API keys, ParkLah can show **available lots**:

- **LTA DataMall**: `CarParkAvailabilityv2` (requires `LTA_DATAMALL_ACCOUNT_KEY`)
- **URA uraDataService**: carpark availability + detail geometry (requires `URA_ACCESS_KEY`)

Because the rate CSV and availability APIs use different naming schemes, ParkLah performs best-effort **matching** using:

- Name normalization + token overlap (with stopwords/aliases)
- Fuzzy name scoring (Jaccard/coverage style scoring)
- Coordinate fallback (nearest matches within thresholds)

It can write a debug report to `parking_rates/lta_match_debug.json` to help you diagnose unmatched rows.

### Geocoding + caching (optional, automatic)
Your rate CSV only has postal codes, so ParkLah can resolve missing coordinates and store them in:

- `parking_rates/carpark_coordinates_cache.json`

The server geocodes in a background thread and updates the cache periodically.

### Place search (optional)
The UI “Search by place” uses the backend endpoint `/api/place-search`, which tries:

1. **OneMap** (Singapore government geocoding/search)
2. Fallback to **OpenStreetMap Nominatim**

---

## How it works (high level)

ParkLah has two parts:

- `mapcn_web/` is the frontend (Vite + React + MapLibre).
- The backend logic (pricing, matching, availability fetching, place search, geocoding helpers) lives in Python modules such as `serve_live_map.py` and `calc_parking_cost.py`.

The frontend expects JSON endpoints under `/api/*` (for example, `/api/carparks`, `/api/status`, and `/api/place-search`).

---

## Project layout

- `serve_live_map.py` — main server (static + API)
- `calc_parking_cost.py` — CLI tool to estimate cost for a single carpark + duration
- `fetch_motorist_rates.py` — scraper that builds `parking_rates/CarparkRates.csv`
- `match_lta_lots.py` — fetch LTA and annotate the CSV with matched availability fields
- `parking_rates/CarparkRates.csv` — default rate dataset (CSV)
- `parking_rates/carpark_coordinates_cache.json` — postal code → coordinates cache (generated)
- `mapcn_web/` — frontend (React + MapLibre via Vite)

---

## Requirements

- Python **3.10+** (the code uses `X | None` type syntax)
- Node.js (for the frontend), plus `npm`

The Python scripts use only the standard library (no `pip install` needed).

---

## Configuration

### Environment variables
Set these if you want live availability:

- `LTA_DATAMALL_ACCOUNT_KEY` (or `LTA_ACCOUNT_KEY`)
- `URA_ACCESS_KEY`

### Vercel frontend + external backend
If you deploy `mapcn_web/` to Vercel, the repo includes `mapcn_web/vercel.json` with a rewrite that proxies `/api/*` to `https://parklah.onrender.com/api/*`.

This keeps frontend calls same-origin (`/api/...`) without changing React code.

### Deploy backend on Render
This repo includes `render.yaml` so you can deploy `serve_live_map.py` as a Render web service.

1. Push this repo to GitHub.
2. In Render, create a new **Blueprint** and select this repo.
3. Render will create `parklah-backend` using `render.yaml`.
4. After deploy, copy the backend URL (for example `https://parklah-backend.onrender.com`).
5. In Vercel, set `BACKEND_ORIGIN` to that URL and redeploy the frontend project.

Optional env vars in Render (for live lots):
- `LTA_DATAMALL_ACCOUNT_KEY`
- `URA_ACCESS_KEY`

---

## Backend API endpoints (what the frontend calls)

- `GET /api/carparks`
  - Query params:
    - `stay_min`: total stay minutes (e.g. `120`)
    - `stay_from`: start minute-of-day (e.g. `570` for 9:30am)
    - `stay_to`: end minute-of-day (e.g. `690` for 11:30am)
    - `include_unlocated=1`: include carparks without coordinates
  - Returns carparks with:
    - coordinates (if available)
    - `price_now_estimate`, `price_now_label`, plus cost breakdown segments
    - optional `available_lots` + availability metadata (if LTA/URA enabled and matched)

- `GET /api/status`
  - Returns geocoding progress and availability refresh stats/errors.

- `GET /api/place-search?q=...&limit=...`
  - Returns a list of candidate places (tries OneMap first, then Nominatim).

---

## Utilities

This repo includes helper scripts used to prepare and validate data:

- `fetch_motorist_rates.py`: scrapes Motorist’s parking-rates pages into the CSV format ParkLah expects.
- `calc_parking_cost.py`: estimates pricing for a single carpark row (useful for validating rate parsing).
- `match_lta_lots.py`: pulls LTA availability and produces an annotated CSV + a matching debug report.

---

## CSV format (rates dataset)

The default CSV is `parking_rates/CarparkRates.csv`. Required columns:

- `carpark`
- `address`
- `postal_code`
- `weekdays_rate_1`
- `weekdays_rate_2`
- `saturday_rate`
- `sunday_publicholiday_rate`

Optional: additional rate columns are supported if they follow the numbered convention:

- `weekdays_rate_3`, `weekdays_rate_4`, ...
- `saturday_rate_2`, `saturday_rate_3`, ...
- `sunday_publicholiday_rate_2`, `sunday_publicholiday_rate_3`, ...

These are appended in order when computing the day’s effective rate text.

---

## Notes / limitations

- All pricing is **best-effort parsing** of free-form text. Always verify against signage / official sources.
- “Free parking” labels often have eligibility criteria (resident-only, minimum spend, etc.).
- Availability matching is heuristic. If a carpark doesn’t match, check `parking_rates/lta_match_debug.json`.
- Please be considerate with scraping/geocoding and API usage (rate limits, terms of service).
