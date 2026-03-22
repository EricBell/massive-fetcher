# massive-fetcher — Project Overview

## Purpose

CLI tool to download 1-minute OHLCV (Open, High, Low, Close, Volume) candlestick bars from the **Polygon.io "Massive" API** for specified stock tickers and date ranges. Outputs data to CSV files.

---

## File Structure

```
/
├── fetch.py          # Entire application (321 lines, zero external deps)
├── pyproject.toml    # Project config (Python 3.11+, hatchling build)
└── .env.example      # Environment variable template
```

---

## Usage

```bash
uv run python fetch.py SPY QQQ --date 2026-03-22
uv run python fetch.py SPY --from 2026-03-01 --to 2026-03-22
uv run python fetch.py SPY --date 2026-03-22 --time 09:30-16:00 --out ./data
```

**CLI Arguments:**
| Argument | Description |
|---|---|
| `TICKER` (positional, repeatable) | One or more stock symbols |
| `--date YYYY-MM-DD` | Single date (mutually exclusive with `--from`/`--to`) |
| `--from YYYY-MM-DD` | Start of date range (inclusive) |
| `--to YYYY-MM-DD` | End of date range (inclusive; required with `--from`) |
| `--time HH:MM-HH:MM` | Time window in US/Eastern (e.g., `09:30-16:00`) |
| `--out DIR` | Output directory (default: `./data`) |

---

## Environment Variables

| Variable | Required | Default | Notes |
|---|---|---|---|
| `MASSIVE_API_KEY` | Yes | — | Polygon.io API key |
| `DEFAULT_TIME_RANGE` | No | `04:00-20:00` | US/Eastern; overridden by `--time` |

Loading priority: shell env > `.env` file > built-in defaults.

---

## Data Flow

```
CLI args / .env
    ↓
parse args → resolve time range → build (ticker × date) pairs
    ↓
For each pair:
  1. Sleep 13s (rate limit: safe for 5 req/min free tier)
  2. fetch_bars() → GET /v2/aggs/ticker/{symbol}/range/1/minute/{from_ms}/{to_ms}
     - params: adjusted=false, sort=asc, limit=50000, apiKey=...
     - 403 → skip; 429 → retry once after 60s; network error → skip
  3. write_csv() → {out_dir}/{SYMBOL}_{YYYY-MM-DD}.csv
```

---

## Output CSV Schema

Filename: `{SYMBOL}_{YYYY-MM-DD}.csv`

```
timestamp,open,high,low,close,volume,vwap
```

- `timestamp` — ISO 8601 UTC string (converted from Polygon ms timestamp `t`)
- `open`, `high`, `low`, `close` — price fields (`o`, `h`, `l`, `c`)
- `volume` — `v`
- `vwap` — volume-weighted average price (`vw`)

---

## API Details

- **Base URL:** `https://api.polygon.io/v2/aggs/ticker`
- **Endpoint:** `/v2/aggs/ticker/{symbol}/range/1/minute/{from_ms}/{to_ms}`
- Auth: `apiKey` query parameter
- `from_ms` / `to_ms`: millisecond UTC timestamps derived from US/Eastern time input

---

## Core Functions

| Function | Role |
|---|---|
| `load_env()` | Parses `.env` file (KEY=value); shell env takes precedence |
| `parse_args()` | argparse setup for all CLI flags |
| `resolve_time_range()` | Resolves time window: CLI > env var > default `04:00-20:00` |
| `date_range()` | Generator yielding each date from start to end inclusive |
| `fetch_bars()` | HTTP GET to Polygon, handles errors, returns list of bar dicts |
| `write_csv()` | Writes bars to CSV; creates output dir if needed |
| `main()` | Top-level orchestration |

---

## Key Design Notes

- **Zero external dependencies** — stdlib only (`argparse`, `csv`, `json`, `os`, `sys`, `time`, `urllib`, `datetime`, `pathlib`, `zoneinfo`)
- **Rate limit:** 13s sleep between calls (conservative for free-tier 5 req/min)
- **Timezone:** All user-facing times are US/Eastern; converted to UTC ms for API
- **Resilience:** 403 → skip gracefully; 429 → auto-retry after 60s
- **Logging:** progress to stdout, errors to stderr
