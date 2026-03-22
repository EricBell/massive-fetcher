# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the tool

```bash
# Single date
uv run python fetch.py SPY QQQ --date 2026-03-22

# Date range with time window
uv run python fetch.py SPY --from 2026-03-01 --to 2026-03-22 --time 09:30-16:00

# Custom output directory
uv run python fetch.py SPY --date 2026-03-22 --out ./data
```

Requires `MASSIVE_API_KEY` in `.env` or shell environment. Copy `.env.example` to `.env` to get started.

## Architecture

Single-file app (`fetch.py`). No external dependencies — stdlib only. Python 3.11+ required for `zoneinfo`.

**Call order in `main()`:**
1. `load_env()` — reads `.env` (shell env wins on conflicts)
2. `parse_args()` — argparse; `--date` and `--from`/`--to` are mutually exclusive
3. `resolve_time_range()` — priority: `--time` > `DEFAULT_TIME_RANGE` env var > `04:00-20:00`
4. Iterates `tickers × dates`, sleeping `_RATE_LIMIT_SLEEP` (13s) between every call except the first
5. `fetch_bars()` — builds URL with ms timestamps, handles 403 (skip) and 429 (one retry after 60s)
6. `write_csv()` — writes `{SYMBOL}_{YYYY-MM-DD}.csv` to output dir

**Timezone:** All user-facing times are US/Eastern (`ZoneInfo("US/Eastern")`). `_build_url()` converts them to millisecond UTC timestamps for the Polygon API.

**API endpoint:** `GET /v2/aggs/ticker/{symbol}/range/1/minute/{from_ms}/{to_ms}?adjusted=false&sort=asc&limit=50000&apiKey=...`

**CSV columns:** `timestamp` (ISO 8601 UTC), `open`, `high`, `low`, `close`, `volume`, `vwap`

## Versioning

Version is defined once in `pyproject.toml` (`version = "..."`) and follows semver:

| Change type | When to bump | Example |
|---|---|---|
| **Major** (`X.0.0`) | Breaking CLI change — removed/renamed flags, changed CSV schema, changed output format | `1.0.0` → `2.0.0` |
| **Minor** (`x.Y.0`) | New capability added in a backwards-compatible way — new flag, new output field, new API support | `1.0.0` → `1.1.0` |
| **Patch** (`x.y.Z`) | Bug fix, rate limit tweak, error handling improvement, refactor with no behavior change | `1.0.0` → `1.0.1` |

When bumping, update `version` in `pyproject.toml`. No other files track the version.
