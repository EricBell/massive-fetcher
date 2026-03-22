"""
fetch.py — Download 1-minute OHLCV bars from Polygon.io (Massive) for a list of tickers.

Usage:
    python fetch.py SPY QQQ --date 2026-03-22
    python fetch.py SPY --from 2026-03-01 --to 2026-03-22 --time 09:30-10:30
    python fetch.py SPY --date 2026-03-22 --out ./data

Config (in .env or shell env):
    MASSIVE_API_KEY=<your key>
    DEFAULT_TIME_RANGE=09:30-16:00   # HH:MM-HH:MM in US/Eastern; default 04:00-20:00
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

_ET = ZoneInfo("US/Eastern")
_BASE_URL = "https://api.polygon.io/v2/aggs/ticker"
_RATE_LIMIT_SLEEP = 13   # seconds between calls — safe for free tier (5 req/min)
_RETRY_SLEEP = 60        # seconds to wait after a 429


# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------

def load_env() -> None:
    """Load KEY=value pairs from .env in the script's directory.

    Shell environment takes precedence over .env values.
    """
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    with open(env_path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # Shell env wins
            if key not in os.environ:
                os.environ[key] = value


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch 1-minute OHLCV bars from Polygon.io and write CSV files."
    )
    parser.add_argument(
        "tickers",
        nargs="+",
        metavar="TICKER",
        help="One or more ticker symbols (e.g. SPY QQQ AAPL)",
    )

    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Single date to fetch",
    )
    date_group.add_argument(
        "--from",
        dest="date_from",
        metavar="YYYY-MM-DD",
        help="Start of date range (inclusive)",
    )

    parser.add_argument(
        "--to",
        dest="date_to",
        metavar="YYYY-MM-DD",
        help="End of date range (inclusive); required with --from",
    )
    parser.add_argument(
        "--time",
        metavar="HH:MM-HH:MM",
        help="Time window in US/Eastern (e.g. 09:30-10:30); overrides DEFAULT_TIME_RANGE",
    )
    parser.add_argument(
        "--out",
        default="data",
        metavar="DIR",
        help="Output directory for CSV files (default: ./data)",
    )

    args = parser.parse_args()

    # Validate date arguments
    if args.date_from and not args.date_to:
        parser.error("--to is required when using --from")

    return args


# ---------------------------------------------------------------------------
# Time range resolution
# ---------------------------------------------------------------------------

def _parse_time(s: str) -> tuple[int, int]:
    """Parse 'HH:MM' into (hour, minute). Raises ValueError on bad format."""
    parts = s.split(":")
    if len(parts) != 2:
        raise ValueError(f"Expected HH:MM, got {s!r}")
    return int(parts[0]), int(parts[1])


def resolve_time_range(args: argparse.Namespace) -> tuple[tuple[int, int], tuple[int, int]]:
    """Return ((start_h, start_m), (end_h, end_m)) in US/Eastern.

    Priority: --time CLI flag > DEFAULT_TIME_RANGE env var > 04:00-20:00 default.
    """
    raw = args.time or os.environ.get("DEFAULT_TIME_RANGE") or "04:00-20:00"
    parts = raw.split("-")
    if len(parts) != 2:
        print(f"Error: time range must be HH:MM-HH:MM, got {raw!r}", file=sys.stderr)
        sys.exit(1)
    try:
        start = _parse_time(parts[0])
        end = _parse_time(parts[1])
    except ValueError as exc:
        print(f"Error: invalid time range {raw!r}: {exc}", file=sys.stderr)
        sys.exit(1)
    return start, end


# ---------------------------------------------------------------------------
# Date iteration
# ---------------------------------------------------------------------------

def date_range(from_date: date, to_date: date):
    """Yield each calendar date from from_date to to_date (inclusive)."""
    current = from_date
    while current <= to_date:
        yield current
        current += timedelta(days=1)


# ---------------------------------------------------------------------------
# Polygon.io fetch
# ---------------------------------------------------------------------------

def _build_url(
    symbol: str,
    d: date,
    start: tuple[int, int],
    end: tuple[int, int],
    api_key: str,
) -> str:
    start_h, start_m = start
    end_h, end_m = end
    from_dt = datetime(d.year, d.month, d.day, start_h, start_m, 0, tzinfo=_ET)
    to_dt   = datetime(d.year, d.month, d.day, end_h,   end_m,   59, tzinfo=_ET)
    from_ms = int(from_dt.timestamp() * 1000)
    to_ms   = int(to_dt.timestamp() * 1000)
    return (
        f"{_BASE_URL}/{symbol}/range/1/minute/{from_ms}/{to_ms}"
        f"?adjusted=false&sort=asc&limit=50000&apiKey={api_key}"
    )


def fetch_bars(
    symbol: str,
    d: date,
    start: tuple[int, int],
    end: tuple[int, int],
    api_key: str,
) -> list[dict] | None:
    """Fetch 1-minute bars for one symbol + date.

    Returns list of raw bar dicts on success, None on unrecoverable error.
    Handles 403 (data unavailable) and 429 (rate limited, one retry).
    """
    url = _build_url(symbol, d, start, end, api_key)

    for attempt in range(2):
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            if exc.code == 403:
                print(
                    f"  [{symbol} {d}] 403 Forbidden — data unavailable for this plan/date, skipping.",
                    file=sys.stderr,
                )
                return None
            if exc.code == 429:
                if attempt == 0:
                    print(
                        f"  [{symbol} {d}] 429 Rate limited — waiting {_RETRY_SLEEP}s and retrying...",
                        file=sys.stderr,
                    )
                    time.sleep(_RETRY_SLEEP)
                    continue
                print(f"  [{symbol} {d}] 429 Rate limited on retry, skipping.", file=sys.stderr)
                return None
            print(f"  [{symbol} {d}] HTTP error {exc.code}: {exc}, skipping.", file=sys.stderr)
            return None
        except Exception as exc:
            print(f"  [{symbol} {d}] Network error: {exc}, skipping.", file=sys.stderr)
            return None

        status = data.get("status", "")
        if status not in ("OK", "DELAYED"):
            print(
                f"  [{symbol} {d}] Unexpected API status {status!r}, skipping.",
                file=sys.stderr,
            )
            return None

        bars = data.get("results") or []
        return bars

    return None


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def write_csv(bars: list[dict], symbol: str, d: date, out_dir: Path) -> int:
    """Write bars to {SYMBOL}_{YYYY-MM-DD}.csv. Returns number of rows written."""
    if not bars:
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    filename = out_dir / f"{symbol}_{d}.csv"

    with open(filename, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["timestamp", "open", "high", "low", "close", "volume", "vwap"])
        for bar in bars:
            ts = datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc).isoformat()
            writer.writerow([
                ts,
                bar.get("o", ""),
                bar.get("h", ""),
                bar.get("l", ""),
                bar.get("c", ""),
                bar.get("v", ""),
                bar.get("vw", ""),
            ])

    return len(bars)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    load_env()
    args = parse_args()

    api_key = os.environ.get("MASSIVE_API_KEY", "")
    if not api_key:
        print("Error: MASSIVE_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    start, end = resolve_time_range(args)
    out_dir = Path(args.out)

    # Build list of dates
    if args.date:
        dates = [date.fromisoformat(args.date)]
    else:
        dates = list(date_range(
            date.fromisoformat(args.date_from),
            date.fromisoformat(args.date_to),
        ))

    tickers = [t.upper() for t in args.tickers]

    total_calls = len(tickers) * len(dates)
    print(
        f"Fetching {len(tickers)} ticker(s) × {len(dates)} date(s) = {total_calls} API call(s).",
        flush=True,
    )

    first_call = True
    for ticker in tickers:
        for d in dates:
            if not first_call:
                time.sleep(_RATE_LIMIT_SLEEP)
            first_call = False

            print(f"  {ticker} {d} ...", end=" ", flush=True)
            bars = fetch_bars(ticker, d, start, end, api_key)

            if bars is None:
                # Error already printed inside fetch_bars
                continue

            count = write_csv(bars, ticker, d, out_dir)
            if count == 0:
                print("no bars returned (market closed or no data).")
            else:
                print(f"{count} bars → {out_dir}/{ticker}_{d}.csv")

    print("Done.")


if __name__ == "__main__":
    main()
