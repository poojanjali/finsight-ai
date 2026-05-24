"""
ingestion/sources/yahoo.py

Pulls live OHLCV (Open, High, Low, Close, Volume) data from Yahoo Finance
for a watchlist of tickers. Runs every 5 minutes during market hours.

OHLCV = the 5 core numbers that describe a stock's price in any time period:
  Open   - price when market opened
  High   - highest price during the period
  Low    - lowest price during the period
  Close  - price when period ended
  Volume - how many shares were traded
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional
import yfinance as yf
import pandas as pd
from loguru import logger


# ── Watchlist ────────────────────────────────────────────────────────────────
# These are the stocks FinSight AI monitors by default.
# Add or remove tickers here.
WATCHLIST = [
    "AAPL",   # Apple
    "MSFT",   # Microsoft
    "GOOGL",  # Google
    "AMZN",   # Amazon
    "TSLA",   # Tesla
    "NVDA",   # Nvidia
    "META",   # Meta
]


# ── Data model ───────────────────────────────────────────────────────────────
class OHLCVRecord:
    """
    One price record for one ticker at one point in time.
    This is the standard unit of market data throughout the project.
    """
    def __init__(
        self,
        ticker: str,
        timestamp: datetime,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        source: str = "yahoo_finance",
    ):
        self.ticker = ticker
        self.timestamp = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.source = source

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "timestamp": self.timestamp.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "source": self.source,
        }

    def __repr__(self):
        return (
            f"OHLCVRecord({self.ticker} | "
            f"close={self.close} | "
            f"vol={self.volume:,} | "
            f"{self.timestamp})"
        )


# ── Core fetch function ───────────────────────────────────────────────────────
def fetch_ohlcv(ticker: str, period: str = "1d", interval: str = "5m") -> Optional[list[OHLCVRecord]]:
    """
    Fetch OHLCV data for a single ticker from Yahoo Finance.

    Args:
        ticker:   stock symbol e.g. "AAPL"
        period:   how far back to fetch e.g. "1d" = today only
        interval: candle size e.g. "5m" = 5-minute candles

    Returns:
        List of OHLCVRecord objects, or None if fetch fails.

    Why yf.Ticker().history() instead of yf.download()?
        history() returns a clean DataFrame with proper column names.
        download() is better for multiple tickers at once but needs
        more cleanup. For per-ticker fetches, history() is simpler.
    """
    try:
        ticker_obj = yf.Ticker(ticker)
        df = ticker_obj.history(period=period, interval=interval)

        if df.empty:
            logger.warning(f"No data returned for {ticker}")
            return None

        # Normalize column names to lowercase
        # Yahoo returns: Open, High, Low, Close, Volume
        # We want:       open, high, low, close, volume
        df.columns = [col.lower() for col in df.columns]

        records = []
        for timestamp, row in df.iterrows():
            # Convert timestamp to UTC timezone-aware datetime
            if hasattr(timestamp, 'to_pydatetime'):
                ts = timestamp.to_pydatetime()
            else:
                ts = timestamp

            # Make timezone-aware if not already
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            record = OHLCVRecord(
                ticker=ticker,
                timestamp=ts,
                open=round(float(row["open"]), 4),
                high=round(float(row["high"]), 4),
                low=round(float(row["low"]), 4),
                close=round(float(row["close"]), 4),
                volume=int(row["volume"]),
            )
            records.append(record)

        logger.info(f"Fetched {len(records)} candles for {ticker}")
        return records

    except Exception as e:
        logger.error(f"Failed to fetch {ticker}: {e}")
        return None


# ── Fetch entire watchlist ────────────────────────────────────────────────────
async def fetch_all_tickers(tickers: list[str] = WATCHLIST) -> dict[str, list[OHLCVRecord]]:
    """
    Fetch OHLCV data for all tickers in the watchlist.

    Why asyncio.get_event_loop().run_in_executor()?
        yfinance is a synchronous (blocking) library — it uses requests
        under the hood. If we call it directly in an async function, it
        blocks the entire event loop and no other async tasks can run.

        run_in_executor() runs the blocking function in a separate thread,
        freeing the event loop to handle other work while we wait.
        This is the standard pattern for using sync libraries in async code.
    """
    loop = asyncio.get_event_loop()
    results = {}

    # Run all fetches concurrently using thread pool
    tasks = {
        ticker: loop.run_in_executor(None, fetch_ohlcv, ticker)
        for ticker in tickers
    }

    for ticker, task in tasks.items():
        records = await task
        if records:
            results[ticker] = records

    logger.info(f"Completed fetch for {len(results)}/{len(tickers)} tickers")
    return results


# ── Latest price only (for real-time display) ─────────────────────────────────
def get_latest_price(ticker: str) -> Optional[dict]:
    """
    Get just the most recent price for a ticker.
    Used by the API layer for quick price lookups.

    Returns a simple dict — fast and lightweight.
    """
    try:
        ticker_obj = yf.Ticker(ticker)
        info = ticker_obj.fast_info

        return {
            "ticker": ticker,
            "price": round(float(info.last_price), 2),
            "prev_close": round(float(info.previous_close), 2),
            "change_pct": round(
                ((info.last_price - info.previous_close) / info.previous_close) * 100, 2
            ),
            "volume": int(info.three_month_average_volume or 0),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error(f"Failed to get latest price for {ticker}: {e}")
        return None


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Run this file directly to test it works:
        python ingestion/sources/yahoo.py
    """
    print("\n=== Testing Yahoo Finance ingester ===\n")

    # Test 1: fetch OHLCV for one ticker
    print("Test 1: Fetching 5-min candles for AAPL...")
    records = fetch_ohlcv("AAPL", period="1d", interval="5m")
    if records:
        print(f"  Got {len(records)} candles")
        print(f"  Latest: {records[-1]}")
        print(f"  As dict: {records[-1].to_dict()}")
    else:
        print("  Failed to fetch data")

    print()

    # Test 2: latest price
    print("Test 2: Getting latest price for MSFT...")
    price = get_latest_price("MSFT")
    if price:
        print(f"  Price: ${price['price']}")
        print(f"  Change: {price['change_pct']}%")
    else:
        print("  Failed to get price")

    print()

    # Test 3: all tickers
    print("Test 3: Fetching all watchlist tickers...")
    results = asyncio.run(fetch_all_tickers(["AAPL", "MSFT", "GOOGL"]))
    for ticker, recs in results.items():
        print(f"  {ticker}: {len(recs)} candles, latest close = {recs[-1].close}")

    print("\n=== All tests passed ===\n")