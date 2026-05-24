"""
ingestion/storage.py

Saves raw data to disk as JSON files before any processing happens.
This is the "Bronze layer" in medallion architecture.

Why save raw data first?
    If your processing logic changes (better sentiment model, new indicators),
    you can reprocess historical data WITHOUT re-hitting the APIs.
    It's your safety net and audit trail.

Folder structure created:
    data/raw/
        yahoo_finance/
            2026-05-25/
                AAPL.json
                MSFT.json
                GOOGL.json
        news/
            2026-05-25/
                headlines.json
        reddit/
            2026-05-25/
                wallstreetbets.json
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from loguru import logger


# ── Base path ─────────────────────────────────────────────────────────────────
# All raw data lives under data/raw/
# Path(__file__) = this file's location (ingestion/storage.py)
# .parent.parent  = project root (finsight-ai/)
BASE_RAW_PATH = Path(__file__).parent.parent / "data" / "raw"


# ── Core save function ────────────────────────────────────────────────────────
def save_raw(
    data: Any,
    source: str,
    filename: str,
    date: datetime = None,
) -> Path:
    """
    Save any data as a JSON file under data/raw/{source}/{YYYY-MM-DD}/{filename}.

    Args:
        data:     anything JSON-serialisable — dict, list, etc.
        source:   folder name for the data source e.g. "yahoo_finance", "news"
        filename: file name e.g. "AAPL.json"
        date:     which date folder to save under (defaults to today UTC)

    Returns:
        Path to the saved file.

    Why this structure?
        data/raw/yahoo_finance/2026-05-25/AAPL.json
        - Easy to find data for a specific date
        - Easy to replay a specific day
        - Easy to delete old data to save disk space
    """
    if date is None:
        date = datetime.now(timezone.utc)

    # Build folder path: data/raw/{source}/{YYYY-MM-DD}/
    date_str = date.strftime("%Y-%m-%d")
    folder = BASE_RAW_PATH / source / date_str

    # Create folder if it doesn't exist
    # exist_ok=True means no error if it already exists
    folder.mkdir(parents=True, exist_ok=True)

    # Full file path
    filepath = folder / filename

    # Write JSON with indentation for human readability
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
        # default=str handles datetime objects that aren't
        # JSON serialisable by converting them to strings

    logger.info(f"Saved raw data → {filepath}")
    return filepath


# ── Save OHLCV records ────────────────────────────────────────────────────────
def save_ohlcv(ticker: str, records: list) -> Path:
    """
    Save OHLCV records for one ticker.
    Converts OHLCVRecord objects to dicts before saving.

    Args:
        ticker:  stock symbol e.g. "AAPL"
        records: list of OHLCVRecord objects from yahoo.py

    Returns:
        Path to the saved file.
    """
    # Convert OHLCVRecord objects → plain dicts
    data = {
        "ticker": ticker,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(records),
        "records": [r.to_dict() for r in records],
    }

    return save_raw(
        data=data,
        source="yahoo_finance",
        filename=f"{ticker}.json",
    )


# ── Save news articles ────────────────────────────────────────────────────────
def save_news(articles: list[dict], source_name: str = "newsapi") -> Path:
    """
    Save a list of news articles.

    Args:
        articles:    list of article dicts
        source_name: which news source e.g. "newsapi", "rss_reuters"
    """
    data = {
        "source": source_name,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "article_count": len(articles),
        "articles": articles,
    }

    filename = f"{source_name}_{datetime.now(timezone.utc).strftime('%H%M')}.json"

    return save_raw(
        data=data,
        source="news",
        filename=filename,
    )


# ── Save Reddit posts ─────────────────────────────────────────────────────────
def save_reddit(posts: list[dict], subreddit: str) -> Path:
    """
    Save Reddit posts from a subreddit.

    Args:
        posts:     list of post dicts
        subreddit: e.g. "wallstreetbets", "stocks", "investing"
    """
    data = {
        "subreddit": subreddit,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "post_count": len(posts),
        "posts": posts,
    }

    filename = f"{subreddit}_{datetime.now(timezone.utc).strftime('%H%M')}.json"

    return save_raw(
        data=data,
        source="reddit",
        filename=filename,
    )


# ── Load raw data back ────────────────────────────────────────────────────────
def load_raw(source: str, filename: str, date: datetime = None) -> Any:
    """
    Load a previously saved raw JSON file back into memory.
    Used for reprocessing historical data without hitting APIs again.

    Args:
        source:   folder name e.g. "yahoo_finance"
        filename: file name e.g. "AAPL.json"
        date:     which date to load from (defaults to today)

    Returns:
        Parsed JSON data, or None if file doesn't exist.
    """
    if date is None:
        date = datetime.now(timezone.utc)

    date_str = date.strftime("%Y-%m-%d")
    filepath = BASE_RAW_PATH / source / date_str / filename

    if not filepath.exists():
        logger.warning(f"Raw file not found: {filepath}")
        return None

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    logger.info(f"Loaded raw data ← {filepath}")
    return data


# ── List available dates ──────────────────────────────────────────────────────
def list_available_dates(source: str) -> list[str]:
    """
    List all dates we have raw data for a given source.
    Useful for knowing what historical data is available to replay.

    Returns:
        Sorted list of date strings e.g. ["2026-05-23", "2026-05-24", "2026-05-25"]
    """
    source_path = BASE_RAW_PATH / source

    if not source_path.exists():
        return []

    dates = [
        d.name for d in source_path.iterdir()
        if d.is_dir() and d.name[0].isdigit()
    ]

    return sorted(dates)


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Run this file directly to test it:
        python ingestion/storage.py
    """
    # Import yahoo fetcher to get real data
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from ingestion.sources.yahoo import fetch_ohlcv, get_latest_price

    print("\n=== Testing Storage ===\n")

    # Test 1: save OHLCV data
    print("Test 1: Fetching and saving AAPL data...")
    records = fetch_ohlcv("AAPL", period="1d", interval="5m")
    if records:
        path = save_ohlcv("AAPL", records)
        print(f"  Saved to: {path}")

    # Test 2: save MSFT data
    print("Test 2: Saving MSFT data...")
    records = fetch_ohlcv("MSFT", period="1d", interval="5m")
    if records:
        path = save_ohlcv("MSFT", records)
        print(f"  Saved to: {path}")

    # Test 3: load it back
    print("Test 3: Loading AAPL data back from disk...")
    data = load_raw("yahoo_finance", "AAPL.json")
    if data:
        print(f"  Ticker: {data['ticker']}")
        print(f"  Records: {data['record_count']}")
        print(f"  First record: {data['records'][0]}")

    # Test 4: list available dates
    print("Test 4: Available dates for yahoo_finance...")
    dates = list_available_dates("yahoo_finance")
    print(f"  Dates: {dates}")

    print("\n=== Storage tests passed ===\n")
    print("Check your data/raw/ folder in VS Code — you should see the JSON files!")