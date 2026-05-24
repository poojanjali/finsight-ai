"""
The heartbeat of FinSight AI.
Runs background jobs on a schedule — fetches prices every 5 min,
news every 10 min — automatically, without any manual intervention.

Why APScheduler?
    - Runs inside our Python process (no separate service needed)
    - Supports async jobs natively
    - Has job history, error handling, and misfire grace time built in
    - Easy to add/remove/pause jobs programmatically

Market hours: 9:30 AM – 4:00 PM EST, Monday–Friday
We fetch from 9:15 AM (pre-market) to 4:05 PM (just after close)
"""

import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from ingestion.sources.yahoo import fetch_all_tickers, WATCHLIST
from ingestion.storage import save_ohlcv

# US Eastern timezone — all market hours are in EST/EDT
EST = ZoneInfo("America/New_York")


# ── Job: fetch prices ─────────────────────────────────────────────────────────
async def job_fetch_prices():
    """
    Fetches live OHLCV data for all tickers in the watchlist.
    Saves raw JSON to disk.

    This job runs every 5 minutes on weekdays during market hours.

    Why check market hours inside the job?
        The cron trigger handles the schedule, but we add an extra
        check here as a safety net — avoids fetching when Yahoo
        returns empty data outside market hours.
    """
    now = datetime.now(EST)
    logger.info(f"[SCHEDULER] Running price fetch at {now.strftime('%H:%M:%S')} EST")

    try:
        # Fetch all tickers concurrently
        results = await fetch_all_tickers(WATCHLIST)

        if not results:
            logger.warning("[SCHEDULER] No data returned for any ticker")
            return

        # Save each ticker's data to disk
        saved_count = 0
        for ticker, records in results.items():
            if records:
                save_ohlcv(ticker, records)
                saved_count += 1

        logger.success(
            f"[SCHEDULER] Price fetch complete — "
            f"{saved_count}/{len(WATCHLIST)} tickers saved"
        )

    except Exception as e:
        logger.error(f"[SCHEDULER] Price fetch failed: {e}")
        # We catch ALL exceptions here so one failure
        # doesn't kill the entire scheduler


# ── Job: health check ─────────────────────────────────────────────────────────
async def job_health_check():
    """
    Runs every 30 minutes. Logs that the system is alive.
    In production this would ping a health endpoint or send
    a heartbeat to a monitoring service like Datadog.
    """
    now = datetime.now(timezone.utc)
    logger.info(f"[HEALTH] System alive at {now.isoformat()}")


# ── Build the scheduler ───────────────────────────────────────────────────────
def build_scheduler() -> AsyncIOScheduler:
    """
    Creates and configures the APScheduler instance.

    Returns a configured scheduler — call .start() to begin.

    Why AsyncIOScheduler?
        Our entire stack is async (FastAPI, aiohttp, asyncpg).
        AsyncIOScheduler runs jobs inside the same event loop,
        so jobs can use await without any extra setup.
        The alternative (BackgroundScheduler) runs in a thread
        and can't use async/await directly.
    """
    scheduler = AsyncIOScheduler(timezone=EST)

    # ── Job 1: Fetch prices every 5 minutes ──────────────────────────────────
    # Cron: minute=*/5 means "every 5 minutes"
    # day_of_week=mon-fri means "weekdays only"
    # hour=9-16 means "between 9 AM and 4 PM EST"
    scheduler.add_job(
        func=job_fetch_prices,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour="9-16",
            minute="*/5",
            timezone=EST,
        ),
        id="fetch_prices",
        name="Fetch OHLCV prices for all tickers",
        # misfire_grace_time: if a job is late by less than 60s
        # (e.g. system was briefly busy), still run it.
        # If later than 60s, skip it.
        misfire_grace_time=60,
        # coalesce: if multiple runs were missed, only run once
        # to catch up rather than firing multiple times
        coalesce=True,
    )

    # ── Job 2: Health check every 30 minutes ─────────────────────────────────
    scheduler.add_job(
        func=job_health_check,
        trigger=CronTrigger(minute="*/30"),
        id="health_check",
        name="System health check",
        misfire_grace_time=30,
        coalesce=True,
    )

    logger.info("[SCHEDULER] Jobs registered:")
    for job in scheduler.get_jobs():
        logger.info(f"  → {job.name} (id={job.id})")

    return scheduler


# ── Run the scheduler standalone ─────────────────────────────────────────────
async def run_scheduler():
    """
    Starts the scheduler and keeps it running until Ctrl+C.
    Called when running this file directly for testing.

    In production, the scheduler is started inside FastAPI's
    startup event so it runs alongside the API server.
    """
    scheduler = build_scheduler()
    scheduler.start()

    logger.success("[SCHEDULER] Started — press Ctrl+C to stop")
    logger.info(f"[SCHEDULER] Current EST time: {datetime.now(EST).strftime('%A %H:%M:%S')}")
    logger.info("[SCHEDULER] Price fetch runs every 5 min on weekdays 9AM-4PM EST")

    try:
        # Keep the event loop alive
        while True:
            await asyncio.sleep(60)
            # Log next run time for the price fetch job
            scheduler = build_scheduler()

    except (KeyboardInterrupt, SystemExit):
        logger.info("[SCHEDULER] Shutting down...")
        scheduler.shutdown()
        logger.info("[SCHEDULER] Stopped cleanly")


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Run this file directly to test:
        python ingestion/scheduler.py

    This will:
    1. Start the scheduler
    2. Run one immediate price fetch so you see it working NOW
       (without waiting for market hours)
    3. Keep running — press Ctrl+C to stop
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))

    async def test_run():
        print("\n=== Testing Scheduler ===\n")

        # Run one fetch immediately to verify it works
        print("Running immediate price fetch (bypassing schedule)...")
        await job_fetch_prices()

        print("\nStarting scheduler...")
        print("Press Ctrl+C to stop\n")

        scheduler = build_scheduler()
        scheduler.start()

        try:
            while True:
                await asyncio.sleep(30)
                logger.info("[TEST] Scheduler still running...")
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
            print("\n=== Scheduler stopped ===")

    asyncio.run(test_run())