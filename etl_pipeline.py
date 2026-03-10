"""
Weather Data ETL Pipeline
=========================
Extracts weather data from Open-Meteo API (free, no key required),
transforms it with Python, and loads into MySQL database.

Usage:
    python etl_pipeline.py                    # Run full ETL
    python etl_pipeline.py --city "London"    # Single city
    python etl_pipeline.py --dry-run          # Extract + transform only
"""

import argparse
import logging
import sys
import time
from datetime import datetime

from config import CITIES, DB_CONFIG, PIPELINE_CONFIG
from extractor import WeatherExtractor
from transformer import WeatherTransformer
from loader import WeatherLoader
from logger import setup_logger


def run_pipeline(cities: list[dict], dry_run: bool = False) -> dict:
    """
    Orchestrates the full ETL pipeline.

    Args:
        cities: List of city dicts with name, lat, lon
        dry_run: If True, skip the Load step

    Returns:
        Summary dict with record counts and status
    """
    logger = logging.getLogger("etl.pipeline")
    summary = {
        "started_at": datetime.utcnow().isoformat(),
        "cities_processed": 0,
        "records_extracted": 0,
        "records_transformed": 0,
        "records_loaded": 0,
        "errors": [],
    }

    extractor = WeatherExtractor()
    transformer = WeatherTransformer()
    loader = WeatherLoader(DB_CONFIG) if not dry_run else None

    if loader:
        logger.info("🗄️  Initialising database schema...")
        loader.init_schema()

    for city in cities:
        city_name = city["name"]
        logger.info(f"━━━ Processing: {city_name} ━━━")

        try:
            # ── EXTRACT ──────────────────────────────────────────────
            logger.info(f"[EXTRACT] Fetching weather data for {city_name}")
            raw_data = extractor.fetch(city)
            summary["records_extracted"] += len(raw_data.get("hourly", {}).get("time", []))
            logger.info(f"[EXTRACT] ✓  {len(raw_data.get('hourly', {}).get('time', []))} hourly records")

            # ── TRANSFORM ────────────────────────────────────────────
            logger.info(f"[TRANSFORM] Cleaning and enriching data for {city_name}")
            transformed = transformer.transform(raw_data, city)
            summary["records_transformed"] += len(transformed)
            logger.info(f"[TRANSFORM] ✓  {len(transformed)} records ready")

            # ── LOAD ─────────────────────────────────────────────────
            if loader:
                logger.info(f"[LOAD] Inserting records into MySQL for {city_name}")
                inserted = loader.load(transformed)
                summary["records_loaded"] += inserted
                logger.info(f"[LOAD] ✓  {inserted} rows upserted")

            summary["cities_processed"] += 1

        except Exception as exc:
            logger.error(f"Pipeline failed for {city_name}: {exc}", exc_info=True)
            summary["errors"].append({"city": city_name, "error": str(exc)})

        # Respect API rate limits
        time.sleep(PIPELINE_CONFIG["api_delay_seconds"])
        logger.info(f"Waiting {PIPELINE_CONFIG['api_delay_seconds']} seconds before next request")
    summary["finished_at"] = datetime.utcnow().isoformat()
    summary["status"] = "SUCCESS" if not summary["errors"] else "PARTIAL"
    return summary


def main():
    setup_logger()
    logger = logging.getLogger("etl.pipeline")

    parser = argparse.ArgumentParser(description="Weather ETL Pipeline")
    parser.add_argument("--city", help="Run for a single city name (must exist in config)")
    parser.add_argument("--dry-run", action="store_true", help="Skip loading step")
    args = parser.parse_args()

    cities = CITIES
    if args.city:
        cities = [c for c in CITIES if c["name"].lower() == args.city.lower()]
        if not cities:
            logger.error(f"City '{args.city}' not found in config. Available: {[c['name'] for c in CITIES]}")
            sys.exit(1)

    mode = "DRY RUN" if args.dry_run else "FULL ETL"
    logger.info(f"🚀 Weather ETL Pipeline starting [{mode}] — {len(cities)} city/cities")

    summary = run_pipeline(cities, dry_run=args.dry_run)

    logger.info("=" * 55)
    logger.info("📊 PIPELINE SUMMARY")
    logger.info(f"   Status            : {summary['status']}")
    logger.info(f"   Cities processed  : {summary['cities_processed']}/{len(cities)}")
    logger.info(f"   Records extracted : {summary['records_extracted']}")
    logger.info(f"   Records transformed: {summary['records_transformed']}")
    logger.info(f"   Records loaded    : {summary['records_loaded']}")
    if summary["errors"]:
        logger.warning(f"   Errors ({len(summary['errors'])}): {summary['errors']}")
    logger.info("=" * 55)

    sys.exit(0 if summary["status"] != "FAILURE" else 1)


if __name__ == "__main__":
    main()
