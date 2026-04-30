"""
Runner standalone del scraper de ZonaProp.

Ideal para ejecutar desde cron, Railway scheduled jobs o GitHub Actions:

    python scraper_scheduler.py
    python scraper_scheduler.py --max-pages 5

Variables de entorno:
    CIUDAD3D_DB_PATH: ruta de la base SQLite (default: ./ciudad3d.db)
    SCRAPER_MAX_PAGES: máximo de páginas a scrapear (default: 20)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

import database as db
import scraper

logger = logging.getLogger("ciudad3d.scheduler")


async def run_job(max_pages: int) -> dict:
    started_at = datetime.now(timezone.utc)
    logger.info("Scrape iniciado a las %s (max_pages=%d)", started_at.isoformat(), max_pages)

    await db.init_db()
    listings = await scraper.scrape(max_pages=max_pages)
    stats = await db.upsert_terrenos(listings)

    finished_at = datetime.now(timezone.utc)
    duration = (finished_at - started_at).total_seconds()

    logger.info(
        "Scrape OK: %d listings (%d nuevos, %d actualizados, %d total) en %.1fs",
        len(listings),
        stats["new"],
        stats["updated"],
        stats["total"],
        duration,
    )

    return {
        "scraped": len(listings),
        "new": stats["new"],
        "updated": stats["updated"],
        "total_in_db": stats["total"],
        "duration_seconds": duration,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape ZonaProp terrenos CABA → SQLite.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=int(os.getenv("SCRAPER_MAX_PAGES", "20")),
        help="Máximo de páginas a scrapear (default: 20)",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="DEBUG, INFO, WARNING, ERROR (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        result = asyncio.run(run_job(max_pages=args.max_pages))
    except KeyboardInterrupt:
        logger.warning("Cancelado por el usuario")
        return 130
    except Exception as e:
        logger.exception("Scrape falló: %s", e)
        return 1

    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
