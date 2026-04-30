"""
Runner standalone del scraper de ZonaProp.

Ideal para ejecutar desde cron, Railway scheduled jobs o GitHub Actions:

    python scraper_scheduler.py
    python scraper_scheduler.py --max-pages 5

Variables de entorno:
    DATABASE_URL: URL postgres (Railway). Si está seteada, escribe a Postgres.
    CIUDAD3D_DB_PATH: ruta de la base SQLite (default: ./ciudad3d.db). Sólo
        se usa cuando DATABASE_URL no está definida (modo local).
    SCRAPER_MAX_PAGES: máximo de páginas a scrapear (default: 20)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import database as db
import scraper

logger = logging.getLogger("ciudad3d.scheduler")

LAST_RUN_PATH = Path(__file__).resolve().parent / "last_run.json"


async def run_job(max_pages: int) -> dict:
    started_at = datetime.now(timezone.utc)
    started_at_local = datetime.now().replace(microsecond=0)
    logger.info("Scrape iniciado a las %s (max_pages=%d)", started_at.isoformat(), max_pages)

    await db.init_db()
    try:
        listings = await scraper.scrape(max_pages=max_pages)
        stats = await db.upsert_terrenos(listings)

        new_rows = await db.get_new_terrenos()
        new_listings = [
            {
                "id": row["id"],
                "price": row.get("price"),
                "currency": row.get("currency"),
                "surface_total": row.get("surface_total"),
                "address": row.get("address"),
                "url": row.get("url"),
            }
            for row in new_rows
        ]

        last_run_payload = {
            "ran_at": started_at_local.isoformat(),
            "new_count": stats["new"],
            "total_count": stats["total"],
            "new_listings": new_listings,
        }
        LAST_RUN_PATH.write_text(
            json.dumps(last_run_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("last_run.json escrito en %s", LAST_RUN_PATH)

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
    finally:
        await db.close_pg_pool()


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
