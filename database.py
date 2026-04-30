"""
Persistencia SQLite (async) para terrenos scrapeados de ZonaProp.

Una sola tabla `terrenos` con todos los campos del scraper + flag `is_new` que
se setea en True para los IDs que no estaban en el scrape anterior.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Iterable

import aiosqlite

DEFAULT_DB_PATH = os.getenv("CIUDAD3D_DB_PATH", "ciudad3d.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS terrenos (
    id              TEXT PRIMARY KEY,
    title           TEXT,
    price           REAL,
    currency        TEXT,
    surface_total   REAL,
    surface_covered REAL,
    address         TEXT,
    lat             REAL,
    lng             REAL,
    url             TEXT,
    description     TEXT,
    photos          TEXT,
    scraped_at      TEXT NOT NULL,
    first_seen_at   TEXT NOT NULL,
    last_seen_at    TEXT NOT NULL,
    is_new          INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_terrenos_address ON terrenos(address);
CREATE INDEX IF NOT EXISTS idx_terrenos_price   ON terrenos(price);
CREATE INDEX IF NOT EXISTS idx_terrenos_surface ON terrenos(surface_total);
CREATE INDEX IF NOT EXISTS idx_terrenos_scraped ON terrenos(scraped_at);

CREATE TABLE IF NOT EXISTS precio_historial (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    terreno_id      TEXT NOT NULL,
    precio_anterior REAL,
    precio_nuevo    REAL,
    currency        TEXT,
    detected_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_historial_terreno  ON precio_historial(terreno_id);
CREATE INDEX IF NOT EXISTS idx_historial_detected ON precio_historial(detected_at);
"""


async def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Crea las tablas si no existen."""
    async with aiosqlite.connect(db_path) as db:
        await db.executescript(SCHEMA)
        await db.commit()


async def _existing_ids(db: aiosqlite.Connection) -> set[str]:
    cur = await db.execute("SELECT id FROM terrenos")
    rows = await cur.fetchall()
    await cur.close()
    return {r[0] for r in rows}


async def _existing_prices(
    db: aiosqlite.Connection,
) -> dict[str, tuple[float | None, str | None]]:
    cur = await db.execute("SELECT id, price, currency FROM terrenos")
    rows = await cur.fetchall()
    await cur.close()
    return {r[0]: (r[1], r[2]) for r in rows}


async def upsert_terrenos(
    listings: Iterable[dict[str, Any]],
    db_path: str = DEFAULT_DB_PATH,
) -> dict[str, int]:
    """
    Inserta o actualiza los listings. Marca como `is_new=1` los IDs que no
    estaban en la base anterior y como `is_new=0` los que ya existían.

    Si un listing ya existía y cambió de precio, registra el cambio en
    `precio_historial`.

    Devuelve {"new": int, "updated": int, "total": int, "price_changes": int}.
    """
    listings = list(listings)
    now = datetime.now(timezone.utc).isoformat()

    async with aiosqlite.connect(db_path) as db:
        prior_prices = await _existing_prices(db)
        prior_ids = set(prior_prices.keys())

        await db.execute("UPDATE terrenos SET is_new = 0")

        new_count = 0
        updated_count = 0
        price_change_count = 0
        for item in listings:
            is_new_row = item["id"] not in prior_ids
            if is_new_row:
                new_count += 1
            else:
                updated_count += 1
                old_price, old_currency = prior_prices[item["id"]]
                new_price = item.get("price")
                new_currency = item.get("currency")
                if (
                    old_price is not None
                    and new_price is not None
                    and (old_price != new_price or old_currency != new_currency)
                ):
                    await db.execute(
                        """
                        INSERT INTO precio_historial (
                            terreno_id, precio_anterior, precio_nuevo, currency, detected_at
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (item["id"], old_price, new_price, new_currency, now),
                    )
                    price_change_count += 1

            await db.execute(
                """
                INSERT INTO terrenos (
                    id, title, price, currency,
                    surface_total, surface_covered,
                    address, lat, lng, url,
                    description, photos,
                    scraped_at, first_seen_at, last_seen_at, is_new
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    title           = excluded.title,
                    price           = excluded.price,
                    currency        = excluded.currency,
                    surface_total   = excluded.surface_total,
                    surface_covered = excluded.surface_covered,
                    address         = excluded.address,
                    lat             = excluded.lat,
                    lng             = excluded.lng,
                    url             = excluded.url,
                    description     = excluded.description,
                    photos          = excluded.photos,
                    scraped_at      = excluded.scraped_at,
                    last_seen_at    = excluded.last_seen_at,
                    is_new          = 0
                """,
                (
                    item["id"],
                    item.get("title"),
                    item.get("price"),
                    item.get("currency"),
                    item.get("surface_total"),
                    item.get("surface_covered"),
                    item.get("address"),
                    item.get("lat"),
                    item.get("lng"),
                    item.get("url"),
                    item.get("description"),
                    item.get("photos"),
                    item.get("scraped_at") or now,
                    now,
                    now,
                    1 if is_new_row else 0,
                ),
            )

        await db.commit()

        cur = await db.execute("SELECT COUNT(*) FROM terrenos")
        (total,) = await cur.fetchone()
        await cur.close()

    return {
        "new": new_count,
        "updated": updated_count,
        "total": int(total),
        "price_changes": price_change_count,
    }


def _row_to_dict(row: aiosqlite.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


async def get_terrenos(
    filters: dict[str, Any] | None = None,
    limit: int = 100,
    db_path: str = DEFAULT_DB_PATH,
) -> list[dict[str, Any]]:
    """
    Filtros soportados:
      - zona (str): substring case-insensitive contra address y title
      - precio_max (number)
      - precio_min (number)
      - superficie_min (number)
      - superficie_max (number)
      - currency (str)
      - only_new (bool): sólo los marcados como is_new=1
    """
    filters = filters or {}
    where: list[str] = []
    params: list[Any] = []

    if zona := filters.get("zona"):
        where.append("(LOWER(COALESCE(address,'')) LIKE ? OR LOWER(COALESCE(title,'')) LIKE ?)")
        params.extend([f"%{zona.lower()}%", f"%{zona.lower()}%"])

    if (pmax := filters.get("precio_max")) is not None:
        where.append("price <= ?")
        params.append(pmax)

    if (pmin := filters.get("precio_min")) is not None:
        where.append("price >= ?")
        params.append(pmin)

    if (smin := filters.get("superficie_min")) is not None:
        where.append("surface_total >= ?")
        params.append(smin)

    if (smax := filters.get("superficie_max")) is not None:
        where.append("surface_total <= ?")
        params.append(smax)

    if (cur := filters.get("currency")):
        where.append("UPPER(COALESCE(currency,'')) = ?")
        params.append(cur.upper())

    if filters.get("only_new"):
        where.append("is_new = 1")

    sql = "SELECT * FROM terrenos"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY last_seen_at DESC LIMIT ?"
    params.append(limit)

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()

    return [_row_to_dict(r) for r in rows]


async def get_new_terrenos(
    since: str | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> list[dict[str, Any]]:
    """
    Devuelve los terrenos nuevos.
    Si `since` (ISO date string) está seteado, filtra por first_seen_at >= since.
    Si no, devuelve los que tienen is_new=1 del último scrape.
    """
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        if since:
            cur = await db.execute(
                "SELECT * FROM terrenos WHERE first_seen_at >= ? ORDER BY first_seen_at DESC",
                (since,),
            )
        else:
            cur = await db.execute(
                "SELECT * FROM terrenos WHERE is_new = 1 ORDER BY first_seen_at DESC"
            )
        rows = await cur.fetchall()
        await cur.close()

    return [_row_to_dict(r) for r in rows]


async def get_terreno_by_id(
    listing_id: str, db_path: str = DEFAULT_DB_PATH
) -> dict[str, Any] | None:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM terrenos WHERE id = ?", (listing_id,))
        row = await cur.fetchone()
        await cur.close()
    return _row_to_dict(row) if row else None


async def get_terreno_by_url(
    url: str, db_path: str = DEFAULT_DB_PATH
) -> dict[str, Any] | None:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM terrenos WHERE url = ?", (url,))
        row = await cur.fetchone()
        await cur.close()
    return _row_to_dict(row) if row else None


async def count_terrenos(db_path: str = DEFAULT_DB_PATH) -> int:
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("SELECT COUNT(*) FROM terrenos")
        (n,) = await cur.fetchone()
        await cur.close()
    return int(n)


async def get_historial_precio(
    terreno_id: str, db_path: str = DEFAULT_DB_PATH
) -> list[dict[str, Any]]:
    """Devuelve todos los cambios de precio de un terreno, ordenados por fecha."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id, terreno_id, precio_anterior, precio_nuevo, currency, detected_at
            FROM precio_historial
            WHERE terreno_id = ?
            ORDER BY detected_at ASC
            """,
            (terreno_id,),
        )
        rows = await cur.fetchall()
        await cur.close()
    return [_row_to_dict(r) for r in rows]


async def get_terrenos_con_bajas(
    dias: int = 7, db_path: str = DEFAULT_DB_PATH
) -> list[dict[str, Any]]:
    """
    Terrenos cuyo precio bajó en los últimos `dias` días, ordenados por la
    mayor caída absoluta (precio_anterior - precio_nuevo).

    Si un mismo terreno tuvo varias bajas en la ventana, se considera la más
    reciente.
    """
    from datetime import timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(days=dias)).isoformat()

    sql = """
        WITH ultimas AS (
            SELECT terreno_id, MAX(detected_at) AS max_detected
            FROM precio_historial
            WHERE detected_at >= ? AND precio_nuevo < precio_anterior
            GROUP BY terreno_id
        )
        SELECT
            ph.terreno_id,
            ph.precio_anterior,
            ph.precio_nuevo,
            ph.currency,
            ph.detected_at,
            (ph.precio_anterior - ph.precio_nuevo) AS caida_absoluta,
            t.title,
            t.address,
            t.url,
            t.surface_total
        FROM precio_historial ph
        JOIN ultimas u
            ON ph.terreno_id = u.terreno_id AND ph.detected_at = u.max_detected
        LEFT JOIN terrenos t ON t.id = ph.terreno_id
        ORDER BY caida_absoluta DESC
    """

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, (cutoff,))
        rows = await cur.fetchall()
        await cur.close()
    return [_row_to_dict(r) for r in rows]
