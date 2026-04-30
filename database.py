"""
Persistencia de terrenos scrapeados de ZonaProp.

Backend dual:
- PostgreSQL (asyncpg) cuando hay `DATABASE_URL` — modo producción en Railway,
  donde el filesystem es efímero y SQLite no sobrevive a deploys/restarts.
- SQLite (aiosqlite) cuando no hay `DATABASE_URL` — modo desarrollo local.

La API pública (`init_db`, `upsert_terrenos`, `get_terrenos`, ...) es
backend-agnóstica: el dispatch al motor correcto ocurre dentro de cada función.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Iterable

DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_DB_PATH = os.getenv("CIUDAD3D_DB_PATH", "ciudad3d.db")
IS_POSTGRES = bool(DATABASE_URL)

if IS_POSTGRES:
    import asyncpg
else:
    import aiosqlite


# ── Schemas ───────────────────────────────────────────────────────────────────

PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS terrenos (
    id              TEXT PRIMARY KEY,
    title           TEXT,
    price           DOUBLE PRECISION,
    currency        TEXT,
    surface_total   DOUBLE PRECISION,
    surface_covered DOUBLE PRECISION,
    address         TEXT,
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,
    url             TEXT,
    description     TEXT,
    photos          TEXT,
    scraped_at      TEXT NOT NULL,
    first_seen_at   TEXT NOT NULL,
    last_seen_at    TEXT NOT NULL,
    is_new          BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_terrenos_address ON terrenos(address);
CREATE INDEX IF NOT EXISTS idx_terrenos_price   ON terrenos(price);
CREATE INDEX IF NOT EXISTS idx_terrenos_surface ON terrenos(surface_total);
CREATE INDEX IF NOT EXISTS idx_terrenos_scraped ON terrenos(scraped_at);

CREATE TABLE IF NOT EXISTS precio_historial (
    id              SERIAL PRIMARY KEY,
    terreno_id      TEXT NOT NULL,
    precio_anterior DOUBLE PRECISION,
    precio_nuevo    DOUBLE PRECISION,
    currency        TEXT,
    detected_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_historial_terreno  ON precio_historial(terreno_id);
CREATE INDEX IF NOT EXISTS idx_historial_detected ON precio_historial(detected_at);
"""

SQLITE_SCHEMA = """
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


# ── Pool de PostgreSQL (por event-loop) ───────────────────────────────────────
# Las tools de tools.py corren cada llamada con `asyncio.run()` dentro de un
# thread, lo que crea un event-loop nuevo cada vez. asyncpg.Pool está atado al
# loop en el que se creó, así que cacheamos un pool por loop. El loop principal
# de FastAPI obtiene su pool en startup y lo reutiliza para todas las requests.

_pg_pools: dict[int, "asyncpg.Pool"] = {}


async def _get_pg_pool() -> "asyncpg.Pool":
    loop = asyncio.get_running_loop()
    key = id(loop)
    pool = _pg_pools.get(key)
    if pool is not None and not pool._closed:
        return pool
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )
    _pg_pools[key] = pool
    return pool


@asynccontextmanager
async def _pg_conn() -> AsyncIterator["asyncpg.Connection"]:
    pool = await _get_pg_pool()
    async with pool.acquire() as conn:
        yield conn


async def close_pg_pool() -> None:
    """Cierra el pool del loop actual (útil para tests / shutdown limpio)."""
    if not IS_POSTGRES:
        return
    loop = asyncio.get_running_loop()
    pool = _pg_pools.pop(id(loop), None)
    if pool is not None and not pool._closed:
        await pool.close()


# ── init_db ──────────────────────────────────────────────────────────────────

async def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Crea las tablas si no existen y (en pg) deja el pool listo para el loop."""
    if IS_POSTGRES:
        async with _pg_conn() as conn:
            await conn.execute(PG_SCHEMA)
    else:
        async with aiosqlite.connect(db_path) as db:
            await db.executescript(SQLITE_SCHEMA)
            await db.commit()


# ── upsert_terrenos ──────────────────────────────────────────────────────────

async def upsert_terrenos(
    listings: Iterable[dict[str, Any]],
    db_path: str = DEFAULT_DB_PATH,
) -> dict[str, int]:
    """
    Inserta o actualiza los listings. Marca como `is_new=True` los IDs que no
    estaban en la base anterior y como `is_new=False` los que ya existían.

    Si un listing ya existía y cambió de precio, registra el cambio en
    `precio_historial`.

    Devuelve {"new": int, "updated": int, "total": int, "price_changes": int}.
    """
    listings = list(listings)
    now = datetime.now(timezone.utc).isoformat()
    if IS_POSTGRES:
        return await _upsert_pg(listings, now)
    return await _upsert_sqlite(listings, now, db_path)


async def _upsert_pg(listings: list[dict[str, Any]], now: str) -> dict[str, int]:
    async with _pg_conn() as conn:
        async with conn.transaction():
            rows = await conn.fetch("SELECT id, price, currency FROM terrenos")
            prior_prices: dict[str, tuple[float | None, str | None]] = {
                r["id"]: (r["price"], r["currency"]) for r in rows
            }
            prior_ids = set(prior_prices.keys())

            await conn.execute("UPDATE terrenos SET is_new = FALSE")

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
                        await conn.execute(
                            """
                            INSERT INTO precio_historial (
                                terreno_id, precio_anterior, precio_nuevo, currency, detected_at
                            ) VALUES ($1, $2, $3, $4, $5)
                            """,
                            item["id"], old_price, new_price, new_currency, now,
                        )
                        price_change_count += 1

                await conn.execute(
                    """
                    INSERT INTO terrenos (
                        id, title, price, currency,
                        surface_total, surface_covered,
                        address, lat, lng, url,
                        description, photos,
                        scraped_at, first_seen_at, last_seen_at, is_new
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16)
                    ON CONFLICT (id) DO UPDATE SET
                        title           = EXCLUDED.title,
                        price           = EXCLUDED.price,
                        currency        = EXCLUDED.currency,
                        surface_total   = EXCLUDED.surface_total,
                        surface_covered = EXCLUDED.surface_covered,
                        address         = EXCLUDED.address,
                        lat             = EXCLUDED.lat,
                        lng             = EXCLUDED.lng,
                        url             = EXCLUDED.url,
                        description     = EXCLUDED.description,
                        photos          = EXCLUDED.photos,
                        scraped_at      = EXCLUDED.scraped_at,
                        last_seen_at    = EXCLUDED.last_seen_at,
                        is_new          = FALSE
                    """,
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
                    bool(is_new_row),
                )

            total = await conn.fetchval("SELECT COUNT(*) FROM terrenos")

    return {
        "new": new_count,
        "updated": updated_count,
        "total": int(total),
        "price_changes": price_change_count,
    }


async def _upsert_sqlite(
    listings: list[dict[str, Any]], now: str, db_path: str
) -> dict[str, int]:
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("SELECT id, price, currency FROM terrenos")
        rows = await cur.fetchall()
        await cur.close()
        prior_prices: dict[str, tuple[float | None, str | None]] = {
            r[0]: (r[1], r[2]) for r in rows
        }
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


# ── Lecturas ─────────────────────────────────────────────────────────────────

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
      - only_new (bool): sólo los marcados como is_new
    """
    filters = filters or {}
    if IS_POSTGRES:
        return await _get_terrenos_pg(filters, limit)
    return await _get_terrenos_sqlite(filters, limit, db_path)


async def _get_terrenos_pg(
    filters: dict[str, Any], limit: int
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []

    def _ph() -> str:
        return f"${len(params) + 1}"

    if zona := filters.get("zona"):
        like = f"%{zona.lower()}%"
        params.append(like)
        a = _ph()
        params.append(like)
        b = _ph()
        where.append(
            f"(LOWER(COALESCE(address,'')) LIKE {a} OR LOWER(COALESCE(title,'')) LIKE {b})"
        )

    if (pmax := filters.get("precio_max")) is not None:
        params.append(pmax)
        where.append(f"price <= {_ph()}")

    if (pmin := filters.get("precio_min")) is not None:
        params.append(pmin)
        where.append(f"price >= {_ph()}")

    if (smin := filters.get("superficie_min")) is not None:
        params.append(smin)
        where.append(f"surface_total >= {_ph()}")

    if (smax := filters.get("superficie_max")) is not None:
        params.append(smax)
        where.append(f"surface_total <= {_ph()}")

    if cur := filters.get("currency"):
        params.append(cur.upper())
        where.append(f"UPPER(COALESCE(currency,'')) = {_ph()}")

    if filters.get("only_new"):
        where.append("is_new = TRUE")

    sql = "SELECT * FROM terrenos"
    if where:
        sql += " WHERE " + " AND ".join(where)
    params.append(limit)
    sql += f" ORDER BY last_seen_at DESC LIMIT {_ph()}"

    async with _pg_conn() as conn:
        rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


async def _get_terrenos_sqlite(
    filters: dict[str, Any], limit: int, db_path: str
) -> list[dict[str, Any]]:
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

    if cur := filters.get("currency"):
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
    return [{k: r[k] for k in r.keys()} for r in rows]


async def get_new_terrenos(
    since: str | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> list[dict[str, Any]]:
    """
    Devuelve los terrenos nuevos.
    Si `since` (ISO date string) está seteado, filtra por first_seen_at >= since.
    Si no, devuelve los que tienen is_new del último scrape.
    """
    if IS_POSTGRES:
        async with _pg_conn() as conn:
            if since:
                rows = await conn.fetch(
                    "SELECT * FROM terrenos WHERE first_seen_at >= $1 ORDER BY first_seen_at DESC",
                    since,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM terrenos WHERE is_new = TRUE ORDER BY first_seen_at DESC"
                )
        return [dict(r) for r in rows]

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
    return [{k: r[k] for k in r.keys()} for r in rows]


async def get_terreno_by_id(
    listing_id: str, db_path: str = DEFAULT_DB_PATH
) -> dict[str, Any] | None:
    if IS_POSTGRES:
        async with _pg_conn() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM terrenos WHERE id = $1", listing_id
            )
        return dict(row) if row else None

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM terrenos WHERE id = ?", (listing_id,))
        row = await cur.fetchone()
        await cur.close()
    return {k: row[k] for k in row.keys()} if row else None


async def get_terreno_by_url(
    url: str, db_path: str = DEFAULT_DB_PATH
) -> dict[str, Any] | None:
    if IS_POSTGRES:
        async with _pg_conn() as conn:
            row = await conn.fetchrow("SELECT * FROM terrenos WHERE url = $1", url)
        return dict(row) if row else None

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM terrenos WHERE url = ?", (url,))
        row = await cur.fetchone()
        await cur.close()
    return {k: row[k] for k in row.keys()} if row else None


async def count_terrenos(db_path: str = DEFAULT_DB_PATH) -> int:
    if IS_POSTGRES:
        async with _pg_conn() as conn:
            n = await conn.fetchval("SELECT COUNT(*) FROM terrenos")
        return int(n)

    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("SELECT COUNT(*) FROM terrenos")
        (n,) = await cur.fetchone()
        await cur.close()
    return int(n)


async def get_historial_precio(
    terreno_id: str, db_path: str = DEFAULT_DB_PATH
) -> list[dict[str, Any]]:
    """Devuelve todos los cambios de precio de un terreno, ordenados por fecha."""
    if IS_POSTGRES:
        async with _pg_conn() as conn:
            rows = await conn.fetch(
                """
                SELECT id, terreno_id, precio_anterior, precio_nuevo, currency, detected_at
                FROM precio_historial
                WHERE terreno_id = $1
                ORDER BY detected_at ASC
                """,
                terreno_id,
            )
        return [dict(r) for r in rows]

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
    return [{k: r[k] for k in r.keys()} for r in rows]


async def get_terrenos_con_bajas(
    dias: int = 7, db_path: str = DEFAULT_DB_PATH
) -> list[dict[str, Any]]:
    """
    Terrenos cuyo precio bajó en los últimos `dias` días, ordenados por la
    mayor caída absoluta (precio_anterior - precio_nuevo).

    Si un mismo terreno tuvo varias bajas en la ventana, se considera la más
    reciente.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=dias)).isoformat()

    if IS_POSTGRES:
        sql = """
            WITH ultimas AS (
                SELECT terreno_id, MAX(detected_at) AS max_detected
                FROM precio_historial
                WHERE detected_at >= $1 AND precio_nuevo < precio_anterior
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
        async with _pg_conn() as conn:
            rows = await conn.fetch(sql, cutoff)
        return [dict(r) for r in rows]

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
    return [{k: r[k] for k in r.keys()} for r in rows]
