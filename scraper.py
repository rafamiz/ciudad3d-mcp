"""
Scraper de ZonaProp para terrenos en venta en CABA.

Fetcha listados desde https://www.zonaprop.com.ar/terrenos-venta-capital-federal-pagina-{n}.html
y parsea el JSON embebido en `window.__PRELOADED_STATE__` (variable global de
ZonaProp con el state inicial del bundle React). Camina a `listStore.listPostings`,
que es un array de objetos con `postingId`, `priceOperationTypes`, `mainFeatures`,
`postingLocation`, `visiblePictures`, etc.

Usa `curl_cffi` (asíncrono, drop-in tipo httpx) impersonando Chrome para evadir
el bloqueo a nivel TLS-fingerprint que aplica ZonaProp a clientes Python "puros".
Maneja paginación, delays educados (3–7s) y fallback gracioso si bloquean.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from datetime import datetime, timezone
from typing import Any

from curl_cffi import requests as cc_requests

logger = logging.getLogger("ciudad3d.scraper")

BASE_URL = "https://www.zonaprop.com.ar"
LISTING_URL_TEMPLATE = f"{BASE_URL}/terrenos-venta-capital-federal-pagina-{{page}}.html"

DEFAULT_MAX_PAGES = 20
DEFAULT_TIMEOUT = 30.0
DELAY_RANGE_SECONDS = (3.0, 7.0)

# curl_cffi impersonation profiles. Rotamos para reducir patterns repetitivos.
IMPERSONATE_PROFILES = ["chrome124", "chrome120", "chrome110"]

PRELOADED_STATE_RE = re.compile(
    r"window\.__PRELOADED_STATE__\s*=\s*(\{.*?\});\s*window\.",
    re.DOTALL,
)


def _extra_headers() -> dict[str, str]:
    return {
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


async def _polite_sleep() -> None:
    await asyncio.sleep(random.uniform(*DELAY_RANGE_SECONDS))


def _parse_preloaded_state(html: str) -> dict[str, Any] | None:
    """Extrae y parsea `window.__PRELOADED_STATE__` del HTML."""
    m = PRELOADED_STATE_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as e:
        logger.warning("No pude parsear __PRELOADED_STATE__: %s", e)
        return None


def _to_float(x: Any) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).replace(",", ".").strip()
    s = "".join(c for c in s if c.isdigit() or c == ".")
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _extract_price(raw: dict[str, Any]) -> tuple[float | None, str | None]:
    pot = raw.get("priceOperationTypes")
    if isinstance(pot, list) and pot:
        prices = pot[0].get("prices") if isinstance(pot[0], dict) else None
        if isinstance(prices, list) and prices:
            p = prices[0]
            return _to_float(p.get("amount")), p.get("currency")
    return None, None


def _extract_surface_from_main_features(raw: dict[str, Any], feature_id: str) -> float | None:
    feats = raw.get("mainFeatures")
    if not isinstance(feats, dict):
        return None
    node = feats.get(feature_id)
    if isinstance(node, dict):
        return _to_float(node.get("value"))
    return None


def _extract_address(raw: dict[str, Any]) -> str | None:
    loc = raw.get("postingLocation")
    if not isinstance(loc, dict):
        return None
    addr = loc.get("address")
    if isinstance(addr, dict):
        name = addr.get("name")
        if name:
            barrio = None
            inner = loc.get("location")
            if isinstance(inner, dict):
                barrio = inner.get("name")
            if barrio:
                return f"{name}, {barrio}"
            return name
    return None


def _extract_coords(raw: dict[str, Any]) -> tuple[float | None, float | None]:
    loc = raw.get("postingLocation")
    if isinstance(loc, dict):
        geo = loc.get("postingGeolocation")
        if isinstance(geo, dict):
            inner = geo.get("geolocation")
            if isinstance(inner, dict):
                return _to_float(inner.get("latitude")), _to_float(inner.get("longitude"))
    return None, None


def _extract_first_photo(raw: dict[str, Any]) -> str | None:
    vp = raw.get("visiblePictures")
    if isinstance(vp, dict):
        pics = vp.get("pictures")
        if isinstance(pics, list) and pics and isinstance(pics[0], dict):
            for key in ("url730x532", "url360x266", "url130x70"):
                url = pics[0].get(key)
                if url:
                    return url
    house = raw.get("house")
    if isinstance(house, dict):
        return house.get("image")
    return None


def _extract_url(raw: dict[str, Any]) -> str | None:
    url = raw.get("url")
    if not url:
        return None
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"{BASE_URL}{url}"
    return f"{BASE_URL}/{url}"


def normalize_listing(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Convierte un objeto listing crudo de ZonaProp en el shape que persistimos."""
    listing_id = raw.get("postingId") or raw.get("id")
    if not listing_id:
        return None

    price, currency = _extract_price(raw)
    lat, lng = _extract_coords(raw)

    return {
        "id": str(listing_id),
        "title": raw.get("title") or raw.get("generatedTitle") or "",
        "price": price,
        "currency": currency,
        "surface_total": _extract_surface_from_main_features(raw, "CFT100"),
        "surface_covered": _extract_surface_from_main_features(raw, "CFT101"),
        "address": _extract_address(raw),
        "lat": lat,
        "lng": lng,
        "url": _extract_url(raw),
        "description": raw.get("descriptionNormalized") or raw.get("iadescription"),
        "photos": _extract_first_photo(raw),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


async def fetch_page(
    session: cc_requests.AsyncSession, page: int
) -> tuple[list[dict[str, Any]], bool]:
    """
    Devuelve (listings_normalizados, has_more).
    has_more=False si la página vino vacía o bloqueada (cortar paginación).
    """
    url = LISTING_URL_TEMPLATE.format(page=page)
    try:
        resp = await session.get(url, headers=_extra_headers(), timeout=DEFAULT_TIMEOUT)
    except Exception as e:
        logger.warning("Error de red en página %d: %s", page, e)
        return [], False

    if resp.status_code in (403, 429, 503):
        logger.warning("ZonaProp respondió %s en página %d (posible bloqueo)", resp.status_code, page)
        return [], False
    if resp.status_code >= 400:
        logger.warning("Status %s en página %d", resp.status_code, page)
        return [], False

    state = _parse_preloaded_state(resp.text)
    if state is None:
        logger.warning("Sin __PRELOADED_STATE__ en página %d", page)
        return [], False

    list_store = state.get("listStore")
    if not isinstance(list_store, dict):
        logger.warning("listStore ausente en página %d", page)
        return [], False

    raw_listings = list_store.get("listPostings") or []
    if not isinstance(raw_listings, list):
        return [], False

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_listings:
        if not isinstance(raw, dict):
            continue
        norm = normalize_listing(raw)
        if norm and norm["id"] not in seen:
            seen.add(norm["id"])
            normalized.append(norm)

    return normalized, len(normalized) > 0


async def scrape(max_pages: int = DEFAULT_MAX_PAGES) -> list[dict[str, Any]]:
    """Scrapea hasta `max_pages` páginas y devuelve todos los listings únicos."""
    all_listings: dict[str, dict[str, Any]] = {}
    impersonate = random.choice(IMPERSONATE_PROFILES)

    async with cc_requests.AsyncSession(impersonate=impersonate) as session:
        for page in range(1, max_pages + 1):
            listings, has_more = await fetch_page(session, page)
            logger.info("Página %d: %d listings", page, len(listings))
            for item in listings:
                all_listings[item["id"]] = item
            if not has_more:
                logger.info("Corto paginación en página %d (no hay más resultados)", page)
                break
            if page < max_pages:
                await _polite_sleep()
    return list(all_listings.values())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    results = asyncio.run(scrape(max_pages=2))
    print(f"Total scrapeado: {len(results)} terrenos")
    for r in results[:3]:
        print(json.dumps(r, indent=2, ensure_ascii=False))
