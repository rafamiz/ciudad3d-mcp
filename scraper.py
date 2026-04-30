"""
Scraper de ZonaProp para terrenos en venta en CABA.

Fetcha listados desde https://www.zonaprop.com.ar/terrenos-venta-capital-federal-pagina-{n}.html,
parsea el JSON embebido en `__NEXT_DATA__` y devuelve listings normalizados.

Uso async con httpx + BeautifulSoup. Manejo de paginación, delays educados y
fallback gracioso si el sitio bloquea o cambia el shape del payload.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import datetime, timezone
from typing import Any, Iterable

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("ciudad3d.scraper")

BASE_URL = "https://www.zonaprop.com.ar"
LISTING_URL_TEMPLATE = f"{BASE_URL}/terrenos-venta-capital-federal-pagina-{{page}}.html"

DEFAULT_MAX_PAGES = 20
DEFAULT_TIMEOUT = 30.0
DELAY_RANGE_SECONDS = (3.0, 7.0)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]


def _headers() -> dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }


async def _polite_sleep() -> None:
    await asyncio.sleep(random.uniform(*DELAY_RANGE_SECONDS))


def _parse_next_data(html: str) -> dict[str, Any] | None:
    """Extrae el JSON del tag <script id="__NEXT_DATA__">."""
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag is None or not tag.string:
        return None
    try:
        return json.loads(tag.string)
    except json.JSONDecodeError as e:
        logger.warning("No pude parsear __NEXT_DATA__: %s", e)
        return None


def _walk_for_listings(node: Any) -> Iterable[dict[str, Any]]:
    """
    Camina el tree del NEXT_DATA buscando objetos que se vean como listings.
    El shape de ZonaProp varía: a veces es `props.pageProps.listings`, otras
    `props.pageProps.initialProps.listingsResult.listings`. Ser tolerantes.
    """
    if isinstance(node, dict):
        if (
            "postingId" in node
            or ("id" in node and "priceOperationTypes" in node)
            or ("postingId" in node and "title" in node)
        ):
            yield node
            return
        for v in node.values():
            yield from _walk_for_listings(v)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_for_listings(item)


def _first(d: dict | None, *keys: str) -> Any:
    """Devuelve el primer valor no-None entre las claves dadas."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        v = d.get(k)
        if v not in (None, "", []):
            return v
    return None


def _extract_price(raw: dict[str, Any]) -> tuple[float | None, str | None]:
    """Devuelve (price, currency). Tolera múltiples shapes."""
    pot = raw.get("priceOperationTypes")
    if isinstance(pot, list) and pot:
        prices = pot[0].get("prices") if isinstance(pot[0], dict) else None
        if isinstance(prices, list) and prices:
            p = prices[0]
            amount = _first(p, "amount", "value", "price")
            currency = _first(p, "currency", "currencySymbol")
            try:
                return (float(amount) if amount is not None else None, currency)
            except (TypeError, ValueError):
                return (None, currency)

    expenses = _first(raw, "expenses")
    amount = _first(raw, "price", "priceAmount", "amount")
    currency = _first(raw, "currency", "priceCurrency")
    try:
        return (float(amount) if amount is not None else None, currency)
    except (TypeError, ValueError):
        return (None, currency)


def _extract_surfaces(raw: dict[str, Any]) -> tuple[float | None, float | None]:
    """Devuelve (surface_total, surface_covered)."""

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

    total = _to_float(_first(raw, "totalSurface", "surfaceTotal", "surface"))
    covered = _to_float(_first(raw, "coveredSurface", "surfaceCovered"))

    if total is None and isinstance(raw.get("mainFeatures"), dict):
        feats = raw["mainFeatures"]
        for key in ("CFT100", "totalSurface"):
            if key in feats:
                total = _to_float(feats[key].get("value") if isinstance(feats[key], dict) else feats[key])
                if total is not None:
                    break
    return total, covered


def _extract_coords(raw: dict[str, Any]) -> tuple[float | None, float | None]:
    """Devuelve (lat, lng) tolerando múltiples shapes."""
    geo = raw.get("postingLocation") or raw.get("location") or {}
    if isinstance(geo, dict):
        coords = geo.get("postingGeolocation") or geo.get("geolocation") or geo
        if isinstance(coords, dict):
            lat = _first(coords, "latitude", "lat")
            lng = _first(coords, "longitude", "lng", "lon")
            try:
                return (
                    float(lat) if lat is not None else None,
                    float(lng) if lng is not None else None,
                )
            except (TypeError, ValueError):
                pass

    lat = _first(raw, "latitude", "lat")
    lng = _first(raw, "longitude", "lng", "lon")
    try:
        return (
            float(lat) if lat is not None else None,
            float(lng) if lng is not None else None,
        )
    except (TypeError, ValueError):
        return (None, None)


def _extract_address(raw: dict[str, Any]) -> str | None:
    loc = raw.get("postingLocation") or raw.get("location") or {}
    if isinstance(loc, dict):
        addr = _first(
            loc,
            "address",
            "addressName",
            "name",
            "fullAddress",
            "shortLocation",
        )
        if isinstance(addr, dict):
            return _first(addr, "name", "value")
        if addr:
            return str(addr)
    return _first(raw, "address", "addressName", "fullAddress")


def _extract_first_photo(raw: dict[str, Any]) -> str | None:
    photos = (
        raw.get("postingPicturesUrl")
        or raw.get("postingPictures")
        or raw.get("pictures")
        or raw.get("media")
    )
    if isinstance(photos, list) and photos:
        first = photos[0]
        if isinstance(first, str):
            return first
        if isinstance(first, dict):
            return _first(first, "url", "image", "src", "originalUrl")
    return _first(raw, "thumbnail", "mainPicture")


def _extract_url(raw: dict[str, Any]) -> str | None:
    url = _first(raw, "url", "permalink", "publicUrl", "shareUrl")
    if not url:
        return None
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"{BASE_URL}{url}"
    return f"{BASE_URL}/{url}"


def normalize_listing(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Convierte un objeto listing crudo en el shape que persistimos."""
    listing_id = _first(raw, "postingId", "id")
    if not listing_id:
        return None

    price, currency = _extract_price(raw)
    surface_total, surface_covered = _extract_surfaces(raw)
    lat, lng = _extract_coords(raw)

    return {
        "id": str(listing_id),
        "title": _first(raw, "title", "postingTitle", "name") or "",
        "price": price,
        "currency": currency,
        "surface_total": surface_total,
        "surface_covered": surface_covered,
        "address": _extract_address(raw),
        "lat": lat,
        "lng": lng,
        "url": _extract_url(raw),
        "description": _first(raw, "description", "postingDescription"),
        "photos": _extract_first_photo(raw),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


async def fetch_page(
    client: httpx.AsyncClient, page: int
) -> tuple[list[dict[str, Any]], bool]:
    """
    Devuelve (listings_normalizados, has_more).
    has_more = False si la página vino vacía o bloqueada (cortar paginación).
    """
    url = LISTING_URL_TEMPLATE.format(page=page)
    try:
        resp = await client.get(url, headers=_headers(), timeout=DEFAULT_TIMEOUT)
    except httpx.HTTPError as e:
        logger.warning("Error de red en página %d: %s", page, e)
        return [], False

    if resp.status_code in (403, 429, 503):
        logger.warning("ZonaProp respondió %s en página %d (posible bloqueo)", resp.status_code, page)
        return [], False
    if resp.status_code >= 400:
        logger.warning("Status %s en página %d", resp.status_code, page)
        return [], False

    data = _parse_next_data(resp.text)
    if data is None:
        logger.warning("Sin __NEXT_DATA__ en página %d", page)
        return [], False

    raw_listings = list(_walk_for_listings(data))
    normalized = []
    seen_ids: set[str] = set()
    for raw in raw_listings:
        norm = normalize_listing(raw)
        if norm and norm["id"] not in seen_ids:
            seen_ids.add(norm["id"])
            normalized.append(norm)

    return normalized, len(normalized) > 0


async def scrape(max_pages: int = DEFAULT_MAX_PAGES) -> list[dict[str, Any]]:
    """Scrapea hasta `max_pages` páginas y devuelve todos los listings únicos."""
    all_listings: dict[str, dict[str, Any]] = {}
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            listings, has_more = await fetch_page(client, page)
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
