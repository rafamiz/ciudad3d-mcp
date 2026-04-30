"""
MCP Server para Ciudad 3D — Buenos Aires.

Wrapper FastMCP sobre las funciones definidas en tools.py.
Expone las 20 herramientas a clientes MCP (Claude Desktop, Cursor, Cowork, etc.).

Por defecto corre con transporte stdio (uso local). Para hostearlo como MCP
remoto en Railway/Render, exportá MCP_TRANSPORT=streamable-http (ver README).
"""

from __future__ import annotations

import asyncio
import os

from mcp.server.fastmcp import FastMCP

import database as db
import scraper
import tools as t

mcp = FastMCP("Ciudad 3D CABA")


def _run_async(coro):
    """Ejecuta una corrutina desde contexto sync; abre nuevo loop si hace falta."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    return loop.run_until_complete(coro)


# ── Tools registradas ─────────────────────────────────────────────────────────

@mcp.tool()
def resolver_direccion(direccion: str) -> dict:
    """
    Normaliza y geolocaliza una dirección de CABA.
    Devuelve coordenadas (lng, lat) y la dirección normalizada.

    Args:
        direccion: Dirección en texto libre, ej: "Corrientes 1234".
    """
    return t.resolver_direccion(direccion)


@mcp.tool()
def get_parcela_por_coordenadas(lng: float, lat: float) -> dict:
    """Datos básicos de la parcela a partir de coordenadas (lng, lat)."""
    return t.get_parcela_por_coordenadas(lng, lat)


@mcp.tool()
def get_parcela_por_smp(smp: str) -> dict:
    """Datos básicos de una parcela por su SMP."""
    return t.get_parcela_por_smp(smp)


@mcp.tool()
def get_edificabilidad(smp: str) -> dict:
    """Parámetros urbanísticos del CUR para la parcela."""
    return t.get_edificabilidad(smp)


@mcp.tool()
def get_afectaciones(smp: str) -> dict:
    """Afectaciones urbanísticas (riesgo hídrico, LEP, ensanches, etc.)."""
    return t.get_afectaciones(smp)


@mcp.tool()
def get_plusvalia(smp: str) -> dict:
    """Plusvalía urbanística según el CUR."""
    return t.get_plusvalia(smp)


@mcp.tool()
def get_usos_del_suelo(smp: str) -> dict:
    """Mixtura de usos del suelo permitidos."""
    return t.get_usos_del_suelo(smp)


@mcp.tool()
def get_categorias_uso() -> list:
    """Lista de categorías de uso del suelo del CUR."""
    return t.get_categorias_uso()


@mcp.tool()
def get_rubros_por_categoria(categoria_id: int) -> dict:
    """Rubros de uso para una categoría dada."""
    return t.get_rubros_por_categoria(categoria_id)


@mcp.tool()
def get_obras(smp: str) -> dict:
    """Obras registradas e iniciadas en la parcela."""
    return t.get_obras(smp)


@mcp.tool()
def get_inspecciones(smp: str) -> dict:
    """Inspecciones municipales y CER de fachadas."""
    return t.get_inspecciones(smp)


@mcp.tool()
def get_patrimonio(smp: str) -> dict:
    """Catalogación patrimonial y Monumento Histórico Nacional."""
    return t.get_patrimonio(smp)


@mcp.tool()
def get_expedientes_sade(smp: str) -> dict:
    """Expedientes y tratas SADE."""
    return t.get_expedientes_sade(smp)


@mcp.tool()
def get_estado_parcelario(smp: str) -> dict:
    """Constitución y estado parcelario catastral."""
    return t.get_estado_parcelario(smp)


@mcp.tool()
def get_enrase(smp: str) -> dict:
    """Si la parcela es plausible de enrase con linderos."""
    return t.get_enrase(smp)


@mcp.tool()
def get_microcentro(smp: str) -> dict:
    """Si la parcela está en el área Microcentro de CABA."""
    return t.get_microcentro(smp)


@mcp.tool()
def get_datos_contextuales(lng: float, lat: float) -> dict:
    """Datos del entorno urbano para un punto (barrio, transporte, servicios)."""
    return t.get_datos_contextuales(lng, lat)


@mcp.tool()
def get_fotos_parcela(smp: str) -> dict:
    """Metadata y URLs de fotos de fachada de la parcela."""
    return t.get_fotos_parcela(smp)


@mcp.tool()
def get_geometria_parcela(smp: str) -> dict:
    """Geometría GeoJSON (polígono) de la parcela."""
    return t.get_geometria_parcela(smp)


@mcp.tool()
def get_informe_completo(smp: str) -> dict:
    """Informe completo combinando todas las consultas (due diligence)."""
    return t.get_informe_completo(smp)


# ── ZonaProp terrenos (scraper + SQLite) ─────────────────────────────────────

@mcp.tool()
def buscar_terrenos(
    zona: str | None = None,
    precio_max: float | None = None,
    superficie_min: float | None = None,
) -> dict:
    """
    Busca terrenos en venta (CABA) cacheados en SQLite, filtrando opcionalmente
    por zona (substring contra dirección/título), precio máximo y superficie
    mínima.

    Args:
        zona: Substring para matchear barrio o dirección (ej: "Palermo").
        precio_max: Precio máximo en la moneda nominal del listing.
        superficie_min: Superficie total mínima en m².
    """
    filters = {
        "zona": zona,
        "precio_max": precio_max,
        "superficie_min": superficie_min,
    }
    filters = {k: v for k, v in filters.items() if v is not None}

    async def _run():
        await db.init_db()
        items = await db.get_terrenos(filters=filters, limit=100)
        total = await db.count_terrenos()
        return {"count": len(items), "total_en_db": total, "results": items}

    return _run_async(_run())


@mcp.tool()
def terreno_detalle(url_or_id: str) -> dict:
    """
    Devuelve un terreno cacheado y lo cruza con datos urbanísticos de GCBA
    (parcela, edificabilidad, afectaciones, plusvalía, datos contextuales)
    usando lat/lng del listing.

    Args:
        url_or_id: URL completa de ZonaProp o el postingId.
    """
    async def _fetch():
        await db.init_db()
        if url_or_id.startswith("http"):
            return await db.get_terreno_by_url(url_or_id)
        return await db.get_terreno_by_id(url_or_id)

    listing = _run_async(_fetch())
    if not listing:
        return {"error": "terreno no encontrado en cache. Probá actualizar_terrenos() primero."}

    out: dict = {"listing": listing, "gcba": {}}
    lat = listing.get("lat")
    lng = listing.get("lng")
    if lat is None or lng is None:
        out["gcba"]["error"] = "el listing no tiene coordenadas"
        return out

    parcela = t.get_parcela_por_coordenadas(lng=lng, lat=lat)
    out["gcba"]["parcela"] = parcela
    out["gcba"]["contexto"] = t.get_datos_contextuales(lng=lng, lat=lat)

    smp = None
    if isinstance(parcela, dict):
        smp = parcela.get("smp") or parcela.get("SMP")
    if smp:
        out["gcba"]["smp"] = smp
        out["gcba"]["edificabilidad"] = t.get_edificabilidad(smp)
        out["gcba"]["afectaciones"] = t.get_afectaciones(smp)
        out["gcba"]["plusvalia"] = t.get_plusvalia(smp)
        out["gcba"]["usos"] = t.get_usos_del_suelo(smp)

    return out


@mcp.tool()
def actualizar_terrenos(max_pages: int = 20) -> dict:
    """
    Lanza un scrape fresco de ZonaProp (terrenos venta CABA), actualiza SQLite
    y devuelve un resumen con cuántos listings nuevos se encontraron.

    Args:
        max_pages: Máximo de páginas a scrapear (default 20).
    """
    async def _run():
        await db.init_db()
        listings = await scraper.scrape(max_pages=max_pages)
        stats = await db.upsert_terrenos(listings)
        return {
            "scraped": len(listings),
            "nuevos": stats["new"],
            "actualizados": stats["updated"],
            "total_en_db": stats["total"],
        }

    return _run_async(_run())


@mcp.tool()
def historial_precios(terreno_id: str) -> dict:
    """
    Devuelve la evolución de precio de un terreno cacheado: todos los cambios
    detectados (precio anterior → precio nuevo) ordenados cronológicamente.

    Args:
        terreno_id: postingId del listing en ZonaProp.
    """
    async def _run():
        await db.init_db()
        listing = await db.get_terreno_by_id(terreno_id)
        historial = await db.get_historial_precio(terreno_id)
        return {
            "terreno_id": terreno_id,
            "listing": listing,
            "cambios": historial,
            "total_cambios": len(historial),
        }

    return _run_async(_run())


@mcp.tool()
def terrenos_con_bajas(dias: int = 7) -> dict:
    """
    Lista terrenos cuyo precio bajó en los últimos `dias` días, ordenados por
    la mayor caída absoluta.

    Args:
        dias: Ventana de días hacia atrás para considerar bajas (default 7).
    """
    async def _run():
        await db.init_db()
        items = await db.get_terrenos_con_bajas(dias=dias)
        return {"dias": dias, "count": len(items), "results": items}

    return _run_async(_run())


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    if transport == "streamable-http":
        # Modo remoto: bind a 0.0.0.0:$PORT (Railway/Render lo setean automáticamente).
        mcp.settings.host = "0.0.0.0"
        mcp.settings.port = int(os.getenv("PORT", "8000"))
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")
