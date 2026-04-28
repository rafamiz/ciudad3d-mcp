"""
MCP Server para Ciudad 3D — Buenos Aires.

Wrapper FastMCP sobre las funciones definidas en tools.py.
Expone las 20 herramientas a clientes MCP (Claude Desktop, Cursor, Cowork, etc.).

Por defecto corre con transporte stdio (uso local). Para hostearlo como MCP
remoto en Railway/Render, exportá MCP_TRANSPORT=streamable-http (ver README).
"""

from __future__ import annotations

import os

from mcp.server.fastmcp import FastMCP

import tools as t

mcp = FastMCP("Ciudad 3D CABA")


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
