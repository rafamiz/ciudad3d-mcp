"""
Ciudad 3D CABA — biblioteca compartida de herramientas.

Define las funciones puras que consultan los servicios públicos de GCBA y los
schemas de tool-use de Anthropic. Tanto el servidor MCP (ciudad3d_mcp.py) como
el backend del chat web (web.py) importan desde acá.
"""

from __future__ import annotations

import asyncio

import httpx

import database as db
import report_generator

# ── Base URLs ──────────────────────────────────────────────────────────────────
EPOK = "https://epok.buenosaires.gob.ar"
WS_USIG = "https://ws.usig.buenosaires.gob.ar"
FOTOS_USIG = "https://fotos.usig.buenosaires.gob.ar"
SERV_USIG = "https://servicios.usig.buenosaires.gob.ar"

DEFAULT_TIMEOUT = 10.0


def _get(url: str, params: dict | None = None) -> dict | list:
    """Helper HTTP GET con manejo de errores."""
    try:
        r = httpx.get(url, params=params, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as e:
        return {"error": f"HTTP {e.response.status_code}", "url": url}
    except Exception as e:
        return {"error": str(e), "url": url}


# ── Funciones (impl) ──────────────────────────────────────────────────────────

def resolver_direccion(direccion: str) -> dict:
    return _get(
        f"{SERV_USIG}/normalizar/",
        params={"direccion": direccion, "geocodificar": "true", "srid": "4326"},
    )


def get_parcela_por_coordenadas(lng: float, lat: float) -> dict:
    return _get(f"{EPOK}/catastro/parcela/", params={"lng": lng, "lat": lat})


def get_parcela_por_smp(smp: str) -> dict:
    return _get(f"{EPOK}/catastro/parcela/", params={"smp": smp})


def get_edificabilidad(smp: str) -> dict:
    return _get(f"{EPOK}/cur3d/seccion_edificabilidad/", params={"smp": smp})


def get_afectaciones(smp: str) -> dict:
    return _get(f"{EPOK}/cur3d/afectaciones/", params={"smp": smp})


def get_plusvalia(smp: str) -> dict:
    return _get(f"{EPOK}/cur3d/calcular_plusvalia/", params={"smp": smp})


def get_usos_del_suelo(smp: str) -> dict:
    return _get(f"{EPOK}/cur3d/mixtura_usos/", params={"smp": smp})


def get_categorias_uso() -> list:
    return _get(f"{EPOK}/cur3d/categorias")


def get_rubros_por_categoria(categoria_id: int) -> dict:
    return _get(f"{EPOK}/cur3d/cuadrosdeuso/rubros/", params={"categoria": categoria_id})


def get_obras(smp: str) -> dict:
    return _get(f"{EPOK}/cur3d/obras/", params={"smp": smp})


def get_inspecciones(smp: str) -> dict:
    return _get(f"{EPOK}/cur3d/inspecciones/", params={"smp": smp})


def get_patrimonio(smp: str) -> dict:
    return {
        "catalogacion": _get(f"{EPOK}/cur3d/fichadecatalogacion/", params={"smp": smp}),
        "monumento_historico_nacional": _get(
            f"{EPOK}/cur3d/monumento_historico_nacional/", params={"smp": smp}
        ),
    }


def get_expedientes_sade(smp: str) -> dict:
    return _get(f"{EPOK}/cur3d/sade/", params={"smp": smp})


def get_estado_parcelario(smp: str) -> dict:
    return _get(f"{EPOK}/cur3d/constitucion_estado_parcelario/", params={"smp": smp})


def get_enrase(smp: str) -> dict:
    return _get(f"{EPOK}/cur3d/parcelas_plausibles_a_enrase/", params={"smp": smp})


def get_microcentro(smp: str) -> dict:
    return _get(f"{EPOK}/cur3d/parcela_en_microcentro/", params={"smp": smp})


def get_datos_contextuales(lng: float, lat: float) -> dict:
    return _get(f"{WS_USIG}/datos_utiles", params={"x": lng, "y": lat})


def get_fotos_parcela(smp: str) -> dict:
    metadata = _get(f"{FOTOS_USIG}/getDatosFotos", params={"smp": smp})
    fotos_urls = []
    if isinstance(metadata, dict) and not metadata.get("error"):
        cantidad = metadata.get("cantidad", 0)
        for i in range(cantidad):
            fotos_urls.append(f"{FOTOS_USIG}/getFoto?smp={smp}&i={i}&w=600")
    return {"metadata": metadata, "urls": fotos_urls}


def get_geometria_parcela(smp: str) -> dict:
    return _get(f"{EPOK}/catastro/geometria/", params={"smp": smp})


def get_informe_completo(smp: str) -> dict:
    return {
        "parcela": _get(f"{EPOK}/catastro/parcela/", params={"smp": smp}),
        "edificabilidad": _get(f"{EPOK}/cur3d/seccion_edificabilidad/", params={"smp": smp}),
        "afectaciones": _get(f"{EPOK}/cur3d/afectaciones/", params={"smp": smp}),
        "plusvalia": _get(f"{EPOK}/cur3d/calcular_plusvalia/", params={"smp": smp}),
        "usos": _get(f"{EPOK}/cur3d/mixtura_usos/", params={"smp": smp}),
        "obras": _get(f"{EPOK}/cur3d/obras/", params={"smp": smp}),
        "inspecciones": _get(f"{EPOK}/cur3d/inspecciones/", params={"smp": smp}),
        "patrimonio": _get(f"{EPOK}/cur3d/fichadecatalogacion/", params={"smp": smp}),
        "monumento": _get(f"{EPOK}/cur3d/monumento_historico_nacional/", params={"smp": smp}),
        "enrase": _get(f"{EPOK}/cur3d/parcelas_plausibles_a_enrase/", params={"smp": smp}),
        "microcentro": _get(f"{EPOK}/cur3d/parcela_en_microcentro/", params={"smp": smp}),
        "estado_parcelario": _get(
            f"{EPOK}/cur3d/constitucion_estado_parcelario/", params={"smp": smp}
        ),
    }


# ── ZonaProp terrenos (SQLite cache + scraper) ────────────────────────────────
# Wrappers sync que corren las funciones async del módulo database. Usan
# asyncio.run() porque las tools del registry se invocan desde threads (vía
# asyncio.to_thread) y por lo tanto no hay loop activo en ese contexto.


def buscar_terrenos(
    zona: str | None = None,
    precio_max: float | None = None,
    superficie_min: float | None = None,
) -> dict:
    filters = {
        "zona": zona,
        "precio_max": precio_max,
        "superficie_min": superficie_min,
    }
    filters = {k: v for k, v in filters.items() if v is not None}

    async def _run():
        items = await db.get_terrenos(filters=filters, limit=50)
        total = await db.count_terrenos()
        return {"count": len(items), "total_en_db": total, "results": items}

    return asyncio.run(_run())


def terreno_detalle(terreno_id: str) -> dict:
    async def _fetch():
        return await db.get_terreno_by_id(terreno_id)

    listing = asyncio.run(_fetch())
    if not listing:
        return {
            "error": f"terreno {terreno_id} no encontrado en cache",
        }

    out: dict = {"listing": listing, "gcba": {}}
    lat = listing.get("lat")
    lng = listing.get("lng")
    if lat is None or lng is None:
        out["gcba"]["error"] = "el listing no tiene coordenadas"
        return out

    parcela = get_parcela_por_coordenadas(lng=lng, lat=lat)
    out["gcba"]["parcela"] = parcela
    out["gcba"]["contexto"] = get_datos_contextuales(lng=lng, lat=lat)

    smp = None
    if isinstance(parcela, dict):
        smp = parcela.get("smp") or parcela.get("SMP")
    if smp:
        out["gcba"]["smp"] = smp
        out["gcba"]["edificabilidad"] = get_edificabilidad(smp)
        out["gcba"]["afectaciones"] = get_afectaciones(smp)
        out["gcba"]["plusvalia"] = get_plusvalia(smp)
        out["gcba"]["usos"] = get_usos_del_suelo(smp)

    return out


def terrenos_con_bajas(dias: int = 7) -> dict:
    async def _run():
        return await db.get_terrenos_con_bajas(dias=dias)

    items = asyncio.run(_run())
    return {"dias": dias, "count": len(items), "results": items}


def historial_precios(terreno_id: str) -> dict:
    async def _run():
        listing = await db.get_terreno_by_id(terreno_id)
        cambios = await db.get_historial_precio(terreno_id)
        return listing, cambios

    listing, cambios = asyncio.run(_run())
    return {
        "terreno_id": terreno_id,
        "listing": listing,
        "cambios": cambios,
        "total_cambios": len(cambios),
    }


def generar_informe(terreno_id: str) -> dict:
    async def _fetch():
        listing = await db.get_terreno_by_id(terreno_id)
        cambios = await db.get_historial_precio(terreno_id)
        return listing, cambios

    listing, historial = asyncio.run(_fetch())
    if not listing:
        return {"error": f"terreno {terreno_id} no encontrado en cache"}

    gcba: dict = {}
    lat = listing.get("lat")
    lng = listing.get("lng")
    if lat is not None and lng is not None:
        parcela = get_parcela_por_coordenadas(lng=lng, lat=lat)
        gcba["parcela"] = parcela
        gcba["contexto"] = get_datos_contextuales(lng=lng, lat=lat)
        smp = None
        if isinstance(parcela, dict):
            smp = parcela.get("smp") or parcela.get("SMP")
        if smp:
            gcba["smp"] = smp
            gcba["edificabilidad"] = get_edificabilidad(smp)
            gcba["afectaciones"] = get_afectaciones(smp)
            gcba["plusvalia"] = get_plusvalia(smp)
            gcba["usos"] = get_usos_del_suelo(smp)
            gcba["patrimonio"] = get_patrimonio(smp)

    pdf_path = report_generator.generate_report({
        "listing": listing,
        "gcba": gcba,
        "historial": historial,
    })

    return {
        "ok": True,
        "terreno_id": terreno_id,
        "pdf_path": pdf_path,
        "message": f"Informe generado y guardado en: {pdf_path}",
    }


# ── Registry para tool-use ────────────────────────────────────────────────────
# Mapea nombre → (función, descripción, schema input)

TOOL_REGISTRY: dict[str, dict] = {
    "resolver_direccion": {
        "fn": resolver_direccion,
        "description": (
            "Normaliza y geolocaliza una dirección de CABA. Devuelve coordenadas "
            "(lng, lat) y la dirección normalizada. Usar como primer paso cuando el "
            "usuario da una dirección en texto libre."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "direccion": {
                    "type": "string",
                    "description": "Dirección en texto libre, ej: 'Corrientes 1234' o 'Av. Santa Fe 3000'.",
                }
            },
            "required": ["direccion"],
        },
    },
    "get_parcela_por_coordenadas": {
        "fn": get_parcela_por_coordenadas,
        "description": (
            "Obtiene los datos básicos de la parcela a partir de coordenadas geográficas. "
            "Devuelve el SMP (Sección-Manzana-Parcela), dirección, barrio, comuna y superficies."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "lng": {"type": "number", "description": "Longitud, ej: -58.4308"},
                "lat": {"type": "number", "description": "Latitud, ej: -34.6128"},
            },
            "required": ["lng", "lat"],
        },
    },
    "get_parcela_por_smp": {
        "fn": get_parcela_por_smp,
        "description": (
            "Obtiene los datos básicos de una parcela por su SMP. Devuelve dirección, "
            "barrio, comuna, superficie, pisos, unidades funcionales, puertas."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "smp": {"type": "string", "description": "Identificador SMP, ej: '045-143-001J'"},
            },
            "required": ["smp"],
        },
    },
    "get_edificabilidad": {
        "fn": get_edificabilidad,
        "description": (
            "Parámetros urbanísticos del Código Urbanístico para la parcela: FOT, altura "
            "máxima, superficie edificable, plusvalía, distrito, catalogación patrimonial "
            "(APH), afectaciones, parcelas linderas y enrase."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string", "description": "SMP, ej: '045-143-001J'"}},
            "required": ["smp"],
        },
    },
    "get_afectaciones": {
        "fn": get_afectaciones,
        "description": (
            "Afectaciones urbanísticas: riesgo hídrico, LEP, ensanche/apertura de calle, CI digital."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string", "description": "SMP"}},
            "required": ["smp"],
        },
    },
    "get_plusvalia": {
        "fn": get_plusvalia,
        "description": (
            "Calcula la plusvalía urbanística según el CUR. Devuelve EM, PL, SL, "
            "incidencia UVA, alícuota y distrito CPU."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string", "description": "SMP"}},
            "required": ["smp"],
        },
    },
    "get_usos_del_suelo": {
        "fn": get_usos_del_suelo,
        "description": "Mixtura de usos del suelo permitidos en la parcela (3 niveles).",
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string", "description": "SMP"}},
            "required": ["smp"],
        },
    },
    "get_categorias_uso": {
        "fn": get_categorias_uso,
        "description": "Lista de categorías de uso del suelo del CUR.",
        "input_schema": {"type": "object", "properties": {}},
    },
    "get_rubros_por_categoria": {
        "fn": get_rubros_por_categoria,
        "description": "Rubros de uso para una categoría dada.",
        "input_schema": {
            "type": "object",
            "properties": {
                "categoria_id": {"type": "integer", "description": "ID de categoría"}
            },
            "required": ["categoria_id"],
        },
    },
    "get_obras": {
        "fn": get_obras,
        "description": "Obras registradas e iniciadas en la parcela y CER urbanísticos.",
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string"}},
            "required": ["smp"],
        },
    },
    "get_inspecciones": {
        "fn": get_inspecciones,
        "description": "Inspecciones municipales y CER de fachadas.",
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string"}},
            "required": ["smp"],
        },
    },
    "get_patrimonio": {
        "fn": get_patrimonio,
        "description": "Catalogación patrimonial y Monumento Histórico Nacional.",
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string"}},
            "required": ["smp"],
        },
    },
    "get_expedientes_sade": {
        "fn": get_expedientes_sade,
        "description": "Expedientes y tratas SADE asociados a la parcela.",
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string"}},
            "required": ["smp"],
        },
    },
    "get_estado_parcelario": {
        "fn": get_estado_parcelario,
        "description": "Constitución y estado parcelario catastral.",
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string"}},
            "required": ["smp"],
        },
    },
    "get_enrase": {
        "fn": get_enrase,
        "description": "Si la parcela es plausible de enrase con linderos.",
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string"}},
            "required": ["smp"],
        },
    },
    "get_microcentro": {
        "fn": get_microcentro,
        "description": "Si la parcela está dentro del área Microcentro de CABA.",
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string"}},
            "required": ["smp"],
        },
    },
    "get_datos_contextuales": {
        "fn": get_datos_contextuales,
        "description": "Datos del entorno urbano de un punto: barrio, comuna, servicios, transporte.",
        "input_schema": {
            "type": "object",
            "properties": {
                "lng": {"type": "number"},
                "lat": {"type": "number"},
            },
            "required": ["lng", "lat"],
        },
    },
    "get_fotos_parcela": {
        "fn": get_fotos_parcela,
        "description": "Metadata y URLs de fotos de fachada de la parcela.",
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string"}},
            "required": ["smp"],
        },
    },
    "get_geometria_parcela": {
        "fn": get_geometria_parcela,
        "description": "Geometría GeoJSON (polígono) de la parcela.",
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string"}},
            "required": ["smp"],
        },
    },
    "get_informe_completo": {
        "fn": get_informe_completo,
        "description": (
            "Informe completo combinando datos básicos, edificabilidad, afectaciones, "
            "plusvalía, usos, obras, inspecciones, patrimonio, enrase, microcentro y "
            "estado parcelario. Ideal para due diligence."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"smp": {"type": "string"}},
            "required": ["smp"],
        },
    },
    "buscar_terrenos": {
        "fn": buscar_terrenos,
        "description": (
            "Busca terrenos en venta en CABA scrapeados desde ZonaProp y cacheados en "
            "SQLite. Filtros opcionales por zona (substring contra dirección/título), "
            "precio máximo y superficie mínima. Devuelve hasta 50 listings con id, "
            "title, price, currency, surface_total, address, lat/lng y URL."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "zona": {
                    "type": "string",
                    "description": "Substring case-insensitive para barrio/dirección, ej: 'Palermo'.",
                },
                "precio_max": {
                    "type": "integer",
                    "description": "Precio máximo en la moneda nominal del listing (USD o ARS).",
                },
                "superficie_min": {
                    "type": "integer",
                    "description": "Superficie total mínima en m².",
                },
            },
        },
    },
    "terreno_detalle": {
        "fn": terreno_detalle,
        "description": (
            "Devuelve un terreno cacheado de ZonaProp y lo cruza automáticamente con "
            "los datos urbanísticos de GCBA (parcela, edificabilidad, afectaciones, "
            "plusvalía, usos, contexto) usando lat/lng del listing. Ideal cuando el "
            "usuario pregunta '¿qué se puede construir en este terreno?'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "terreno_id": {
                    "type": "string",
                    "description": "postingId del listing de ZonaProp (campo `id` de buscar_terrenos).",
                },
            },
            "required": ["terreno_id"],
        },
    },
    "terrenos_con_bajas": {
        "fn": terrenos_con_bajas,
        "description": (
            "Lista terrenos cuyo precio bajó en los últimos N días, ordenados por la "
            "mayor caída absoluta. Útil para detectar oportunidades."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dias": {
                    "type": "integer",
                    "description": "Ventana en días hacia atrás. Default 7.",
                    "default": 7,
                },
            },
        },
    },
    "historial_precios": {
        "fn": historial_precios,
        "description": (
            "Evolución de precios de un terreno: todos los cambios detectados en "
            "scrapes sucesivos (precio anterior → precio nuevo) ordenados "
            "cronológicamente."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "terreno_id": {
                    "type": "string",
                    "description": "postingId del listing de ZonaProp.",
                },
            },
            "required": ["terreno_id"],
        },
    },
    "generar_informe": {
        "fn": generar_informe,
        "description": (
            "Genera un informe PDF profesional para un terreno cacheado, cruzando los "
            "datos de ZonaProp con normativa GCBA (parcela, edificabilidad, "
            "afectaciones, patrimonio, usos) e historial de precios. Devuelve la ruta "
            "del PDF generado en el servidor."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "terreno_id": {
                    "type": "string",
                    "description": "postingId del listing de ZonaProp.",
                },
            },
            "required": ["terreno_id"],
        },
    },
}


def anthropic_tools() -> list[dict]:
    """Devuelve la lista de tools en formato Anthropic Messages API."""
    return [
        {
            "name": name,
            "description": meta["description"],
            "input_schema": meta["input_schema"],
        }
        for name, meta in TOOL_REGISTRY.items()
    ]


def run_tool(name: str, arguments: dict) -> dict | list:
    """Ejecuta una tool del registry por nombre con los argumentos dados."""
    if name not in TOOL_REGISTRY:
        return {"error": f"tool '{name}' no existe"}
    fn = TOOL_REGISTRY[name]["fn"]
    try:
        return fn(**(arguments or {}))
    except TypeError as e:
        return {"error": f"argumentos inválidos para {name}: {e}"}
    except Exception as e:
        return {"error": str(e)}
