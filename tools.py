"""
Ciudad 3D CABA — biblioteca compartida de herramientas.

Define las funciones puras que consultan los servicios públicos de GCBA y los
schemas de tool-use de Anthropic. Tanto el servidor MCP (ciudad3d_mcp.py) como
el backend del chat web (web.py) importan desde acá.
"""

from __future__ import annotations

import httpx

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
