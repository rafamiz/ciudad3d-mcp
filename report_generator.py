"""
Generador de informes PDF para terrenos cruzados con datos urbanísticos GCBA.

Recibe un dict con `listing` (datos de ZonaProp), `gcba` (edificabilidad,
afectaciones, patrimonio, contexto, etc.) y `historial` (cambios de precio) y
produce un PDF profesional con reportlab Platypus en la carpeta `reports/`.
"""

from __future__ import annotations

import io
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    HRFlowable,
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# Paleta — azul/blanco profesional, mismo espíritu que ciudad3d_analisis.pdf
COLOR_PRIMARY = colors.HexColor("#1E3A5F")
COLOR_ACCENT = colors.HexColor("#2E6FA7")
COLOR_LIGHT = colors.HexColor("#EAF1F8")
COLOR_BORDER = colors.HexColor("#C5D3E2")
COLOR_TEXT = colors.HexColor("#1A1A1A")
COLOR_MUTED = colors.HexColor("#666666")


# ── Helpers de formato ────────────────────────────────────────────────────────

def _fmt_money(price: Any, currency: str | None) -> str:
    if price is None:
        return "Consultar"
    cur = (currency or "").upper() or "USD"
    try:
        return f"{cur} {float(price):,.0f}".replace(",", ".")
    except (TypeError, ValueError):
        return f"{cur} {price}"


def _fmt_m2(value: Any) -> str:
    if value in (None, "", 0):
        return "—"
    try:
        return f"{float(value):,.0f} m²".replace(",", ".")
    except (TypeError, ValueError):
        return str(value)


def _fmt_num(value: Any, decimals: int = 2) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return str(value)


def _fmt_date(iso: str | None) -> str:
    if not iso:
        return "—"
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).strftime("%d/%m/%Y")
    except Exception:
        return iso[:10] if len(iso) >= 10 else iso


def _safe_get(d: Any, *keys: str, default: Any = None) -> Any:
    """Anida .get() seguro: _safe_get(d, 'a', 'b', 'c')."""
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def _first_present(*values: Any) -> Any:
    for v in values:
        if v not in (None, "", [], {}):
            return v
    return None


def _is_error_payload(value: Any) -> bool:
    """True si la respuesta es un dict con clave 'error' o 'has_error'."""
    if not isinstance(value, dict):
        return False
    if "error" in value:
        return True
    if value.get("has_error"):
        return True
    return False


def _extract_fot_value(fot: Any) -> float | None:
    """Devuelve un FOT numérico representativo (máximo de las variantes)."""
    if fot is None:
        return None
    if isinstance(fot, (int, float)):
        return float(fot) if fot > 0 else None
    if isinstance(fot, dict):
        candidates: list[float] = []
        for k in ("fot_medianera", "fot_perim_libre", "fot_semi_libre", "fot", "FOT"):
            v = fot.get(k)
            if isinstance(v, (int, float)) and v > 0:
                candidates.append(float(v))
        if candidates:
            return max(candidates)
        return None
    try:
        f = float(fot)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _format_fot_breakdown(fot: Any) -> str:
    """Renderiza el FOT como 'Medianera: X | Perím. libre: Y | Semi-libre: Z'."""
    if fot is None:
        return "No disponible"
    if isinstance(fot, (int, float)):
        return _fmt_num(fot)
    if isinstance(fot, dict):
        labels = (
            ("fot_medianera", "Medianera"),
            ("fot_perim_libre", "Perím. libre"),
            ("fot_semi_libre", "Semi-libre"),
        )
        parts = []
        for key, label in labels:
            v = fot.get(key)
            if isinstance(v, (int, float)):
                parts.append(f"{label}: {_fmt_num(v)}")
        if parts:
            return " | ".join(parts)
        # Fallback: si no matchea ninguna clave conocida pero hay valores
        for k, v in fot.items():
            if isinstance(v, (int, float)):
                parts.append(f"{k}: {_fmt_num(v)}")
        if parts:
            return " | ".join(parts)
        return "No disponible"
    return str(fot)


def _extract_altura_value(altura: Any) -> float | None:
    """Para `altura_max` que viene como [17.2, 0, 0, 0]."""
    if altura is None:
        return None
    if isinstance(altura, (int, float)):
        return float(altura) if altura > 0 else None
    if isinstance(altura, list):
        valid = [float(x) for x in altura if isinstance(x, (int, float)) and x > 0]
        if valid:
            return max(valid)
        return None
    try:
        f = float(altura)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _extract_distrito(edif: dict, parcela: dict) -> str | None:
    """Busca el distrito/zona urbanística en varias rutas posibles."""
    # plusvalia.distrito_cpu es el lugar más confiable en seccion_edificabilidad
    cpu = _safe_get(edif, "plusvalia", "distrito_cpu")
    if cpu:
        return str(cpu)

    # distrito_especial: array de dicts con distrito_agrupado y distrito_especifico
    de = edif.get("distrito_especial") if isinstance(edif, dict) else None
    if isinstance(de, list):
        for d in de:
            if isinstance(d, dict):
                val = d.get("distrito_especifico") or d.get("distrito_agrupado")
                if val and str(val).strip():
                    return str(val)

    # Fallbacks legacy
    return _first_present(
        _safe_get(edif, "distrito"),
        _safe_get(edif, "zonificacion"),
        _safe_get(edif, "codigo_urbanistico", "distrito"),
        _safe_get(parcela, "distrito"),
    )


def _download_image(url: str, max_bytes: int = 4_000_000) -> io.BytesIO | None:
    try:
        r = httpx.get(url, timeout=8.0, follow_redirects=True)
        r.raise_for_status()
        if len(r.content) > max_bytes:
            return None
        return io.BytesIO(r.content)
    except Exception:
        return None


# ── Estilos ───────────────────────────────────────────────────────────────────

def _build_styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "title", parent=base["Title"],
            fontName="Helvetica-Bold", fontSize=20, leading=24,
            textColor=COLOR_PRIMARY, alignment=TA_LEFT, spaceAfter=4,
        ),
        "subtitle": ParagraphStyle(
            "subtitle", parent=base["Normal"],
            fontName="Helvetica", fontSize=11, leading=14,
            textColor=COLOR_MUTED, alignment=TA_LEFT, spaceAfter=6,
        ),
        "h2": ParagraphStyle(
            "h2", parent=base["Heading2"],
            fontName="Helvetica-Bold", fontSize=13, leading=16,
            textColor=colors.white, alignment=TA_LEFT,
            backColor=COLOR_PRIMARY, borderPadding=(6, 8, 6, 8),
            spaceBefore=10, spaceAfter=8,
        ),
        "body": ParagraphStyle(
            "body", parent=base["Normal"],
            fontName="Helvetica", fontSize=10, leading=13,
            textColor=COLOR_TEXT, alignment=TA_JUSTIFY,
        ),
        "small": ParagraphStyle(
            "small", parent=base["Normal"],
            fontName="Helvetica", fontSize=8, leading=10,
            textColor=COLOR_MUTED,
        ),
        "label": ParagraphStyle(
            "label", parent=base["Normal"],
            fontName="Helvetica-Bold", fontSize=9, leading=11,
            textColor=COLOR_PRIMARY,
        ),
        "value": ParagraphStyle(
            "value", parent=base["Normal"],
            fontName="Helvetica", fontSize=10, leading=12,
            textColor=COLOR_TEXT,
        ),
        "footer": ParagraphStyle(
            "footer", parent=base["Normal"],
            fontName="Helvetica-Oblique", fontSize=8, leading=10,
            textColor=COLOR_MUTED, alignment=TA_CENTER,
        ),
    }


def _kv_table(
    rows: list[tuple[Any, Any]],
    styles: dict,
    col_widths: tuple[float, float] = (4.5 * cm, 11.5 * cm),
) -> Table:
    """Tabla 2 columnas key/value con filas alternadas.

    Tanto el label como el value se envuelven en Paragraph para que el texto
    largo se ajuste correctamente al ancho de la columna.
    """
    data = []
    for k, v in rows:
        label = k if not isinstance(k, str) else Paragraph(k, styles["label"])
        value = v if not isinstance(v, str) else Paragraph(v, styles["value"])
        data.append([label, value])
    t = Table(data, colWidths=list(col_widths))
    style = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("LINEBELOW", (0, 0), (-1, -1), 0.4, COLOR_BORDER),
    ]
    for i in range(len(data)):
        if i % 2 == 0:
            style.append(("BACKGROUND", (0, i), (-1, i), COLOR_LIGHT))
    t.setStyle(TableStyle(style))
    return t


# ── Construcción de secciones ────────────────────────────────────────────────

def _build_header(listing: dict, styles: dict) -> list:
    address = _first_present(listing.get("address"), listing.get("title"), "Terreno sin dirección")
    price = _fmt_money(listing.get("price"), listing.get("currency"))
    surface = _fmt_m2(listing.get("surface_total"))

    elements: list = [
        Paragraph("INFORME DE TERRENO", styles["subtitle"]),
        Paragraph(str(address), styles["title"]),
        Paragraph(f"<b>{price}</b> &nbsp;&nbsp;·&nbsp;&nbsp; {surface} totales", styles["subtitle"]),
        HRFlowable(width="100%", thickness=2, color=COLOR_ACCENT, spaceBefore=4, spaceAfter=10),
    ]
    return elements


def _build_section_terreno(listing: dict, styles: dict) -> list:
    rows: list[tuple[str, str]] = [
        ("Dirección", str(_first_present(listing.get("address"), "—"))),
        ("Precio publicado", _fmt_money(listing.get("price"), listing.get("currency"))),
        ("Superficie total", _fmt_m2(listing.get("surface_total"))),
        ("Superficie cubierta", _fmt_m2(listing.get("surface_covered"))),
    ]
    if listing.get("lat") is not None and listing.get("lng") is not None:
        rows.append(("Coordenadas", f"{listing['lat']:.5f}, {listing['lng']:.5f}"))
    if listing.get("url"):
        rows.append(("Listing ZonaProp", f'<link href="{listing["url"]}" color="#2E6FA7">{listing["url"]}</link>'))
    if listing.get("first_seen_at"):
        rows.append(("Visto por primera vez", _fmt_date(listing.get("first_seen_at"))))

    elements: list = [
        Paragraph("1. Datos del terreno", styles["h2"]),
        _kv_table(rows, styles),
    ]

    photo_url = _extract_first_photo(listing)
    if photo_url:
        buf = _download_image(photo_url)
        if buf is not None:
            try:
                img = Image(buf, width=14 * cm, height=8 * cm, kind="proportional")
                elements.append(Spacer(1, 8))
                elements.append(img)
            except Exception:
                pass

    return elements


def _extract_first_photo(listing: dict) -> str | None:
    photos = listing.get("photos")
    if not photos:
        return None
    if isinstance(photos, str):
        try:
            parsed = json.loads(photos)
        except Exception:
            return photos if photos.startswith("http") else None
        photos = parsed
    if isinstance(photos, list) and photos:
        first = photos[0]
        if isinstance(first, str):
            return first
        if isinstance(first, dict):
            return first.get("url") or first.get("src")
    return None


def _build_section_normativa(gcba: dict, styles: dict) -> list:
    edif = gcba.get("edificabilidad") or {}
    parcela = gcba.get("parcela") or {}
    usos = gcba.get("usos") or {}

    edif_ok = isinstance(edif, dict) and not _is_error_payload(edif)
    parcela_ok = isinstance(parcela, dict) and not _is_error_payload(parcela)

    distrito = _extract_distrito(edif if edif_ok else {}, parcela if parcela_ok else {})
    fot_raw = _safe_get(edif, "fot") if edif_ok else None
    fos_raw = _safe_get(edif, "fos") if edif_ok else None  # No suele venir
    altura_raw = (
        _safe_get(edif, "altura_max")
        or _safe_get(edif, "altura_maxima")
        or _safe_get(edif, "alturaMaxima")
        or _safe_get(edif, "altura")
    ) if edif_ok else None
    altura_plano = _safe_get(edif, "altura_max_plano_limite") if edif_ok else None
    sup_edif = _first_present(
        _safe_get(edif, "sup_edificable_planta") if edif_ok else None,
        _safe_get(edif, "sup_max_edificable") if edif_ok else None,
        _safe_get(edif, "superficie_edificable") if edif_ok else None,
        _safe_get(edif, "superficieEdificable") if edif_ok else None,
    )
    sup_parcela = _safe_get(edif, "superficie_parcela") if edif_ok else None
    smp = _first_present(
        _safe_get(parcela, "smp") if parcela_ok else None,
        _safe_get(parcela, "SMP") if parcela_ok else None,
        gcba.get("smp"),
    )
    subzona = _safe_get(edif, "subzona") if edif_ok else None

    altura_val = _extract_altura_value(altura_raw)

    rows: list[tuple[str, str]] = [
        ("SMP (Sección-Manzana-Parcela)", str(smp) if smp else "No disponible"),
        ("Distrito / Zona", str(distrito) if distrito else "No disponible"),
    ]
    if subzona and str(subzona).strip():
        rows.append(("Subzona", str(subzona)))

    rows.append(("FOT (Factor de Ocupación Total)", _format_fot_breakdown(fot_raw)))
    rows.append((
        "FOS (Factor de Ocupación del Suelo)",
        _fmt_num(fos_raw) if fos_raw is not None else "No disponible (no provisto por API)",
    ))

    if altura_val is not None:
        rows.append(("Altura máxima", f"{_fmt_num(altura_val)} m"))
    else:
        rows.append(("Altura máxima", "No disponible"))

    if isinstance(altura_plano, (int, float)) and altura_plano > 0:
        rows.append(("Altura máx. plano límite", f"{_fmt_num(altura_plano)} m"))

    if isinstance(sup_parcela, (int, float)) and sup_parcela > 0:
        rows.append(("Superficie de parcela (CUR)", _fmt_m2(sup_parcela)))

    if isinstance(sup_edif, (int, float)) and sup_edif > 0:
        rows.append(("Superficie edificable (CUR)", _fmt_m2(sup_edif)))
    else:
        rows.append(("Superficie edificable (CUR)", "No disponible"))

    usos_str = _summarize_usos(usos)
    if usos_str:
        rows.append(("Usos habilitados", usos_str))

    elements: list = [Paragraph("2. Normativa urbanística", styles["h2"])]
    if not edif_ok:
        elements.append(Paragraph(
            "<i>No se pudieron obtener datos de edificabilidad de GCBA para este lote.</i>",
            styles["small"],
        ))
    elements.append(_kv_table(rows, styles))
    return elements


def _summarize_usos(usos: Any) -> str:
    if not isinstance(usos, dict) or _is_error_payload(usos):
        return ""
    # Forma frecuente del endpoint: {"usos": [n1, n2, n3]} con conteos por nivel
    arr = usos.get("usos")
    if isinstance(arr, list) and len(arr) >= 1 and all(isinstance(x, (int, float)) for x in arr):
        nivels = ["Nivel 1", "Nivel 2", "Nivel 3"]
        parts = []
        for i, count in enumerate(arr[:3]):
            label = nivels[i] if i < len(nivels) else f"Nivel {i + 1}"
            parts.append(f"{label}: {int(count)}")
        main = " | ".join(parts)
        return (
            f"{main}<br/>"
            f"<font size=\"8\" color=\"#666666\"><i>"
            "Consultar mixtura completa en EPOK"
            "</i></font>"
        )

    # Formas alternativas con listas nominadas
    candidates: list[str] = []
    for key in ("nivel1", "nivel2", "nivel3", "categorias", "rubros"):
        val = usos.get(key)
        if isinstance(val, list):
            for item in val[:5]:
                if isinstance(item, str):
                    candidates.append(item)
                elif isinstance(item, dict):
                    name = item.get("nombre") or item.get("descripcion") or item.get("rubro")
                    if name:
                        candidates.append(str(name))
    if not candidates:
        return ""
    return ", ".join(candidates[:8])


def _build_section_restricciones(gcba: dict, styles: dict) -> list:
    afect = gcba.get("afectaciones") or {}
    edif = gcba.get("edificabilidad") or {}
    patrim = gcba.get("patrimonio") or {}

    # Las afectaciones pueden venir tanto en /cur3d/afectaciones como anidadas
    # dentro de edificabilidad. Mergeamos con prioridad al endpoint dedicado.
    afect_edif = _safe_get(edif, "afectaciones") if isinstance(edif, dict) else None
    afect_merged: dict = {}
    if isinstance(afect_edif, dict):
        afect_merged.update(afect_edif)
    if isinstance(afect, dict) and not _is_error_payload(afect):
        afect_merged.update(afect)

    items: list[tuple[str, str]] = []

    riesgo = _first_present(
        afect_merged.get("riesgo_hidrico"),
        afect_merged.get("riesgoHidrico"),
        afect_merged.get("hidrico"),
    )
    items.append(("Riesgo hídrico", _format_afectacion(riesgo)))

    lep = _first_present(afect_merged.get("lep"), afect_merged.get("LEP"))
    items.append(("Línea de Edificación (LEP)", _format_afectacion(lep)))

    ensanche = _first_present(
        afect_merged.get("ensanche"),
        afect_merged.get("apertura"),
        afect_merged.get("apertura_calle"),
    )
    items.append(("Ensanche / apertura de calle", _format_afectacion(ensanche)))

    ci_digital = afect_merged.get("ci_digital")
    if ci_digital is not None:
        items.append(("CI digital", _format_afectacion(ci_digital)))

    items.append(("Catalogación patrimonial", _format_catalogacion(patrim, edif)))
    items.append(("Monumento histórico nacional", _format_monumento(patrim)))

    return [
        Paragraph("3. Restricciones y afectaciones", styles["h2"]),
        _kv_table(items, styles),
    ]


def _format_afectacion(value: Any) -> str:
    """API devuelve 0/1 (o None) por afectación. 0 = sin afectación."""
    if value is None:
        return "No disponible"
    if isinstance(value, bool):
        return "Sí" if value else "Sin afectación"
    if isinstance(value, (int, float)):
        if value == 0:
            return "Sin afectación"
        if value == 1:
            return "Sí"
        return f"Sí ({_fmt_num(value)})"
    if isinstance(value, str):
        if not value.strip():
            return "Sin afectación"
        return value
    if isinstance(value, dict):
        if _is_error_payload(value):
            return "No disponible"
        for k in ("descripcion", "nombre", "valor", "estado", "categoria"):
            if value.get(k):
                return str(value[k])
        return "Ver detalle (datos crudos)"
    if isinstance(value, list):
        if not value:
            return "Sin afectación"
        return f"{len(value)} ítem(s)"
    return str(value)


def _format_catalogacion(patrim: Any, edif: Any) -> str:
    """
    /cur3d/fichadecatalogacion devuelve {'exists': bool, 'has_error': bool}.
    edificabilidad.catalogacion devuelve un dict con denominacion, proteccion, etc.

    Devuelve un string con markup de Paragraph: valor principal + sub-línea
    pequeña con detalles cuando estén disponibles.
    """
    def _detail_line(cat_edif: dict) -> str:
        labels = (
            ("denominacion", "denominación"),
            ("proteccion", "protección"),
            ("estado", "estado"),
            ("ley_3056", "ley 3056"),
        )
        parts = []
        for k, label in labels:
            v = cat_edif.get(k)
            if v not in (None, "", []):
                parts.append(f"{label}: {v}")
        if not parts:
            return ""
        return f"<br/><font size=\"8\" color=\"#666666\">{' · '.join(parts)}</font>"

    def _nivel_proteccion(cat_edif: dict) -> str | None:
        for k in ("proteccion", "denominacion"):
            v = cat_edif.get(k)
            if v not in (None, "", []):
                return str(v)
        return None

    cat_patrim = _safe_get(patrim, "catalogacion") if isinstance(patrim, dict) else None
    cat_edif = _safe_get(edif, "catalogacion") if isinstance(edif, dict) else None

    if isinstance(cat_patrim, dict):
        if cat_patrim.get("has_error"):
            return "No disponible"
        if "exists" in cat_patrim:
            if cat_patrim.get("exists"):
                main = "Catalogada"
                if isinstance(cat_edif, dict):
                    nivel = _nivel_proteccion(cat_edif)
                    if nivel:
                        main = f"Catalogada — Nivel {nivel}"
                    return main + _detail_line(cat_edif)
                return main
            # No catalogada según fichadecatalogacion
            if isinstance(cat_edif, dict):
                return "Sin catalogación" + _detail_line(cat_edif)
            return "Sin catalogación"

    if isinstance(cat_edif, dict):
        if all(v in (None, "", []) for v in cat_edif.values()):
            return "Sin catalogación"
        nivel = _nivel_proteccion(cat_edif)
        main = f"Catalogada — Nivel {nivel}" if nivel else "Catalogada"
        return main + _detail_line(cat_edif)

    return "No disponible"


def _format_monumento(patrim: Any) -> str:
    """
    /cur3d/monumento_historico_nacional devuelve {'data': [...], 'has_error': bool}.
    """
    m = _safe_get(patrim, "monumento_historico_nacional") if isinstance(patrim, dict) else None
    if not isinstance(m, dict):
        return "No disponible"
    if m.get("has_error"):
        return "No disponible"
    data = m.get("data")
    if isinstance(data, list):
        if not data:
            return "Sin declaratoria"
        return f"Declarado MHN ({len(data)} registro(s))"
    if data:
        return "Declarado MHN"
    return "Sin declaratoria"


def _build_section_potencial(listing: dict, gcba: dict, styles: dict) -> list:
    surface = listing.get("surface_total")
    edif = gcba.get("edificabilidad") or {}

    fot_raw = _safe_get(edif, "fot") if isinstance(edif, dict) else None
    fot_f = _extract_fot_value(fot_raw)
    altura_raw = (
        _safe_get(edif, "altura_max")
        or _safe_get(edif, "altura_maxima")
        or _safe_get(edif, "alturaMaxima")
    ) if isinstance(edif, dict) else None
    altura_val = _extract_altura_value(altura_raw)

    # Preferir la superficie de parcela del CUR (más precisa) si existe
    sup_parcela_cur = _safe_get(edif, "superficie_parcela") if isinstance(edif, dict) else None
    # Fallback al CUR si surface es None, 0 o falsy.
    surface_for_calc = surface if surface else sup_parcela_cur

    elements: list = [Paragraph("4. Análisis de potencial constructivo", styles["h2"])]

    try:
        surface_f = float(surface_for_calc) if surface_for_calc is not None else None
    except (TypeError, ValueError):
        surface_f = None

    if surface_f is None and fot_f is None:
        elements.append(Paragraph(
            "No se puede estimar el potencial constructivo: ni superficie del lote "
            "ni FOT están disponibles.",
            styles["body"],
        ))
        return elements

    if surface_f is None:
        elements.append(Paragraph(
            f"FOT aplicable: <b>{_fmt_num(fot_f)}</b>. No se puede calcular m² "
            "construibles porque falta la superficie del lote.",
            styles["body"],
        ))
        return elements

    if fot_f is None:
        # Mostrar al menos el desglose del FOT si vino como dict con ceros u otra forma
        fot_display = _format_fot_breakdown(fot_raw) if fot_raw is not None else "No disponible"
        elements.append(Paragraph(
            f"Superficie del lote: <b>{_fmt_m2(surface_f)}</b>. FOT aplicable: "
            f"<b>{fot_display}</b>. No se puede estimar m² construibles sin un FOT "
            "numérico mayor a cero.",
            styles["body"],
        ))
        if altura_val is not None:
            elements.append(Spacer(1, 4))
            elements.append(Paragraph(
                f"Altura máxima permitida: <b>{_fmt_num(altura_val)} m</b>.",
                styles["body"],
            ))
        return elements

    m2_construibles = surface_f * fot_f
    unidades_60 = int(m2_construibles // 60)
    price = listing.get("price")
    incidencia = None
    if price and m2_construibles > 0:
        try:
            incidencia = float(price) / m2_construibles
        except (TypeError, ValueError):
            incidencia = None

    rows: list[tuple[str, str]] = [
        ("Superficie del lote", _fmt_m2(surface_f)),
        ("FOT aplicable (máx. variantes)", _fmt_num(fot_f)),
        ("Detalle FOT", _format_fot_breakdown(fot_raw)),
        ("Metros² construibles estimados", f"{m2_construibles:,.0f} m²".replace(",", ".")),
        ("Unidades posibles (60 m² c/u)", f"{unidades_60} unidades"),
    ]
    if altura_val is not None:
        rows.append(("Altura máxima permitida", f"{_fmt_num(altura_val)} m"))
    if incidencia is not None:
        cur = (listing.get("currency") or "USD").upper()
        rows.append(("Incidencia (precio / m² construible)", f"{cur} {incidencia:,.0f}/m²".replace(",", ".")))

    elements.append(_kv_table(rows, styles))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(
        "<i>Estimación referencial. La capacidad real depende de retiros, alturas "
        "linderas, plusvalía, premios urbanísticos y proyecto arquitectónico. "
        "Consultar el Código Urbanístico vigente y un profesional matriculado.</i>",
        styles["small"],
    ))
    return elements


def _extract_usos_text(usos: Any) -> list[str]:
    """Aplana usos a una lista de strings minúsculas para matchear keywords."""
    if not isinstance(usos, dict) or _is_error_payload(usos):
        return []
    texts: list[str] = []
    for key in ("nivel1", "nivel2", "nivel3", "categorias", "rubros"):
        val = usos.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str):
                    texts.append(item.lower())
                elif isinstance(item, dict):
                    for k in ("nombre", "descripcion", "rubro", "uso"):
                        v = item.get(k)
                        if isinstance(v, str):
                            texts.append(v.lower())
    return texts


def _is_planta_baja_comercial(usos: Any, distrito: str | None) -> bool:
    """Heurística: ¿permite local comercial en planta baja según los usos?"""
    keywords = ("local", "comercio", "comercial", "planta baja", "oficina")
    for t in _extract_usos_text(usos):
        for kw in keywords:
            if kw in t:
                return True
    return False


def _build_section_escenarios(listing: dict, gcba: dict, styles: dict) -> list:
    edif = gcba.get("edificabilidad") or {}
    parcela = gcba.get("parcela") or {}
    usos = gcba.get("usos") or {}

    surface = listing.get("surface_total")
    sup_parcela_cur = _safe_get(edif, "superficie_parcela") if isinstance(edif, dict) else None
    # Fallback al CUR si surface es None, 0 o falsy.
    surface_for_calc = surface if surface else sup_parcela_cur
    try:
        surface_f = float(surface_for_calc) if surface_for_calc is not None else None
    except (TypeError, ValueError):
        surface_f = None

    fot_raw = _safe_get(edif, "fot") if isinstance(edif, dict) else None
    fot_f = _extract_fot_value(fot_raw)

    altura_raw = (
        _safe_get(edif, "altura_max")
        or _safe_get(edif, "altura_maxima")
        or _safe_get(edif, "alturaMaxima")
    ) if isinstance(edif, dict) else None
    altura_val = _extract_altura_value(altura_raw)

    distrito = _extract_distrito(
        edif if isinstance(edif, dict) else {},
        parcela if isinstance(parcela, dict) else {},
    )

    elements: list = [Paragraph("5. Escenarios de desarrollo sugeridos", styles["h2"])]

    if not surface_f or not fot_f or surface_f <= 0 or fot_f <= 0:
        elements.append(Paragraph(
            "<i>Datos insuficientes para calcular escenarios "
            "(se requiere superficie del lote y FOT).</i>",
            styles["small"],
        ))
        return elements

    m2_totales = surface_f * fot_f
    if altura_val and altura_val > 0:
        pisos = max(1, math.ceil(altura_val / 3.0))
    else:
        # Estimación si no hay altura: FOT / FOS típico 0.6
        pisos = max(1, math.ceil(fot_f / 0.6))

    pb_comercial = _is_planta_baja_comercial(usos, distrito)

    # Si la PB se destina a local comercial y hay más de un piso, separamos
    # la superficie de la PB del cómputo de viviendas.
    if pb_comercial and pisos > 1:
        m2_pb = m2_totales / pisos
        m2_para_viviendas = (m2_totales - m2_pb) * 0.80
        pisos_viv = pisos - 1
    else:
        m2_para_viviendas = m2_totales * 0.80
        pisos_viv = pisos

    units_a = max(0, int(m2_para_viviendas // 35))
    units_b = max(0, int(m2_para_viviendas // 55))
    units_c = max(0, int(m2_para_viviendas // 75))

    pb_note = " · planta baja apta para local comercial" if pb_comercial and pisos > 1 else ""

    scenarios = [
        (
            "A",
            "Monoambientes (1 ambiente)",
            f"<b>{units_a}</b> monoambientes de 35 m² en {pisos_viv} piso(s){pb_note}",
            "Mayor cantidad de unidades, ticket más bajo, alta rotación.",
        ),
        (
            "B",
            "Departamentos de 2 ambientes",
            f"<b>{units_b}</b> departamentos de 2 ambientes (55 m²) en {pisos_viv} piso(s){pb_note}",
            "Producto balanceado, demanda sostenida del mercado.",
        ),
        (
            "C",
            "Departamentos de 3 ambientes (premium)",
            f"<b>{units_c}</b> departamentos de 3 ambientes (75 m²) en {pisos_viv} piso(s){pb_note}",
            "Producto más espacioso, menor cantidad de unidades pero ticket mayor.",
        ),
    ]

    badge_style = ParagraphStyle(
        "badge", fontName="Helvetica-Bold", fontSize=22, leading=24,
        textColor=colors.white, alignment=TA_CENTER,
    )

    for letter, title, line1, line2 in scenarios:
        badge = Paragraph(letter, badge_style)
        content = [
            Paragraph(f"<b>{title}</b>", styles["value"]),
            Spacer(1, 2),
            Paragraph(line1, styles["body"]),
            Paragraph(f"<i>{line2}</i>", styles["small"]),
        ]
        t = Table([[badge, content]], colWidths=[1.6 * cm, 14.4 * cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (0, 0), COLOR_ACCENT),
            ("BACKGROUND", (1, 0), (1, 0), COLOR_LIGHT),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (0, 0), "CENTER"),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ("BOX", (0, 0), (-1, -1), 0.4, COLOR_BORDER),
        ]))
        elements.append(t)
        elements.append(Spacer(1, 5))

    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        "<i>Estimaciones indicativas. Sujeto a proyecto arquitectónico "
        "definitivo y normativa vigente. Se aplicó un descuento del 20 % sobre "
        "los m² totales por circulación y estructura.</i>",
        styles["small"],
    ))
    return elements


def _build_section_historial(historial: list[dict], listing: dict, styles: dict) -> list:
    elements: list = [Paragraph("6. Historial de precio", styles["h2"])]

    if not historial:
        precio_actual = _fmt_money(listing.get("price"), listing.get("currency"))
        elements.append(Paragraph(
            f"Sin cambios de precio registrados. Precio actual: <b>{precio_actual}</b>.",
            styles["body"],
        ))
        return elements

    header = ["Fecha", "Precio anterior", "Precio nuevo", "Variación"]
    data = [header]
    for h in historial:
        ant = h.get("precio_anterior")
        nue = h.get("precio_nuevo")
        cur = h.get("currency") or listing.get("currency")
        var_str = "—"
        if ant and nue:
            try:
                pct = (float(nue) - float(ant)) / float(ant) * 100
                sign = "+" if pct >= 0 else ""
                var_str = f"{sign}{pct:,.1f} %".replace(",", ".")
            except (TypeError, ValueError, ZeroDivisionError):
                pass
        data.append([
            _fmt_date(h.get("detected_at")),
            _fmt_money(ant, cur),
            _fmt_money(nue, cur),
            var_str,
        ])

    t = Table(data, colWidths=[3.5 * cm, 4.5 * cm, 4.5 * cm, 3.5 * cm])
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("ALIGN", (0, 1), (0, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("GRID", (0, 0), (-1, -1), 0.4, COLOR_BORDER),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            style.append(("BACKGROUND", (0, i), (-1, i), COLOR_LIGHT))
    t.setStyle(TableStyle(style))
    elements.append(t)
    return elements


# ── Footer ────────────────────────────────────────────────────────────────────

def _on_page(canvas, doc):
    canvas.saveState()
    width = doc.pagesize[0]
    canvas.setStrokeColor(COLOR_BORDER)
    canvas.setLineWidth(0.5)
    canvas.line(2 * cm, 1.5 * cm, width - 2 * cm, 1.5 * cm)
    canvas.setFont("Helvetica-Oblique", 8)
    canvas.setFillColor(COLOR_MUTED)
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
    canvas.drawString(2 * cm, 1 * cm, f"Generado el {fecha} · Ciudad 3D CABA")
    canvas.drawRightString(width - 2 * cm, 1 * cm, f"Página {doc.page}")
    canvas.restoreState()


# ── API pública ───────────────────────────────────────────────────────────────

def generate_report(
    data: dict[str, Any],
    output_dir: str | os.PathLike[str] | None = None,
) -> str:
    """
    Genera el PDF y devuelve la ruta absoluta al archivo.

    `data` esperado:
        {
            "listing": {...},                   # fila terrenos de SQLite
            "gcba": {                           # respuestas GCBA
                "parcela": {...},
                "edificabilidad": {...},
                "afectaciones": {...},
                "patrimonio": {...},
                "usos": {...},
                "contexto": {...},
                "smp": "...",
            },
            "historial": [ {...}, ... ],        # filas precio_historial
        }
    """
    listing = data.get("listing") or {}
    gcba = data.get("gcba") or {}
    historial = data.get("historial") or []

    repo_root = Path(__file__).resolve().parent
    out_dir = Path(output_dir) if output_dir else (repo_root / "reports")
    out_dir.mkdir(parents=True, exist_ok=True)

    terreno_id = str(listing.get("id") or "sin_id")
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"informe_{terreno_id}_{today}.pdf"
    out_path = out_dir / filename

    styles = _build_styles()
    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
        title=f"Informe terreno {terreno_id}",
        author="Ciudad 3D CABA",
    )

    story: list = []
    story += _build_header(listing, styles)
    story += _build_section_terreno(listing, styles)
    story += _build_section_normativa(gcba, styles)
    story += _build_section_restricciones(gcba, styles)
    story += _build_section_potencial(listing, gcba, styles)
    story += _build_section_escenarios(listing, gcba, styles)
    story += _build_section_historial(historial, listing, styles)

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    return str(out_path.resolve())
