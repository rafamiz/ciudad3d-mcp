"""
Generador de informes PDF para terrenos cruzados con datos urbanísticos GCBA.

Recibe un dict con `listing` (datos de ZonaProp), `gcba` (edificabilidad,
afectaciones, patrimonio, contexto, etc.) y `historial` (cambios de precio) y
produce un PDF profesional con reportlab Platypus en la carpeta `reports/`.
"""

from __future__ import annotations

import io
import json
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


def _kv_table(rows: list[tuple[str, str]], col_widths: tuple[float, float] = (5 * cm, 11 * cm)) -> Table:
    """Tabla 2 columnas key/value con filas alternadas."""
    data = [[k, v] for k, v in rows]
    t = Table(data, colWidths=list(col_widths))
    style = [
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (1, 0), (1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("TEXTCOLOR", (0, 0), (0, -1), COLOR_PRIMARY),
        ("TEXTCOLOR", (1, 0), (1, -1), COLOR_TEXT),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
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

    rows_paragraphs = [(k, Paragraph(v, styles["value"])) for k, v in rows]

    elements: list = [
        Paragraph("1. Datos del terreno", styles["h2"]),
        _kv_table(rows_paragraphs),
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

    distrito = _first_present(
        _safe_get(edif, "distrito"),
        _safe_get(edif, "zonificacion"),
        _safe_get(edif, "codigo_urbanistico", "distrito"),
        _safe_get(parcela, "distrito"),
    )
    fot = _first_present(_safe_get(edif, "fot"), _safe_get(edif, "FOT"))
    fos = _first_present(_safe_get(edif, "fos"), _safe_get(edif, "FOS"))
    altura = _first_present(
        _safe_get(edif, "altura_maxima"),
        _safe_get(edif, "alturaMaxima"),
        _safe_get(edif, "altura"),
    )
    sup_edif = _first_present(
        _safe_get(edif, "superficie_edificable"),
        _safe_get(edif, "superficieEdificable"),
    )
    smp = _first_present(_safe_get(parcela, "smp"), _safe_get(parcela, "SMP"), gcba.get("smp"))

    rows: list[tuple[str, str]] = [
        ("SMP (Sección-Manzana-Parcela)", str(smp) if smp else "—"),
        ("Distrito / Zona", str(distrito) if distrito else "—"),
        ("FOT (Factor de Ocupación Total)", _fmt_num(fot) if fot is not None else "—"),
        ("FOS (Factor de Ocupación del Suelo)", _fmt_num(fos) if fos is not None else "—"),
        ("Altura máxima", f"{altura} m" if altura is not None else "—"),
        ("Superficie edificable (CUR)", _fmt_m2(sup_edif) if sup_edif else "—"),
    ]

    usos_str = _summarize_usos(usos)
    if usos_str:
        rows.append(("Usos habilitados", usos_str))

    rows_paragraphs = [(k, Paragraph(v, styles["value"])) for k, v in rows]

    return [
        Paragraph("2. Normativa urbanística", styles["h2"]),
        _kv_table(rows_paragraphs),
    ]


def _summarize_usos(usos: Any) -> str:
    if not isinstance(usos, dict) or usos.get("error"):
        return ""
    candidates: list[str] = []
    for key in ("nivel1", "nivel2", "nivel3", "categorias", "rubros", "usos"):
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
    patrim = gcba.get("patrimonio") or {}

    items: list[tuple[str, str]] = []

    riesgo = _first_present(
        _safe_get(afect, "riesgo_hidrico"),
        _safe_get(afect, "riesgoHidrico"),
        _safe_get(afect, "hidrico"),
    )
    items.append(("Riesgo hídrico", _stringify(riesgo)))

    lep = _first_present(_safe_get(afect, "lep"), _safe_get(afect, "LEP"))
    items.append(("Línea de Edificación (LEP)", _stringify(lep)))

    ensanche = _first_present(
        _safe_get(afect, "ensanche"),
        _safe_get(afect, "apertura"),
        _safe_get(afect, "apertura_calle"),
    )
    items.append(("Ensanche / apertura de calle", _stringify(ensanche)))

    catalog = _safe_get(patrim, "catalogacion")
    monumento = _safe_get(patrim, "monumento_historico_nacional")
    items.append(("Catalogación patrimonial", _stringify(catalog)))
    items.append(("Monumento histórico nacional", _stringify(monumento)))

    rows_paragraphs = [(k, Paragraph(v, styles["value"])) for k, v in items]

    return [
        Paragraph("3. Restricciones y afectaciones", styles["h2"]),
        _kv_table(rows_paragraphs),
    ]


def _stringify(value: Any) -> str:
    if value in (None, "", [], {}):
        return "Sin afectación / sin datos"
    if isinstance(value, bool):
        return "Sí" if value else "No"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if value.get("error"):
            return "Sin datos"
        for k in ("descripcion", "nombre", "valor", "estado", "categoria"):
            if value.get(k):
                return str(value[k])
        try:
            return json.dumps(value, ensure_ascii=False)[:120]
        except Exception:
            return "Sí (ver detalle)"
    if isinstance(value, list):
        if not value:
            return "Sin afectación"
        return f"{len(value)} item(s)"
    return str(value)


def _build_section_potencial(listing: dict, gcba: dict, styles: dict) -> list:
    surface = listing.get("surface_total")
    edif = gcba.get("edificabilidad") or {}
    fot = _first_present(_safe_get(edif, "fot"), _safe_get(edif, "FOT"))
    altura = _first_present(_safe_get(edif, "altura_maxima"), _safe_get(edif, "alturaMaxima"))

    elements: list = [Paragraph("4. Análisis de potencial constructivo", styles["h2"])]

    if surface is None or fot is None:
        elements.append(Paragraph(
            "No se puede estimar el potencial constructivo por falta de datos "
            "(superficie del lote y/o FOT no disponibles).",
            styles["body"],
        ))
        return elements

    try:
        surface_f = float(surface)
        fot_f = float(fot)
    except (TypeError, ValueError):
        elements.append(Paragraph("Datos no numéricos; no se puede calcular.", styles["body"]))
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
        ("FOT aplicable", _fmt_num(fot_f)),
        ("Metros² construibles estimados", f"{m2_construibles:,.0f} m²".replace(",", ".")),
        ("Unidades posibles (60 m² c/u)", f"{unidades_60} unidades"),
    ]
    if altura is not None:
        rows.append(("Altura máxima permitida", f"{altura} m"))
    if incidencia is not None:
        cur = (listing.get("currency") or "USD").upper()
        rows.append(("Incidencia (precio / m² construible)", f"{cur} {incidencia:,.0f}/m²".replace(",", ".")))

    rows_paragraphs = [(k, Paragraph(v, styles["value"])) for k, v in rows]
    elements.append(_kv_table(rows_paragraphs))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph(
        "<i>Estimación referencial. La capacidad real depende de retiros, alturas "
        "linderas, plusvalía, premios urbanísticos y proyecto arquitectónico. "
        "Consultar el Código Urbanístico vigente y un profesional matriculado.</i>",
        styles["small"],
    ))
    return elements


def _build_section_historial(historial: list[dict], listing: dict, styles: dict) -> list:
    elements: list = [Paragraph("5. Historial de precio", styles["h2"])]

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
    story += _build_section_historial(historial, listing, styles)

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    return str(out_path.resolve())
