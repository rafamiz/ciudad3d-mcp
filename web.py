"""
Backend del chat web Ciudad 3D.

FastAPI + Anthropic SDK con tool-use. Sirve un frontend estático y expone un
endpoint /chat que recibe el historial de mensajes, ejecuta el loop agéntico
contra Claude usando las tools de tools.py, y devuelve la respuesta final.

Variables de entorno:
- ANTHROPIC_API_KEY  (obligatoria)
- ANTHROPIC_MODEL    (opcional, default: claude-sonnet-4-6)
- PORT               (opcional, lo setea Railway)
- MAX_TURNS          (opcional, default: 10) — corta el loop de tool-use
- ALLOWED_ORIGINS    (opcional, CSV de orígenes para CORS)
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator

from anthropic import AsyncAnthropic
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import database as db
import scraper
from tools import anthropic_tools, run_tool

# ── Config ────────────────────────────────────────────────────────────────────
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
MAX_TURNS = int(os.getenv("MAX_TURNS", "6"))
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "2048"))
REQUEST_TIMEOUT_S = int(os.getenv("REQUEST_TIMEOUT_S", "75"))
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",")]

SYSTEM_PROMPT = """Sos un asistente experto en datos urbanísticos, catastrales y de \
patrimonio de la Ciudad Autónoma de Buenos Aires. Ayudás a arquitectos, \
desarrolladores, agentes inmobiliarios y vecinos a entender qué se puede hacer \
en una parcela específica.

Tenés acceso a dos grandes grupos de tools:

A) Datos urbanísticos del GCBA (EPOK, USIG, Código Urbanístico). Flujo típico:
1. Si el usuario da una dirección en texto, primero llamá `resolver_direccion` para \
   obtener coordenadas.
2. Con las coordenadas, llamá `get_parcela_por_coordenadas` para obtener el SMP.
3. Con el SMP, llamá las tools específicas que correspondan (edificabilidad, \
   plusvalía, patrimonio, etc.) o `get_informe_completo` si querés todo de una.

B) Listings de terrenos en venta en CABA, scrapeados de ZonaProp y actualizados \
varias veces al día en una base local:
- `buscar_terrenos(zona, precio_max, superficie_min)` busca terrenos cacheados.
- `terreno_detalle(terreno_id)` devuelve un terreno + datos urbanísticos GCBA \
  cruzados automáticamente por sus coordenadas (no necesitás llamar tools de GCBA \
  aparte si usás ésta).
- `historial_precios(terreno_id)` muestra todos los cambios de precio detectados.
- `terrenos_con_bajas(dias)` lista los terrenos cuyo precio bajó en los últimos N \
  días, ordenados por mayor caída.
- `generar_informe(terreno_id)` arma un PDF profesional con todo el cruce \
  (ZonaProp + normativa GCBA + historial).

Respondé en español, con tono profesional pero claro. Cuando uses datos de las \
tools, citá el SMP y los valores concretos. Usá tablas markdown para datos \
tabulares (FOT, alturas, plusvalía, listados de terrenos). Si una tool devuelve un \
error, informalo y sugerí qué intentar (ej: verificar la dirección, o probar otra \
zona).

Cuando consultes una parcela específica y el usuario no haya pedido lo contrario, \
llamá también `get_fotos_parcela` y `get_geometria_parcela` — el frontend las \
renderiza como galería de fotos y mini-mapa interactivo, lo cual hace la \
respuesta mucho más útil. No es necesario describir las fotos ni el mapa en el \
texto, solo llamar las tools."""

API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not API_KEY:
    # No fallamos al importar para permitir health checks; fallamos al primer /chat.
    print("WARNING: ANTHROPIC_API_KEY no está seteada — /chat va a devolver 500.")

client = AsyncAnthropic(api_key=API_KEY) if API_KEY else None


# ── Rate limit en memoria ─────────────────────────────────────────────────────
RATE_WINDOW_SEC = 60
RATE_MAX_REQ = 20
_rate_log: dict[str, deque[float]] = defaultdict(deque)


def _rate_limit(ip: str) -> bool:
    now = time.time()
    q = _rate_log[ip]
    while q and q[0] < now - RATE_WINDOW_SEC:
        q.popleft()
    if len(q) >= RATE_MAX_REQ:
        return False
    q.append(now)
    return True


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Ciudad 3D Chat", version="0.1.0")

REPORTS_DIR = Path(__file__).resolve().parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)
app.mount("/reports", StaticFiles(directory=REPORTS_DIR), name="reports")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _startup() -> None:
    # init_db() crea el pool de asyncpg si DATABASE_URL está seteada
    # (Railway), o las tablas SQLite si no (local).
    await db.init_db()


@app.on_event("shutdown")
async def _shutdown() -> None:
    await db.close_pg_pool()


class ChatMessage(BaseModel):
    role: str  # "user" | "assistant"
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatMessage] = Field(min_length=1)


class ToolEvent(BaseModel):
    name: str
    input: dict
    output_preview: str  # primeros ~500 chars del JSON


class ChatResponse(BaseModel):
    reply: str
    tool_events: list[ToolEvent] = []


@app.get("/health")
async def health():
    return {"ok": True, "model": ANTHROPIC_MODEL, "has_api_key": bool(API_KEY)}


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, request: Request):
    if client is None:
        raise HTTPException(500, "ANTHROPIC_API_KEY no configurada en el servidor.")

    ip = request.client.host if request.client else "unknown"
    if not _rate_limit(ip):
        raise HTTPException(429, "Demasiadas consultas, esperá un minuto.")

    messages: list[dict] = [
        {"role": m.role, "content": m.content} for m in req.messages
    ]
    tool_events: list[ToolEvent] = []

    try:
        return await asyncio.wait_for(
            _run_loop(messages, tool_events),
            timeout=REQUEST_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        called = ", ".join(e.name for e in tool_events) or "(ninguna)"
        raise HTTPException(
            504,
            f"La consulta tardó más de {REQUEST_TIMEOUT_S}s. Tools llamadas: {called}. "
            "Probá con una pregunta más específica (ej: pasá directamente el SMP).",
        )
    except Exception as e:
        raise HTTPException(500, f"Error procesando la consulta: {e}")


async def _run_loop(messages: list[dict], tool_events: list[ToolEvent]) -> ChatResponse:
    tools_schema = anthropic_tools()
    for _ in range(MAX_TURNS):
        resp = await client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            tools=tools_schema,
            messages=messages,
        )

        # Append assistant message (incluye tool_use blocks si los hay).
        messages.append({"role": "assistant", "content": resp.content})

        if resp.stop_reason != "tool_use":
            # Respuesta final.
            text = "".join(
                b.text for b in resp.content if getattr(b, "type", None) == "text"
            )
            return ChatResponse(reply=text or "(sin respuesta)", tool_events=tool_events)

        # Ejecutamos las tools pedidas (en paralelo) y devolvemos los resultados.
        tool_use_blocks = [b for b in resp.content if getattr(b, "type", None) == "tool_use"]

        async def _exec(block):
            result = await asyncio.to_thread(run_tool, block.name, block.input)
            preview = str(result)[:500]
            tool_events.append(
                ToolEvent(name=block.name, input=block.input, output_preview=preview)
            )
            return {
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": str(result)[:60_000],  # cap por las dudas
            }

        tool_results = await asyncio.gather(*(_exec(b) for b in tool_use_blocks))
        messages.append({"role": "user", "content": tool_results})

    raise HTTPException(
        500,
        f"Se llegó al límite de {MAX_TURNS} turnos sin respuesta final.",
    )


# ── Scrape endpoints ──────────────────────────────────────────────────────────
LAST_RUN_PATH = Path(__file__).resolve().parent / "last_run.json"

# Estado simple en memoria para evitar scrapes concurrentes en el mismo proceso.
_scrape_state: dict[str, object] = {"running": False, "last_error": None}


async def _run_scrape_job(max_pages: int) -> None:
    """
    Idéntico al run_job de scraper_scheduler salvo que NO cierra el pool de
    asyncpg al terminar — éste corre dentro del proceso del web server, que
    comparte ese pool con todos los otros endpoints.
    """
    if _scrape_state["running"]:
        return
    _scrape_state["running"] = True
    _scrape_state["last_error"] = None
    started_at_local = datetime.now().replace(microsecond=0)
    try:
        listings = await scraper.scrape(max_pages=max_pages)
        stats = await db.upsert_terrenos(listings)
        new_rows = await db.get_new_terrenos()
        new_listings = [
            {
                "id": row["id"],
                "price": row.get("price"),
                "currency": row.get("currency"),
                "surface_total": row.get("surface_total"),
                "address": row.get("address"),
                "url": row.get("url"),
            }
            for row in new_rows
        ]
        LAST_RUN_PATH.write_text(
            json.dumps(
                {
                    "ran_at": started_at_local.isoformat(),
                    "new_count": stats["new"],
                    "total_count": stats["total"],
                    "new_listings": new_listings,
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
    except Exception as e:
        _scrape_state["last_error"] = f"{type(e).__name__}: {e}"
        print(f"[/api/scrape] error: {e}")
    finally:
        _scrape_state["running"] = False


@app.post("/api/scrape")
async def api_scrape(background_tasks: BackgroundTasks, max_pages: int = 20):
    if _scrape_state["running"]:
        total = await db.count_terrenos()
        return {
            "ok": False,
            "new_count": 0,
            "total_count": total,
            "message": "Ya hay un scrape en curso. Consultá /api/scrape/status.",
        }

    total = await db.count_terrenos()
    background_tasks.add_task(_run_scrape_job, max_pages)
    return {
        "ok": True,
        "new_count": 0,
        "total_count": total,
        "message": (
            f"Scrape iniciado en background (max_pages={max_pages}). "
            "Consultá /api/scrape/status para ver el progreso."
        ),
    }


@app.get("/api/scrape/status")
async def api_scrape_status():
    total = await db.count_terrenos()
    last_run: dict | None = None
    if LAST_RUN_PATH.exists():
        try:
            data = json.loads(LAST_RUN_PATH.read_text(encoding="utf-8"))
            last_run = {
                "ran_at": data.get("ran_at"),
                "new_count": data.get("new_count"),
                "total_count": data.get("total_count"),
            }
        except (json.JSONDecodeError, OSError):
            last_run = None

    return {
        "ok": True,
        "running": _scrape_state["running"],
        "last_error": _scrape_state["last_error"],
        "total_count": total,
        "last_run": last_run,
    }


# ── Streaming endpoint ────────────────────────────────────────────────────────

def _ndjson(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False) + "\n"


def _is_geojson(result) -> bool:
    if not isinstance(result, dict) or "error" in result or not result:
        return False
    return any(k in result for k in ("type", "features", "geometry", "coordinates"))


@app.post("/chat/stream")
async def chat_stream(req: ChatRequest, request: Request):
    if client is None:
        raise HTTPException(500, "ANTHROPIC_API_KEY no configurada en el servidor.")
    ip = request.client.host if request.client else "unknown"
    if not _rate_limit(ip):
        raise HTTPException(429, "Demasiadas consultas, esperá un minuto.")

    messages: list[dict] = [
        {"role": m.role, "content": m.content} for m in req.messages
    ]

    async def event_stream() -> AsyncIterator[str]:
        tools_schema = anthropic_tools()
        try:
            for _ in range(MAX_TURNS):
                async with client.messages.stream(
                    model=ANTHROPIC_MODEL,
                    max_tokens=MAX_TOKENS,
                    system=SYSTEM_PROMPT,
                    tools=tools_schema,
                    messages=messages,
                ) as stream:
                    async for text in stream.text_stream:
                        if text:
                            yield _ndjson({"type": "text", "delta": text})
                    final = await stream.get_final_message()

                messages.append({"role": "assistant", "content": final.content})

                if final.stop_reason != "tool_use":
                    yield _ndjson({"type": "done"})
                    return

                tool_use_blocks = [
                    b for b in final.content if getattr(b, "type", None) == "tool_use"
                ]
                tool_results_for_api = []
                for block in tool_use_blocks:
                    yield _ndjson(
                        {"type": "tool_call", "name": block.name, "input": block.input}
                    )
                    result = await asyncio.to_thread(run_tool, block.name, block.input)

                    # Eventos especiales según la tool
                    if block.name == "get_fotos_parcela" and isinstance(result, dict):
                        urls = result.get("urls") or []
                        if urls:
                            yield _ndjson({"type": "photos", "urls": urls})
                    elif block.name == "get_geometria_parcela" and _is_geojson(result):
                        yield _ndjson({"type": "geometry", "geojson": result})

                    yield _ndjson(
                        {"type": "tool_result", "name": block.name, "preview": str(result)[:300]}
                    )
                    tool_results_for_api.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": str(result)[:60_000],
                        }
                    )

                messages.append({"role": "user", "content": tool_results_for_api})

            yield _ndjson(
                {"type": "error", "message": f"Se llegó al límite de {MAX_TURNS} turnos."}
            )
        except Exception as e:
            yield _ndjson({"type": "error", "message": str(e)})

    return StreamingResponse(
        event_stream(),
        media_type="application/x-ndjson",
        headers={
            "X-Accel-Buffering": "no",  # nginx/Railway: no buffer
            "Cache-Control": "no-cache",
        },
    )


# ── Static frontend ───────────────────────────────────────────────────────────
STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    async def index():
        return FileResponse(STATIC_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
