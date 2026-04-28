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
from pathlib import Path
from typing import AsyncIterator

from anthropic import AsyncAnthropic
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

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

Tenés acceso a tools que consultan los servicios públicos del GCBA (EPOK, USIG, \
Código Urbanístico). Flujo típico:
1. Si el usuario da una dirección en texto, primero llamá `resolver_direccion` para \
   obtener coordenadas.
2. Con las coordenadas, llamá `get_parcela_por_coordenadas` para obtener el SMP.
3. Con el SMP, llamá las tools específicas que correspondan (edificabilidad, \
   plusvalía, patrimonio, etc.) o `get_informe_completo` si querés todo de una.

Respondé en español, con tono profesional pero claro. Cuando uses datos de las \
tools, citá el SMP y los valores concretos. Usá tablas markdown para datos \
tabulares (FOT, alturas, plusvalía). Si una tool devuelve un error, informalo y \
sugerí qué intentar (ej: verificar la dirección).

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


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
