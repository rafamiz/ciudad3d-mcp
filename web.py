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
import os
import time
from collections import defaultdict, deque
from pathlib import Path

from anthropic import AsyncAnthropic
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from tools import anthropic_tools, run_tool

# ── Config ────────────────────────────────────────────────────────────────────
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
MAX_TURNS = int(os.getenv("MAX_TURNS", "10"))
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "4096"))
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
tools, citá el SMP y los valores concretos. Si una tool devuelve un error, \
informalo y sugerí qué intentar (ej: verificar la dirección)."""

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

    # Convertimos el historial a formato Anthropic.
    messages: list[dict] = [
        {"role": m.role, "content": m.content} for m in req.messages
    ]

    tool_events: list[ToolEvent] = []
    tools_schema = anthropic_tools()

    # Loop agéntico: mientras Claude pida tools, las ejecutamos y volvemos.
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
