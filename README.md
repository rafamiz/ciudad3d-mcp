# Ciudad 3D — Asistente urbanístico de CABA

Chat web + MCP server que consulta datos catastrales, urbanísticos y de patrimonio
de cualquier parcela de la Ciudad Autónoma de Buenos Aires (EPOK / USIG / Código
Urbanístico).

Dos formas de usarlo:

1. **Chat web** (`web.py`) — sitio público con frontend propio. Cualquier persona
   entra con el navegador y consulta. Pensado para deploy en Railway.
2. **MCP server** (`ciudad3d_mcp.py`) — para usuarios técnicos que quieren las 20
   tools dentro de Claude Desktop / Cursor / Cowork.

Ambos comparten el mismo módulo de tools (`tools.py`).

---

## Estructura

```
ciudad3d-mcp/
├── tools.py              # Funciones puras + schemas para tool-use
├── ciudad3d_mcp.py       # Wrapper FastMCP (stdio o streamable-http)
├── web.py                # FastAPI: /chat + frontend estático
├── static/
│   └── index.html        # Chat UI single-page
├── requirements.txt
├── Procfile              # Para Railway/Heroku
├── railway.json          # Config Railway (healthcheck, etc.)
├── .env.example
└── README.md
```

---

## Deploy en Railway (chat web)

Railway buildea con Nixpacks, detecta `requirements.txt` automáticamente y arranca
con el `Procfile`. Pasos:

### 1. Conseguí una API key de Anthropic

En https://console.anthropic.com → API Keys → **Create Key**. Copiala.

> ⚠️ Esa key es tuya: cada conversación de cualquier usuario va a consumir tu
> crédito. Empezá con un budget bajo (ej. USD 5–10) y monitoreá uso desde
> https://console.anthropic.com/settings/usage. Hay un rate-limit por IP en el
> servidor (20 req/min), pero conviene también poner un spending limit en la
> consola.

### 2. Pushá el repo a GitHub

```bash
cd ciudad3d-mcp
git init
git add .
git commit -m "ciudad3d chat web v0.1"
git branch -M main
git remote add origin git@github.com:TU-USUARIO/ciudad3d-mcp.git
git push -u origin main
```

### 3. Deploy en Railway

1. https://railway.app → **New Project** → **Deploy from GitHub repo** → elegí
   `ciudad3d-mcp`.
2. Railway detecta Python automáticamente. El primer build tarda ~2 min.
3. **Variables** (panel del servicio → Variables):
   - `ANTHROPIC_API_KEY` = `sk-ant-...`
   - `ANTHROPIC_MODEL` = `claude-sonnet-4-6` (opcional)
4. **Settings → Networking → Generate Domain**: te da una URL pública tipo
   `https://ciudad3d-mcp-production.up.railway.app`.
5. Listo. Compartila.

Healthcheck: `GET /health` devuelve `{"ok": true, ...}`.

---

## Probar local antes de deployar

```bash
# 1. Crear venv (Python 3.11+)
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate

# 2. Instalar deps
pip install -r requirements.txt

# 3. Copiar y completar .env
cp .env.example .env
# editá .env y poné tu ANTHROPIC_API_KEY

# 4. Correr (cargando .env)
export $(cat .env | xargs)   # Windows PowerShell: usá `Get-Content .env | ForEach-Object {...}`
python web.py

# Abrí http://localhost:8000
```

---

## Usar el MCP server (modo stdio, local)

Para sumarlo a Claude Desktop / Cursor / Cowork como MCP local:

```json
{
  "mcpServers": {
    "ciudad3d": {
      "command": "python",
      "args": ["/ruta/absoluta/a/ciudad3d-mcp/ciudad3d_mcp.py"]
    }
  }
}
```

Si querés exponerlo también como **MCP remoto** (no como chat web), seteá la
variable `MCP_TRANSPORT=streamable-http` en Railway y armá un segundo servicio
que ejecute `python ciudad3d_mcp.py` en lugar de `uvicorn web:app`.

---

## Costos esperables

- **Railway**: el plan Hobby tiene USD 5/mes de uso incluido. Una app FastAPI
  liviana como esta entra cómoda en ese tier.
- **Anthropic**: depende del modelo y del tamaño de las respuestas. Con
  `claude-sonnet-4-6` y consultas típicas (un tool-use loop de 2–4 pasos), una
  conversación promedio sale entre USD 0.01 y USD 0.05.

---

## Limitaciones conocidas

- Las APIs de USIG/EPOK son públicas pero no documentadas formalmente y a veces
  cambian o están caídas. El helper `_get` captura errores y los devuelve como
  `{"error": ...}`, así Claude puede explicárselo al usuario.
- El rate limit en memoria (20 req/min/IP) se resetea al reiniciar el servicio.
  Si esperás tráfico serio, mové a Redis.
- El historial vive en el cliente (no hay sesiones server-side). Refrescar la
  página borra el contexto.
