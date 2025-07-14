# studio_server.py
import os
import sys
import asyncio
from pathlib import Path
from urllib.parse import urlparse
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import asyncpg
from colorama import init as colorama_init, Fore, Style

# ------------------------------------------------------------
# 1. Environment & configuration
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")  # same behaviour as dotenv.config()

PORT = int(os.getenv("STUDIO_PORT", 1337))
STUDIO_VERBOSE = os.getenv("STUDIO_VERBOSE", "false").lower() == "true"

DB_CONNECTION_STRING = os.getenv("HYREX_DATABASE_URL") or os.getenv(
    "PGURL"
)  # envVariables.getDatabaseUrl()‑equivalent
if not DB_CONNECTION_STRING:
    sys.stderr.write(
        "To run Hyrex Studio you must specify a database connection string "
        "(set HYREX_DATABASE_URL or PGURL).\n"
    )
    sys.exit(1)

# ------------------------------------------------------------
# 2. Colour / banner helpers
# ------------------------------------------------------------
colorama_init(autoreset=True)

COLOR_MAP = {
    "brightYellow": Fore.LIGHTYELLOW_EX,
    "brightWhite": Fore.LIGHTWHITE_EX,
    "brightMagenta": Fore.LIGHTMAGENTA_EX,
    "brightCyan": Fore.LIGHTCYAN_EX,
    "brightGreen": Fore.LIGHTGREEN_EX,
    "brightBlue": Fore.LIGHTBLUE_EX,
    "yellow": Fore.YELLOW,
    "dim": Style.DIM,
    "reset": Style.RESET_ALL,
}


def colorize(text: str, color: str) -> str:
    return COLOR_MAP.get(color, "") + text + COLOR_MAP["reset"]


def create_box(lines, box_color="brightCyan", width=55, style="double"):
    chars = {
        "double": dict(tl="╔", tr="╗", bl="╚", br="╝", h="═", v="║"),
        "single": dict(tl="┌", tr="┐", bl="└", br="┘", h="─", v="│"),
        "rounded": dict(tl="╭", tr="╮", bl="╰", br="╯", h="─", v="│"),
    }[style]

    top = chars["tl"] + chars["h"] * width + chars["tr"]
    bottom = chars["bl"] + chars["h"] * width + chars["br"]

    print(colorize("  " + top, box_color))
    for text, txt_color in lines:
        if text == "":
            line = chars["v"] + " " * width + chars["v"]
        else:
            pad = width - len(text)
            left = pad // 2
            right = pad - left
            line = (
                chars["v"]
                + " " * left
                + colorize(text, txt_color)
                + " " * right
                + chars["v"]
            )
        print(colorize("  " + line, box_color))
    print(colorize("  " + bottom, box_color))


def banner():
    db_name = urlparse(DB_CONNECTION_STRING).path.lstrip("/")
    os.system("cls" if os.name == "nt" else "clear")
    print()
    print(
        colorize("  ✨", "brightYellow"),
        colorize("Welcome to", "brightWhite"),
        colorize("✨", "brightYellow"),
    )
    create_box(
        [("", None), ("🚀 HYREX STUDIO SERVER 🚀", "brightMagenta"), ("", None)],
        "brightCyan",
        55,
        "double",
    )
    print()
    print(
        colorize("  ▸ ", "brightGreen")
        + colorize("Status:", "yellow")
        + " "
        + colorize("● Running", "brightGreen")
    )
    print(
        colorize("  ▸ ", "brightBlue")
        + colorize("Port:", "yellow")
        + " "
        + colorize(str(PORT), "brightWhite")
    )
    print(
        colorize("  ▸ ", "brightMagenta")
        + colorize("Database:", "yellow")
        + " "
        + colorize(db_name, "brightWhite")
    )
    print(
        colorize("  ▸ ", "brightCyan")
        + colorize("Verbose:", "yellow")
        + " "
        + colorize(
            "✓ Enabled" if STUDIO_VERBOSE else "✗ Disabled",
            "brightGreen" if STUDIO_VERBOSE else "dim",
        )
    )
    print()
    create_box(
        [
            ("", None),
            ("🌐 Open Hyrex Studio in your browser:", "brightWhite"),
            ("", None),
            ("👉 https://local.hyrex.studio 👈", "brightCyan"),
            ("", None),
        ],
        "brightBlue",
        55,
        "rounded",
    )
    if not STUDIO_VERBOSE:
        print(colorize("\n  Tip: Set STUDIO_VERBOSE=true to see detailed logs", "dim"))
    print("\n")


# ------------------------------------------------------------
# 3. Database pool – use asyncpg (async, high‑perf, server‑side prepared)
# ------------------------------------------------------------
_pool: asyncpg.pool.Pool | None = None


async def get_pool() -> asyncpg.pool.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=DB_CONNECTION_STRING, min_size=1, max_size=10
        )
    return _pool


# ------------------------------------------------------------
# 4. FastAPI app & models
# ------------------------------------------------------------
class QueryPayload(BaseModel):
    query: str
    params: list | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await get_pool()
    banner()
    yield
    # Shutdown
    global _pool
    if _pool:
        await _pool.close()
        if STUDIO_VERBOSE:
            print("Database pool closed")


app = FastAPI(
    title="Hyrex Studio",
    version="1.0",
    docs_url="/docs" if STUDIO_VERBOSE else None,
    redoc_url=None,
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # adjust if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------
# 5. Endpoints
# ------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "OK", "timestamp": str(asyncio.get_event_loop().time())}


@app.post("/api/query")
async def raw_query(payload: QueryPayload):
    if STUDIO_VERBOSE:
        print("Received query payload:", payload.model_dump_json(indent=2))

    if not payload.query:
        raise HTTPException(status_code=400, detail="Query is required")

    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            stmt = await conn.prepare(payload.query)
            rows = await stmt.fetch(*(payload.params or []))
            # Convert asyncpg Record objects → dict
            rows_dict = [dict(r) for r in rows]
            return {
                "rows": rows_dict,
                "rowCount": len(rows),
                "fields": [
                    {"name": a.name, "dataTypeID": a.type.oid}
                    for a in stmt.get_attributes()
                ],
            }
    except Exception as exc:
        if STUDIO_VERBOSE:
            print("Error executing query:", exc, file=sys.stderr)
        raise HTTPException(status_code=500, detail=str(exc))


# ------------------------------------------------------------
# 6. Entry‑point helper
# ------------------------------------------------------------
if __name__ == "__main__":
    # Run with:  python studio_server.py  (or better: `uvicorn studio_server:app --port 1337`)
    import uvicorn

    uvicorn.run("studio_server:app", host="0.0.0.0", port=PORT, reload=False)
