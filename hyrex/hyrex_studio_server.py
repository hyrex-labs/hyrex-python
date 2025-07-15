# studio_server.py
import os
import sys
import json
import asyncio
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from datetime import datetime, date, time
from decimal import Decimal
from uuid import UUID

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
_loop: asyncio.AbstractEventLoop | None = None


async def get_pool() -> asyncpg.pool.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=DB_CONNECTION_STRING, min_size=1, max_size=10
        )
    return _pool


# ------------------------------------------------------------
# 4. HTTP Request Handler
# ------------------------------------------------------------
class StudioRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Only log if verbose mode is enabled
        if STUDIO_VERBOSE:
            super().log_message(format, *args)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            response = {"status": "OK", "timestamp": str(asyncio.get_event_loop().time())}
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_error(404, "Not Found")

    def do_POST(self):
        if self.path == "/api/query":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length)
            
            try:
                payload = json.loads(post_data.decode("utf-8"))
                
                if STUDIO_VERBOSE:
                    print("Received query payload:", json.dumps(payload, indent=2, default=str))
                
                if not payload.get("query"):
                    self.send_error(400, "Query is required")
                    return
                
                # Run async query in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._execute_query(payload.get("query"), payload.get("params")),
                    _loop
                )
                result = future.result()
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(result, default=str).encode())
                
            except json.JSONDecodeError:
                self.send_error(400, "Invalid JSON")
            except Exception as exc:
                if STUDIO_VERBOSE:
                    print("Error executing query:", exc, file=sys.stderr)
                self.send_error(500, str(exc))
        else:
            self.send_error(404, "Not Found")

    async def _execute_query(self, query: str, params: list | None = None):
        pool = await get_pool()
        async with pool.acquire() as conn:
            stmt = await conn.prepare(query)
            rows = await stmt.fetch(*(params or []))
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


# ------------------------------------------------------------
# 5. Async event loop thread
# ------------------------------------------------------------
def run_async_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


# ------------------------------------------------------------
# 6. Entry‑point helper
# ------------------------------------------------------------
async def initialize():
    """Initialize database pool and show banner"""
    await get_pool()
    banner()


async def cleanup():
    """Clean up resources"""
    global _pool
    if _pool:
        await _pool.close()
        if STUDIO_VERBOSE:
            print("Database pool closed")


def main():
    global _loop
    
    # Create and start async event loop in a separate thread
    _loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=run_async_loop, args=(_loop,), daemon=True)
    loop_thread.start()
    
    # Initialize async resources
    future = asyncio.run_coroutine_threadsafe(initialize(), _loop)
    future.result()
    
    # Create and start HTTP server
    server = HTTPServer(("0.0.0.0", PORT), StudioRequestHandler)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server.shutdown()
        # Clean up async resources
        future = asyncio.run_coroutine_threadsafe(cleanup(), _loop)
        future.result()
        _loop.call_soon_threadsafe(_loop.stop)
        loop_thread.join(timeout=5)


if __name__ == "__main__":
    main()