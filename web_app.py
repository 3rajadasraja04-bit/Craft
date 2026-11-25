"""
ZHD CYBER ENGINE — Web Control Backend v3.0
FastAPI wrapper around vn.py bot loop with WebSocket telemetry
"""
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Set

# ─── Save original streams BEFORE importing vn (vn.py redirects sys.stdout) ───
_original_stdout = sys.stdout
_original_stderr = sys.stderr

# Import bot engine
import vn

# Restore streams (vn.py replaces sys.stdout at import time)
sys.stdout = _original_stdout
sys.stderr = _original_stderr

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# ═══════════════════════════════════════════════════════════════════
#  PATHS & DATA DIRECTORY
# ═══════════════════════════════════════════════════════════════════
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"

# DATA_DIR: mount a Railway volume here for persistence
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR)))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    DATA_DIR = BASE_DIR

ACCOUNTS_FILE   = DATA_DIR / "accounts.json"
WEB_CONFIG_FILE = DATA_DIR / "web_config.json"

# Redirect vn.py's data files into DATA_DIR so they persist with volume
vn.JWT_CACHE_FILE      = str(DATA_DIR / "jwt.json")
vn.HISTORY_FILE        = str(DATA_DIR / "bot_history.json")
vn.MAP_STATS_FILE      = str(DATA_DIR / "map.json")
vn.PROGRESS_SEQ_FILE   = str(DATA_DIR / "progress_seq.json")
vn.PROGRESS_TIMER_FILE = str(DATA_DIR / "progress_timer.json")
vn.CONFIG_FILE         = str(DATA_DIR / "config.json")

# ═══════════════════════════════════════════════════════════════════
#  APP
# ═══════════════════════════════════════════════════════════════════
app = FastAPI(title="ZHD Cyber Engine", version="3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════════
#  BOT MANAGER
# ═══════════════════════════════════════════════════════════════════
class BotManager:
    def __init__(self):
        self.running = False
        self.tasks = []
        self.websockets: Set[WebSocket] = set()
        self.accounts = []
        self.config = {
            "target_uid": "",
            "auto_follow": False,
            "seq_mode": True,
            "start_map": 1,
            "end_map": len(vn.MAP_CODES),
            "resume_mode": True,
            "hold_secs": 60.0,
            "sync_secs": 10.0,
            "rejoin_delay": 2.0,
        }
        self._push_task = None

    # ── WebSocket broadcast ────────────────────────────────────────
    async def broadcast(self, payload: dict):
        dead = set()
        for ws in list(self.websockets):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.add(ws)
        self.websockets -= dead

    def _serialize_state(self) -> dict:
        bots = {}
        for k, v in vn.STATE["bots"].items():
            bots[str(k)] = {
                "uid": v.get("uid", ""),
                "aid": v.get("aid", ""),
                "status": v.get("status", "-"),
                "map": v.get("map", "-"),
                "entity": str(v.get("entity", "-")),
                "matches_played": v.get("matches_played", 0),
                "time_in_match": round(v.get("time_in_match", 0), 1),
                "total_time_played": v.get("total_time_played", 0),
                "follow_done": v.get("follow_done", False),
            }
        social = {}
        for k, v in vn.STATE.get("social", {}).items():
            social[str(k)] = {
                "uid": str(v.get("uid", "")),
                "action": v.get("action", ""),
                "target": v.get("target", ""),
                "status": v.get("status", ""),
                "map": v.get("map", ""),
            }
        return {
            "bots": bots,
            "logs": list(vn.STATE["logs"]),
            "errors": list(vn.STATE["error_logs"]),
            "social": social,
            "totals": {
                "matches": vn.STATE["total_matches"],
                "errors": vn.STATE["total_errors"],
                "follows": vn.STATE["total_follows"],
                "subscribes": vn.STATE["total_subscribes"],
            },
            "config": self.config,
            "running": self.running,
            "uptime": int(time.time() - vn.STATE["start_time"]),
        }

    # ── Config persistence ────────────────────────────────────────
    def save_config(self):
        try:
            WEB_CONFIG_FILE.write_text(json.dumps(self.config, indent=2))
        except Exception as e:
            print(f"[CFG] save error: {e}", file=sys.stderr)

    def load_config(self):
        if not WEB_CONFIG_FILE.exists():
            return
        try:
            data = json.loads(WEB_CONFIG_FILE.read_text())
            self.config.update(data)
        except Exception as e:
            print(f"[CFG] load error: {e}", file=sys.stderr)

    # ── Background pusher ─────────────────────────────────────────
    async def state_pusher(self):
        while True:
            if self.websockets:
                try:
                    await self.broadcast({"type": "state", "data": self._serialize_state()})
                except Exception as e:
                    print(f"[WS] push error: {e}", file=sys.stderr)
            await asyncio.sleep(0.8)

    # ── Start / Stop ──────────────────────────────────────────────
    async def start(self, accounts: list, config: dict) -> dict:
        if self.running:
            return {"ok": False, "msg": "Engine already running"}
        if not accounts:
            return {"ok": False, "msg": "No accounts loaded — save accounts first"}

        self.accounts = accounts
        self.config = {**self.config, **config}
        self.save_config()
        cfg = self.config

        # Reset runtime state
        vn.STATE["bots"].clear()
        vn.STATE["social"].clear()
        vn.STATE["total_matches"] = 0
        vn.STATE["total_errors"] = 0
        vn.STATE["total_follows"] = 0
        vn.STATE["total_subscribes"] = 0
        vn.STATE["start_time"] = time.time()
        vn.STATE["logs"].clear()
        vn.STATE["error_logs"].clear()

        # Apply to vn globals
        vn.SEQ_MODE = bool(cfg["seq_mode"])
        vn.START_MAP_IDX = int(cfg["start_map"])
        vn.END_MAP_IDX = int(cfg["end_map"])
        vn.RESUME_MODE = bool(cfg["resume_mode"])
        vn.AUTO_FOLLOW_TARGET = str(cfg["target_uid"])
        vn.AUTO_FOLLOW_ENABLED = bool(cfg["auto_follow"])

        vn.ROTATION["maps"] = list(vn.MAP_CODES)
        vn.ROTATION["hold_secs"] = float(cfg["hold_secs"])
        vn.ROTATION["sync_secs"] = float(cfg["sync_secs"])
        vn.ROTATION["start_time"] = time.time()
        vn.ROTATION["range_start"] = int(cfg["start_map"]) - 1
        vn.ROTATION["range_end"] = int(cfg["end_map"]) - 1

        self.running = True
        self.tasks = [
            asyncio.create_task(
                vn.loop_matches(
                    i,
                    len(accounts),
                    acc["uid"],
                    acc["password"],
                    acc.get("aid", ""),
                    float(cfg["rejoin_delay"]),
                )
            )
            for i, acc in enumerate(accounts, 1)
        ]

        vn.hdr(f"🚀 ENGINE STARTED — {len(accounts)} BOTS · {'SEQ' if vn.SEQ_MODE else 'TIMER'}")
        return {"ok": True, "count": len(accounts)}

    async def stop(self) -> dict:
        if not self.running:
            return {"ok": False, "msg": "Not running"}
        self.running = False
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []
        for bot in vn.STATE["bots"].values():
            bot["status"] = "Stopped"
        vn.warn("⛔ ENGINE STOPPED BY USER")
        return {"ok": True}

    async def manual_social(self, action_type: str, target: str) -> dict:
        if not self.accounts:
            return {"ok": False, "msg": "No accounts loaded"}
        # Ensure bot slots exist so social state can bind to them
        for i, acc in enumerate(self.accounts, 1):
            if i not in vn.STATE["bots"]:
                vn.STATE["bots"][i] = {
                    "uid": acc["uid"],
                    "aid": acc.get("aid", ""),
                    "status": "Idle",
                    "map": "-",
                    "entity": "-",
                    "matches_played": 0,
                    "time_in_match": 0,
                    "total_time_played": 0,
                    "follow_done": False,
                }
        asyncio.create_task(vn.run_social_actions(self.accounts, action_type, target))
        return {"ok": True, "count": len(self.accounts)}


manager = BotManager()


# ═══════════════════════════════════════════════════════════════════
#  LIFECYCLE
# ═══════════════════════════════════════════════════════════════════
def _clean_accounts(data: list) -> list:
    clean = []
    for e in data:
        if not isinstance(e, dict):
            continue
        uid = str(e.get("uid") or e.get("user") or "").strip()
        pwd = str(e.get("password") or e.get("pass") or "").strip()
        aid = str(e.get("account_id") or e.get("acc_id") or "").strip()
        if uid and pwd:
            clean.append({"uid": uid, "password": pwd, "aid": aid})
    return clean


@app.on_event("startup")
async def _startup():
    manager.load_config()
    if ACCOUNTS_FILE.exists():
        try:
            data = json.loads(ACCOUNTS_FILE.read_text())
            if isinstance(data, dict):
                data = data.get("accounts", [])
            manager.accounts = _clean_accounts(data)
            print(f"[BOOT] Loaded {len(manager.accounts)} accounts from {ACCOUNTS_FILE}")
        except Exception as e:
            print(f"[BOOT] Accounts load failed: {e}", file=sys.stderr)
    manager._push_task = asyncio.create_task(manager.state_pusher())
    print("[BOOT] ⚡ ZHD Cyber Engine online")
    print(f"[BOOT] DATA_DIR = {DATA_DIR}")


@app.on_event("shutdown")
async def _shutdown():
    print("[SHUTDOWN] Cleaning up...")
    try:
        await manager.stop()
    except Exception:
        pass
    if manager._push_task:
        manager._push_task.cancel()


# ═══════════════════════════════════════════════════════════════════
#  HTTP ROUTES
# ═══════════════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def index():
    f = TEMPLATES_DIR / "index.html"
    if not f.exists():
        return HTMLResponse(
            "<h1>UI missing</h1><p>Upload <code>templates/index.html</code> to the container</p>",
            status_code=500,
        )
    return HTMLResponse(f.read_text(encoding="utf-8"))


@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "running": manager.running,
        "bot_count": len(manager.accounts),
        "uptime": int(time.time() - vn.STATE["start_time"]),
    }


@app.get("/api/maps")
async def get_maps():
    return {"maps": vn.MAP_CODES, "count": len(vn.MAP_CODES)}


@app.get("/api/accounts")
async def get_accounts():
    return {"accounts": manager.accounts}


@app.get("/api/config")
async def get_config():
    return {"config": manager.config}


@app.post("/api/config")
async def set_config(config_json: str = Form(...)):
    try:
        cfg = json.loads(config_json)
        manager.config.update(cfg)
        manager.save_config()
        return {"ok": True, "config": manager.config}
    except Exception as e:
        return {"ok": False, "msg": str(e)}


@app.post("/api/accounts")
async def save_accounts(raw_json: str = Form(...)):
    try:
        data = json.loads(raw_json)
        if isinstance(data, dict):
            data = data.get("accounts", [])
        clean = _clean_accounts(data)
        if not clean:
            return {"ok": False, "msg": "No valid accounts (need uid + password)"}
        ACCOUNTS_FILE.write_text(json.dumps(clean, indent=2))
        manager.accounts = clean
        return {"ok": True, "count": len(clean)}
    except Exception as e:
        return {"ok": False, "msg": f"Parse error: {e}"}


@app.post("/api/upload_accounts")
async def upload_accounts(file: UploadFile = File(...)):
    try:
        content = await file.read()
        data = json.loads(content)
        if isinstance(data, dict):
            data = data.get("accounts", [])
        clean = _clean_accounts(data)
        if not clean:
            return {"ok": False, "msg": "No valid accounts in file"}
        ACCOUNTS_FILE.write_text(json.dumps(clean, indent=2))
        manager.accounts = clean
        return {"ok": True, "count": len(clean)}
    except Exception as e:
        return {"ok": False, "msg": str(e)}


@app.post("/api/start")
async def start_engine(config_json: str = Form(...)):
    try:
        cfg = json.loads(config_json)
    except Exception as e:
        return {"ok": False, "msg": f"Bad config: {e}"}
    return await manager.start(manager.accounts, cfg)


@app.post("/api/stop")
async def stop_engine():
    return await manager.stop()


@app.post("/api/social")
async def manual_social(action: str = Form(...), target: str = Form(...)):
    if action not in ("Follow", "Subscribe"):
        return {"ok": False, "msg": "Invalid action"}
    target = target.strip()
    if not target:
        return {"ok": False, "msg": "Target required"}
    return await manager.manual_social(action, target)


# ═══════════════════════════════════════════════════════════════════
#  WEBSOCKET
# ═══════════════════════════════════════════════════════════════════
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    manager.websockets.add(ws)
    try:
        await ws.send_json({"type": "state", "data": manager._serialize_state()})
        while True:
            # Blocks until client sends or disconnects — state pushed via manager
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        manager.websockets.discard(ws)


# ═══════════════════════════════════════════════════════════════════
#  ENTRY
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)