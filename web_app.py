"""
ZHD CYBER ENGINE - Web Control Backend
Wraps vn.py bot loop into a REST + WebSocket API
"""
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# Import bot engine
import vn

# Restore stdout (vn.py redirects it to a dummy writer)
try:
    sys.stdout = vn.original_stdout
except Exception:
    pass

BASE_DIR = Path(__file__).parent
ACCOUNTS_FILE = BASE_DIR / "accounts.json"
TEMPLATES_DIR = BASE_DIR / "templates"

app = FastAPI(title="ZHD Cyber Engine", version="2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════════
#  BOT MANAGER
# ══════════════════════════════════════════════════════════════════════
class BotManager:
    def __init__(self):
        self.running = False
        self.tasks = []
        self.websockets: Set[WebSocket] = set()
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
        self.accounts = []
        self._push_task = None

    # ── WebSocket broadcast ───────────────────────────────────────────
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

    # ── Background state pusher ───────────────────────────────────────
    async def state_pusher(self):
        while True:
            if self.websockets:
                try:
                    await self.broadcast({"type": "state", "data": self._serialize_state()})
                except Exception as e:
                    print("push err:", e)
            await asyncio.sleep(0.8)

    # ── Start/Stop ────────────────────────────────────────────────────
    async def start(self, accounts: list, config: dict):
        if self.running:
            return {"ok": False, "msg": "Engine already running"}
        if not accounts:
            return {"ok": False, "msg": "No accounts loaded"}

        self.accounts = accounts
        self.config = {**self.config, **config}
        cfg = self.config

        # Reset state
        vn.STATE["bots"].clear()
        vn.STATE["social"].clear()
        vn.STATE["total_matches"] = 0
        vn.STATE["total_errors"] = 0
        vn.STATE["total_follows"] = 0
        vn.STATE["total_subscribes"] = 0
        vn.STATE["start_time"] = time.time()
        vn.STATE["logs"].clear()
        vn.STATE["error_logs"].clear()

        # Apply globals to vn module
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

        vn.hdr(f"🚀 ENGINE STARTED — {len(accounts)} BOTS")
        return {"ok": True}

    async def stop(self):
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

    async def manual_social(self, action_type: str, target: str):
        if not self.accounts:
            return {"ok": False, "msg": "No accounts loaded"}
        # Ensure bot slots exist
        for i, acc in enumerate(self.accounts, 1):
            if i not in vn.STATE["bots"]:
                vn.STATE["bots"][i] = {
                    "uid": acc["uid"], "aid": acc.get("aid", ""),
                    "status": "Idle", "map": "-", "entity": "-",
                    "matches_played": 0, "time_in_match": 0,
                    "total_time_played": 0, "follow_done": False,
                }
        asyncio.create_task(vn.run_social_actions(self.accounts, action_type, target))
        return {"ok": True}


manager = BotManager()


# ══════════════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════════════
@app.on_event("startup")
async def _startup():
    # Load persisted accounts
    if ACCOUNTS_FILE.exists():
        try:
            data = json.loads(ACCOUNTS_FILE.read_text())
            if isinstance(data, dict):
                data = data.get("accounts", [])
            manager.accounts = data
            print(f"[BOOT] Loaded {len(data)} accounts from disk")
        except Exception as e:
            print("[BOOT] Accounts load failed:", e)
    manager._push_task = asyncio.create_task(manager.state_pusher())
    print("[BOOT] ZHD Cyber Engine online")


# ══════════════════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def index():
    f = TEMPLATES_DIR / "index.html"
    if not f.exists():
        return HTMLResponse("<h1>UI missing: templates/index.html</h1>", status_code=500)
    return HTMLResponse(f.read_text(encoding="utf-8"))


@app.get("/api/health")
async def health():
    return {"ok": True, "running": manager.running, "bot_count": len(manager.accounts)}


@app.get("/api/accounts")
async def get_accounts():
    return {"accounts": manager.accounts}


@app.post("/api/accounts")
async def save_accounts(raw_json: str = Form(...)):
    try:
        data = json.loads(raw_json)
        if isinstance(data, dict):
            data = data.get("accounts", [])
        clean = []
        for e in data:
            if not isinstance(e, dict):
                continue
            uid = str(e.get("uid") or e.get("user") or "").strip()
            pwd = str(e.get("password") or e.get("pass") or "").strip()
            aid = str(e.get("account_id") or e.get("acc_id") or "").strip()
            if uid and pwd:
                clean.append({"uid": uid, "password": pwd, "aid": aid})
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
        clean = []
        for e in data:
            if not isinstance(e, dict):
                continue
            uid = str(e.get("uid") or e.get("user") or "").strip()
            pwd = str(e.get("password") or e.get("pass") or "").strip()
            aid = str(e.get("account_id") or e.get("acc_id") or "").strip()
            if uid and pwd:
                clean.append({"uid": uid, "password": pwd, "aid": aid})
        if not clean:
            return {"ok": False, "msg": "No valid accounts found"}
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
    if not target.strip():
        return {"ok": False, "msg": "Target required"}
    return await manager.manual_social(action, target.strip())


@app.get("/api/maps")
async def get_maps():
    return {"maps": vn.MAP_CODES, "count": len(vn.MAP_CODES)}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    manager.websockets.add(ws)
    try:
        await ws.send_json({"type": "state", "data": manager._serialize_state()})
        while True:
            await ws.receive_text()  # keepalive
    except WebSocketDisconnect:
        manager.websockets.discard(ws)
    except Exception:
        manager.websockets.discard(ws)