"""
ZHD CYBER ENGINE — Web Control Backend v4.0
Features: Map Editor, Hot Bot Injection, JWT Manager
"""
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Set, Dict

# Save original streams BEFORE importing vn (kyunki vn.py sys.stdout ko redirect karta hai)
_original_stdout = sys.stdout
_original_stderr = sys.stderr

import vn
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

DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR)))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    DATA_DIR = BASE_DIR

ACCOUNTS_FILE   = DATA_DIR / "accounts.json"
WEB_CONFIG_FILE = DATA_DIR / "web_config.json"
MAPS_FILE       = DATA_DIR / "maps.json"

# Redirect vn.py data files to mounted volume
vn.JWT_CACHE_FILE      = str(DATA_DIR / "jwt.json")
vn.HISTORY_FILE        = str(DATA_DIR / "bot_history.json")
vn.MAP_STATS_FILE      = str(DATA_DIR / "map.json")
vn.PROGRESS_SEQ_FILE   = str(DATA_DIR / "progress_seq.json")
vn.PROGRESS_TIMER_FILE = str(DATA_DIR / "progress_timer.json")
vn.CONFIG_FILE         = str(DATA_DIR / "config.json")

# Load Maps dynamically from maps.json
def load_maps():
    if MAPS_FILE.exists():
        try:
            maps = json.loads(MAPS_FILE.read_text())
            if isinstance(maps, list) and len(maps) > 0:
                vn.MAP_CODES = maps
                vn.ROTATION["maps"] = list(maps)
        except Exception as e:
            print(f"[BOOT] Map load error: {e}", file=sys.stderr)
    else:
        # First time run: save default maps to file
        save_maps(vn.MAP_CODES)

def save_maps(maps_list):
    MAPS_FILE.write_text(json.dumps(maps_list, indent=2))
    vn.MAP_CODES = maps_list
    vn.ROTATION["maps"] = list(maps_list)

load_maps()

# ═══════════════════════════════════════════════════════════════════
#  APP
# ═══════════════════════════════════════════════════════════════════
app = FastAPI(title="ZHD Cyber Engine", version="4.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ═══════════════════════════════════════════════════════════════════
#  BOT MANAGER (With Hot-Reload)
# ═══════════════════════════════════════════════════════════════════
class BotManager:
    def __init__(self):
        self.running = False
        self.tasks: Dict[str, asyncio.Task] = {} # Store running tasks by UID
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
        self._bot_idx_counter = 1

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
                "uid": v.get("uid", ""), "aid": v.get("aid", ""),
                "status": v.get("status", "-"), "map": v.get("map", "-"),
                "entity": str(v.get("entity", "-")), "matches_played": v.get("matches_played", 0),
                "time_in_match": round(v.get("time_in_match", 0), 1),
                "total_time_played": v.get("total_time_played", 0), "follow_done": v.get("follow_done", False),
            }
        social = {}
        for k, v in vn.STATE.get("social", {}).items():
            social[str(k)] = {"uid": str(v.get("uid", "")), "action": v.get("action", ""),
                "target": v.get("target", ""), "status": v.get("status", ""), "map": v.get("map", "")}
        return {
            "bots": bots, "logs": list(vn.STATE["logs"]), "errors": list(vn.STATE["error_logs"]),
            "social": social, "totals": {
                "matches": vn.STATE["total_matches"], "errors": vn.STATE["total_errors"],
                "follows": vn.STATE["total_follows"], "subscribes": vn.STATE["total_subscribes"],
            },
            "config": self.config, "running": self.running,
            "uptime": int(time.time() - vn.STATE["start_time"]),
        }

    def save_config(self):
        try: WEB_CONFIG_FILE.write_text(json.dumps(self.config, indent=2))
        except Exception: pass

    def load_config(self):
        if not WEB_CONFIG_FILE.exists(): return
        try: self.config.update(json.loads(WEB_CONFIG_FILE.read_text()))
        except Exception: pass

    async def state_pusher(self):
        while True:
            if self.websockets:
                try: await self.broadcast({"type": "state", "data": self._serialize_state()})
                except Exception: pass
            await asyncio.sleep(0.8)

    # ─── TASK MANAGEMENT (Zero-Downtime) ───
    def start_bot_task(self, acc):
        uid = acc["uid"]
        if uid in self.tasks and not self.tasks[uid].done(): return # Already running
        idx = self._bot_idx_counter
        self._bot_idx_counter += 1
        
        # Add placeholder to STATE so UI updates immediately
        vn.STATE["bots"][idx] = {
            "uid": uid, "aid": acc.get("aid", ""), "status": "Booting", "map": "-", 
            "entity": "-", "matches_played": 0, "time_in_match": 0, 
            "total_time_played": 0, "follow_done": False
        }
        
        # Start actual loop
        task = asyncio.create_task(
            vn.loop_matches(idx, 999, uid, acc["password"], acc.get("aid", ""), float(self.config["rejoin_delay"]))
        )
        self.tasks[uid] = task

    def stop_bot_task(self, uid):
        if uid in self.tasks:
            self.tasks[uid].cancel()
            del self.tasks[uid]
            # Clean from state
            for idx, bot in list(vn.STATE["bots"].items()):
                if bot.get("uid") == uid: 
                    del vn.STATE["bots"][idx]

    # ─── ENGINE CONTROLS ───
    async def start_engine(self, accounts: list, config: dict) -> dict:
        if self.running: return {"ok": False, "msg": "Engine already running"}
        self.accounts = accounts
        self.config.update(config)
        self.save_config()
        
        # Apply config to vn module
        vn.SEQ_MODE = bool(self.config["seq_mode"])
        vn.START_MAP_IDX = int(self.config["start_map"])
        vn.END_MAP_IDX = int(self.config["end_map"])
        vn.RESUME_MODE = bool(self.config["resume_mode"])
        vn.AUTO_FOLLOW_TARGET = str(self.config["target_uid"])
        vn.AUTO_FOLLOW_ENABLED = bool(self.config["auto_follow"])
        vn.ROTATION["hold_secs"] = float(self.config["hold_secs"])
        vn.ROTATION["sync_secs"] = float(self.config["sync_secs"])
        vn.ROTATION["start_time"] = time.time()
        vn.ROTATION["range_start"] = int(self.config["start_map"]) - 1
        vn.ROTATION["range_end"] = int(self.config["end_map"]) - 1

        self.running = True
        for acc in self.accounts:
            self.start_bot_task(acc)
        vn.hdr(f"🚀 ENGINE STARTED — {len(self.accounts)} BOTS")
        return {"ok": True}

    async def stop_engine(self) -> dict:
        if not self.running: return {"ok": False, "msg": "Not running"}
        self.running = False
        for uid in list(self.tasks.keys()):
            self.stop_bot_task(uid)
        vn.warn("⛔ ENGINE STOPPED BY USER")
        return {"ok": True}

    # ─── HOT INJECTION LOGIC ───
    async def inject_bots(self, new_accounts: list) -> dict:
        if not self.running: return {"ok": False, "msg": "Start engine first"}
        existing_uids = {a["uid"] for a in self.accounts}
        added_count = 0
        
        for acc in new_accounts:
            if acc["uid"] not in existing_uids:
                self.accounts.append(acc)
                self.start_bot_task(acc)
                added_count += 1
                
        ACCOUNTS_FILE.write_text(json.dumps(self.accounts, indent=2))
        if added_count > 0:
            vn.ok(f"🔥 INJECTED {added_count} NEW BOTS LIVE")
        return {"ok": True, "added": added_count, "total": len(self.accounts)}

    async def replace_bots(self, new_accounts: list) -> dict:
        if not self.running: return {"ok": False, "msg": "Start engine first"}
        new_uids = {a["uid"] for a in new_accounts}
        old_uids = {a["uid"] for a in self.accounts}
        
        # Kill removed bots
        for uid in (old_uids - new_uids):
            self.stop_bot_task(uid)
            
        # Start new ones
        added = 0
        for acc in new_accounts:
            if acc["uid"] not in old_uids:
                self.start_bot_task(acc)
                added += 1
                
        self.accounts = new_accounts
        ACCOUNTS_FILE.write_text(json.dumps(self.accounts, indent=2))
        vn.warn(f"♻️ BOTS REPLACED. Added: {added}, Removed: {len(old_uids - new_uids)}")
        return {"ok": True, "total": len(self.accounts)}

    async def clear_all(self) -> dict:
        await self.stop_engine()
        self.accounts = []
        ACCOUNTS_FILE.write_text("[]")
        vn.STATE["bots"].clear()
        vn.warn("🗑️ ALL BOTS KILLED & CLEARED")
        return {"ok": True}

    def clear_jwt(self):
        try:
            if os.path.exists(vn.JWT_CACHE_FILE):
                os.remove(vn.JWT_CACHE_FILE)
            vn.ok("🔑 JWT CACHE CLEARED. Bots will re-login on next cycle.")
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "msg": str(e)}

manager = BotManager()

# ═══════════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════════
@app.on_event("startup")
async def _startup():
    manager.load_config()
    if ACCOUNTS_FILE.exists():
        try:
            data = json.loads(ACCOUNTS_FILE.read_text())
            manager.accounts = [a for a in data if isinstance(a, dict) and "uid" in a]
        except Exception: pass
    manager._push_task = asyncio.create_task(manager.state_pusher())

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse((TEMPLATES_DIR / "index.html").read_text(encoding="utf-8"))

# --- MAP APIs ---
@app.get("/api/maps")
async def get_maps():
    return {"maps": vn.MAP_CODES}

@app.post("/api/maps")
async def update_maps(maps_json: str = Form(...)):
    try:
        maps = json.loads(maps_json)
        if not isinstance(maps, list): return {"ok": False, "msg": "Must be array"}
        save_maps(maps)
        return {"ok": True, "count": len(maps)}
    except Exception as e: return {"ok": False, "msg": str(e)}

# --- ACCOUNT APIs ---
@app.get("/api/accounts")
async def get_accounts():
    return {"accounts": manager.accounts}

@app.post("/api/accounts/inject")
async def inject_bots(raw_json: str = Form(...)):
    try:
        data = json.loads(raw_json)
        return await manager.inject_bots(data)
    except Exception as e: return {"ok": False, "msg": str(e)}

@app.post("/api/accounts/replace")
async def replace_bots(raw_json: str = Form(...)):
    try:
        data = json.loads(raw_json)
        return await manager.replace_bots(data)
    except Exception as e: return {"ok": False, "msg": str(e)}

@app.post("/api/accounts/clear")
async def clear_bots():
    return await manager.clear_all()

# --- JWT API ---
@app.post("/api/jwt/clear")
async def clear_jwt():
    return manager.clear_jwt()

# --- ENGINE APIs ---
@app.post("/api/start")
async def start_engine(config_json: str = Form(...)):
    try: cfg = json.loads(config_json)
    except: return {"ok": False, "msg": "Bad JSON"}
    return await manager.start_engine(manager.accounts, cfg)

@app.post("/api/stop")
async def stop_engine():
    return await manager.stop_engine()

# --- SOCIAL APIs ---
@app.post("/api/social")
async def manual_social(action: str = Form(...), target: str = Form(...)):
    if not manager.accounts: return {"ok": False, "msg": "No accounts"}
    asyncio.create_task(vn.run_social_actions(manager.accounts, action, target))
    return {"ok": True}

# --- WEBSOCKET ---
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    manager.websockets.add(ws)
    try:
        await ws.send_json({"type": "state", "data": manager._serialize_state()})
        while True: await ws.receive_text()
    except: pass
    finally: manager.websockets.discard(ws)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
