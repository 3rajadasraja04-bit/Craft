"""
RAJA ULTIMATE ENGINE - Web Backend
"""
import sys
_ORIG_STDOUT = sys.stdout

import asyncio
import threading
import time
import json
import os
import re
import base64
import traceback
import logging
from datetime import datetime
from collections import deque

import requests
import urllib3
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from flask import Flask, jsonify, request, render_template, send_file
from flask_cors import CORS

import vn
sys.stdout = _ORIG_STDOUT

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

vn.STATE["logs"] = deque(maxlen=300)
vn.STATE["error_logs"] = deque(maxlen=300)

BASE = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_FILE = os.path.join(BASE, "accounts.json")
MAPS_FILE     = os.path.join(BASE, "maps.json")
CONFIG_FILE   = os.path.join(BASE, "config.json")
HISTORY_FILE  = os.path.join(BASE, "bot_history.json")
JWT_FILE      = os.path.join(BASE, "jwt.json")
SOCIAL_LOG    = os.path.join(BASE, "social_history.json")

DEFAULT_CONFIG = {
    "seq_mode": True, "start_map": 1, "end_map": 24, "resume": True,
    "auto_subscribe": False, "hold_secs": 60.0, "sync_secs": 10.0, "rejoin_delay": 2.0,
}

_accounts_lock = threading.RLock()

def load_json(path, default):
    if not os.path.exists(path): return default
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return default

def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)
        return True
    except Exception as e:
        print(f"save_json error {path}: {e}")
        return False

def load_accounts():
    with _accounts_lock:
        data = load_json(ACCOUNTS_FILE, [])
        if isinstance(data, dict): data = data.get("accounts", [])
        jwt_cache = load_json(JWT_FILE, {})
        out = []
        for e in data:
            if not isinstance(e, dict): continue
            uid = str(e.get("uid") or e.get("user") or "").strip()
            pwd = str(e.get("password") or e.get("pass") or "").strip()
            if not uid or not pwd: continue
            cached_jwt = jwt_cache.get(str(uid), {})
            jwt_aid = str(cached_jwt.get("acc_id", "")).strip()
            aid = jwt_aid if jwt_aid and jwt_aid != "0" else str(e.get("aid") or e.get("account_id") or e.get("acc_id") or uid).strip()
            out.append({"uid": uid, "password": pwd, "aid": aid, "region": str(e.get("region", "IND")).upper(), "enabled": bool(e.get("enabled", True))})
        return out

def save_accounts(accounts):
    with _accounts_lock:
        return save_json(ACCOUNTS_FILE, accounts)

_last_ban_check = 0
def auto_remove_banned():
    global _last_ban_check
    now = time.time()
    if now - _last_ban_check < 5: return
    _last_ban_check = now
    banned_uids = {str(b.get("uid")) for b in list(vn.STATE.get("bots", {}).values()) if "BANNED" in str(b.get("status", ""))}
    if not banned_uids: return
    with _accounts_lock:
        raw_data = load_json(ACCOUNTS_FILE, [])
        if not raw_data: return
        updated = False
        if isinstance(raw_data, list):
            initial_len = len(raw_data)
            raw_data = [a for a in raw_data if isinstance(a, dict) and str(a.get("uid")) not in banned_uids]
            if len(raw_data) < initial_len: updated = True; save_json(ACCOUNTS_FILE, raw_data)
        elif isinstance(raw_data, dict) and "accounts" in raw_data:
            initial_len = len(raw_data["accounts"])
            raw_data["accounts"] = [a for a in raw_data["accounts"] if isinstance(a, dict) and str(a.get("uid")) not in banned_uids]
            if len(raw_data["accounts"]) < initial_len: updated = True; save_json(ACCOUNTS_FILE, raw_data)

def load_maps():
    data = load_json(MAPS_FILE, None)
    if data and isinstance(data, dict) and isinstance(data.get("maps"), list): return data["maps"]
    maps = list(vn.MAP_CODES)
    save_json(MAPS_FILE, {"maps": maps})
    return maps

def save_maps(maps):
    save_json(MAPS_FILE, {"maps": maps})
    try: vn.MAP_CODES[:] = list(maps)
    except Exception: pass

def load_config():
    cfg = dict(DEFAULT_CONFIG)
    data = load_json(CONFIG_FILE, {})
    if isinstance(data, dict): cfg.update({k: v for k, v in data.items() if k in DEFAULT_CONFIG})
    return cfg

def save_config(cfg): return save_json(CONFIG_FILE, cfg)

engine_state = {
    "running": False, "thread": None, "loop": None, "tasks": [], "bot_tasks": {}, "monitor_task": None,
    "next_bot_idx": 1, "start_time": None, "config": {}, "follow_targets": {}, "subscribe_targets": {},
}

_social_lock = threading.Lock()

def _patch_social_tracking():
    if getattr(vn, "_tracked", False): return
    orig = vn.execute_social_action
    async def tracked(idx, uid, acc_id, action_type, target, pwd, is_auto=False, map_name="", is_batch=False):
        try:
            if action_type == "Follow":
                with _social_lock: engine_state["follow_targets"][idx] = target
            elif action_type == "Subscribe":
                with _social_lock: engine_state["subscribe_targets"][idx] = target
            log_social_action(idx, uid, action_type, target, "Sent")
        except Exception: pass
        result = await orig(idx, uid, acc_id, action_type, target, pwd, is_auto=is_auto, map_name=map_name, is_batch=is_batch)
        try:
            status = vn.STATE.get("social", {}).get(idx, {}).get("status", "?")
            log_social_action(idx, uid, action_type, target, status)
        except Exception: pass
        return result
    vn.execute_social_action = tracked
    vn._tracked = True

def log_social_action(bot_idx, uid, action, target, status):
    hist = load_json(SOCIAL_LOG, [])
    hist.append({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "bot": bot_idx, "uid": uid, "action": action, "target": target, "status": status})
    if len(hist) > 500: hist = hist[-500:]
    save_json(SOCIAL_LOG, hist)

async def _live_bot_monitor(cfg):
    while engine_state["running"]:
        await asyncio.sleep(2)
        if not engine_state["running"]: break
        try:
            accounts = load_accounts()
            current_uids = set()
            for acc in accounts:
                uid = acc["uid"]
                if not acc.get("enabled", True): continue
                current_uids.add(uid)
                task = engine_state["bot_tasks"].get(uid)
                is_banned = any(b.get("uid") == uid and "BANNED" in str(b.get("status", "")) for b in vn.STATE["bots"].values())
                if not is_banned and (task is None or task.done()):
                    if task is not None and task.done():
                        for old_idx, old_b in list(vn.STATE["bots"].items()):
                            if old_b.get("uid") == uid: del vn.STATE["bots"][old_idx]; break
                    new_idx = engine_state.get("next_bot_idx", 1)
                    engine_state["next_bot_idx"] = new_idx + 1
                    vn.STATE["bots"][new_idx] = {"uid": uid, "aid": acc.get("aid", uid), "status": "Booting", "map": "-", "entity": "-", "matches_played": 0, "time_in_match": 0, "total_time_played": 0, "follow_done": False}
                    t = asyncio.create_task(vn.loop_matches(new_idx, len(accounts), uid, acc["password"], acc.get("aid", uid), cfg["rejoin_delay"]))
                    engine_state["bot_tasks"][uid] = t
                    engine_state["tasks"].append(t)
            removed_uids = [uid for uid in list(engine_state["bot_tasks"].keys()) if uid not in current_uids]
            for uid in removed_uids:
                task = engine_state["bot_tasks"].pop(uid)
                if not task.done(): task.cancel()
                for idx, b in list(vn.STATE["bots"].items()):
                    if b.get("uid") == uid: del vn.STATE["bots"][idx]; break
        except Exception as e: print(f"[Live Monitor] Error: {e}")

def _engine_runner(cfg, accounts):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    engine_state["loop"] = loop
    vn.SEQ_MODE = cfg["seq_mode"]; vn.START_MAP_IDX = cfg["start_map"]; vn.END_MAP_IDX = cfg["end_map"]; vn.RESUME_MODE = cfg["resume"]; vn.AUTO_SUBSCRIBE_ENABLED = cfg["auto_subscribe"]
    vn.ROTATION["maps"] = list(vn.MAP_CODES); vn.ROTATION["hold_secs"] = cfg["hold_secs"]; vn.ROTATION["sync_secs"] = cfg["sync_secs"]; vn.ROTATION["start_time"] = time.time(); vn.ROTATION["range_start"] = vn.START_MAP_IDX - 1; vn.ROTATION["range_end"] = vn.END_MAP_IDX - 1
    enabled = [a for a in accounts if a.get("enabled", True)]
    if not enabled:
        engine_state["running"] = False; engine_state["loop"] = None; loop.close(); return
    async def runner():
        vn.STATE["bots"].clear()
        engine_state["bot_tasks"] = {}; engine_state["tasks"] = []
        max_idx = 0
        for i, acc in enumerate(enabled):
            orig_idx = i + 1
            max_idx = max(max_idx, orig_idx)
            task = asyncio.create_task(vn.loop_matches(orig_idx, len(enabled), acc["uid"], acc["password"], acc.get("aid", acc["uid"]), cfg["rejoin_delay"]))
            engine_state["bot_tasks"][acc["uid"]] = task; engine_state["tasks"].append(task)
        engine_state["next_bot_idx"] = max_idx + 1
        engine_state["monitor_task"] = asyncio.create_task(_live_bot_monitor(cfg))
        all_tasks = list(engine_state["tasks"]) + [engine_state["monitor_task"]]
        try: await asyncio.gather(*all_tasks, return_exceptions=True)
        except asyncio.CancelledError: pass
        finally:
            for t in engine_state["tasks"]:
                if not t.done(): t.cancel()
            if engine_state.get("monitor_task") and not engine_state["monitor_task"].done(): engine_state["monitor_task"].cancel()
            await asyncio.gather(*engine_state["tasks"], return_exceptions=True)
            if engine_state.get("monitor_task"): await asyncio.gather(engine_state["monitor_task"], return_exceptions=True)
    try: loop.run_until_complete(runner())
    except Exception: traceback.print_exc()
    finally:
        engine_state.update({"running": False, "loop": None, "tasks": [], "bot_tasks": {}, "monitor_task": None})
        vn.STATE["bots"].clear() # 🔴 FIX: Memory clear on stop
        try: loop.close()
        except Exception: pass

def start_engine(cfg):
    if engine_state["running"]: return False, "Engine already running"
    accounts = load_accounts()
    if not any(a.get("enabled", True) for a in accounts): return False, "No enabled bots."
    _patch_social_tracking()
    engine_state["config"] = cfg; engine_state["running"] = True; engine_state["start_time"] = time.time(); engine_state["follow_targets"] = {}; engine_state["subscribe_targets"] = {}
    save_config(cfg)
    engine_state["thread"] = threading.Thread(target=_engine_runner, args=(cfg, accounts), daemon=True); engine_state["thread"].start()
    return True, "Engine started"

def stop_engine():
    if not engine_state["running"]: return False, "Engine not running"
    loop = engine_state["loop"]
    tasks = list(engine_state["bot_tasks"].values())
    if engine_state.get("monitor_task"): tasks.append(engine_state["monitor_task"])
    if loop and loop.is_running():
        def _cancel():
            for t in tasks:
                if not t.done(): t.cancel()
        try: loop.call_soon_threadsafe(_cancel)
        except Exception: pass
    return True, "Stopping engine..."

def run_async_job(coro):
    loop = engine_state.get("loop")
    if engine_state["running"] and loop and loop.is_running(): return asyncio.run_coroutine_threadsafe(coro, loop)
    def _run():
        try:
            l = asyncio.new_event_loop(); asyncio.set_event_loop(l); l.run_until_complete(coro); l.close()
        except Exception: traceback.print_exc()
    threading.Thread(target=_run, daemon=True).start(); return None

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

_TAG_RE = re.compile(r"\[/?[^\]]*\]")
_TIME_RE = re.compile(r"\[(\d{2}:\d{2}:\d{2})\]")

def parse_log_line(line):
    time_str = ""; m = _TIME_RE.search(line)
    if m: time_str = m.group(1); line = line.replace(m.group(0), "", 1)
    return {"time": time_str, "message": _TAG_RE.sub("", line).strip()}

def serialize_logs(logs): return [parse_log_line(ln) for ln in list(logs)]

def jwt_summary():
    cache = load_json(JWT_FILE, {}); now = time.time(); rows = []; fresh = expired = unknown = 0; accounts = load_accounts(); seen = set()
    for uid, d in cache.items():
        seen.add(uid); exp = d.get("expires_at") or (d.get("saved_at", 0) + vn.JWT_TTL if d.get("saved_at") else 0)
        if exp and now < exp: status = "Fresh"; fresh += 1
        elif exp: status = "Expired"; expired += 1
        else: status = "Unknown"; unknown += 1
        jwt = d.get("jwt", "")
        rows.append({"uid": uid, "acc_id": d.get("acc_id", "-"), "region": d.get("region", "?"), "jwt_preview": (jwt[:22] + "…") if jwt else "-", "expires_at": exp, "expires_in": int(exp - now) if exp else 0, "status": status})
    for a in accounts:
        if a["uid"] not in seen:
            unknown += 1
            rows.append({"uid": a["uid"], "acc_id": a.get("aid", "-"), "region": a.get("region", "?"), "jwt_preview": "-", "expires_at": 0, "expires_in": 0, "status": "Unknown"})
    return {"rows": rows, "fresh": fresh, "expired": expired, "unknown": unknown, "total": len(rows), "ttl": vn.JWT_TTL}

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/status")
def api_status():
    auto_remove_banned()
    with _social_lock: follow_targets = dict(engine_state["follow_targets"]); subscribe_targets = dict(engine_state["subscribe_targets"])
    bots = []
    for idx, b in list(vn.STATE["bots"].items()):
        map_str = b.get("map", "-"); map_num = "-"
        if map_str in vn.MAP_CODES: map_num = vn.MAP_CODES.index(map_str) + 1
        bots.append({"idx": idx, "uid": b.get("uid", ""), "aid": b.get("aid", ""), "status": b.get("status", "-"), "map": map_str, "map_num": map_num, "entity": str(b.get("entity", "-")), "time_in_match": round(float(b.get("time_in_match", 0)), 1), "matches_played": b.get("matches_played", 0), "total_time_played": round(float(b.get("total_time_played", 0)), 1), "follow_done": bool(b.get("follow_done", False)), "following": follow_targets.get(idx, "-") if b.get("follow_done") else follow_targets.get(idx, "-"), "subscribed_map": subscribe_targets.get(idx, "-")})
    uptime = 0
    if engine_state["running"] and engine_state["start_time"]: uptime = int(time.time() - engine_state["start_time"])
    cfg = engine_state.get("config") or load_config(); subscribed_maps = list(vn.STATE.get("subscribed_maps", set()))
    return jsonify({"engine": {"running": engine_state["running"], "uptime": uptime, "mode": "SEQ" if vn.SEQ_MODE else "TIMER", "seq_mode": vn.SEQ_MODE, "auto_subscribe": vn.AUTO_SUBSCRIBE_ENABLED, "config": cfg}, "bots": bots, "logs": serialize_logs(vn.STATE["logs"]), "error_logs": serialize_logs(vn.STATE["error_logs"]), "totals": {"matches": vn.STATE.get("total_matches", 0), "errors": vn.STATE.get("total_errors", 0), "follows": vn.STATE.get("total_follows", 0), "subscribes": vn.STATE.get("total_subscribes", 0)}, "current_target": vn.STATE.get("current_target"), "subscribed_maps": subscribed_maps, "batch": dict(vn.STATE.get("batch", {})), "social": {str(k): v for k, v in vn.STATE.get("social", {}).items()}})

@app.route("/api/engine/start", methods=["POST"])
def api_engine_start():
    data = request.get_json(silent=True) or {}; cfg = dict(DEFAULT_CONFIG)
    for k in DEFAULT_CONFIG:
        if k in data: cfg[k] = data[k]
    cfg["seq_mode"] = bool(cfg["seq_mode"]); cfg["resume"] = bool(cfg["resume"]); cfg["auto_subscribe"] = bool(cfg["auto_subscribe"]); cfg["start_map"] = max(1, int(cfg["start_map"])); cfg["end_map"] = max(cfg["start_map"], int(cfg["end_map"])); cfg["hold_secs"] = max(1.0, float(cfg["hold_secs"])); cfg["sync_secs"] = max(0.1, float(cfg["sync_secs"])); cfg["rejoin_delay"] = max(0.1, float(cfg["rejoin_delay"]))
    ok_flag, msg = start_engine(cfg); return jsonify({"ok": ok_flag, "message": msg, "config": cfg})

@app.route("/api/engine/stop", methods=["POST"])
def api_engine_stop(): ok_flag, msg = stop_engine(); return jsonify({"ok": ok_flag, "message": msg})

@app.route("/api/bots", methods=["GET"])
def api_bots_get(): return jsonify({"bots": load_accounts()})

@app.route("/api/bots", methods=["POST"])
def api_bots_add():
    data = request.get_json(silent=True) or {}; uid = str(data.get("uid", "")).strip(); pwd = str(data.get("password", "")).strip(); aid = str(data.get("account_id", uid)).strip() or uid; region = str(data.get("region", "IND")).upper()
    if not uid or not pwd: return jsonify({"ok": False, "error": "UID and password required"}), 400
    with _accounts_lock:
        accounts = load_accounts()
        if any(a["uid"] == uid for a in accounts): return jsonify({"ok": False, "error": "UID already exists"}), 400
        accounts.append({"uid": uid, "password": pwd, "aid": aid, "region": region, "enabled": True}); save_accounts(accounts)
    return jsonify({"ok": True, "bots": accounts})

@app.route("/api/bots/all", methods=["DELETE"])
def api_bots_delete_all():
    save_accounts([])
    # 🔴 FIX: Clear memory when deleting all bots
    with _accounts_lock:
        vn.STATE["bots"].clear()
    return jsonify({"ok": True, "message": "All bots deleted"})

@app.route("/api/bots/<uid>", methods=["DELETE"])
def api_bots_delete(uid):
    with _accounts_lock:
        accounts = load_accounts(); n = len(accounts); accounts = [a for a in accounts if a["uid"] != uid]
        if len(accounts) == n: return jsonify({"ok": False, "error": "Bot not found"}), 404
        save_accounts(accounts)
        # 🔴 FIX: Clear specific bot from memory
        for idx, b in list(vn.STATE["bots"].items()):
            if b.get("uid") == uid:
                del vn.STATE["bots"][idx]
                break
    return jsonify({"ok": True, "bots": accounts})

@app.route("/api/bots/import", methods=["POST"])
def api_bots_import():
    data = request.get_json(silent=True) or {}; raw_text = data.get("raw_text", ""); parsed_bots = []
    if raw_text:
        try:
            parsed = json.loads(re.sub(r',\s*([\]}])', r'\1', raw_text))
            if isinstance(parsed, dict): parsed = [parsed]
            if isinstance(parsed, list): parsed_bots = parsed
        except json.JSONDecodeError: pass
    if not parsed_bots and raw_text:
        matches = list(re.finditer(r'"uid"\s*:\s*"([^"]+)"', raw_text))
        for i, m in enumerate(matches):
            uid = m.group(1).strip(); start = m.start(); end = matches[i+1].start() if i+1 < len(matches) else len(raw_text); chunk = raw_text[start:end]
            pwd_match = re.search(r'"(?:password|pass)"\s*:\s*"([^"]+)"', chunk); aid_match = re.search(r'"(?:account_id|acc_id|aid)"\s*:\s*"([^"]+)"', chunk); reg_match = re.search(r'"region"\s*:\s*"([^"]+)"', chunk)
            if pwd_match: parsed_bots.append({"uid": uid, "password": pwd_match.group(1).strip(), "account_id": aid_match.group(1).strip() if aid_match else uid, "region": reg_match.group(1).strip().upper() if reg_match else "IND", "enabled": True})
    if not parsed_bots and "bots" in data: parsed_bots = data.get("bots") if isinstance(data.get("bots"), list) else data.get("bots", {}).get("accounts", [])
    if not isinstance(parsed_bots, list): return jsonify({"ok": False, "error": "Could not parse bots"}), 400
    with _accounts_lock:
        accounts = load_accounts(); existing = {a["uid"] for a in accounts}; added = 0
        for e in parsed_bots:
            if not isinstance(e, dict): continue
            uid = str(e.get("uid") or e.get("user") or "").strip(); pwd = str(e.get("password") or e.get("pass") or "").strip()
            if not uid or not pwd or uid in existing: continue
            accounts.append({"uid": uid, "password": pwd, "aid": str(e.get("account_id") or e.get("acc_id") or e.get("aid") or uid).strip(), "region": str(e.get("region", "IND")).upper(), "enabled": bool(e.get("enabled", True))}); existing.add(uid); added += 1
        save_accounts(accounts)
    return jsonify({"ok": True, "added": added, "bots": accounts})

@app.route("/api/bots/export", methods=["GET"])
def api_bots_export():
    accounts = load_accounts(); return jsonify({"accounts": [{"uid": a["uid"], "password": a["password"], "account_id": a.get("aid", a["uid"]), "region": a.get("region", "IND"), "enabled": a.get("enabled", True)} for a in accounts]})

@app.route("/api/maps", methods=["GET"])
def api_maps_get():
    maps = load_maps(); cfg = load_config(); start = cfg["start_map"] - 1; end = cfg["end_map"] - 1
    return jsonify({"maps": [{"idx": i, "code": m, "short": m[:40] + "…" if len(m) > 40 else m, "in_range": start <= i <= end, "active": engine_state["running"] and start <= i <= end} for i, m in enumerate(maps)], "start": cfg["start_map"], "end": cfg["end_map"]})

@app.route("/api/maps", methods=["POST"])
def api_maps_add():
    code = str((request.get_json(silent=True) or {}).get("code", "")).strip()
    if not code: return jsonify({"ok": False, "error": "Code required"}), 400
    maps = load_maps(); maps.append(code); save_maps(maps); return jsonify({"ok": True, "maps": maps})

@app.route("/api/maps/<int:idx>", methods=["PUT"])
def api_maps_update(idx):
    code = str((request.get_json(silent=True) or {}).get("code", "")).strip(); maps = load_maps()
    if not code or idx < 0 or idx >= len(maps): return jsonify({"ok": False, "error": "Invalid request"}), 400
    maps[idx] = code; save_maps(maps); return jsonify({"ok": True, "maps": maps})

@app.route("/api/maps/<int:idx>", methods=["DELETE"])
def api_maps_delete(idx):
    maps = load_maps()
    if idx < 0 or idx >= len(maps): return jsonify({"ok": False, "error": "Index out of range"}), 404
    maps.pop(idx); save_maps(maps); return jsonify({"ok": True, "maps": maps})

FRIEND_KEY = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
FRIEND_IV  = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])

def _enc_aes(data_bytes):
    cipher = AES.new(FRIEND_KEY, AES.MODE_CBC, FRIEND_IV)
    return cipher.encrypt(pad(data_bytes, AES.block_size))

def Encrypt_ID(x):
    x = int(x)
    dec = ['80','81','82','83','84','85','86','87','88','89','8a','8b','8c','8d','8e','8f','90','91','92','93','94','95','96','97','98','99','9a','9b','9c','9d','9e','9f','a0','a1','a2','a3','a4','a5','a6','a7','a8','a9','aa','ab','ac','ad','ae','af','b0','b1','b2','b3','b4','b5','b6','b7','b8','b9','ba','bb','bc','bd','be','bf','c0','c1','c2','c3','c4','c5','c6','c7','c8','c9','ca','cb','cc','cd','ce','cf','d0','d1','d2','d3','d4','d5','d6','d7','d8','d9','da','db','dc','dd','de','df','e0','e1','e2','e3','e4','e5','e6','e7','e8','e9','ea','eb','ec','ed','ee','ef','f0','f1','f2','f3','f4','f5','f6','f7','f8','f9','fa','fb','fc','fd','fe','ff']
    xxx = ['1','01','02','03','04','05','06','07','08','09','0a','0b','0c','0d','0e','0f','10','11','12','13','14','15','16','17','18','19','1a','1b','1c','1d','1e','1f','20','21','22','23','24','25','26','27','28','29','2a','2b','2c','2d','2e','2f','30','31','32','33','34','35','36','37','38','39','3a','3b','3c','3d','3e','3f','40','41','42','43','44','45','46','47','48','49','4a','4b','4c','4d','4e','4f','50','51','52','53','54','55','56','57','58','59','5a','5b','5c','5d','5e','5f','60','61','62','63','64','65','66','67','68','69','6a','6b','6c','6d','6e','6f','70','71','72','73','74','75','76','77','78','79','7a','7b','7c','7d','7e','7f']
    x = x / 128
    if x > 128:
        x = x / 128
        if x > 128:
            x = x / 128
            if x > 128:
                x = x / 128
                strx = int(x)
                y = (x - int(strx)) * 128
                stry = str(int(y))
                z = (y - int(stry)) * 128
                strz = str(int(z))
                n = (z - int(strz)) * 128
                strn = str(int(n))
                m = (n - int(strn)) * 128
                return dec[int(m)] + dec[int(n)] + dec[int(z)] + dec[int(y)] + xxx[int(x)]
            else:
                strx = int(x)
                y = (x - int(strx)) * 128
                stry = str(int(y))
                z = (y - int(stry)) * 128
                strz = str(int(z))
                n = (z - int(strz)) * 128
                strn = str(int(n))
                return dec[int(n)] + dec[int(z)] + dec[int(y)] + xxx[int(x)]
        else:
            strx = int(x)
            y = (x - int(strx)) * 128
            stry = str(int(y))
            z = (y - int(stry)) * 128
            strz = str(int(z))
            return dec[int(z)] + dec[int(y)] + xxx[int(x)]
    else:
        strx = int(x)
        if strx == 0:
            y = (x - int(strx)) * 128
            return xxx[int(y)]
        else:
            y = (x - int(strx)) * 128
            return dec[int(y)] + xxx[int(x)]

def send_add_friend(target_uid, token):
    try:
        parts = token.split(".")
        p2 = parts[1] + "=" * (-len(parts[1]) % 4)
        payload_data = json.loads(base64.urlsafe_b64decode(p2))
        region = str(payload_data.get("lock_region") or "IND").upper()
    except:
        region = "IND"

    if region == "IND": base_url = "https://client.ind.freefiremobile.com/"
    elif region in {"BR", "US", "SAC", "NA"}: base_url = "https://client.us.freefiremobile.com/"
    else: base_url = "https://clientbp.ppmainecoonghj.com/"

    encrypted_id = Encrypt_ID(target_uid)
    payload = f"08a7c4839f1e10{encrypted_id}1801"
    enc = _enc_aes(bytes.fromhex(payload))
    
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Unity-Version": "2018.4.12f1",
        "X-GA": "v1 1",
        "ReleaseVersion": "OB55",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Dalvik/2.1.0 (Linux; Android 9)",
    }
    try:
        r = requests.post(base_url + "RequestAddingFriend", headers=headers, data=enc, timeout=15, verify=False)
        return (True, "Success") if r.status_code == 200 else (False, f"HTTP {r.status_code}")
    except Exception as e:
        return False, str(e)

@app.route("/api/social/add-friend", methods=["POST"])
def api_social_add_friend():
    data = request.get_json(silent=True) or {}
    target = str(data.get("target", "")).strip()
    limit = int(data.get("limit", 0))
    
    if not target.isdigit():
        return jsonify({"ok": False, "error": "Invalid Target UID"}), 400
        
    jwt_cache = load_json(JWT_FILE, {})
    valid_jwts = [d["jwt"] for d in jwt_cache.values() if "jwt" in d]
            
    if not valid_jwts:
        return jsonify({"ok": False, "error": "No JWTs found. Login bots first."}), 400
        
    if limit > 0:
        valid_jwts = valid_jwts[:limit]
        
    def _worker():
        ok, fail = 0, 0
        for token in valid_jwts:
            success, msg = send_add_friend(target, token)
            if success: 
                ok += 1
                vn.STATE["total_follows"] += 1
            else: 
                fail += 1
            time.sleep(0.8)
            
        log_social_action("ALL", "BATCH", "Friend Request", target, f"Success: {ok} | Fail: {fail}")
        
    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({"ok": True, "message": f"Sending requests from {len(valid_jwts)} bots to {target}..."})

@app.route("/api/social/bulk-follow", methods=["POST"])
def api_social_bulk_follow():
    target = str((request.get_json(silent=True) or {}).get("target", "")).strip()
    if not target: return jsonify({"ok": False, "error": "target required"}), 400
    vn.STATE["current_target"] = target; accounts = [a for a in load_accounts() if a.get("enabled", True)]
    if not accounts: return jsonify({"ok": False, "error": "no enabled bots"}), 400
    run_async_job(vn.run_batch_social_actions([{"uid": a["uid"], "password": a["password"], "aid": a.get("aid", a["uid"])} for a in accounts], "Follow", target))
    log_social_action("ALL", "BATCH", "Bulk Follow", target, "Sent"); return jsonify({"ok": True, "message": f"Bulk follow sent ({len(accounts)} bots)"})

@app.route("/api/social/bulk-subscribe", methods=["POST"])
def api_social_bulk_subscribe():
    target = str((request.get_json(silent=True) or {}).get("target", "")).strip()
    if not target: return jsonify({"ok": False, "error": "target required"}), 400
    accounts = [a for a in load_accounts() if a.get("enabled", True)]
    if not accounts: return jsonify({"ok": False, "error": "no enabled bots"}), 400
    run_async_job(vn.run_batch_social_actions([{"uid": a["uid"], "password": a["password"], "aid": a.get("aid", a["uid"])} for a in accounts], "Subscribe", target))
    log_social_action("ALL", "BATCH", "Bulk Subscribe", target, "Sent"); return jsonify({"ok": True, "message": f"Bulk subscribe sent ({len(accounts)} bots)"})

@app.route("/api/jwt", methods=["GET"])
def api_jwt(): return jsonify(jwt_summary())

@app.route("/api/jwt/clear", methods=["POST"])
def api_jwt_clear():
    save_json(JWT_FILE, {})
    return jsonify({"ok": True, "message": "All JWT cleared"})

@app.route("/api/jwt/refresh", methods=["POST"])
def api_jwt_refresh():
    uid = str((request.get_json(silent=True) or {}).get("uid", "")).strip(); accounts = load_accounts(); targets = [a for a in accounts if a["uid"] == uid] if uid else accounts
    if not targets: return jsonify({"ok": False, "error": "bot not found"}), 404
    def _worker():
        for acc in targets:
            try: vn.save_jwt_cache(acc["uid"], vn.login(acc["uid"], acc["password"]))
            except Exception: pass
    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({"ok": True, "message": f"Refreshing {len(targets)} JWT(s)"})

@app.route("/api/config", methods=["GET"])
def api_config_get(): return jsonify(load_config())

@app.route("/api/config", methods=["POST"])
def api_config_set():
    data = request.get_json(silent=True) or {}; cfg = load_config()
    for k in DEFAULT_CONFIG:
        if k in data: cfg[k] = data[k]
    save_config(cfg)
    if not engine_state["running"]: vn.SEQ_MODE = bool(cfg["seq_mode"]); vn.START_MAP_IDX = int(cfg["start_map"]); vn.END_MAP_IDX = int(cfg["end_map"]); vn.RESUME_MODE = bool(cfg["resume"]); vn.AUTO_SUBSCRIBE_ENABLED = bool(cfg["auto_subscribe"])
    return jsonify({"ok": True, "config": cfg})

@app.route("/api/logs/clear", methods=["POST"])
def api_logs_clear(): vn.STATE["logs"].clear(); vn.STATE["error_logs"].clear(); return jsonify({"ok": True})

if __name__ == "__main__":
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 RAJA ENGINE Web UI running on http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
