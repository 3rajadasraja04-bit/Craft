#!/usr/bin/env python3
"""
RAJA ULTRA ENGINE — WebSocket Server (V6 - Final Social Fix)
"""
import asyncio
import csv
import io
import json
import os
import sys
import time
import math
import struct
import secrets
import socket
import random
import base64
import re
import ssl
from collections import deque
from datetime import datetime, timedelta

import aiohttp
import requests
import urllib3
from aiohttp import web, WSMsgType
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from constants import (
    MATCH_OPCODE, PRELOAD_HEX, CONNECT_TIMEOUT, PRELOAD_WAIT, MATCH_WAIT,
    TCP_HEARTBEAT, PING_INTERVAL, IDLE_INTERVAL, ACK_FORCE_AFTER,
    DEFAULT_MASK, PING_TICK_BASE, AES_KEY, AES_IV
)
from protocol import (
    _ts, aes_enc, udp_dec, decode_header, strip_tea_pad, parse_player_sync,
    find_udp_server, _resolve_mask, login, tcp_frame, tcp_heartbeat,
    FrameReader, region_byte, resolve_host, parse_proto, _first, to_int, to_str,
    _fv, _fb, _fs, pkt_hello, pkt_ack, pkt_auth3043, pkt_auth448,
    pkt_ping, pkt_idle, build_match_start, parse_map_code, build_pkt,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_MAP_CODES = [
    "FREEFIREB8F45D361E2734E681D1399BFFE521BEQ540", "FREEFIREEBC3E906B050092F6792FE2A2994D260Q540",
    "FREEFIRE574AA13E8E1F7E6DEDFFD02F6D49E309Q540", "FREEFIRE2FB96754038F3AE595F2B00331586B44Q540",
    "FREEFIREEAAD9F1F165160BFBD64DAFEBC458A2AQ540", "FREEFIREE95C203C8B24478F962A9B2C7CA7EF88Q132",
    "FREEFIRE7CA6FC15EEBC878E7A037A79F54196E5Q132", "FREEFIREEFFFE5674B037E409968E08B1C0C93AAQ132",
    "FREEFIRE3A662FF87EAEDB7380D4C215E4E2D797Q132", "FREEFIREB4EE478E27F2CD689B7EBFE764165EA7Q132",
    "FREEFIREDBEE58254D509335FA3EA195AF239E28Q132", "FREEFIREE97775F56DFFDB7F7A4341EA23B73C0CQ132",
    "FREEFIRE3B2F7F43A89982406399C6B17431EF2BQ132", "FREEFIRE3C7A2846988FBFC9FD92C5474C2E2B9FQ132",
    "FREEFIRE447390A73B8E015A1FB7993DC3BAB896Q132", "FREEFIRE938B26DEDE92BC1BF1427A46501CCD28Q132",
    "FREEFIREE48B0B4F28C032BF2D3BAC3BE09D363CQ132", "FREEFIRE753CB453A176741ECB6B5E85B494CA60Q132",
    "FREEFIRE1625A163CD529BCBE5D6D5893915BFC9Q132", "FREEFIREC52A36AC1D15E751BBA5DEEE0A8B9805Q540",
    "FREEFIRE276B461B84D646ADC78843A191D63BC7Q500", "FREEFIRE7A5E709682C79D94C466DF8ABD846C23Q500",
    "FREEFIRE4A16139F7C50D409B0B32CCB7D361B50Q500", "FREEFIRE0B5A31AD22E5A0AE2302F6CF90F814C9Q500",
]

DATA_DIR = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", ".")
if DATA_DIR != ".":
    try: os.makedirs(DATA_DIR, exist_ok=True)
    except Exception: DATA_DIR = "."

JWT_CACHE_FILE       = os.path.join(DATA_DIR, "jwt.json")
HISTORY_FILE         = os.path.join(DATA_DIR, "bot_history.json")
MAP_STATS_FILE       = os.path.join(DATA_DIR, "map.json")
PROGRESS_SEQ_FILE    = os.path.join(DATA_DIR, "progress_seq.json")
PROGRESS_TIMER_FILE  = os.path.join(DATA_DIR, "progress_timer.json")
SETTINGS_FILE        = os.path.join(DATA_DIR, "raja_settings.json")
MAPS_FILE            = os.path.join(DATA_DIR, "raja_maps.json")
ACCOUNTS_FILE        = os.path.join(DATA_DIR, "accounts.json")

DEFAULT_SETTINGS = {
    "rotation": {"seq_mode": True, "start_map": 1, "end_map": 24, "resume_mode": True, "hold_secs": 60.0, "sync_secs": 10.0, "reconnect_delay": 2.0},
    "engine": {"move_hz": 5.0, "entity_timeout": 30.0, "jwt_refresh_hours": 6},
    "automation": {"auto_restart": True, "max_retries": 3, "panic_enabled": True, "panic_threshold": 50},
    "network": {"proxy_enabled": False, "proxy_url": "", "tcp_timeout": 10.0},
    "alerts": {"discord_webhook": "", "sound_enabled": True, "sound_volume": 0.5, "notify_on_error": True, "notify_on_start": False, "notify_on_map_change": False},
    "ui": {"theme": "cyberpunk", "performance_mode": False, "show_notifications": True},
}

SETTINGS = json.loads(json.dumps(DEFAULT_SETTINGS))
MAP_CODES = list(DEFAULT_MAP_CODES)
ACCOUNTS = []
STATE = {"bots": {}, "logs": deque(maxlen=200), "error_logs": deque(maxlen=200), "total_matches": 0, "total_errors": 0, "start_time": time.time()}
ROTATION = {"maps": MAP_CODES, "hold_secs": 60.0, "sync_secs": 10.0, "start_time": 0.0, "range_start": 0, "range_end": 23}

WS_CLIENTS = set()
WS_LOG_QUEUE = deque(maxlen=2000)
RUNNING = False
STOP_EVENT = asyncio.Event()
BOT_TASKS = {}
PANIC_PENDING = False

PAUSED_BOTS = set()
BOT_MAP_OVERRIDES = {}
EMOTE_IDS = [909000001, 909000002, 909000003, 909000007, 909000013, 909000016, 909000019, 909000025]

SEQ_MODE = True
START_MAP_IDX = 1
END_MAP_IDX = 24
RESUME_MODE = True
JWT_TTL = 6 * 3600
ENTITY_WAIT = 30.0
MOVE_HZ = 5.0
MOVE_INTERVAL = 1.0 / MOVE_HZ
PUNCH_VEL = 390
PUNCH_STEP = PUNCH_VEL / MOVE_HZ

POSE_DASHING = 2
CMD_PLAYER_STATE_UPLOAD = 2001
CMD_EMOTE = 371

class MatchEnd(Exception): pass

# ================= SOCIAL LOGIC (USING REQUESTS) =================
UNITY_VERSION = "2018.4.12f1"
REGION_URLS = {"IND": "https://client.ind.freefiremobile.com/", "BR": "https://client.us.freefiremobile.com/"}

def _enc_varint_sync(n):
    if n < 0: return b''
    out = []
    while True:
        b = n & 0x7F; n >>= 7
        if n: b |= 0x80
        out.append(b)
        if not n: break
    return bytes(out)

def _varint_field_sync(fn, v): return _enc_varint_sync((fn << 3) | 0) + _enc_varint_sync(v)
def _length_field_sync(fn, v):
    d = v.encode() if isinstance(v, str) else v
    return _enc_varint_sync((fn << 3) | 2) + _enc_varint_sync(len(d)) + d

def _build_proto_sync(fields):
    buf = bytearray()
    for f, v in fields.items():
        if isinstance(v, dict): buf.extend(_length_field_sync(f, _build_proto_sync(v)))
        elif isinstance(v, int): buf.extend(_varint_field_sync(f, v))
        elif isinstance(v, (str, bytes)): buf.extend(_length_field_sync(f, v))
    return bytes(buf)

def _build_subscribe_proto(map_code): return _build_proto_sync({1: 1, 2: map_code, 3: 26, 4: "en"})
def _build_follow_proto(target_uid):
    try: 
        return _build_proto_sync({1: int(target_uid), 2: 4})
    except ValueError: 
        return _build_proto_sync({1: target_uid, 2: 4})

def _extract_needed_maps(data):
    off = 0
    while off < len(data):
        try:
            key = shift = 0
            while True:
                b = data[off]; off += 1
                key |= (b & 0x7F) << shift
                if not (b & 0x80): break
                shift += 7
        except Exception:
            break
        fn, wt = key >> 3, key & 7
        try:
            if wt == 0:
                v = shift = 0
                while True:
                    b = data[off]; off += 1
                    v |= (b & 0x7F) << shift
                    if not (b & 0x80): break
                    shift += 7
                if fn == 2: return v
            elif wt == 2:
                ln = shift = 0
                while True:
                    b = data[off]; off += 1
                    ln |= (b & 0x7F) << shift
                    if not (b & 0x80): break
                    shift += 7
                off += ln
            elif wt == 1: off += 8
            elif wt == 5: off += 4
            else: break
        except Exception:
            break
    return 0

async def _do_social_request(jwt, region, action_type, target):
    base_url = REGION_URLS.get(region.upper(), REGION_URLS["IND"])
    
    if action_type == "Subscribe":
        # 🔥 FIX: Strip #FREEFIRE or FREEFIRE from map code
        if target.lower().startswith("#freefire"):
            target = target[9:]
        elif target.lower().startswith("freefire"):
            target = target[8:]
        elif target.startswith("#"):
            target = target[1:]
            
        endpoint = "/SubscribeWorkshopCode"
        proto = _build_subscribe_proto(target)
    else:
        endpoint = "/Follow"
        proto = _build_follow_proto(target)

    encrypted = AES.new(AES_KEY, AES.MODE_CBC, AES_IV).encrypt(pad(proto, 16))
    full_url = base_url + endpoint
    headers = {
        "User-Agent": f"UnityPlayer/{UNITY_VERSION} (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "Accept": "*/*", "Accept-Encoding": "deflate, gzip", "X-Ga-Sv": "1795270504",
        "Authorization": f"Bearer {jwt}", "X-Ga": "v1 1",
        "ReleaseVersion": "OB55", "Content-Type": "application/x-www-form-urlencoded",
        "X-Unity-Version": UNITY_VERSION,
    }

    # 🔥 FIX: Use requests instead of aiohttp to bypass TLS fingerprinting (HTTP 500)
    def _do_post():
        s = requests.Session()
        try:
            return s.post(full_url, headers=headers, data=encrypted, verify=False, timeout=25)
        except Exception:
            return None
        finally:
            s.close()

    try:
        resp = await asyncio.to_thread(_do_post)
        if not resp:
            return False, "Network Timeout/Error"
            
        if resp.status_code == 200:
            body = resp.content
            if b"BR_WORKSHOP_INSUFFICIENT_MAPS" in body:
                needed = _extract_needed_maps(body)
                return False, f"Need {needed} more maps to play"
            if b"BR_WORKSHOP_ALREADY_FOLLOWED" in body:
                return True, "Already Followed/Subscribed"
            if len(body) > 50:
                return True, "Success (Profile Received)"
            return True, "Success"
            
        return False, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, str(e)
# ===============================================================

def _deep_merge(dst, src):
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict): _deep_merge(dst[k], v)
        else: dst[k] = v

def load_settings():
    global SETTINGS
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f: data = json.load(f)
            _deep_merge(SETTINGS, data)
        except Exception: pass

def save_settings():
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f: json.dump(SETTINGS, f, indent=2)
    except Exception: pass

def load_maps():
    global MAP_CODES
    if os.path.exists(MAPS_FILE):
        try:
            with open(MAPS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and data: MAP_CODES = [str(x) for x in data]
        except Exception: pass
    ROTATION["maps"] = MAP_CODES
    ROTATION["range_end"] = max(0, len(MAP_CODES) - 1)

def save_maps():
    try:
        with open(MAPS_FILE, "w", encoding="utf-8") as f: json.dump(MAP_CODES, f, indent=2)
    except Exception: pass

def load_accounts():
    global ACCOUNTS
    if not os.path.exists(ACCOUNTS_FILE):
        ACCOUNTS = []
        return
    try:
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f: data = json.load(f)
        if isinstance(data, dict): data = data.get("accounts", [])
        out = []
        for e in data:
            if not isinstance(e, dict): continue
            uid = str(e.get("uid") or e.get("user") or "").strip()
            pwd = str(e.get("password") or e.get("pass") or "").strip()
            aid = str(e.get("account_id") or e.get("acc_id") or "").strip()
            if uid and pwd: out.append({"uid": uid, "password": pwd, "aid": aid})
        ACCOUNTS = out
    except Exception: ACCOUNTS = []

def save_accounts():
    try:
        with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f: json.dump(ACCOUNTS, f, indent=4)
    except Exception: pass

def add_log(msg, log_type="info"):
    ts = datetime.now().strftime("%H:%M:%S")
    STATE["logs"].append(f"[{ts}] {msg}")
    WS_LOG_QUEUE.append({"msg": str(msg), "type": log_type})

def add_error(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    STATE["error_logs"].append(f"[{ts}] {msg}")
    WS_LOG_QUEUE.append({"msg": str(msg), "type": "error"})
    STATE["total_errors"] += 1

def hdr(t):  add_log(f"⚡ {t}", "phase")
def ok(m):   add_log(f"✅ {m}", "success")
def info(m): add_log(f"💠 {m}", "info")
def warn(m):
    add_log(f"⚠️ {m}", "warn")
    add_error(m)
def err(m):
    add_log(f"❌ {m}", "error")
    add_error(m)

async def broadcast(msg):
    dead = []
    for ws in list(WS_CLIENTS):
        try: await ws.send_json(msg)
        except Exception: dead.append(ws)
    for ws in dead: WS_CLIENTS.discard(ws)

async def broadcast_accounts(): await broadcast({"type": "accounts_file", "accounts": ACCOUNTS})

def _progress_file(is_seq): return PROGRESS_SEQ_FILE if is_seq else PROGRESS_TIMER_FILE

def load_progress(is_seq):
    f = _progress_file(is_seq)
    if not os.path.exists(f): return {}
    try:
        with open(f, "r", encoding="utf-8") as fh: return json.load(fh)
    except Exception: return {}

def save_progress(is_seq, uid, map_idx):
    f = _progress_file(is_seq)
    data = load_progress(is_seq)
    data[str(uid)] = {"map_index": int(map_idx), "saved_at": time.time()}
    try:
        with open(f, "w", encoding="utf-8") as fh: json.dump(data, fh, indent=2)
    except Exception: pass

def get_bot_progress_map(is_seq, uid):
    data = load_progress(is_seq)
    entry = data.get(str(uid))
    if entry and isinstance(entry.get("map_index"), int): return entry["map_index"]
    return None

def load_map_stats():
    if not os.path.exists(MAP_STATS_FILE): return {}
    try:
        with open(MAP_STATS_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return {}

def save_map_stats(stats):
    try:
        with open(MAP_STATS_FILE, "w", encoding="utf-8") as f: json.dump(stats, f, indent=2)
    except Exception: pass

def update_map_stats(map_code, duration):
    if duration is None or duration <= 0: return
    stats = load_map_stats()
    s = stats.get(map_code)
    if not s:
        s = {"sessions": 0, "total_secs": 0.0, "avg_secs": 0.0, "min_secs": duration, "max_secs": duration, "last_duration": duration}
        stats[map_code] = s
    s["sessions"] += 1
    s["total_secs"] += duration
    s["avg_secs"] = s["total_secs"] / s["sessions"]
    s["min_secs"] = min(s["min_secs"], duration)
    s["max_secs"] = max(s["max_secs"], duration)
    s["last_duration"] = duration
    save_map_stats(stats)

def save_history():
    data = {}
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f: data = json.load(f)
        except Exception: pass
    for idx, b in STATE["bots"].items():
        uid = b.get("uid")
        if not uid: continue
        if uid not in data: data[uid] = {"matches_played": 0, "total_time_played": 0}
        data[uid]["matches_played"] = b.get("matches_played", 0)
        data[uid]["total_time_played"] = b.get("total_time_played", 0)
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f: json.dump(data, f, indent=4)
    except Exception: pass

def load_jwt_cache():
    if not os.path.exists(JWT_CACHE_FILE): return {}
    try:
        with open(JWT_CACHE_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return {}

def save_jwt_cache(uid, data):
    cache = load_jwt_cache()
    dc = dict(data)
    if isinstance(dc.get("key"), bytes): dc["key"] = dc["key"].hex()
    if isinstance(dc.get("iv"), bytes): dc["iv"] = dc["iv"].hex()
    now = time.time()
    dc["created_at"] = now
    dc["expires_at"] = now + JWT_TTL
    dc["saved_at"] = now
    cache[str(uid)] = dc
    try:
        with open(JWT_CACHE_FILE, "w", encoding="utf-8") as f: json.dump(cache, f, indent=4)
    except Exception: pass

def get_jwt_data_for_ui():
    cache = load_jwt_cache()
    now = time.time()
    out = []
    for uid, d in cache.items():
        expires_at = d.get("expires_at") or (d.get("saved_at", 0) + JWT_TTL)
        hours_left = max(0.0, (expires_at - now) / 3600.0)
        out.append({"uid": uid, "acc_id": d.get("acc_id", "-"), "expires_at": datetime.fromtimestamp(expires_at).strftime("%Y-%m-%d %H:%M:%S"), "hours_left": round(hours_left, 2), "is_valid": hours_left > 0})
    return out

def sync_config_to_globals():
    global SEQ_MODE, START_MAP_IDX, END_MAP_IDX, RESUME_MODE, JWT_TTL, ENTITY_WAIT, MOVE_HZ, MOVE_INTERVAL, PUNCH_STEP
    r = SETTINGS["rotation"]
    SEQ_MODE = bool(r.get("seq_mode", True))
    START_MAP_IDX = max(1, int(r.get("start_map", 1)))
    END_MAP_IDX = max(START_MAP_IDX, int(r.get("end_map", len(MAP_CODES))))
    RESUME_MODE = bool(r.get("resume_mode", True))
    ROTATION["hold_secs"] = float(r.get("hold_secs", 60.0))
    ROTATION["sync_secs"] = float(r.get("sync_secs", 10.0))
    ROTATION["maps"] = MAP_CODES
    ROTATION["start_time"] = time.time()
    ROTATION["range_start"] = START_MAP_IDX - 1
    ROTATION["range_end"] = min(END_MAP_IDX - 1, len(MAP_CODES) - 1)
    JWT_TTL = int(SETTINGS["engine"].get("jwt_refresh_hours", 6)) * 3600
    ENTITY_WAIT = float(SETTINGS["engine"].get("entity_timeout", 30.0))
    MOVE_HZ = float(SETTINGS["engine"].get("move_hz", 5.0))
    MOVE_INTERVAL = 1.0 / MOVE_HZ if MOVE_HZ > 0 else 0.2
    PUNCH_STEP = PUNCH_VEL / MOVE_HZ if MOVE_HZ > 0 else 78.0

def get_current_map_slot():
    maps = ROTATION["maps"]
    hold = ROTATION["hold_secs"]
    sync = ROTATION.get("sync_secs", 10.0)
    start = ROTATION["start_time"]
    rs = ROTATION.get("range_start", 0)
    re_ = ROTATION.get("range_end", len(maps) - 1)
    if not maps or hold <= 0: return (maps[rs] if maps else "-", rs, hold, "play")
    subset_size = re_ - rs + 1
    if subset_size <= 0: subset_size = 1
    cycle_time = hold + sync
    elapsed = time.time() - start
    slot_rel = int(elapsed // cycle_time) % subset_size
    slot_idx = rs + slot_rel
    into_cycle = elapsed % cycle_time
    if into_cycle < hold: return (maps[slot_idx], slot_idx, hold - into_cycle, "play")
    return (maps[slot_idx], slot_idx, cycle_time - into_cycle, "sync")

def get_display_map():
    playing = [b for b in STATE["bots"].values() if "Playing" in b.get("status", "")]
    if playing:
        m = playing[0].get("map", "?")
        return f"{m[8:12]}..{m[-4:]}" if len(m) > 15 else m
    if SEQ_MODE: return "SEQ-IDLE" if not RUNNING else "SEQ"
    try:
        mc, _, _, _ = get_current_map_slot()
        return f"{mc[8:12]}..{mc[-4:]}" if len(mc) > 15 else mc
    except Exception: return "-"

def get_display_timer():
    if SEQ_MODE: return 0
    try:
        _, _, secs, _ = get_current_map_slot()
        return int(secs)
    except Exception: return 0

def _pv(out, v):
    v = int(v)
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v & 0x7F)

def _pzig(out, v): _pv(out, ((int(v) << 1) ^ (int(v) >> 63)) & 0xFFFFFFFFFFFFFFFF)
def _pfloat(out, f): _pv(out, struct.unpack("<I", struct.pack("<f", f))[0])

def _build_player_state(pid, pos, rot, tdir, vel, aim, ps=0, pp=0, plat=0, ping=0):
    o = bytearray()
    _pv(o, pid)
    for v in [pos[0], pos[1], pos[2], rot[0], rot[1], rot[2], tdir[0], tdir[1], vel[0], vel[1], vel[2]]: _pzig(o, v)
    o.append(ps & 0xFF)
    o.append(pp & 0xFF)
    _pfloat(o, aim[0]); _pfloat(o, aim[1]); _pfloat(o, aim[2])
    _pv(o, plat); o.append(0); _pv(o, ping); o.append(0); _pv(o, 0); o.append(0)
    return bytes(o)

def _build_c2s_state(pid, pos, rot, tdir, vel, aim, server_time, ps=0, pp=0, ping=0):
    o = bytearray(_build_player_state(pid, pos, rot, tdir, vel, aim, ps=ps, pp=pp, ping=ping))
    _pv(o, server_time & 0xFFFFFFFF)
    return bytes(o)

def build_state_packet(body, key, mask, order_id=0):
    return build_pkt(CMD_PLAYER_STATE_UPLOAD, body, key, mask, reliable=False, order_id=order_id, send_option=0, flags=1, msg_key=0x6A)

def build_emote_packet(key, mask):
    emote_id = random.choice(EMOTE_IDS)
    b = bytearray()
    _pv(b, emote_id); b += b"\x00\x00\x00"; _pv(b, 0xFFFFFFFF); _pv(b, int(time.time() * 1000) & 0xFFFFFFFF); _pv(b, 0); _pzig(b, 0); b += b"\x00\x00\x00"
    return build_pkt(CMD_EMOTE, bytes(b), key, mask, reliable=True, order_id=0, send_option=1, flags=1, msg_key=0x6A)

class BotBrain:
    def __init__(self, pid, spawn=None):
        self.pid = pid; self.my_pos = spawn; self.yaw = 0.0; self.random_goal = None; self._last_sent_pos = spawn; self.joined_at = time.time()
        self.pause_until = 0; self.turn_until = 0

    def build_next_state(self, now):
        if not self.my_pos: return None
        mx, my, mz = self.my_pos
        if now < self.pause_until:
            return _build_c2s_state(self.pid, (int(mx), int(my), int(mz)), (0,0,0), (0,0), (0,0,0), (0.0,0.0,0.0), int((now - self.joined_at)*1000)&0xFFFFFFFF, ps=0, pp=0, ping=90)
        if random.random() < 0.03:
            if random.random() < 0.5: self.pause_until = now + random.uniform(2, 4)
            else: self.turn_until = now + random.uniform(1, 2)
        if now < self.turn_until:
            self.yaw += random.uniform(-45, 45)
            rad = math.radians(self.yaw)
            fx, fz = math.sin(rad), math.cos(rad)
            hdg = ((self.yaw + 180.0) % 360.0) - 180.0
            return _build_c2s_state(self.pid, (int(mx), int(my), int(mz)), (int(hdg*100),0,0), (int(fx*100),int(fz*100)), (0,0,0), (fx,0.0,fz), int((now - self.joined_at)*1000)&0xFFFFFFFF, ps=0, pp=POSE_DASHING, ping=90)
        if (not self.random_goal or math.hypot(self.random_goal[0] - mx, self.random_goal[1] - mz) < 100): self.random_goal = (mx + random.uniform(-500, 500), mz + random.uniform(-500, 500))
        gx, gz = self.random_goal
        dx, dz = gx - mx, gz - mz
        d = math.hypot(dx, dz)
        if d > 1:
            ux, uz = dx / d, dz / d
            step = min(PUNCH_STEP, d)
            self.yaw = math.degrees(math.atan2(ux, uz))
            new = (mx + ux * step, my, mz + uz * step)
        else: new = (mx, my, mz)
        old = self._last_sent_pos or self.my_pos
        nx, ny, nz = int(new[0]), int(new[1]), int(new[2])
        ox, oy, oz = int(old[0]), int(old[1]), int(old[2])
        dx, dz = nx - ox, nz - oz
        dist = math.hypot(dx, dz)
        if dist >= 1:
            ux, uz = dx / dist, dz / dist
            vel = (int(ux * PUNCH_VEL), 0, int(uz * PUNCH_VEL))
            hdg = math.degrees(math.atan2(ux, uz))
            hdg = ((hdg + 180.0) % 360.0) - 180.0
            rot = (int(hdg * 100), 0, 0)
            tdir = (int(ux * 100), int(uz * 100))
            aim = (ux, 0.0, uz)
        else:
            vel = (0, 0, 0)
            rad = math.radians(self.yaw)
            fx, fz = math.sin(rad), math.cos(rad)
            hdg = ((self.yaw + 180.0) % 360.0) - 180.0
            rot = (int(hdg * 100), 0, 0)
            tdir = (int(fx * 100), int(fz * 100))
            aim = (fx, 0.0, fz)
        self.my_pos = new
        self._last_sent_pos = new
        return _build_c2s_state(self.pid, (nx, ny, nz), rot, tdir, vel, aim, int((now - self.joined_at) * 1000.0) & 0xFFFFFFFF, ps=0, pp=POSE_DASHING, ping=90)

def phase1_build_match_start(map_code):
    hdr("PHASE ① MATCH-START")
    try: parse_map_code(map_code)
    except ValueError as e:
        err(f"map fail: {e}")
        raise
    return build_match_start(map_code)

async def phase2_send_and_wait(uid, pwd, raw_match_start, bot_idx):
    hdr("PHASE ② TCP LOGIN")
    STATE["bots"][bot_idx]["status"] = "Logging in"
    cache = load_jwt_cache()
    s = cache.get(str(uid))
    is_valid = False
    if s:
        if "expires_at" in s: is_valid = time.time() < s["expires_at"]
        elif "saved_at" in s: is_valid = (time.time() - s["saved_at"]) < JWT_TTL
    if is_valid:
        if isinstance(s.get("key"), str): s["key"] = bytes.fromhex(s["key"])
        if isinstance(s.get("iv"), str): s["iv"] = bytes.fromhex(s["iv"])
    else: s = None

    if not s or int(s.get("acc_id", 0)) == 0:
        info(f"Bot {bot_idx} JWT expired/missing. Re-logging in...")
        try:
            s = await asyncio.to_thread(login, uid, pwd)
            save_jwt_cache(uid, s)
            ok(f"Bot {bot_idx} JWT regenerated!")
        except Exception as e:
            err(f"Bot {bot_idx} login fail: {e}")
            return None

    try: reader, writer = await asyncio.wait_for(asyncio.open_connection(s["host"], int(s["port"])), timeout=CONNECT_TIMEOUT)
    except Exception as e:
        err(f"Bot {bot_idx} TCP fail: {e}")
        return None

    fr = FrameReader(reader)
    stop = asyncio.Event()
    bt = None
    try:
        enc = aes_enc(s["jwt"].encode(), s["key"], s["iv"])
        auth = (bytes([0x83, region_byte(s["region"])]) + int(s["acc_id"]).to_bytes(8, "big") + int(s["kts"]).to_bytes(4, "big") + bytes(4) + len(enc).to_bytes(4, "big") + enc)
        writer.write(auth)
        await writer.drain()
        t0 = time.time()
        while time.time() - t0 < 3.0:
            f = await fr.next_frame(timeout=0.5)
            if f: pass
            elif reader.at_eof(): return None
        c = (_fv(1, 6) + _fb(2, _fv(1, 3) + _fs(2, "2019121269") + _fv(3, 2) + _fv(4, 1)))
        writer.write(tcp_frame("1d14", c, s["key"], s["iv"]))
        await writer.drain()
        await asyncio.sleep(0.4)
        writer.write(tcp_frame(MATCH_OPCODE, bytes.fromhex(PRELOAD_HEX), s["key"], s["iv"]))
        await writer.drain()
        pre_deadline = time.time() + PRELOAD_WAIT
        while time.time() < pre_deadline:
            f = await fr.next_frame(timeout=0.5)
            if not f: continue
            op, payload = f
            if op.hex() == "0e00" and to_int(_first(parse_proto(payload), 4)) == 32: break
        writer.write(tcp_frame(MATCH_OPCODE, raw_match_start, s["key"], s["iv"]))
        await writer.drain()
        bt = asyncio.create_task(tcp_heartbeat(writer, stop, s, TCP_HEARTBEAT))
        deadline = time.time() + MATCH_WAIT
        while time.time() < deadline:
            f = await fr.next_frame(timeout=1.0)
            if not f: continue
            op, payload = f
            if op.hex() == "0e00":
                tree = parse_proto(payload)
                if to_int(_first(tree, 4)) == 16:
                    inner = _first(tree, 5)
                    if isinstance(inner, (bytes, bytearray)): inner = parse_proto(bytes(inner))
                    if isinstance(inner, dict):
                        udp = to_str(_first(inner, 2))
                        if udp and ":" in udp:
                            return {"writer": writer, "fr": fr, "stop": stop, "session": s, "udp_server": udp, "udp_key": to_str(_first(inner, 3)), "match_id": to_int(_first(inner, 1)), "account_id": int(s["acc_id"]), "room_code": to_str(_first(inner, 19)), "room_name": to_str(_first(inner, 20)), "mask": _resolve_mask(to_str(_first(inner, 42)))}
                udp = find_udp_server(tree)
                if udp:
                    return {"writer": writer, "fr": fr, "stop": stop, "session": s, "udp_server": udp, "udp_key": "", "match_id": 0, "account_id": int(s["acc_id"]), "room_code": "", "room_name": "", "mask": DEFAULT_MASK}
        err(f"Bot {bot_idx} no UDP server")
        return None
    except Exception as e:
        err(f"Bot {bot_idx} TCP Auth/Preload: {e}")
        return None
    finally:
        if bt and not bt.done():
            stop.set()
            bt.cancel()

async def phase3_start_udp(match, duration, bot_idx, map_code):
    hdr("PHASE ③ UDP JOIN")
    STATE["bots"][bot_idx]["status"] = "Joining UDP"
    udp_server = match["udp_server"]
    key_hex = match["udp_key"]
    mask = match["mask"] or DEFAULT_MASK
    try: key = bytes.fromhex(key_hex)
    except Exception: return None
    if len(key) != 16: return None
    if not (isinstance(mask, str) and len(mask) == 5 and sorted(mask) == list("01234")): mask = DEFAULT_MASK
    try:
        host, port = udp_server.rsplit(":", 1)
        port = int(port)
    except Exception: return None
    ip = await resolve_host(host)
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    try: sock.connect((ip, port))
    except Exception:
        sock.close()
        return None

    recv_orders = set()
    ack_next = [0]
    stop_udp = asyncio.Event()
    t_448 = [time.time()]
    cmd176_seen = [False]
    brain = None
    brain_task = None
    my_entity = {"id": None, "spawn": (0.0, 0.0, 0.0)}
    timing = {"entity": None, "ended": False}
    result = {"duration": None}
    tasks = []

    async def tx(pkt, label=""):
        try: await loop.sock_sendall(sock, pkt)
        except OSError: pass

    async def send_ack(n): await tx(pkt_ack(n, key, mask, msg_key=0x68), f"ACK next={n}")

    def body_plain(h, pkt):
        off = h["header_len"] + h.get("extra", 0)
        b = pkt[off:off + h["length"]]
        if not b: return b
        if h["flags"] & 1:
            pl = udp_dec(b, key)
            stripped = strip_tea_pad(pl)
            if h["cmd"] == 176 and len(stripped) < 10: return pl
            return stripped
        return b

    async def handle(data):
        nonlocal brain, brain_task
        h = decode_header(data, key, mask)
        if not h: return None
        if h["cmd"] in (4, 103):
            timing["ended"] = True
            raise MatchEnd()
        if h["cmd"] == 176 and not cmd176_seen[0]:
            plain = body_plain(h, data)
            eid, aid, pos = parse_player_sync(plain)
            if eid == 0:
                try:
                    eid = struct.unpack("<I", plain[:4])[0]
                    pos = (0.0, 0.0, 0.0)
                except Exception: pass
            if eid != 0:
                cmd176_seen[0] = True
                my_entity["id"] = eid
                my_entity["spawn"] = pos
                STATE["bots"][bot_idx]["entity"] = eid
                STATE["bots"][bot_idx]["status"] = "Playing"
                timing["entity"] = time.time()
                ok(f"Bot {bot_idx} Entity: {eid}")
                if brain is None:
                    brain = BotBrain(eid, spawn=pos)
                    brain_task = asyncio.create_task(_brain_loop(brain, key, mask, sock, loop, stop_udp))
        if h.get("orderId") is not None and h["cmd"] != 2:
            recv_orders.add(h["orderId"])
            while ack_next[0] in recv_orders: ack_next[0] += 1
            await send_ack(ack_next[0])

    try:
        suffix = str(1000 + secrets.randbelow(9000))
        await tx(pkt_hello(str(match["account_id"]), suffix, key, mask), "HELLO")
        await asyncio.sleep(1.0)
        await tx(pkt_auth3043(1, match["account_id"], match["match_id"], key, mask, order_id=0, msg_key=0x60), "AUTH-3043 #1")
        await tx(pkt_auth3043(2, match["account_id"], match["match_id"], key, mask, order_id=1, msg_key=0x5E), "AUTH-3043 #2")
        await asyncio.sleep(1.0)
        t_448[0] = time.time()
        await tx(pkt_auth448(match["account_id"], match["match_id"], match["room_code"], match["room_name"], key, mask, order_id=2, msg_key=0x60), "AUTH-448")

        ent_deadline = time.time() + ENTITY_WAIT
        while time.time() < ent_deadline and not cmd176_seen[0]:
            if STOP_EVENT.is_set(): break
            try: data = await asyncio.wait_for(loop.sock_recv(sock, 65536), timeout=0.4)
            except asyncio.TimeoutError: continue
            if data: await handle(data)
        if not cmd176_seen[0]: warn(f"Bot {bot_idx} Entity timeout")

        if duration > 0 or SEQ_MODE:
            tasks = [asyncio.create_task(_udp_ping(key, mask, sock, loop, stop_udp, t_448)), asyncio.create_task(_udp_idle(key, mask, sock, loop, stop_udp))]
            t0 = time.time()
            while True:
                if STOP_EVENT.is_set() or bot_idx in PAUSED_BOTS: break
                if not SEQ_MODE and duration > 0 and (time.time() - t0 >= duration): break
                STATE["bots"][bot_idx]["time_in_match"] = time.time() - t0
                try: data = await asyncio.wait_for(loop.sock_recv(sock, 65536), timeout=0.5)
                except asyncio.TimeoutError: continue
                if data: await handle(data)
    except MatchEnd: add_log(f"Bot {bot_idx} SERVER END (103)", "phase")
    except asyncio.CancelledError: add_log(f"Bot {bot_idx} cancelled", "info")
    except Exception as e: err(f"Bot {bot_idx} UDP Error: {e}")
    finally:
        stop_udp.set()
        if brain_task: brain_task.cancel()
        for t in tasks: t.cancel()
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
        try: sock.close()
        except Exception: pass
        if timing["entity"] and timing["ended"]:
            actual = time.time() - timing["entity"]
            update_map_stats(map_code, actual)
            result["duration"] = actual
            ok(f"Map {map_code[8:12]} actual: {actual:.1f}s")
    return result["duration"]

async def _brain_loop(brain, key, mask, sock, loop, stop_udp):
    last_emote = time.time()
    order = 0
    while not stop_udp.is_set() and not STOP_EVENT.is_set():
        try:
            now = time.time()
            body = brain.build_next_state(now)
            if body:
                pkt = build_state_packet(body, key, mask, order_id=order)
                await loop.sock_sendall(sock, pkt)
                order = (order + 1) & 0xFFFF
            if now - last_emote > EMOTE_EVERY:
                await loop.sock_sendall(sock, build_emote_packet(key, mask))
                last_emote = now
        except Exception: break
        try:
            await asyncio.wait_for(stop_udp.wait(), timeout=MOVE_INTERVAL)
            break
        except asyncio.TimeoutError: pass

async def _udp_ping(key, mask, sock, loop, stop_udp, t_448):
    while not stop_udp.is_set() and not STOP_EVENT.is_set():
        tick = (PING_TICK_BASE - 2 * int((time.time() - t_448[0]) * 1000)) & 0xFFFFFFFF
        try: await loop.sock_sendall(sock, pkt_ping(tick, key, mask))
        except Exception: break
        try:
            await asyncio.wait_for(stop_udp.wait(), timeout=PING_INTERVAL)
            break
        except asyncio.TimeoutError: pass

async def _udp_idle(key, mask, sock, loop, stop_udp):
    while not stop_udp.is_set() and not STOP_EVENT.is_set():
        try: await loop.sock_sendall(sock, pkt_idle(key, mask))
        except Exception: break
        try:
            await asyncio.wait_for(stop_udp.wait(), timeout=IDLE_INTERVAL)
            break
        except asyncio.TimeoutError: pass

async def run(uid, pwd, map_code, duration, bot_idx):
    if STOP_EVENT.is_set(): return None
    try: raw = phase1_build_match_start(map_code)
    except Exception: return None
    STATE["bots"][bot_idx]["map"] = map_code
    STATE["bots"][bot_idx]["time_in_match"] = 0
    match = await phase2_send_and_wait(uid, pwd, raw, bot_idx)
    if not match: return None
    
    if len(map_code) >= 36 and "FREEFIRE" in map_code:
        jwt = match["session"]["jwt"]
        region = match["session"]["region"]
        success, msg = await _do_social_request(jwt, region, "Subscribe", map_code)
        if success:
            ok(f"Bot {bot_idx} Subscribed to Map: {map_code[8:12]}")
        else:
            info(f"Bot {bot_idx} Subscribe skipped: {msg}")

    try:
        actual = await phase3_start_udp(match, duration, bot_idx, map_code)
        STATE["total_matches"] += 1
        STATE["bots"][bot_idx]["matches_played"] = STATE["bots"][bot_idx].get("matches_played", 0) + 1
        STATE["bots"][bot_idx]["total_time_played"] = (STATE["bots"][bot_idx].get("total_time_played", 0) + (actual if actual else duration))
        save_history()
        return actual
    finally:
        STATE["bots"][bot_idx]["status"] = "Cooldown"
        match["stop"].set()
        try:
            match["writer"].close()
            await match["writer"].wait_closed()
        except Exception: pass

async def loop_matches(idx, total, uid, pwd, aid, rejoin_delay):
    STATE["bots"][idx] = {"uid": uid, "aid": aid, "status": "Booting", "map": "-", "entity": "-", "matches_played": 0, "time_in_match": 0, "total_time_played": 0}
    start_idx_0 = START_MAP_IDX - 1
    end_idx_0 = min(END_MAP_IDX - 1, len(MAP_CODES) - 1)
    if end_idx_0 < start_idx_0: end_idx_0 = start_idx_0
    bot_map_idx = start_idx_0
    if RESUME_MODE:
        saved = get_bot_progress_map(SEQ_MODE, uid)
        if saved is not None and start_idx_0 <= saved <= end_idx_0:
            bot_map_idx = saved
            ok(f"Bot {idx} 💾 RESUME at map #{saved+1}")
    retries = 0
    while not STOP_EVENT.is_set():
        if idx in PAUSED_BOTS:
            STATE["bots"][idx]["status"] = "Paused ⏸️"
            await asyncio.sleep(2)
            continue
            
        if idx in BOT_MAP_OVERRIDES:
            override_map = BOT_MAP_OVERRIDES[idx]
            if override_map in MAP_CODES:
                bot_map_idx = MAP_CODES.index(override_map)

        if SEQ_MODE:
            map_code = MAP_CODES[bot_map_idx]
            add_log(f"Bot {idx} -> SEQ Map #{bot_map_idx+1}", "phase")
            save_progress(True, uid, bot_map_idx)
            try:
                await run(uid, pwd, map_code, 0, idx)
                retries = 0
            except asyncio.CancelledError: break
            except Exception as e:
                STATE["bots"][idx]["status"] = "Error ❌"
                add_error(f"Bot {idx} crash: {e}")
                retries += 1
                if not SETTINGS["automation"].get("auto_restart", True):
                    STATE["bots"][idx]["status"] = "Failed"
                    break
                if retries > SETTINGS["automation"].get("max_retries", 3):
                    STATE["bots"][idx]["status"] = "Failed"
                    add_error(f"Bot {idx} max retries exceeded")
                    break
            await asyncio.sleep(rejoin_delay)
            if idx not in BOT_MAP_OVERRIDES: 
                bot_map_idx += 1
                if bot_map_idx > end_idx_0: bot_map_idx = start_idx_0
            continue
            
        map_code, map_idx, secs_left, phase = get_current_map_slot()
        if phase == "sync":
            STATE["bots"][idx]["status"] = "Wait Sync"
            await asyncio.sleep(secs_left + 0.5)
            continue
        if secs_left < 15.0:
            STATE["bots"][idx]["status"] = "Next Map"
            await asyncio.sleep(secs_left + 0.5)
            continue
        try:
            await run(uid, pwd, map_code, secs_left, idx)
            retries = 0
        except asyncio.CancelledError: break
        except Exception as e:
            STATE["bots"][idx]["status"] = "Error ❌"
            add_error(f"Bot {idx} crash: {e}")
            retries += 1
            if retries > SETTINGS["automation"].get("max_retries", 3):
                STATE["bots"][idx]["status"] = "Failed"
                break
        await asyncio.sleep(rejoin_delay)

async def start_all_bots():
    global RUNNING, PANIC_PENDING
    if RUNNING:
        add_log("Already running", "warn")
        return
    if not ACCOUNTS:
        add_log("No accounts loaded", "error")
        await broadcast({"type": "state_update", "bots": [], "current_map": "-", "timer": 0, "total_matches": STATE["total_matches"], "log": {"msg": "❌ No accounts loaded", "type": "error"}})
        return
    RUNNING = True
    STOP_EVENT.clear()
    PAUSED_BOTS.clear()
    PANIC_PENDING = False
    STATE["bots"].clear()
    STATE["total_matches"] = 0
    sync_config_to_globals()
    rejoin_delay = float(SETTINGS["rotation"].get("reconnect_delay", 2.0))
    for i, acc in enumerate(ACCOUNTS, 1):
        BOT_TASKS[i] = asyncio.create_task(loop_matches(i, len(ACCOUNTS), acc["uid"], acc["password"], acc.get("aid", ""), rejoin_delay))
    add_log(f"🚀 STARTED {len(ACCOUNTS)} bots", "success")

async def stop_all_bots(panic=False):
    global RUNNING, PANIC_PENDING
    STOP_EVENT.set()
    RUNNING = False
    for idx in list(BOT_TASKS.keys()):
        task = BOT_TASKS.get(idx)
        if task and not task.done(): task.cancel()
    BOT_TASKS.clear()
    for idx in STATE["bots"]: STATE["bots"][idx]["status"] = "Stopped"
    add_log("🛑 All bots stopped" + (" (PANIC)" if panic else ""), "warn")
    if panic: PANIC_PENDING = True

async def handle_command(ws, action, payload):
    try:
        if action == "START_ALL": await start_all_bots()
        elif action == "STOP_ALL": await stop_all_bots(False)
        elif action == "PANIC_STOP":
            if SETTINGS["automation"].get("panic_enabled", True): await stop_all_bots(True)
        elif action == "STOP_BOT":
            if isinstance(payload, dict) and "id" in payload:
                bot_id = int(payload["id"])
                if bot_id in BOT_TASKS:
                    task = BOT_TASKS[bot_id]
                    if task and not task.done(): task.cancel()
                    del BOT_TASKS[bot_id]
                    if bot_id in STATE["bots"]: STATE["bots"][bot_id]["status"] = "Stopped ⏹️"
                    add_log(f"🛑 Bot {bot_id} stopped manually", "warn")
        elif action == "PAUSE_BOT":
            if isinstance(payload, dict) and "id" in payload:
                PAUSED_BOTS.add(int(payload["id"]))
                add_log(f"⏸️ Bot {payload['id']} paused", "warn")
        elif action == "RESUME_BOT":
            if isinstance(payload, dict) and "id" in payload:
                PAUSED_BOTS.discard(int(payload["id"]))
                add_log(f"▶️ Bot {payload['id']} resumed", "success")
        elif action == "SET_BOT_MAP":
            if isinstance(payload, dict) and "id" in payload and "map" in payload:
                BOT_MAP_OVERRIDES[int(payload["id"])] = payload["map"]
                add_log(f"🗺️ Bot {payload['id']} map overridden to {payload['map'][8:12]}", "info")
        elif action == "REORDER_MAPS":
            if isinstance(payload, list):
                MAP_CODES[:] = payload
                save_maps()
                await broadcast({"type": "maps", "maps": MAP_CODES})

        # 🔥 MANUAL SOCIAL COMMANDS 🔥
        elif action == "MANUAL_SUBSCRIBE":
            if isinstance(payload, dict) and "map_code" in payload:
                map_code = payload["map_code"].strip()
                add_log(f"🔄 Sending Subscribe requests for map: {map_code[8:12] if len(map_code)>12 else map_code}", "info")
                cache = load_jwt_cache()
                success_count = 0
                for acc in ACCOUNTS:
                    uid = acc["uid"]
                    entry = cache.get(uid)
                    if entry and entry.get("expires_at", 0) > time.time():
                        res, msg = await _do_social_request(entry["jwt"], entry.get("region", "IND"), "Subscribe", map_code)
                        if res: 
                            success_count += 1
                        else:
                            add_error(f"Bot {uid} Subscribe failed: {msg}")
                add_log(f"✅ Subscribe finished. Success: {success_count}/{len(ACCOUNTS)}", "success")

        elif action == "MANUAL_FOLLOW":
            if isinstance(payload, dict) and "target_uid" in payload:
                target_uid = payload["target_uid"].strip()
                add_log(f"🔄 Sending Follow requests to UID: {target_uid}", "info")
                cache = load_jwt_cache()
                success_count = 0
                for acc in ACCOUNTS:
                    uid = acc["uid"]
                    entry = cache.get(uid)
                    if entry and entry.get("expires_at", 0) > time.time():
                        res, msg = await _do_social_request(entry["jwt"], entry.get("region", "IND"), "Follow", target_uid)
                        if res: 
                            success_count += 1
                        else:
                            add_error(f"Bot {uid} Follow failed: {msg}")
                add_log(f"✅ Follow finished. Success: {success_count}/{len(ACCOUNTS)}", "success")

        elif action == "GET_ACCOUNTS": await ws.send_json({"type": "accounts_file", "accounts": ACCOUNTS})
        elif action == "REPLACE_BOTS":
            if isinstance(payload, list):
                out = []
                for e in payload:
                    if not isinstance(e, dict): continue
                    uid = str(e.get("uid") or e.get("user") or "").strip()
                    pwd = str(e.get("password") or e.get("pass") or "").strip()
                    aid = str(e.get("account_id") or e.get("acc_id") or "").strip()
                    if uid and pwd: out.append({"uid": uid, "password": pwd, "aid": aid})
                ACCOUNTS.clear()
                ACCOUNTS.extend(out)
                save_accounts()
                add_log(f"Replaced accounts with {len(ACCOUNTS)} entries", "success")
                await broadcast_accounts()
        elif action == "ADD_BOTS":
            if isinstance(payload, list):
                for e in payload:
                    if not isinstance(e, dict): continue
                    uid = str(e.get("uid") or e.get("user") or "").strip()
                    pwd = str(e.get("password") or e.get("pass") or "").strip()
                    aid = str(e.get("account_id") or e.get("acc_id") or "").strip()
                    if uid and pwd: ACCOUNTS.append({"uid": uid, "password": pwd, "aid": aid})
                save_accounts()
                add_log(f"Added {len(payload)} accounts", "success")
                await broadcast_accounts()
        elif action == "GET_SETTINGS": await ws.send_json({"type": "settings", "settings": SETTINGS, "maps": MAP_CODES})
        elif action == "UPDATE_SETTINGS":
            if isinstance(payload, dict):
                _deep_merge(SETTINGS, payload)
                save_settings()
                if not RUNNING: sync_config_to_globals()
        elif action == "GET_JWT_DATA": await ws.send_json({"type": "jwt_data", "data": get_jwt_data_for_ui()})
        elif action == "CLEAR_JWT":
            try:
                if os.path.exists(JWT_CACHE_FILE): os.remove(JWT_CACHE_FILE)
                add_log("JWT cache cleared", "success")
                await ws.send_json({"type": "jwt_data", "data": []})
            except Exception as e: add_error(f"JWT clear fail: {e}")
        elif action == "ADD_MAP":
            if isinstance(payload, str) and len(payload) >= 10:
                MAP_CODES.append(payload)
                save_maps()
                await broadcast({"type": "maps", "maps": MAP_CODES})
        elif action == "DELETE_MAP":
            if isinstance(payload, int) and 0 <= payload < len(MAP_CODES):
                MAP_CODES.pop(payload)
                save_maps()
                await broadcast({"type": "maps", "maps": MAP_CODES})
        elif action == "RESET_MAPS":
            MAP_CODES[:] = list(DEFAULT_MAP_CODES)
            save_maps()
            await broadcast({"type": "maps", "maps": MAP_CODES})
            add_log(f"Maps reset to default ({len(MAP_CODES)})", "success")
        elif action == "TEST_WEBHOOK":
            url = SETTINGS["alerts"].get("discord_webhook", "")
            if not url: await ws.send_json({"type": "webhook_test", "ok": False, "msg": "No webhook URL set"})
            else:
                sent = await _send_webhook(url, "🧪 RAJA Test Webhook — backend alive!")
                await ws.send_json({"type": "webhook_test", "ok": bool(sent), "msg": "Sent" if sent else "Failed to send"})
        elif action == "EXPORT_CSV":
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["ID", "UID", "Status", "Entity", "Map", "Time", "Matches"])
            for idx, b in STATE["bots"].items(): w.writerow([idx, b.get("uid", ""), b.get("status", ""), b.get("entity", ""), b.get("map", ""), b.get("time_in_match", 0), b.get("matches_played", 0)])
            await ws.send_json({"type": "csv_export", "data": buf.getvalue()})
        elif action == "CLEAR_LOGS":
            STATE["logs"].clear()
            STATE["error_logs"].clear()
            WS_LOG_QUEUE.clear()
            add_log("Logs cleared", "info")
        else: add_log(f"Unknown action: {action}", "warn")
    except Exception as e: add_error(f"Command '{action}' failed: {e}")

async def _send_webhook(url, content):
    try:
        import aiohttp
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=6)) as s:
            async with s.post(url, json={"content": content}) as r: return r.status in (200, 204)
    except Exception: return False

async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=60)
    await ws.prepare(request)
    WS_CLIENTS.add(ws)
    add_log(f"Client connected ({len(WS_CLIENTS)} total)", "info")
    try:
        await ws.send_json({"type": "accounts_file", "accounts": ACCOUNTS})
        await ws.send_json({"type": "settings", "settings": SETTINGS, "maps": MAP_CODES})
        await ws.send_json({"type": "jwt_data", "data": get_jwt_data_for_ui()})
    except Exception: pass
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    action = data.get("action")
                    payload = data.get("payload")
                    await handle_command(ws, action, payload)
                except json.JSONDecodeError: pass
            elif msg.type == WSMsgType.ERROR: break
    finally:
        WS_CLIENTS.discard(ws)
        add_log(f"Client disconnected ({len(WS_CLIENTS)} left)", "info")
    return ws

async def state_broadcaster():
    global PANIC_PENDING
    while True:
        try:
            bots_list = []
            for idx, b in STATE["bots"].items():
                bots_list.append({"id": idx, "uid": b.get("uid", ""), "status": b.get("status", "?"), "entity": b.get("entity", "-"), "map": (b.get("map") or "-")[:15], "time": round(float(b.get("time_in_match", 0)), 1), "matches": int(b.get("matches_played", 0))})
            log = None
            if WS_LOG_QUEUE: log = WS_LOG_QUEUE.popleft()
            msg = {"type": "state_update", "bots": bots_list, "current_map": get_display_map(), "timer": get_display_timer(), "total_matches": STATE["total_matches"], "log": log, "panic_triggered": PANIC_PENDING}
            PANIC_PENDING = False
            await broadcast(msg)
        except Exception: pass
        await asyncio.sleep(0.2)

async def index_handler(request): return web.FileResponse("index.html")

async def main():
    load_settings()
    load_maps()
    load_accounts()
    sync_config_to_globals()
    if not os.path.exists(ACCOUNTS_FILE): save_accounts()
    app = web.Application()
    app.router.add_get("/", index_handler)
    app.router.add_get("/ws", ws_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    asyncio.create_task(state_broadcaster())
    print("=" * 60)
    print("🔥 RAJA ULTRA ENGINE — WEBSOCKET SERVER (V6 FIXED)")
    print("=" * 60)
    print(f"📂 Data dir  : {DATA_DIR}")
    print(f"👤 Accounts  : {len(ACCOUNTS)} loaded")
    print(f"🗺️  Maps      : {len(MAP_CODES)} loaded")
    print("=" * 60)
    try: await asyncio.Event().wait()
    except asyncio.CancelledError: pass

def run_server():
    try: asyncio.run(main())
    except KeyboardInterrupt: print("\n[server] Shutdown requested.")
    except Exception as e: print(f"[server] Fatal error: {e}")

if __name__ == "__main__": run_server()