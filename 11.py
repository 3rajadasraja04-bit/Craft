import asyncio
import secrets
import socket
import time
import math
import struct
import random
import sys
import os
import json
import re
from collections import deque
from datetime import datetime

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.align import Align
from rich import box
from rich.progress_bar import ProgressBar

from constants import (
    MATCH_OPCODE, PRELOAD_HEX, CONNECT_TIMEOUT, PRELOAD_WAIT, MATCH_WAIT,
    TCP_HEARTBEAT, ENTITY_WAIT, PING_INTERVAL, IDLE_INTERVAL, ACK_FORCE_AFTER,
    DEFAULT_MASK, PING_TICK_BASE,
)

from protocol import (
    hdr as _hdr, ok as _ok, info as _info, warn as _warn, err as _err,
    _ts, aes_enc, udp_dec, decode_header, strip_tea_pad, parse_player_sync,
    find_udp_server, _resolve_mask, login, tcp_frame, tcp_heartbeat,
    FrameReader, region_byte, resolve_host, parse_proto, _first, to_int, to_str,
    _fv, _fb, _fs, pkt_hello, pkt_ack, pkt_auth3043, pkt_auth448,
    pkt_ping, pkt_idle, build_match_start, parse_map_code, build_pkt
)

# ═══════════════════════════════════════════════════════════════════════
#  HARDCODED MAP CODES (24 maps)
# ═══════════════════════════════════════════════════════════════════════
MAP_CODES = [
    "FREEFIREB8F45D361E2734E681D1399BFFE521BEQ540", 
    "FREEFIREEBC3E906B050092F6792FE2A2994D260Q540",
    "FREEFIRE574AA13E8E1F7E6DEDFFD02F6D49E309Q540", 
    "FREEFIRE2FB96754038F3AE595F2B00331586B44Q540",
    "FREEFIREEAAD9F1F165160BFBD64DAFEBC458A2AQ540", 
    "FREEFIREE95C203C8B24478F962A9B2C7CA7EF88Q132",
    "FREEFIRE7CA6FC15EEBC878E7A037A79F54196E5Q132", 
    "FREEFIREEFFFE5674B037E409968E08B1C0C93AAQ132",
    "FREEFIRE3A662FF87EAEDB7380D4C215E4E2D797Q132", 
    "FREEFIREB4EE478E27F2CD689B7EBFE764165EA7Q132",
    "FREEFIREDBEE58254D509335FA3EA195AF239E28Q132", 
    "FREEFIREE97775F56DFFDB7F7A4341EA23B73C0CQ132",
    "FREEFIRE3B2F7F43A89982406399C6B17431EF2BQ132", 
    "FREEFIRE3C7A2846988FBFC9FD92C5474C2E2B9FQ132",
    "FREEFIRE447390A73B8E015A1FB7993DC3BAB896Q132", 
    "FREEFIRE938B26DEDE92BC1BF1427A46501CCD28Q132",
    "FREEFIREE48B0B4F28C032BF2D3BAC3BE09D363CQ132", 
    "FREEFIRE753CB453A176741ECB6B5E85B494CA60Q132",
    "FREEFIRE1625A163CD529BCBE5D6D5893915BFC9Q132", 
    "FREEFIREC52A36AC1D15E751BBA5DEEE0A8B9805Q540",
    "FREEFIRE276B461B84D646ADC78843A191D63BC7Q500",
    "FREEFIRE7A5E709682C79D94C466DF8ABD846C23Q500",
    "FREEFIRE4A16139F7C50D409B0B32CCB7D361B50Q500",
    "FREEFIRE0B5A31AD22E5A0AE2302F6CF90F814C9Q500",
]

# ═══════════════════════════════════════════════════════════════════════
#  🎨 NEON COLORS
# ═══════════════════════════════════════════════════════════════════════
NEON_CYAN = "#00FFFF"
NEON_PINK = "#FF007F"
NEON_GREEN = "#39FF14"
NEON_PURPLE = "#BC13FE"
NEON_YELLOW = "#FFE600"
NEON_ORANGE = "#FF5F1F"
NEON_RED = "#FF3131"
DIM_GRAY = "#555555"

# ═══════════════════════════════════════════════════════════════════════
#  📁 FILE PATHS
# ═══════════════════════════════════════════════════════════════════════
JWT_CACHE_FILE       = "jwt.json"
HISTORY_FILE         = "bot_history.json"
MAP_STATS_FILE       = "map.json"
PROGRESS_SEQ_FILE    = "progress_seq.json"
PROGRESS_TIMER_FILE  = "progress_timer.json"

# ═══════════════════════════════════════════════════════════════════════
#  ⚙️ GLOBAL CONFIG (set from prompts)
# ═══════════════════════════════════════════════════════════════════════
SEQ_MODE      = False   # True = server-end mode, False = timer mode
START_MAP_IDX = 1       # 1-based (user-facing)
END_MAP_IDX   = 24      # 1-based, inclusive
RESUME_MODE   = True    # use saved progress files

# ═══════════════════════════════════════════════════════════════════════
#  GLOBAL TUI STATE
# ═══════════════════════════════════════════════════════════════════════
STATE = {
    "bots": {},
    "logs": deque(maxlen=12),
    "error_logs": deque(maxlen=12),
    "total_matches": 0,
    "total_errors": 0,
    "start_time": time.time()
}

ROTATION = {"maps": [], "hold_secs": 60.0, "sync_secs": 10.0, "start_time": 0.0,
            "range_start": 0, "range_end": 0}

# ═══════════════════════════════════════════════════════════════════════
#  💾 PROGRESS FILE HELPERS
# ═══════════════════════════════════════════════════════════════════════
def _progress_file(is_seq):
    return PROGRESS_SEQ_FILE if is_seq else PROGRESS_TIMER_FILE

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
    if entry and isinstance(entry.get("map_index"), int):
        return entry["map_index"]
    return None

# ═══════════════════════════════════════════════════════════════════════
#  📊 MAP.JSON (global per-map actual duration stats)
# ═══════════════════════════════════════════════════════════════════════
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
    """Record actual duration = (entity received → server end)."""
    if duration is None or duration <= 0: return
    stats = load_map_stats()
    s = stats.get(map_code)
    if not s:
        s = {"sessions": 0, "total_secs": 0.0, "avg_secs": 0.0,
             "min_secs": duration, "max_secs": duration, "last_duration": duration}
        stats[map_code] = s
    s["sessions"]     += 1
    s["total_secs"]   += duration
    s["avg_secs"]      = s["total_secs"] / s["sessions"]
    s["min_secs"]      = min(s["min_secs"], duration)
    s["max_secs"]      = max(s["max_secs"], duration)
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
        if uid not in data:
            data[uid] = {"matches_played": 0, "total_time_played": 0}
        data[uid]["matches_played"]     = b.get("matches_played", 0)
        data[uid]["total_time_played"]  = b.get("total_time_played", 0)
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f: json.dump(data, f, indent=4)
    except Exception: pass

# ═══════════════════════════════════════════════════════════════════════
#  LOG HELPERS
# ═══════════════════════════════════════════════════════════════════════
def add_log(msg, style=NEON_CYAN, icon="ℹ"):
    ts = datetime.now().strftime("%H:%M:%S")
    short = msg[:42] + "..." if len(msg) > 45 else msg
    STATE["logs"].append(f"[{DIM_GRAY}][{ts}][/] [bold {style}]{icon} {short}[/]")

def add_error(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    STATE["error_logs"].append(f"[{DIM_GRAY}][{ts}][/] [bold {NEON_RED}]✘ {msg}[/]")

class RichStdout:
    def write(self, text): pass
    def flush(self): pass

original_stdout = sys.stdout
sys.stdout = RichStdout()

def hdr(t): add_log(f"{t}", NEON_PURPLE, "⚡")
def ok(m): add_log(m, NEON_GREEN, "✅")
def info(m): add_log(m, NEON_CYAN, "💠")
def warn(m):
    add_log(m, NEON_YELLOW, "⚠️"); add_error(m); STATE["total_errors"] += 1
def err(m):
    add_log(m, NEON_RED, "❌"); add_error(m); STATE["total_errors"] += 1

# ═══════════════════════════════════════════════════════════════════════
#  🎨 TUI
# ═══════════════════════════════════════════════════════════════════════
def get_current_map_slot():
    maps = ROTATION["maps"]
    hold = ROTATION["hold_secs"]
    sync = ROTATION.get("sync_secs", 10.0)
    start = ROTATION["start_time"]
    rs = ROTATION.get("range_start", 0)
    re_ = ROTATION.get("range_end", len(maps) - 1)
    if not maps or hold <= 0:
        return (maps[rs] if maps else "Q07D64", rs, hold, "play")
    subset_size = re_ - rs + 1
    if subset_size <= 0: subset_size = 1
    cycle_time = hold + sync
    elapsed = time.time() - start
    slot_rel = int(elapsed // cycle_time) % subset_size
    slot_idx = rs + slot_rel
    into_cycle = elapsed % cycle_time
    if into_cycle < hold:
        return (maps[slot_idx], slot_idx, hold - into_cycle, "play")
    else:
        return (maps[slot_idx], slot_idx, cycle_time - into_cycle, "sync")

def make_layout():
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=4),
        Layout(name="body", ratio=3),
        Layout(name="stats", ratio=4)
    )
    layout["body"].split_row(
        Layout(name="left", ratio=2),
        Layout(name="right", ratio=3)
    )
    layout["right"].split_column(
        Layout(name="logs", ratio=1),
        Layout(name="errors", ratio=1)
    )
    return layout

def update_ui():
    layout = make_layout()
    map_code, map_idx, secs_left, phase = get_current_map_slot()
    short_map = f"{map_code[8:12]}..{map_code[-4:]}"
    rs = ROTATION.get("range_start", 0) + 1
    re_ = ROTATION.get("range_end", len(ROTATION["maps"]) - 1) + 1

    header_text = Text()
    header_text.append("🔥 ZHD ENGINE ", style=f"bold {NEON_ORANGE}")
    header_text.append(f"| 📊 RANGE: ", style=f"bold {NEON_CYAN}")
    header_text.append(f"#{rs}-#{re_} ", style=f"bold {NEON_YELLOW}")
    header_text.append(f"| 💾 RESUME: ", style=f"bold {NEON_CYAN}")
    header_text.append(f"{'ON' if RESUME_MODE else 'OFF'} ", style=f"bold {NEON_GREEN if RESUME_MODE else NEON_RED}")
    header_text.append("| ", style=f"bold {NEON_CYAN}")

    if SEQ_MODE:
        header_text.append("🏁 SEQ MODE - SERVER END ", style=f"bold {NEON_PURPLE}")
    else:
        header_text.append(f"🗺️ MAP: {short_map} ", style=f"bold {NEON_YELLOW}")
        if phase == "sync":
            header_text.append(f"| ⏳ SYNC: {secs_left:.0f}s", style=f"bold {NEON_RED}")
        else:
            header_text.append(f"| ⏳ PLAY: {secs_left:.0f}s", style=f"bold {NEON_GREEN}")

    if SEQ_MODE:
        bar = ProgressBar(total=100, completed=100, width=25,
                          complete_style=NEON_PURPLE, finished_style=NEON_PURPLE)
    else:
        progress = int(((ROTATION['hold_secs'] - secs_left) / ROTATION['hold_secs']) * 100) if phase == "play" else 100
        bar = ProgressBar(total=100, completed=max(0, min(100, progress)), width=25,
                          complete_style=NEON_PINK, finished_style=NEON_GREEN)

    layout["header"].update(Panel(Group(Align.center(header_text), Align.center(bar)),
                                  style="black", border_style=NEON_CYAN, box=box.ROUNDED))

    bot_table = Table(expand=True, border_style=NEON_PINK, show_header=True,
                      header_style=f"bold {NEON_PINK}", box=box.SIMPLE, padding=(0, 0))
    bot_table.add_column("ID", width=3, justify="center")
    bot_table.add_column("UID", width=7, overflow="ellipsis", no_wrap=True)
    bot_table.add_column("Status", width=9, overflow="ellipsis", no_wrap=True)
    for idx, b in STATE["bots"].items():
        status = b["status"]
        if "Playing" in status: color, icon = NEON_GREEN, "🎮"
        elif "Wait" in status: color, icon = NEON_YELLOW, "⏳"
        elif "Error" in status or "Fail" in status: color, icon = NEON_RED, "❌"
        else: color, icon = NEON_CYAN, "⚙️"
        uid_short = f"{b['uid'][:4]}..{b['uid'][-2:]}" if len(b['uid']) > 6 else b['uid']
        bot_table.add_row(f"[{NEON_PURPLE}]{idx}[/]", uid_short, f"[{color}]{icon} {status[:6]}[/]")
    layout["left"].update(Panel(bot_table, title=f"[bold {NEON_PINK}]🤖 BOTS[/]",
                                border_style=NEON_PINK, box=box.ROUNDED, style="black"))

    log_text = Text.from_markup("\n".join(STATE["logs"]))
    layout["logs"].update(Panel(log_text, title=f"[bold {NEON_GREEN}]📡 MAIN LOGS[/]",
                                border_style=NEON_GREEN, box=box.ROUNDED, style="black"))
    err_text = Text.from_markup("\n".join(STATE["error_logs"]))
    layout["errors"].update(Panel(err_text, title=f"[bold {NEON_RED}]🚨 ERRORS & EVENTS[/]",
                                  border_style=NEON_RED, box=box.ROUNDED, style="black"))

    stats_table = Table(expand=True, border_style=NEON_PURPLE, show_header=True,
                        header_style=f"bold {NEON_PURPLE}", box=box.SIMPLE, padding=(0, 0))
    stats_table.add_column("#", justify="center", width=2)
    stats_table.add_column("Account ID", style=NEON_CYAN, width=12)
    stats_table.add_column("Map", style=NEON_PINK, width=8, overflow="ellipsis")
    stats_table.add_column("Entity ID", style=NEON_GREEN, width=10)
    stats_table.add_column("Time", justify="right", style=NEON_ORANGE, width=5)
    stats_table.add_column("Total Maps", justify="right", style=NEON_YELLOW, width=10)
    for idx, b in STATE["bots"].items():
        eid = str(b.get("entity", "-"))
        t   = f"{b.get('time_in_match', 0):.0f}s"
        m   = str(b.get("matches_played", 0))
        aid = b.get("aid", b["uid"])
        stats_table.add_row(f"{idx}", aid, b.get("map", "-")[:7] + "..", eid, t, f"[bold]{m}[/]")

    stats_title = (f"[bold {NEON_PURPLE}]📊 LIVE STATS[/] | "
                   f"[{NEON_GREEN}]✅ Success: {STATE['total_matches']}[/] | "
                   f"[{NEON_RED}]❌ Errors: {STATE['total_errors']}[/]")
    layout["stats"].update(Panel(stats_table, title=stats_title, border_style=NEON_PURPLE,
                                 box=box.ROUNDED, style="black"))
    return layout

async def ui_loop(stop_event):
    console = Console(file=original_stdout)
    with Live(make_layout(), console=console, refresh_per_second=5, screen=True) as live:
        while not stop_event.is_set():
            live.update(update_ui())
            await asyncio.sleep(0.2)
        live.update(make_layout())

# ═══════════════════════════════════════════════════════════════════════
#  CORE LOGIC
# ═══════════════════════════════════════════════════════════════════════
MOVE_HZ = 5.0; MOVE_INTERVAL = 1.0 / MOVE_HZ; PUNCH_VEL = 390
PUNCH_STEP = PUNCH_VEL / MOVE_HZ; POSE_DASHING = 2; EMOTE_ID = 909000001; EMOTE_EVERY = 4.0
CMD_PLAYER_STATE_UPLOAD = 2001; CMD_EMOTE = 371; CMD_OTHER_STATES = 2012; CMD_SERVER_TIME = 2000

class MatchEnd(Exception): pass

def _pv(out, v):
    v = int(v)
    while v >= 0x80: out.append((v & 0x7F) | 0x80); v >>= 7
    out.append(v & 0x7F)
def _pzig(out, v): _pv(out, ((int(v) << 1) ^ (int(v) >> 63)) & 0xFFFFFFFFFFFFFFFF)
def _pfloat(out, f): _pv(out, struct.unpack("<I", struct.pack("<f", f))[0])
def _rvi(b, p):
    v = i = 0
    while True:
        c = b[p + i]; v |= (c & 0x7F) << (7 * i); i += 1
        if not c & 0x80: return v, p + i
def _zag(v): return (v >> 1) ^ -(v & 1)

def _build_player_state(pid, pos, rot, tdir, vel, aim, ps=0, pp=0, plat=0, ping=0):
    o = bytearray(); _pv(o, pid)
    for v in [pos[0], pos[1], pos[2], rot[0], rot[1], rot[2],
              tdir[0], tdir[1], vel[0], vel[1], vel[2]]:
        _pzig(o, v)
    o.append(ps & 0xFF); o.append(pp & 0xFF)
    _pfloat(o, aim[0]); _pfloat(o, aim[1]); _pfloat(o, aim[2])
    _pv(o, plat); o.append(0); _pv(o, ping); o.append(0); _pv(o, 0); o.append(0)
    return bytes(o)

def _build_c2s_state(pid, pos, rot, tdir, vel, aim, server_time, ps=0, pp=0, ping=0):
    o = bytearray(_build_player_state(pid, pos, rot, tdir, vel, aim, ps=ps, pp=pp, ping=ping))
    _pv(o, server_time & 0xFFFFFFFF)
    return bytes(o)

def build_state_packet(body, key, mask, order_id=0):
    return build_pkt(CMD_PLAYER_STATE_UPLOAD, body, key, mask, reliable=False,
                     order_id=order_id, send_option=0, flags=1, msg_key=0x6A)

def build_emote_packet(key, mask):
    b = bytearray(); _pv(b, EMOTE_ID); b += b"\x00\x00\x00"; _pv(b, 0xFFFFFFFF)
    _pv(b, int(time.time() * 1000) & 0xFFFFFFFF); _pv(b, 0)
    _pzig(b, 0); b += b"\x00\x00\x00"
    return build_pkt(CMD_EMOTE, bytes(b), key, mask, reliable=True, order_id=0,
                     send_option=1, flags=1, msg_key=0x6A)

class BotBrain:
    def __init__(self, pid, spawn=None):
        self.pid = pid; self.my_pos = spawn
        self.yaw = 0.0; self.random_goal = None; self._last_sent_pos = spawn
        self.joined_at = time.time()
    def build_next_state(self, now):
        if not self.my_pos: return None
        mx, my, mz = self.my_pos
        if not self.random_goal or math.hypot(self.random_goal[0] - mx, self.random_goal[1] - mz) < 100:
            self.random_goal = (mx + random.uniform(-500, 500), mz + random.uniform(-500, 500))
        gx, gz = self.random_goal; dx, dz = gx - mx, gz - mz; d = math.hypot(dx, dz)
        if d > 1:
            ux, uz = dx / d, dz / d; step = min(PUNCH_STEP, d)
            self.yaw = math.degrees(math.atan2(ux, uz))
            new = (mx + ux * step, my, mz + uz * step)
        else: new = (mx, my, mz)
        old = self._last_sent_pos or self.my_pos
        nx, ny, nz = int(new[0]), int(new[1]), int(new[2])
        ox, oy, oz = int(old[0]), int(old[1]), int(old[2])
        dx, dz = nx - ox, nz - oz; dist = math.hypot(dx, dz)
        if dist >= 1:
            ux, uz = dx / dist, dz / dist; vel = (int(ux * PUNCH_VEL), 0, int(uz * PUNCH_VEL))
            hdg = math.degrees(math.atan2(ux, uz)); hdg = ((hdg + 180.0) % 360.0) - 180.0
            rot = (int(hdg * 100), 0, 0); tdir = (int(ux * 100), int(uz * 100)); aim = (ux, 0.0, uz)
        else:
            vel = (0, 0, 0); rad = math.radians(self.yaw); fx, fz = math.sin(rad), math.cos(rad)
            hdg = ((self.yaw + 180.0) % 360.0) - 180.0; rot = (int(hdg * 100), 0, 0)
            tdir = (int(fx * 100), int(fz * 100)); aim = (fx, 0.0, fz)
        self.my_pos = new; self._last_sent_pos = new
        return _build_c2s_state(self.pid, (nx, ny, nz), rot, tdir, vel, aim,
                                int((now - self.joined_at) * 1000.0) & 0xFFFFFFFF,
                                ps=0, pp=POSE_DASHING, ping=90)

# JWT
JWT_TTL = 6 * 3600
def load_jwt_cache():
    if not os.path.exists(JWT_CACHE_FILE): return {}
    try:
        with open(JWT_CACHE_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return {}

def save_jwt_cache(uid, data):
    cache = load_jwt_cache(); dc = dict(data)
    if isinstance(dc.get("key"), bytes): dc["key"] = dc["key"].hex()
    if isinstance(dc.get("iv"), bytes):  dc["iv"]  = dc["iv"].hex()
    
    # ⏱️ Save Created & Expire Time
    now = time.time()
    dc["created_at"] = now
    dc["expires_at"] = now + JWT_TTL
    dc["saved_at"]   = now # for backward compatibility
    
    cache[str(uid)] = dc
    try:
        with open(JWT_CACHE_FILE, "w", encoding="utf-8") as f: json.dump(cache, f, indent=4)
    except Exception: pass

# ═══════════════════════════════════════════════════════════════════════
#  PHASES
# ═══════════════════════════════════════════════════════════════════════
def phase1_build_match_start(map_code: str) -> bytes:
    hdr("PHASE ① MATCH-START")
    try: parse_map_code(map_code)
    except ValueError as e: err(f"map fail: {e}"); raise
    return build_match_start(map_code)

async def phase2_send_and_wait(uid: str, pwd: str, raw_match_start: bytes, bot_idx: int):
    hdr("PHASE ② TCP LOGIN")
    STATE["bots"][bot_idx]["status"] = "Logging in"
    cache = load_jwt_cache(); s = cache.get(str(uid))
    
    # ── Check JWT Expiry ──
    is_valid = False
    if s:
        if "expires_at" in s:
            is_valid = time.time() < s["expires_at"]
        elif "saved_at" in s:
            is_valid = (time.time() - s["saved_at"]) < JWT_TTL
            
    if is_valid:
        if isinstance(s.get("key"), str): s["key"] = bytes.fromhex(s["key"])
        if isinstance(s.get("iv"), str):  s["iv"]  = bytes.fromhex(s["iv"])
    else:
        s = None # JWT expired or invalid, force re-login

    # ── Generate New JWT if expired/missing ──
    if not s or int(s.get("acc_id", 0)) == 0:
        info(f"Bot {bot_idx} JWT expired/missing. Re-logging in...")
        try: 
            s = await asyncio.to_thread(login, uid, pwd)
            save_jwt_cache(uid, s)
            ok(f"Bot {bot_idx} JWT regenerated successfully!")
        except Exception as e: 
            err(f"Bot {bot_idx} login fail: {e}")
            return None
            
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(s["host"], int(s["port"])), 
            timeout=CONNECT_TIMEOUT
        )
    except Exception as e: 
        err(f"Bot {bot_idx} TCP fail: {str(e)}")
        return None
        
    fr = FrameReader(reader); stop = asyncio.Event(); bt = None
    try:
        enc = aes_enc(s["jwt"].encode(), s["key"], s["iv"])
        auth = (bytes([0x83, region_byte(s["region"])]) + int(s["acc_id"]).to_bytes(8, "big")
                + int(s["kts"]).to_bytes(4, "big") + bytes(4) + len(enc).to_bytes(4, "big") + enc)
        writer.write(auth); await writer.drain()
        t0 = time.time()
        while time.time() - t0 < 3.0:
            f = await fr.next_frame(timeout=0.5)
            if f: pass
            elif reader.at_eof(): return None
        c = (_fv(1, 6) + _fb(2, _fv(1, 3) + _fs(2, "2019121269") + _fv(3, 2) + _fv(4, 1)))
        writer.write(tcp_frame("1d14", c, s["key"], s["iv"])); await writer.drain()
        await asyncio.sleep(0.4)
        writer.write(tcp_frame(MATCH_OPCODE, bytes.fromhex(PRELOAD_HEX), s["key"], s["iv"])); await writer.drain()
        pre_deadline = time.time() + PRELOAD_WAIT
        while time.time() < pre_deadline:
            f = await fr.next_frame(timeout=0.5)
            if not f: continue
            op, payload = f
            if op.hex() == "0e00" and to_int(_first(parse_proto(payload), 4)) == 32: break
        writer.write(tcp_frame(MATCH_OPCODE, raw_match_start, s["key"], s["iv"])); await writer.drain()
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
                            return {"writer": writer, "fr": fr, "stop": stop, "session": s,
                                    "udp_server": udp, "udp_key": to_str(_first(inner, 3)),
                                    "match_id": to_int(_first(inner, 1)),
                                    "account_id": int(s["acc_id"]),
                                    "room_code": to_str(_first(inner, 19)),
                                    "room_name": to_str(_first(inner, 20)),
                                    "mask": _resolve_mask(to_str(_first(inner, 42)))}
                udp = find_udp_server(tree)
                if udp:
                    return {"writer": writer, "fr": fr, "stop": stop, "session": s,
                            "udp_server": udp, "udp_key": "", "match_id": 0,
                            "account_id": int(s["acc_id"]), "room_code": "",
                            "room_name": "", "mask": DEFAULT_MASK}
        err(f"Bot {bot_idx} no UDP server"); return None
    except Exception as e:
        err(f"Bot {bot_idx} TCP Auth/Preload Error: {str(e)}")
        return None
    finally:
        if bt and not bt.done(): stop.set(); bt.cancel()

async def phase3_start_udp(match: dict, duration: float, bot_idx: int, map_code: str):
    hdr("PHASE ③ UDP JOIN")
    STATE["bots"][bot_idx]["status"] = "Joining UDP"
    udp_server = match["udp_server"]; key_hex = match["udp_key"]; mask = match["mask"] or DEFAULT_MASK
    try: key = bytes.fromhex(key_hex)
    except Exception: return None
    if len(key) != 16: return None
    if not (isinstance(mask, str) and len(mask) == 5 and sorted(mask) == list("01234")): mask = DEFAULT_MASK
    try: host, port = udp_server.rsplit(":", 1); port = int(port)
    except Exception: return None
    ip = await resolve_host(host); loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); sock.setblocking(False)
    try: sock.connect((ip, port))
    except Exception: sock.close(); return None

    recv_orders, ack_next = set(), [0]
    ack_sent = [0]; last_ack_time = [time.time()]
    stop_udp = asyncio.Event(); t_448 = [time.time()]
    cmd176_seen = [False]; brain = None; brain_task = None
    my_entity = {"id": None, "spawn": (0.0, 0.0, 0.0)}
    timing = {"entity": None, "ended": False}   # ⏱️ T3 & T4
    result = {"duration": None}

    async def tx(pkt, label):
        try: await loop.sock_sendall(sock, pkt)
        except OSError: pass

    async def send_ack(n):
        ack_sent[0] = n; last_ack_time[0] = time.time()
        await tx(pkt_ack(n, key, mask, msg_key=0x68), f"ACK next={n}")

    def body_plain(h, pkt):
        off = h["header_len"] + h.get("extra", 0); b = pkt[off:off + h["length"]]
        if not b: return b
        if h["flags"] & 1:
            pl = udp_dec(b, key); stripped = strip_tea_pad(pl)
            if h["cmd"] == 176 and len(stripped) < 10: return pl
            return stripped
        return b

    async def handle(data):
        nonlocal brain, brain_task
        h = decode_header(data, key, mask)
        if not h: return None
        if h["cmd"] in (4, 103):
            timing["ended"] = True          # ⏱️ T4
            raise MatchEnd()
        if h["cmd"] == 176 and not cmd176_seen[0]:
            plain = body_plain(h, data)
            eid, aid, pos = parse_player_sync(plain)
            if eid == 0:
                try: eid = struct.unpack("<I", plain[:4])[0]; pos = (0.0, 0.0, 0.0)
                except Exception: pass
            if eid != 0:
                cmd176_seen[0] = True; my_entity["id"] = eid; my_entity["spawn"] = pos
                STATE["bots"][bot_idx]["entity"] = eid
                STATE["bots"][bot_idx]["status"] = "Playing"
                timing["entity"] = time.time()   # ⏱️ T3
                ok(f"Bot {bot_idx} Entity: {eid}")
                if brain is None:
                    brain = BotBrain(eid, spawn=pos)
                    brain_task = asyncio.create_task(_brain_loop(brain, key, mask, sock, loop))
        if h.get("orderId") is not None and h["cmd"] != 2:
            recv_orders.add(h["orderId"])
            while ack_next[0] in recv_orders: ack_next[0] += 1
            await send_ack(ack_next[0])

    async def _brain_loop(brain, key, mask, sock, loop):
        last_emote, order = time.time(), 0
        while not stop_udp.is_set():
            try:
                now = time.time(); body = brain.build_next_state(now)
                if body:
                    pkt = build_state_packet(body, key, mask, order_id=order)
                    await loop.sock_sendall(sock, pkt); order = (order + 1) & 0xFFFF
                if now - last_emote > EMOTE_EVERY:
                    await loop.sock_sendall(sock, build_emote_packet(key, mask)); last_emote = now
            except Exception: break
            try: await asyncio.wait_for(stop_udp.wait(), timeout=MOVE_INTERVAL); break
            except asyncio.TimeoutError: pass

    try:
        suffix = str(1000 + secrets.randbelow(9000))
        await tx(pkt_hello(str(match["account_id"]), suffix, key, mask), f"HELLO sfx={suffix}")
        await asyncio.sleep(1.0)
        await tx(pkt_auth3043(1, match["account_id"], match["match_id"], key, mask, order_id=0, msg_key=0x60), "AUTH-3043 #1")
        await tx(pkt_auth3043(2, match["account_id"], match["match_id"], key, mask, order_id=1, msg_key=0x5E), "AUTH-3043 #2")
        await asyncio.sleep(1.0)
        t_448[0] = time.time()
        await tx(pkt_auth448(match["account_id"], match["match_id"], match["room_code"], match["room_name"],
                             key, mask, order_id=2, msg_key=0x60), "AUTH-448")
        ent_deadline = time.time() + ENTITY_WAIT
        while time.time() < ent_deadline and not cmd176_seen[0]:
            try: data = await asyncio.wait_for(loop.sock_recv(sock, 65536), timeout=0.4)
            except asyncio.TimeoutError: continue
            if data: await handle(data)
        if not cmd176_seen[0]:
            warn(f"Bot {bot_idx} Entity timeout")
        if duration > 0 or SEQ_MODE:
            tasks = [asyncio.create_task(_udp_ping(key, mask, sock, loop, stop_udp, t_448)),
                     asyncio.create_task(_udp_idle(key, mask, sock, loop, stop_udp))]
            t0 = time.time()
            while True:
                if not SEQ_MODE and duration > 0 and (time.time() - t0 >= duration):
                    break
                STATE["bots"][bot_idx]["time_in_match"] = time.time() - t0
                try: data = await asyncio.wait_for(loop.sock_recv(sock, 65536), timeout=0.5)
                except asyncio.TimeoutError: continue
                if data: await handle(data)
    except MatchEnd:
        add_log(f"Bot {bot_idx} SERVER END (103)", NEON_ORANGE, "⏹️")
    except Exception as e:
        err(f"Bot {bot_idx} UDP Error: {str(e)}")
    finally:
        stop_udp.set()
        if brain_task: brain_task.cancel()
        if 'tasks' in locals():
            for t in tasks: t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        sock.close()
        # ⏱️ C = entity → server-end only
        if timing["entity"] and timing["ended"]:
            actual = time.time() - timing["entity"]
            update_map_stats(map_code, actual)
            result["duration"] = actual
            ok(f"Map {map_code[8:12]} actual: {actual:.1f}s")
    return result["duration"]

async def _udp_ping(key, mask, sock, loop, stop_udp, t_448):
    while not stop_udp.is_set():
        tick = (PING_TICK_BASE - 2 * int((time.time() - t_448[0]) * 1000)) & 0xFFFFFFFF
        try: await loop.sock_sendall(sock, pkt_ping(tick, key, mask))
        except Exception: break
        try: await asyncio.wait_for(stop_udp.wait(), timeout=PING_INTERVAL); break
        except asyncio.TimeoutError: pass

async def _udp_idle(key, mask, sock, loop, stop_udp):
    while not stop_udp.is_set():
        try: await loop.sock_sendall(sock, pkt_idle(key, mask))
        except Exception: break
        try: await asyncio.wait_for(stop_udp.wait(), timeout=IDLE_INTERVAL); break
        except asyncio.TimeoutError: pass

async def run(uid: str, pwd: str, map_code: str, duration: float, bot_idx: int):
    try: raw = phase1_build_match_start(map_code)
    except Exception: return None
    STATE["bots"][bot_idx]["map"] = map_code
    STATE["bots"][bot_idx]["time_in_match"] = 0
    match = await phase2_send_and_wait(uid, pwd, raw, bot_idx)
    if not match: return None
    try:
        actual = await phase3_start_udp(match, duration, bot_idx, map_code)
        STATE["total_matches"] += 1
        STATE["bots"][bot_idx]["matches_played"] = STATE["bots"][bot_idx].get("matches_played", 0) + 1
        STATE["bots"][bot_idx]["total_time_played"] = (STATE["bots"][bot_idx].get("total_time_played", 0)
                                                        + (actual if actual else duration))
        save_history()
        return actual
    finally:
        STATE["bots"][bot_idx]["status"] = "Cooldown"
        match["stop"].set()
        try: match["writer"].close(); await match["writer"].wait_closed()
        except Exception: pass

# ═══════════════════════════════════════════════════════════════════════
#  PER-BOT LOOP
# ═══════════════════════════════════════════════════════════════════════
async def loop_matches(idx: int, total: int, uid: str, pwd: str, aid: str, rejoin_delay: float):
    STATE["bots"][idx] = {
        "uid": uid, "aid": aid, "status": "Booting", "map": "-", "entity": "-",
        "matches_played": 0, "time_in_match": 0, "total_time_played": 0
    }
    start_idx_0 = START_MAP_IDX - 1
    end_idx_0   = END_MAP_IDX   - 1

    # ── Resume: pick bot's start map ──
    bot_map_idx = start_idx_0
    if RESUME_MODE:
        saved = get_bot_progress_map(SEQ_MODE, uid)
        if saved is not None and start_idx_0 <= saved <= end_idx_0:
            bot_map_idx = saved
            ok(f"Bot {idx} 💾 RESUME at map #{saved+1}")

    while True:
        if SEQ_MODE:
            map_code = MAP_CODES[bot_map_idx]
            add_log(f"Bot {idx} -> SEQ Map #{bot_map_idx+1}", NEON_PURPLE, "🏁")
            save_progress(True, uid, bot_map_idx)   # save BEFORE play → resume same map on crash
            try: await run(uid, pwd, map_code, 0, idx)
            except Exception as e:
                STATE["bots"][idx]["status"] = "Error ❌"
                add_error(f"Bot {idx} crash: {str(e)}") # FULL ERROR LOGGING
            await asyncio.sleep(rejoin_delay)
            bot_map_idx += 1
            if bot_map_idx > end_idx_0: bot_map_idx = start_idx_0
            continue

        # ── Timer mode ──
        if RESUME_MODE:
            # Per-bot sequential with fixed duration (hold_secs)
            map_code = MAP_CODES[bot_map_idx]
            add_log(f"Bot {idx} -> TIMER Map #{bot_map_idx+1}", NEON_PURPLE, "🔄")
            save_progress(False, uid, bot_map_idx)
            try: await run(uid, pwd, map_code, ROTATION["hold_secs"], idx)
            except Exception as e:
                STATE["bots"][idx]["status"] = "Error ❌"
                add_error(f"Bot {idx} crash: {str(e)}") # FULL ERROR LOGGING
            await asyncio.sleep(rejoin_delay)
            bot_map_idx += 1
            if bot_map_idx > end_idx_0: bot_map_idx = start_idx_0
            continue

        # ── Timer mode, no resume → follow global timer ──
        map_code, map_idx, secs_left, phase = get_current_map_slot()
        if phase == "sync":
            STATE["bots"][idx]["status"] = "Wait Sync"
            await asyncio.sleep(secs_left + 0.5)
            continue
        if secs_left < 15.0:
            STATE["bots"][idx]["status"] = "Next Map"
            await asyncio.sleep(secs_left + 0.5)
            continue
        try: await run(uid, pwd, map_code, secs_left, idx)
        except Exception as e:
            STATE["bots"][idx]["status"] = "Error ❌"
            add_error(f"Bot {idx} crash: {str(e)}") # FULL ERROR LOGGING
        await asyncio.sleep(rejoin_delay)

# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════
def _ask_int(prompt_markup, default, lo, hi):
    try:
        raw = input().strip()
        val = int(raw) if raw else default
    except Exception:
        val = default
    return max(lo, min(val, hi))

def _ask_float(prompt_markup, default, lo=0.1):
    try:
        raw = input().strip()
        val = float(raw) if raw else default
    except Exception:
        val = default
    return max(lo, val)

def _ask_yes(prompt_markup, default_yes=True):
    raw = input().strip().lower()
    if not raw: return default_yes
    return raw in ("y", "yes", "1", "true")

async def main_async():
    global SEQ_MODE, START_MAP_IDX, END_MAP_IDX, RESUME_MODE
    console = Console(file=original_stdout)

    console.print()
    console.print(Panel.fit(
        "[bold #00FFFF]⚡ ZHD ULTRA ENGINE INITIALIZING... ⚡[/]\n"
        "[bold #BC13FE]💀 NEON CYBERSPACE MODE 💀[/]",
        border_style=NEON_CYAN, box=box.DOUBLE
    ))
    console.print()

    console.print("[bold #FFE600]🔥 ACCOUNTS JSON FILE[/] [#888888][accounts.json][/]: ", end="")
    accounts_file = input().strip() or "accounts.json"
    try:
        with open(accounts_file, "r", encoding="utf-8") as f: data = json.load(f)
        if isinstance(data, dict): data = data.get("accounts", [])
        accounts = []
        for e in data:
            if not isinstance(e, dict): continue
            uid = str(e.get("uid") or e.get("user") or "").strip()
            pwd = str(e.get("password") or e.get("pass") or "").strip()
            aid = str(e.get("account_id") or e.get("acc_id") or "").strip()
            if uid and pwd: accounts.append({"uid": uid, "password": pwd, "aid": aid})
    except Exception as e:
        console.print(f"[bold #FF3131]❌ ERROR LOADING ACCOUNTS: {e}[/]")
        return
    if not accounts:
        console.print("[bold #FF3131]❌ NO ACCOUNTS FOUND[/]")
        return
    console.print(f"[bold #39FF14]✅ {len(accounts)} BOTS LOADED[/]")
    console.print()

    console.print("[bold #FFE600]🚀 SEQUENTIAL SERVER END MODE? (Y/N)[/] [#888888][Y][/]: ", end="")
    SEQ_MODE = _ask_yes("", True)
    console.print()

    n_maps = len(MAP_CODES)
    console.print(f"[bold #00FFFF]▶ START FROM MAP NUMBER [1-{n_maps}][/] [#888888][1][/]: ", end="")
    START_MAP_IDX = _ask_int("", 1, 1, n_maps)

    console.print(f"[bold #00FFFF]▶ END AT MAP NUMBER [1-{n_maps}][/] [#888888][{n_maps}][/]: ", end="")
    END_MAP_IDX = _ask_int("", n_maps, START_MAP_IDX, n_maps)
    console.print()

    mode_label = "SEQ" if SEQ_MODE else "TIMER"
    console.print(f"[bold #BC13FE]💾 RESUME FROM SAVED {mode_label} PROGRESS? (Y/N)[/] [#888888][Y][/]: ", end="")
    RESUME_MODE = _ask_yes("", True)
    console.print()

    hold_secs = 60.0; sync_secs = 10.0
    if not SEQ_MODE:
        console.print("[bold #FFE600]⏱️ HOLD EACH MAP FOR SECONDS[/] [#888888][60][/]: ", end="")
        hold_secs = _ask_float("", 60.0, 1.0)
        console.print("[bold #FFE600]⏳ GAP BETWEEN MAPS (SECONDS)[/] [#888888][10][/]: ", end="")
        sync_secs = _ask_float("", 10.0, 0.1)
        console.print()

    console.print("[bold #FFE600]🔄 RECONNECT COOLDOWN (SECONDS)[/] [#888888][2][/]: ", end="")
    rejoin_delay = _ask_float("", 2.0, 0.1)
    console.print()

    # ── Resume summary ──
    if RESUME_MODE:
        prog = load_progress(SEQ_MODE)
        resumable = sum(1 for a in accounts
                        if str(a["uid"]) in prog
                        and START_MAP_IDX - 1 <= prog[str(a["uid"])].get("map_index", -1) <= END_MAP_IDX - 1)
        if resumable:
            console.print(f"[bold #39FF14]💾 {resumable}/{len(accounts)} BOTS WILL RESUME FROM SAVED MAPS[/]")
        else:
            console.print(f"[bold #FFE600]💾 NO SAVED PROGRESS IN RANGE — FRESH START[/]")
    console.print()

    console.print("[bold #BC13FE]🚀 IGNITION... ENTERING CYBERSPACE DASHBOARD![/]\n")
    await asyncio.sleep(1)

    ROTATION["maps"]        = list(MAP_CODES)
    ROTATION["hold_secs"]   = hold_secs
    ROTATION["sync_secs"]   = sync_secs
    ROTATION["start_time"]  = time.time()
    ROTATION["range_start"] = START_MAP_IDX - 1
    ROTATION["range_end"]   = END_MAP_IDX - 1

    tasks = [asyncio.create_task(
                loop_matches(i, len(accounts), acc["uid"], acc["password"], acc["aid"], rejoin_delay))
             for i, acc in enumerate(accounts, 1)]
    stop_ui = asyncio.Event()
    ui_task = asyncio.create_task(ui_loop(stop_ui))

    try: await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        stop_ui.set()
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

def main():
    try: asyncio.run(main_async())
    except KeyboardInterrupt: pass

if __name__ == "__main__":
    main()