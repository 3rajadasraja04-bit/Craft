import asyncio
import getpass
import secrets
import socket
import time
import math
import struct
import random
import sys
import os
import json
import contextvars
import traceback
import re
from collections import deque
from datetime import datetime

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.align import Align

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
#  HARDCODED MAP CODES — 20 maps
# ═══════════════════════════════════════════════════════════════════════
MAP_CODES = [
    "V08H6Q", "FREEFIREEBC3E906B050092F6792FE2A2994D260Q540",
    "FREEFIRE574AA13E8E1F7E6DEDFFD02F6D49E309Q540", "FREEFIRE2FB96754038F3AE595F2B00331586B44Q540",
    "FREEFIREEAAD9F1F165160BFBD64DAFEBC458A2AQ540", "FREEFIREE95C203C8B24478F962A9B2C7CA7EF88Q132",
    "FREEFIRE7CA6FC15EEBC878E7A037A79F54196E5Q132", "FREEFIREEFFFE5674B037E409968E08B1C0C93AAQ132",
    "FREEFIRE3A662FF87EAEDB7380D4C215E4E2D797Q132", "FREEFIREB4EE478E27F2CD689B7EBFE764165EA7Q132",
    "FREEFIREDBEE58254D509335FA3EA195AF239E28Q132", "FREEFIREE97775F56DFFDB7F7A4341EA23B73C0CQ132",
    "FREEFIRE3B2F7F43A89982406399C6B17431EF2BQ132", "FREEFIRE3C7A2846988FBFC9FD92C5474C2E2B9FQ132",
    "FREEFIRE447390A73B8E015A1FB7993DC3BAB896Q132",
]

# ═══════════════════════════════════════════════════════════════════════
#  GLOBAL TUI STATE
# ═══════════════════════════════════════════════════════════════════════
STATE = {
    "bots": {},          # idx -> {uid, status, map, entity, matches_played, time_in_match}
    "logs": deque(maxlen=15),
    "total_matches": 0,
    "total_errors": 0,
    "start_time": time.time()
}

ROTATION = {"maps": [], "hold_secs": 60.0, "start_time": 0.0}

def get_current_map_slot():
    maps = ROTATION["maps"]
    hold = ROTATION["hold_secs"]
    start = ROTATION["start_time"]
    if not maps or hold <= 0:
        return ("Q07D64", 0, hold, 0.0)
    elapsed = time.time() - start
    slot_idx = int(elapsed // hold) % len(maps)
    secs_left = hold - (elapsed % hold)
    return (maps[slot_idx], slot_idx, secs_left, elapsed)

# ═══════════════════════════════════════════════════════════════════════
#  LOGGER ROUTING (TUI ke liye)
# ═══════════════════════════════════════════════════════════════════════
_current_tag = contextvars.ContextVar("_current_tag", default="")

def add_log(msg, style="cyan"):
    ts = datetime.now().strftime("%H:%M:%S")
    STATE["logs"].append(f"[dim][{ts}][/dim] [{style}]{msg}[/]")

def hdr(t): add_log(f"▶ {t}", "magenta bold")
def ok(m): add_log(f"✔ {m}", "green bold")
def info(m): add_log(f"ℹ {m}", "cyan")
def warn(m): 
    add_log(f"⚠ {m}", "yellow bold")
    STATE["total_errors"] += 1
def err(m): 
    add_log(f"✘ {m}", "red bold")
    STATE["total_errors"] += 1

# ═══════════════════════════════════════════════════════════════════════
#  TUI LAYOUT BUILDER
# ═══════════════════════════════════════════════════════════════════════
def make_layout():
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1),
        Layout(name="stats", size=10)
    )
    layout["body"].split_row(
        Layout(name="left", ratio=1),
        Layout(name="right", ratio=2)
    )
    return layout

def update_ui():
    layout = make_layout()
    
    # --- HEADER ---
    map_code, map_idx, secs_left, _ = get_current_map_slot()
    total_maps = len(ROTATION["maps"])
    uptime = int(time.time() - STATE["start_time"])
    uptime_str = f"{uptime//3600:02d}:{(uptime%3600)//60:02d}:{uptime%60:02d}"
    
    header_text = (
        f"[bold magenta]🔥 FF BULK BOT 🔥[/]  |  "
        f"Target Map: [bold cyan]{map_code[:12]}...{map_code[-4:]}[/] | "
        f"Slot: [bold yellow]{map_idx+1}/{total_maps}[/] | "
        f"Next in: [bold red]{secs_left:.0f}s[/] | "
        f"Uptime: [bold green]{uptime_str}[/]"
    )
    layout["header"].update(Panel(Align.center(header_text), style="bold cyan", border_style="cyan"))

    # --- LEFT PANEL (BOTS STATUS) ---
    bot_table = Table(expand=True, border_style="blue", show_header=True, header_style="bold blue")
    bot_table.add_column("ID", width=4)
    bot_table.add_column("Account", overflow="fold")
    bot_table.add_column("Status", style="cyan")
    
    for idx, b in STATE["bots"].items():
        status_color = "green" if "Playing" in b["status"] else ("yellow" if "Wait" in b["status"] else "white")
        bot_table.add_row(str(idx), b["uid"], f"[{status_color}]{b['status']}[/]")
        
    layout["left"].update(Panel(bot_table, title="[bold blue]BOTS STATUS[/]", border_style="blue"))

    # --- RIGHT PANEL (DEBUG LOGS) ---
    log_text = Text("\n".join(STATE["logs"]))
    layout["right"].update(Panel(log_text, title="[bold green]LIVE DEBUG LOGS[/]", border_style="green"))

    # --- BOTTOM PANEL (LIVE STATS) ---
    stats_table = Table(expand=True, border_style="magenta", show_header=True, header_style="bold magenta")
    stats_table.add_column("Bot", width=4)
    stats_table.add_column("Map")
    stats_table.add_column("Entity ID")
    stats_table.add_column("Match Time")
    stats_table.add_column("Matches", justify="right")
    
    for idx, b in STATE["bots"].items():
        eid = str(b.get("entity", "-"))
        t = f"{b.get('time_in_match', 0):.0f}s"
        m = str(b.get("matches_played", 0))
        stats_table.add_row(str(idx), b.get("map", "-")[:10]+"...", eid, t, m)
        
    layout["stats"].update(Panel(stats_table, title=f"[bold magenta]LIVE MATCH DATA | Success: {STATE['total_matches']} | Errors: {STATE['total_errors']}[/]", border_style="magenta"))

    return layout

async def ui_loop(stop_event):
    with Live(make_layout(), refresh_per_second=4, screen=True) as live:
        while not stop_event.is_set():
            live.update(update_ui())
            await asyncio.sleep(0.25)
        live.update(make_layout())

# ═══════════════════════════════════════════════════════════════════════
#  CORE LOGIC (Bot logic same as before)
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
    o = bytearray()
    _pv(o, pid)
    for v in [pos[0], pos[1], pos[2], rot[0], rot[1], rot[2], tdir[0], tdir[1], vel[0], vel[1], vel[2]]: _pzig(o, v)
    o.append(ps & 0xFF); o.append(pp & 0xFF)
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
    b = bytearray()
    _pv(b, EMOTE_ID); b += b"\x00\x00\x00"; _pv(b, 0xFFFFFFFF)
    _pv(b, int(time.time() * 1000) & 0xFFFFFFFF); _pv(b, 0)
    _pzig(b, 0); b += b"\x00\x00\x00"
    return build_pkt(CMD_EMOTE, bytes(b), key, mask, reliable=True, order_id=0, send_option=1, flags=1, msg_key=0x6A)

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
            ux, uz = dx/d, dz/d; step = min(PUNCH_STEP, d)
            self.yaw = math.degrees(math.atan2(ux, uz))
            new = (mx + ux*step, my, mz + uz*step)
        else: new = (mx, my, mz)
        old = self._last_sent_pos or self.my_pos
        nx, ny, nz = int(new[0]), int(new[1]), int(new[2])
        ox, oy, oz = int(old[0]), int(old[1]), int(old[2])
        dx, dz = nx - ox, nz - oz; dist = math.hypot(dx, dz)
        if dist >= 1:
            ux, uz = dx/dist, dz/dist; vel = (int(ux * PUNCH_VEL), 0, int(uz * PUNCH_VEL))
            hdg = math.degrees(math.atan2(ux, uz)); hdg = ((hdg + 180.0) % 360.0) - 180.0
            rot = (int(hdg * 100), 0, 0); tdir = (int(ux * 100), int(uz * 100)); aim = (ux, 0.0, uz)
        else:
            vel = (0, 0, 0); rad = math.radians(self.yaw); fx, fz = math.sin(rad), math.cos(rad)
            hdg = ((self.yaw + 180.0) % 360.0) - 180.0; rot = (int(hdg * 100), 0, 0)
            tdir = (int(fx * 100), int(fz * 100)); aim = (fx, 0.0, fz)
        self.my_pos = new; self._last_sent_pos = new
        return _build_c2s_state(self.pid, (nx, ny, nz), rot, tdir, vel, aim, int((now - self.joined_at) * 1000.0) & 0xFFFFFFFF, ps=0, pp=POSE_DASHING, ping=90)

# JWT
JWT_CACHE_FILE = "jwt.json"; JWT_TTL = 6 * 3600
def load_jwt_cache():
    if not os.path.exists(JWT_CACHE_FILE): return {}
    try:
        with open(JWT_CACHE_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except: return {}
def save_jwt_cache(uid, data):
    cache = load_jwt_cache(); data_copy = dict(data)
    if isinstance(data_copy.get("key"), bytes): data_copy["key"] = data_copy["key"].hex()
    if isinstance(data_copy.get("iv"), bytes): data_copy["iv"] = data_copy["iv"].hex()
    data_copy["saved_at"] = time.time(); cache[str(uid)] = data_copy
    try:
        with open(JWT_CACHE_FILE, "w", encoding="utf-8") as f: json.dump(cache, f, indent=4)
    except: pass

# Phases
def phase1_build_match_start(map_code: str) -> bytes:
    hdr("PHASE ① — Build MATCH-START")
    try: map_id, is_craftland = parse_map_code(map_code)
    except ValueError as e: err(f"map parse fail: {e}"); raise
    if map_id: ok(f"map: {map_id!r} ({'CRAFT' if is_craftland else 'BR'})")
    return build_match_start(map_code)

async def phase2_send_and_wait(uid: str, pwd: str, raw_match_start: bytes, bot_idx: int):
    hdr("PHASE ② — TCP Login")
    STATE["bots"][bot_idx]["status"] = "Logging in..."
    
    cache = load_jwt_cache(); s = cache.get(str(uid))
    if s and time.time() - s.get("saved_at", 0) < JWT_TTL:
        if isinstance(s.get("key"), str): s["key"] = bytes.fromhex(s["key"])
        if isinstance(s.get("iv"), str): s["iv"] = bytes.fromhex(s["iv"])
    if not s or int(s.get("acc_id", 0)) == 0:
        try: s = await asyncio.to_thread(login, uid, pwd); save_jwt_cache(uid, s)
        except Exception as e: err(f"login fail: {e}"); return None

    try: reader, writer = await asyncio.wait_for(asyncio.open_connection(s["host"], s["port"]), timeout=CONNECT_TIMEOUT)
    except Exception as e: err(f"TCP fail: {e}"); return None

    fr = FrameReader(reader); stop = asyncio.Event(); bt = None
    try:
        enc = aes_enc(s["jwt"].encode(), s["key"], s["iv"])
        auth = bytes([0x83, region_byte(s["region"])]) + int(s["acc_id"]).to_bytes(8, "big") + int(s["kts"]).to_bytes(4, "big") + bytes(4) + len(enc).to_bytes(4, "big") + enc
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
                            return {"writer": writer, "fr": fr, "stop": stop, "session": s, "udp_server": udp, "udp_key": to_str(_first(inner, 3)), "match_id": to_int(_first(inner, 1)), "account_id": int(s["acc_id"]), "room_code": to_str(_first(inner, 19)), "room_name": to_str(_first(inner, 20)), "mask": _resolve_mask(to_str(_first(inner, 42)))}
                udp = find_udp_server(tree)
                if udp: return {"writer": writer, "fr": fr, "stop": stop, "session": s, "udp_server": udp, "udp_key": "", "match_id": 0, "account_id": int(s["acc_id"]), "room_code": "", "room_name": "", "mask": DEFAULT_MASK}
        return None
    finally:
        if bt and not bt.done(): stop.set(); bt.cancel()

async def phase3_start_udp(match: dict, duration: float, bot_idx: int):
    hdr("PHASE ③ — UDP Join")
    STATE["bots"][bot_idx]["status"] = "Joining UDP..."
    
    udp_server = match["udp_server"]; key_hex = match["udp_key"]; mask = match["mask"] or DEFAULT_MASK
    try: key = bytes.fromhex(key_hex)
    except: return None
    if len(key) != 16: return None
    if not (isinstance(mask, str) and len(mask) == 5 and sorted(mask) == list("01234")): mask = DEFAULT_MASK
    try: host, port = udp_server.rsplit(":", 1); port = int(port)
    except: return None

    ip = await resolve_host(host); loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); sock.setblocking(False)
    try: sock.connect((ip, port))
    except: sock.close(); return None

    recv_orders, ack_next = set(), [0]
    stop_udp = asyncio.Event(); t_448 = [time.time()]
    cmd176_seen = [False]; brain = None; brain_task = None
    my_entity = {"id": None, "spawn": (0.0, 0.0, 0.0)}

    def body_plain(h, pkt):
        off = h["header_len"] + h.get("extra", 0); b = pkt[off:off + h["length"]]
        if not b: return b
        if h["flags"] & 1:
            pl = udp_dec(b, key); stripped = strip_tea_pad(pl)
            if h["cmd"] == 176 and len(stripped) < 10: return pl
            return stripped
        return b

    async def send_ack(n):
        try: await loop.sock_sendall(sock, pkt_ack(n, key, mask, msg_key=0x68))
        except: pass

    async def handle(data):
        nonlocal brain, brain_task
        h = decode_header(data, key, mask)
        if not h: return
        if h["cmd"] in (4, 103): raise MatchEnd()

        if h["cmd"] == 176 and not cmd176_seen[0]:
            plain = body_plain(h, data)
            eid, aid, pos = parse_player_sync(plain)
            if eid == 0:
                try: eid = struct.unpack("<I", plain[:4])[0]; pos = (0.0, 0.0, 0.0)
                except: pass
            if eid != 0:
                cmd176_seen[0] = True; my_entity["id"] = eid; my_entity["spawn"] = pos
                STATE["bots"][bot_idx]["entity"] = eid
                STATE["bots"][bot_idx]["status"] = "Playing"
                add_log(f"Bot {bot_idx} Entity Found: {eid}", "bold green")
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
            except: break
            try: await asyncio.wait_for(stop_udp.wait(), timeout=MOVE_INTERVAL); break
            except asyncio.TimeoutError: pass

    try:
        suffix = str(1000 + secrets.randbelow(9000))
        await loop.sock_sendall(sock, pkt_hello(str(match["account_id"]), suffix, key, mask))
        await asyncio.sleep(1.0)
        await loop.sock_sendall(sock, pkt_auth3043(1, match["account_id"], match["match_id"], key, mask, order_id=0, msg_key=0x60))
        await loop.sock_sendall(sock, pkt_auth3043(2, match["account_id"], match["match_id"], key, mask, order_id=1, msg_key=0x5E))
        await asyncio.sleep(1.0)
        t_448[0] = time.time()
        await loop.sock_sendall(sock, pkt_auth448(match["account_id"], match["match_id"], match["room_code"], match["room_name"], key, mask, order_id=2, msg_key=0x60))

        ent_deadline = time.time() + ENTITY_WAIT
        while time.time() < ent_deadline and not cmd176_seen[0]:
            try: data = await asyncio.wait_for(loop.sock_recv(sock, 65536), timeout=0.4)
            except asyncio.TimeoutError: continue
            if data: await handle(data)

        if duration > 0:
            tasks = [asyncio.create_task(_udp_ping(key, mask, sock, loop, stop_udp, t_448)), asyncio.create_task(_udp_idle(key, mask, sock, loop, stop_udp))]
            t0 = time.time()
            while time.time() - t0 < duration:
                STATE["bots"][bot_idx]["time_in_match"] = time.time() - t0
                try: data = await asyncio.wait_for(loop.sock_recv(sock, 65536), timeout=0.5)
                except asyncio.TimeoutError: continue
                if data: await handle(data)
    except MatchEnd: pass
    finally:
        stop_udp.set()
        if brain_task: brain_task.cancel()
        if 'tasks' in locals():
            for t in tasks: t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        sock.close()

async def _udp_ping(key, mask, sock, loop, stop_udp, t_448):
    while not stop_udp.is_set():
        tick = (PING_TICK_BASE - 2 * int((time.time() - t_448[0]) * 1000)) & 0xFFFFFFFF
        try: await loop.sock_sendall(sock, pkt_ping(tick, key, mask))
        except: break
        try: await asyncio.wait_for(stop_udp.wait(), timeout=PING_INTERVAL); break
        except asyncio.TimeoutError: pass

async def _udp_idle(key, mask, sock, loop, stop_udp):
    while not stop_udp.is_set():
        try: await loop.sock_sendall(sock, pkt_idle(key, mask))
        except: break
        try: await asyncio.wait_for(stop_udp.wait(), timeout=IDLE_INTERVAL); break
        except asyncio.TimeoutError: pass

# ═══════════════════════════════════════════════════════════════════════
#  MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════
async def run(uid: str, pwd: str, map_code: str, duration: float, bot_idx: int):
    try: raw = phase1_build_match_start(map_code)
    except: return
    STATE["bots"][bot_idx]["map"] = map_code
    STATE["bots"][bot_idx]["time_in_match"] = 0
    
    match = await phase2_send_and_wait(uid, pwd, raw, bot_idx)
    if not match: return
    
    try:
        await phase3_start_udp(match, duration, bot_idx)
        STATE["total_matches"] += 1
        STATE["bots"][bot_idx]["matches_played"] = STATE["bots"][bot_idx].get("matches_played", 0) + 1
    finally:
        STATE["bots"][bot_idx]["status"] = "Cooldown"
        match["stop"].set()
        try: match["writer"].close(); await match["writer"].wait_closed()
        except: pass

async def loop_matches(idx: int, total: int, uid: str, pwd: str, rejoin_delay: float):
    _current_tag.set(f"[{idx}]")
    STATE["bots"][idx] = {"uid": uid, "status": "Waiting", "map": "-", "entity": "-", "matches_played": 0, "time_in_match": 0}
    last_map_idx = -1

    while True:
        map_code, map_idx, secs_left, _ = get_current_map_slot()
        if secs_left < 5.0:
            STATE["bots"][idx]["status"] = "Next Map"
            await asyncio.sleep(secs_left + 0.5); continue

        if map_idx != last_map_idx:
            add_log(f"Bot {idx} Rotating to Map #{map_idx+1}", "bold magenta")
            last_map_idx = map_idx

        try: await run(uid, pwd, map_code, secs_left, idx)
        except Exception as e: add_log(f"Bot {idx} crash: {e}", "red")

        await asyncio.sleep(rejoin_delay)
        _, _, secs_left2, _ = get_current_map_slot()
        if 1.0 < secs_left2 < ROTATION["hold_secs"]:
            STATE["bots"][idx]["status"] = "Wait Sync"
            await asyncio.sleep(secs_left2 + 0.3)

async def main_async():
    # Input prompts BEFORE starting TUI to avoid glitching
    print("\033[38;5;51m[?]\033[0m Accounts JSON [accounts.json]: ", end="")
    accounts_file = input().strip() or "accounts.json"
    try:
        with open(accounts_file, "r", encoding="utf-8") as f: data = json.load(f)
        if isinstance(data, dict): data = data.get("accounts", [])
        accounts = [{"uid": str(e.get("uid") or e.get("user") or "").strip(), "password": str(e.get("password") or e.get("pass") or "").strip()} for e in data if isinstance(e, dict) and (e.get("uid") or e.get("user")) and (e.get("password") or e.get("pass"))]
    except Exception as e: print(f"\033[38;5;196mError loading accounts: {e}\033[0m"); return
    
    if not accounts: print("\033[38;5;196mNo accounts found.\033[0m"); return

    print(f"\033[38;5;46m[✓] {len(accounts)} bots loaded.\033[0m")
    print("\033[38;5;51m[?]\033[0m Hold each map for seconds [60]: ", end="")
    h = input().strip()
    hold_secs = float(h) if h else 60.0

    print("\033[38;5;51m[?]\033[0m Reconnect cooldown (s) [2]: ", end="")
    r = input().strip()
    rejoin_delay = float(r) if r else 2.0
    
    print("\033[38;5;226m[!] Starting Live TUI Engine...\033[0m")
    await asyncio.sleep(1)

    ROTATION["maps"] = list(MAP_CODES)
    ROTATION["hold_secs"] = hold_secs
    ROTATION["start_time"] = time.time()

    tasks = [asyncio.create_task(loop_matches(i, len(accounts), acc["uid"], acc["password"], rejoin_delay)) for i, acc in enumerate(accounts, 1)]
    
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