import asyncio
import getpass
import secrets
import socket
import time
import math
import struct
import random
import sys
import traceback

from constants import (
    MATCH_OPCODE, PRELOAD_HEX,
    CONNECT_TIMEOUT, PRELOAD_WAIT, MATCH_WAIT, TCP_HEARTBEAT,
    ENTITY_WAIT, PING_INTERVAL, IDLE_INTERVAL, ACK_FORCE_AFTER,
    DEFAULT_MASK, PING_TICK_BASE,
)

from protocol import (
    hdr, ok, info, warn, err, _ts,
    aes_enc, udp_dec, decode_header, strip_tea_pad, parse_player_sync,
    find_udp_server, _resolve_mask, login, tcp_frame, tcp_heartbeat,
    FrameReader, region_byte, resolve_host, parse_proto, _first, to_int, to_str,
    _fv, _fb, _fs, pkt_hello, pkt_ack, pkt_auth3043, pkt_auth448,
    pkt_ping, pkt_idle, build_match_start, parse_map_code, build_pkt
)

# ═══════════════════════════════════════════════════════════════════════
#  LOGGER (Terminal + File)
# ═══════════════════════════════════════════════════════════════════════
class Logger(object):
    def __init__(self, filename="bot_log.txt"):
        self.terminal = sys.stdout
        self.log = open(filename, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

# ═══════════════════════════════════════════════════════════════════════
#  BOT BRAIN (Exact app.py Logic)
# ═══════════════════════════════════════════════════════════════════════
MOVE_HZ           = 5.0
MOVE_INTERVAL     = 1.0 / MOVE_HZ
PUNCH_VEL         = 390
PUNCH_STEP        = PUNCH_VEL / MOVE_HZ
POSE_DASHING      = 2
EMOTE_ID          = 909000001
EMOTE_EVERY       = 4.0

CMD_PLAYER_STATE_UPLOAD = 2001
CMD_EMOTE               = 371
CMD_OTHER_STATES        = 2012
CMD_SERVER_TIME         = 2000

class MatchEnd(Exception):
    """Exception raised when server ends the match (BYEBYE)"""
    pass

def _pv(out, v):
    v = int(v)
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80); v >>= 7
    out.append(v & 0x7F)

def _pzig(out, v):
    z = (int(v) << 1) ^ (int(v) >> 63)
    _pv(out, z & 0xFFFFFFFFFFFFFFFF)

def _pfloat(out, f):
    bits = struct.unpack("<I", struct.pack("<f", f))[0]
    _pv(out, bits)

def _rvi(b, p):
    v = i = 0
    while True:
        c = b[p + i]; v |= (c & 0x7F) << (7 * i); i += 1
        if not c & 0x80: return v, p + i

def _zag(v): return (v >> 1) ^ -(v & 1)

def _build_player_state(pid, pos, rot, tdir, vel, aim, ps=0, pp=0, plat=0, ping=0):
    o = bytearray()
    _pv(o, pid)
    _pzig(o, pos[0]);  _pzig(o, pos[1]);  _pzig(o, pos[2])
    _pzig(o, rot[0]);  _pzig(o, rot[1]);  _pzig(o, rot[2])
    _pzig(o, tdir[0]); _pzig(o, tdir[1])
    _pzig(o, vel[0]);  _pzig(o, vel[1]);  _pzig(o, vel[2])
    o.append(ps & 0xFF); o.append(pp & 0xFF)
    _pfloat(o, aim[0]); _pfloat(o, aim[1]); _pfloat(o, aim[2])
    _pv(o, plat); o.append(0); _pv(o, ping)
    o.append(0); _pv(o, 0); o.append(0)
    return bytes(o)

def _build_c2s_state(pid, pos, rot, tdir, vel, aim, server_time, ps=0, pp=0, ping=0):
    o = bytearray()
    o += _build_player_state(pid, pos, rot, tdir, vel, aim, ps=ps, pp=pp, ping=ping)
    _pv(o, server_time & 0xFFFFFFFF)
    return bytes(o)

def decode_other_states(body):
    players = {}
    if not body or len(body) < 4: return players
    try: count = struct.unpack_from(">H", body, 0)[0]
    except struct.error: return players
    if count == 0 or count > 64: return players
    p = 2
    for _ in range(count):
        if p >= len(body): break
        try:
            sid = body[p]; p += 1
            x, p = _rvi(body, p); y, p = _rvi(body, p); z, p = _rvi(body, p)
            vx, p = _rvi(body, p); vy, p = _rvi(body, p); vz, p = _rvi(body, p)
            yr = body[p]; p += 1; ay = body[p]; p += 1; axz = body[p]; p += 1
            ps = body[p]; p += 1; pp = body[p]; p += 1
            plat, p = _rvi(body, p); cus = body[p]; p += 1
        except (IndexError, ValueError): break
        pos = (_zag(x), _zag(y), _zag(z))
        if not (1 <= sid <= 64) or any(abs(v) > 4000000 for v in pos): break
        players[sid] = {"pos": pos, "physx_pose": pp}
    return players

def decode_my_state(body):
    if not body or len(body) < 4: return None
    try:
        if body[:2] == b"\x00\x02": p = 3
        else:
            p = 0
            _pid, p = _rvi(body, p)
        x, p = _rvi(body, p); y, p = _rvi(body, p); z, p = _rvi(body, p)
    except (IndexError, ValueError):
        return None
    return (_zag(x), _zag(y), _zag(z))

class BotBrain:
    def __init__(self, pid, spawn=None):
        self.pid = pid
        self.my_pos = spawn
        self.server_time_ms = int(time.time() * 1000) & 0xFFFFFFFF
        self.yaw = 0.0
        self.enemies = {}
        self.random_goal = None
        self._last_sent_pos = spawn
        self.joined_at = time.time()

    def server_time_now(self, now):
        return int((now - self.joined_at) * 1000.0) & 0xFFFFFFFF

    def on_server_time(self, ms):
        if ms: self.server_time_ms = int(ms) & 0xFFFFFFFF

    def on_my_state(self, pos):
        if pos: self.my_pos = pos

    def on_other_states(self, players):
        for sid, st in players.items():
            if sid == (self.pid & 0xFF):
                if st["pos"] != (0.0, 0.0, 0.0):
                    self.my_pos = st["pos"]
            else:
                self.enemies[sid] = st

    def build_next_state(self, now):
        if not self.my_pos: 
            return None
            
        mx, my, mz = self.my_pos
        
        if not self.random_goal or math.hypot(self.random_goal[0] - mx, self.random_goal[1] - mz) < 100:
            self.random_goal = (mx + random.uniform(-500, 500), mz + random.uniform(-500, 500))
            
        gx, gz = self.random_goal
        dx, dz = gx - mx, gz - mz
        d = math.hypot(dx, dz)
        
        if d > 1:
            ux, uz = dx/d, dz/d
            step = min(PUNCH_STEP, d)
            self.yaw = math.degrees(math.atan2(ux, uz))
            new = (mx + ux*step, my, mz + uz*step)
        else: 
            new = (mx, my, mz)

        old = self._last_sent_pos or self.my_pos
        nx, ny, nz = int(new[0]), int(new[1]), int(new[2])
        ox, oy, oz = int(old[0]), int(old[1]), int(old[2])
        dx, dz = nx - ox, nz - oz
        dist = math.hypot(dx, dz)

        if dist >= 1:
            ux, uz = dx/dist, dz/dist
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
        
        st = self.server_time_now(now)
        return _build_c2s_state(self.pid, (nx, ny, nz), rot, tdir, vel, aim, st, ps=0, pp=POSE_DASHING, ping=90)

def build_state_packet(body, key, mask, order_id=0):
    return build_pkt(CMD_PLAYER_STATE_UPLOAD, body, key, mask, reliable=False, order_id=order_id, send_option=0, flags=1, msg_key=0x6A)

def build_emote_packet(key, mask):
    b = bytearray()
    _pv(b, EMOTE_ID); b += b"\x00\x00\x00"; _pv(b, 0xFFFFFFFF)
    _pv(b, int(time.time() * 1000) & 0xFFFFFFFF); _pv(b, 0)
    _pzig(b, 0); b += b"\x00\x00\x00"
    return build_pkt(CMD_EMOTE, bytes(b), key, mask, reliable=True, order_id=0, send_option=1, flags=1, msg_key=0x6A)

def phase1_build_match_start(map_code: str) -> bytes:
    hdr("PHASE ① — Build MATCH-START with your map code")
    try:
        map_id, is_craftland = parse_map_code(map_code)
    except ValueError as e:
        err(f"cannot parse map code: {e}")
        raise

    if map_id is None:
        info("no map code supplied → using default BR template (Q07D64)")
    else:
        kind = "CRAFTLAND" if is_craftland else "STANDARD BR"
        ok(f"map code: {map_id!r}  ({kind}, {len(map_id)} chars)")

    raw = build_match_start(map_code)
    info(f"MATCH-START built: {len(raw)} bytes")
    return raw

async def phase2_send_and_wait(uid: str, pwd: str, raw_match_start: bytes):
    hdr("PHASE ② — TCP login → AUTH → 1d14 → preload → send MATCH-START → wait 0e00")
    try:
        s = login(uid, pwd)
    except Exception as e:
        err(f"login failed: {e}")
        return None

    ok(f"account={s['acc_id']} nick={s['nickname']!r} region={s['region']}")
    info(f"TCP target: {s['host']}:{s['port']}")

    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(s["host"], s["port"]), timeout=CONNECT_TIMEOUT)
    except Exception as e:
        err(f"TCP connect failed: {e}")
        return None

    ok("TCP connected")
    fr = FrameReader(reader)
    stop = asyncio.Event()
    bt = None

    try:
        enc = aes_enc(s["jwt"].encode(), s["key"], s["iv"])
        auth = (bytes([0x83, region_byte(s["region"])]) + int(s["acc_id"]).to_bytes(8, "big") + int(s["kts"]).to_bytes(4, "big") + bytes(4) + len(enc).to_bytes(4, "big") + enc)
        writer.write(auth)
        await writer.drain()
        ok(f"AUTH sent ({len(auth)}B)")

        t0 = time.time()
        while time.time() - t0 < 3.0:
            f = await fr.next_frame(timeout=0.5)
            if f: info(f"RX {f[0].hex()} {len(f[1])}B")
            elif reader.at_eof(): err("server closed after AUTH"); return None

        c = (_fv(1, 6) + _fb(2, _fv(1, 3) + _fs(2, "2019121269") + _fv(3, 2) + _fv(4, 1)))
        writer.write(tcp_frame("1d14", c, s["key"], s["iv"]))
        await writer.drain()
        ok("1d14 sent")
        await asyncio.sleep(0.4)

        ok(f"sending preload {MATCH_OPCODE}/{PRELOAD_HEX}")
        writer.write(tcp_frame(MATCH_OPCODE, bytes.fromhex(PRELOAD_HEX), s["key"], s["iv"]))
        await writer.drain()

        pre_deadline = time.time() + PRELOAD_WAIT
        while time.time() < pre_deadline:
            f = await fr.next_frame(timeout=0.5)
            if not f:
                if reader.at_eof(): err("server closed during preload"); return None
                continue
            op, payload = f
            if op.hex() == "df00": continue
            if op.hex() != "0e00": continue
            tree = parse_proto(payload)
            f4 = _first(tree, 4)
            if to_int(f4) == 32:
                ok("preload ack received")
                break

        ok(f"sending MATCH-START ({len(raw_match_start)}B)")
        wire = tcp_frame(MATCH_OPCODE, raw_match_start, s["key"], s["iv"])
        writer.write(wire)
        await writer.drain()

        bt = asyncio.create_task(tcp_heartbeat(writer, stop, s, TCP_HEARTBEAT))
        info(f"waiting ≤{MATCH_WAIT}s for UDP server…")
        deadline = time.time() + MATCH_WAIT
        while time.time() < deadline:
            f = await fr.next_frame(timeout=1.0)
            if not f:
                if reader.at_eof(): err("server closed before match response"); break
                continue

            op, payload = f
            if op.hex() != "0e00": continue

            tree = parse_proto(payload)
            f4 = _first(tree, 4)
            if to_int(f4) == 16:
                inner = _first(tree, 5)
                if isinstance(inner, (bytes, bytearray)): inner = parse_proto(bytes(inner))
                if isinstance(inner, dict):
                    udp = to_str(_first(inner, 2))
                    if udp and ":" in udp:
                        return {
                            "writer": writer, "fr": fr, "stop": stop, "session": s,
                            "udp_server": udp, "udp_key": to_str(_first(inner, 3)),
                            "match_id": to_int(_first(inner, 1)), "account_id": int(s["acc_id"]),
                            "room_code": to_str(_first(inner, 19)), "room_name": to_str(_first(inner, 20)),
                            "mask": _resolve_mask(to_str(_first(inner, 42))),
                        }

            udp = find_udp_server(tree)
            if udp:
                return {
                    "writer": writer, "fr": fr, "stop": stop, "session": s,
                    "udp_server": udp, "udp_key": "", "match_id": 0, "account_id": int(s["acc_id"]),
                    "room_code": "", "room_name": "", "mask": DEFAULT_MASK,
                }

        err("no UDP server received")
        return None
    finally:
        if bt is not None and not bt.done():
            stop.set()
            try: await asyncio.wait_for(bt, timeout=2.0)
            except: bt.cancel()

async def phase3_start_udp(match: dict, duration: float):
    hdr("PHASE ③ — UDP join (HELLO → AUTH-3043 ×2 → AUTH-448 → cmd=176)")
    udp_server = match["udp_server"]; key_hex = match["udp_key"]
    mask = match["mask"] or DEFAULT_MASK; account_id = match["account_id"]
    match_id = match["match_id"]; room_code = match["room_code"]; room_name = match["room_name"]

    try: key = bytes.fromhex(key_hex)
    except: err("bad udp_key"); return None

    if len(key) != 16: err("udp_key must be 16 bytes"); return None
    if not (isinstance(mask, str) and len(mask) == 5 and sorted(mask) == list("01234")): mask = DEFAULT_MASK

    try:
        host, port = udp_server.rsplit(":", 1)
        port = int(port)
    except: err("invalid UDP server"); return None

    ip = await resolve_host(host)
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    try: sock.connect((ip, port))
    except: sock.close(); return None

    ok("UDP socket connected")
    recv_orders, ack_next, ack_sent = set(), [0], [0]
    last_ack_time, last_force = [time.time()], [0.0]
    stop_udp = asyncio.Event(); t_448 = [time.time()]
    my_entity = {"id": None, "spawn": (0.0, 0.0, 0.0), "aid": None}
    cmd176_seen = [False]
    brain = None; brain_task = None

    def _hs(h):
        return f"msg=0x{h['msg_key']:02x} cmd={h['cmd']:<5} len={h['length']}"

    async def tx(pkt, label):
        try: await loop.sock_sendall(sock, pkt)
        except OSError as e: warn(f"send error: {e}"); return
        h = decode_header(pkt, key, mask)
        print(f"[{_ts()}] → {label:<24} ({len(pkt):>3}B) {_hs(h) if h else ''}", flush=True)

    async def rx(timeout=1.0):
        try: return await asyncio.wait_for(loop.sock_recv(sock, 65536), timeout=timeout)
        except asyncio.TimeoutError: return None
        except OSError: return b""

    def body_plain(h, pkt):
        off = h["header_len"] + h.get("extra", 0)
        b = pkt[off:off + h["length"]]
        if not b: return b
        if h["flags"] & 1:
            pl = udp_dec(b, key)
            stripped = strip_tea_pad(pl)
            if h["cmd"] == 176 and len(stripped) < 10:
                return pl
            return stripped
        return b

    async def send_ack(n):
        ack_sent[0] = n; last_ack_time[0] = time.time()
        await tx(pkt_ack(n, key, mask, msg_key=0x68), f"ACK next={n}")

    async def handle(data):
        nonlocal brain, brain_task

        h = decode_header(data, key, mask)
        if not h: return None
        print(f"[{_ts()}] ← {_hs(h)}", flush=True)

        # CRITICAL FIX: Stop loop if server ends match
        if h["cmd"] in (4, 103):
            info(f"Server ended match (cmd={h['cmd']}). Restarting loop...")
            raise MatchEnd()

        if h["cmd"] == 176 and not cmd176_seen[0]:
            plain = body_plain(h, data)
            info(f"cmd=176 body len={len(plain)} head={plain[:32].hex()}")
            eid, aid, pos = parse_player_sync(plain)
            if eid == 0:
                warn(f"cmd=176 parse failed, trying alternative parser...")
                try:
                    eid = struct.unpack("<I", plain[:4])[0]
                    pos = (0.0, 0.0, 0.0) 
                except: pass
            if eid != 0:
                cmd176_seen[0] = True
                my_entity["id"] = eid; my_entity["spawn"] = pos; my_entity["aid"] = aid
                ok(f"PLAYER ENTITY FOUND: {eid}")
                if brain is None:
                    brain = BotBrain(eid, spawn=pos)
                    brain_task = asyncio.create_task(_brain_loop(brain, key, mask, sock, loop))
                    info("BOT BRAIN ACTIVATED (Movement)")

        if brain is not None:
            try:
                plain_ = body_plain(h, data)
                if h["cmd"] == 2012:
                    players = decode_other_states(plain_)
                    if players: brain.on_other_states(players)
                elif h["cmd"] == 2000:
                    try:
                        tick, _ = _rvi(plain_, 0)
                        brain.on_server_time(tick)
                    except: pass
                elif h["cmd"] == 2001:
                    pos = decode_my_state(plain_)
                    if pos: brain.on_my_state(pos)
            except: pass

        oid = h.get("orderId")
        if oid is not None and h["cmd"] != 2:
            recv_orders.add(oid)
            while ack_next[0] in recv_orders: ack_next[0] += 1
            await send_ack(ack_next[0])
        return h

    async def _brain_loop(brain, key, mask, sock, loop):
        last_emote, order = time.time(), 0
        while not stop_udp.is_set():
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
            except: break
            try:
                await asyncio.wait_for(stop_udp.wait(), timeout=MOVE_INTERVAL)
                break
            except asyncio.TimeoutError: pass

    tasks = []
    try:
        suffix = str(1000 + secrets.randbelow(9000))
        await tx(pkt_hello(str(account_id), suffix, key, mask), f"HELLO suffix={suffix}")
        await asyncio.sleep(1.0)

        await tx(pkt_auth3043(1, account_id, match_id, key, mask, order_id=0, msg_key=0x60), "AUTH-3043 #1")
        await tx(pkt_auth3043(2, account_id, match_id, key, mask, order_id=1, msg_key=0x5E), "AUTH-3043 #2")
        await asyncio.sleep(1.0)

        t_448[0] = time.time()
        await tx(pkt_auth448(account_id, match_id, room_code, room_name, key, mask, order_id=2, msg_key=0x60), "AUTH-448")

        hdr(f"Waiting for cmd=176 ≤{ENTITY_WAIT}s")
        ent_deadline = time.time() + ENTITY_WAIT
        while time.time() < ent_deadline and not cmd176_seen[0]:
            data = await rx(timeout=0.4)
            if data: await handle(data)

        if cmd176_seen[0]: ok("cmd=176 received")
        else: warn(f"cmd=176 NOT received in {ENTITY_WAIT}s, but continuing anyway...")

        if duration > 0:
            info(f"running session for {duration:.0f}s (joined={cmd176_seen[0]})")
            tasks = [
                asyncio.create_task(_udp_ping(key, mask, sock, loop, stop_udp, t_448)),
                asyncio.create_task(_udp_idle(key, mask, sock, loop, stop_udp))
            ]
            t0 = time.time()
            
            # Loop to process packets
            while time.time() - t0 < duration:
                data = await rx(timeout=0.5)
                if data is None: continue
                if data == b"": break
                await handle(data)

        joined = cmd176_seen[0]
        return {"joined": joined, "entity_id": my_entity["id"], "spawn": my_entity["spawn"], "account_id": my_entity["aid"]}

    except MatchEnd:
        info("Match ended by server. Breaking session for next loop...")

    finally:
        stop_udp.set()
        if brain_task: brain_task.cancel()
        for task in tasks:
            if not task.done(): task.cancel()
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
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

async def run(uid: str, pwd: str, map_code: str, duration: float):
    try: raw = phase1_build_match_start(map_code)
    except: return
    match = await phase2_send_and_wait(uid, pwd, raw)
    if not match: return
    try:
        result = await phase3_start_udp(match, duration)
        if result:
            print(f"\nRESULT: joined={result['joined']} entity={result['entity_id']} spawn={result['spawn']}")
    finally:
        match["stop"].set()
        try:
            match["writer"].close()
            await match["writer"].wait_closed()
        except: pass

async def loop_matches(uid: str, pwd: str, map_code: str, duration: float, rejoin_delay: float):
    iteration = 1
    while True:
        hdr(f"═══════════ STARTING MATCH ITERATION {iteration} ═══════════")
        try:
            await run(uid, pwd, map_code, duration)
        except Exception as e:
            err(f"Iteration {iteration} crashed: {e}")
            traceback.print_exc()
        
        info(f"Match {iteration} finished. Waiting {rejoin_delay}s before rejoining...")
        await asyncio.sleep(rejoin_delay)
        iteration += 1

def main():
    sys.stdout = Logger("bot_log.txt")
    sys.stderr = sys.stdout

    DEFAULT_UID = "8453148999"
    DEFAULT_PWD = "YUIOG_0VK91IKL0B_2HIM3"
    DEFAULT_MAP = "#FREEFIREE95C203C8B24478F962A9B2C7CA7EF88Q132"
    DEFAULT_DURATION = 3600.0  # 1 hour limit (loop breaks early on BYEBYE)
    REJOIN_DELAY = 5.0

    print("=" * 70)
    print("  FF · INFINITE LOOP MODE (Smart Loop on BYEBYE)")
    print("=" * 70)

    uid = DEFAULT_UID
    pwd = DEFAULT_PWD
    dur = DEFAULT_DURATION
    
    map_code = DEFAULT_MAP
    if map_code.startswith("#"): 
        map_code = map_code[1:].strip()

    print(f"[i] UID      : {uid}")
    print(f"[i] Map Code : {map_code}")
    print(f"[i] Duration : {dur}s per match")
    print("=" * 70)

    try: 
        asyncio.run(loop_matches(uid, pwd, map_code, dur, REJOIN_DELAY))
    except KeyboardInterrupt: 
        print("\n[i] Loop stopped by user (Ctrl+C)")

if __name__ == "__main__":
    try: 
        main()
    except KeyboardInterrupt: 
        print("\n[i] interrupted")