"""Wire layer: crypto, protobuf, login chain, TCP framing, UDP framing."""

import asyncio, socket, struct, random, time
from datetime import datetime

import requests, urllib3
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

from constants import (
    AES_KEY, AES_IV, CLIENT_SECRET, APP_ID, OAUTH_URL, MAJOR_LOGIN_URL,
    GETLOGIN_HOST, CLIENT_VERSION, RELEASE_VERSION, REGION,
    DEFAULT_MASK, UDP_DEVICE_ID, UDP_CLIENT_IP, UDP_DEVICE_HASH,
    FIELD_SIZES, FIELD_NAMES, PING_MSG_KEYS, 
    MATCH_START_HEX_STANDARD, MATCH_START_HEX_CRAFTLAND,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ═══════════════════════════════════════════════════════════════════════
#  Logging
# ═══════════════════════════════════════════════════════════════════════
def _ts(): return time.strftime("%H:%M:%S")
def hdr(t):  print(f"\n{'='*70}\n  {t}\n{'='*70}", flush=True)
def ok(m):   print(f"[{_ts()}] [+] {m}", flush=True)
def info(m): print(f"[{_ts()}] [i] {m}", flush=True)
def warn(m): print(f"[{_ts()}] [!] {m}", flush=True)
def err(m):  print(f"[{_ts()}] [-] {m}", flush=True)

# ═══════════════════════════════════════════════════════════════════════
#  MATCH-START builder (BR + Craftland, dynamic map code)
# ═══════════════════════════════════════════════════════════════════════
CRAFTLAND_DEFAULT_ID = "068B6C8B700BE9C165760D65AED88604Q465"
STANDARD_DEFAULT_CODE = "Q07D64"
# Public alias so main.py can inspect/verify map codes without touching privates.

def _parse_map_code(code: str):
    """Return (map_id, is_craftland) or (None, False) for empty.

    Accepts:
        ""                                            → default
        "Q07D64"            (6-char)                  → BR
        "B01013"            (6-char)                  → BR
        "068B6C8B…Q465"     (36-char hex)             → Craftland
        "FREEFIRE<36>"                                → Craftland
        "#FREEFIRE<36>"                               → Craftland
        "#FREEFIRE<36>/1"                             → Craftland
    """
    code = (code or "").strip()
    if not code:
        return None, False

    if code.startswith("#"):
        code = code[1:]

    up = code.upper()
    if up.startswith("FREEFIRE"):
        map_id = code[len("FREEFIRE"):]
        if "/" in map_id:
            map_id = map_id.split("/", 1)[0]
        return map_id, True

    if len(code) == 36:
        return code, True

    if len(code) == 6:
        return code, False

    raise ValueError(
        f"unrecognised map code: {code!r} "
        f"(expected 6-char BR, 36-char hex, or #FREEFIRE…)"
    )

# Public alias so main.py can inspect/verify map codes without touching privates.
parse_map_code = _parse_map_code
def build_match_start(map_code: str = "") -> bytes:
    """Build the MATCH-START plaintext for the given map code.

    Returns the exact plaintext bytes to feed to `tcp_frame(MATCH_OPCODE, ...)`.
    Raises ValueError on an unrecognised map code.
    """
    map_id, is_craftland = _parse_map_code(map_code)

    if map_id is None:
        # No override → default BR template (Q07D64)
        return bytes.fromhex(MATCH_START_HEX_STANDARD)

    if is_craftland:
        if len(map_id) != 36:
            raise ValueError(
                f"Craftland map ID must be 36 chars, got {len(map_id)}: {map_id!r}"
            )
        template = MATCH_START_HEX_CRAFTLAND
        # 1. Replace the short 36-char ID
        old_short_hex = CRAFTLAND_DEFAULT_ID.encode().hex()
        new_short_hex = map_id.encode().hex()
        template = template.replace(old_short_hex, new_short_hex)
        # 2. Replace the full form "#FREEFIRE<id>/1"
        old_full_hex = f"#FREEFIRE{CRAFTLAND_DEFAULT_ID}/1".encode().hex()
        new_full_hex = f"#FREEFIRE{map_id}/1".encode().hex()
        template = template.replace(old_full_hex, new_full_hex)
        return bytes.fromhex(template)

    # Standard BR map
    if len(map_id) != 6:
        raise ValueError(
            f"BR map code must be 6 chars, got {len(map_id)}: {map_id!r}"
        )
    template = MATCH_START_HEX_STANDARD
    template = template.replace(
        STANDARD_DEFAULT_CODE.encode().hex(),
        map_id.encode().hex(),
    )
    return bytes.fromhex(template)


# ═══════════════════════════════════════════════════════════════════════
#  AES
# ═══════════════════════════════════════════════════════════════════════
def aes_enc(data, key=AES_KEY, iv=AES_IV):
    return AES.new(key, AES.MODE_CBC, iv).encrypt(pad(data, AES.block_size))

def aes_dec(data, key=AES_KEY, iv=AES_IV):
    if not data or len(data) % 16: return None
    try:
        try: return unpad(AES.new(key, AES.MODE_CBC, iv).decrypt(data), 16)
        except ValueError: return AES.new(key, AES.MODE_CBC, iv).decrypt(data)
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════════════════
#  CRC7
# ═══════════════════════════════════════════════════════════════════════
_CRC7_TABLE = bytes([
    0,9,18,27,36,45,54,63,72,65,90,83,108,101,126,119,25,16,11,2,61,52,47,38,
    81,88,67,74,117,124,103,110,50,59,32,41,22,31,4,13,122,115,104,97,94,87,
    76,69,43,34,57,48,15,6,29,20,99,106,113,120,71,78,85,92,100,109,118,127,
    64,73,82,91,44,37,62,55,8,1,26,19,125,116,111,102,89,80,75,66,53,60,39,
    46,17,24,3,10,86,95,68,77,114,123,96,105,30,23,12,5,58,51,40,33,79,70,
    93,84,107,98,121,112,7,14,21,28,35,42,49,56,65,72,83,90,101,108,119,126,
    9,0,27,18,45,36,63,54,88,81,74,67,124,117,110,103,16,25,2,11,52,61,38,
    47,115,122,97,104,87,94,69,76,59,50,41,32,31,22,13,4,106,99,120,113,78,
    71,92,85,34,43,48,57,6,15,20,29,37,44,55,62,1,8,19,26,109,100,127,118,
    73,64,91,82,60,53,46,39,24,17,10,3,116,125,102,111,80,89,66,75,23,30,5,
    12,51,58,33,40,95,86,77,68,123,114,105,96,14,7,28,21,42,35,56,49,70,79,
    84,93,98,107,112,121,
])

def crc7(buf, c=0):
    c &= 0x7F
    for b in buf:
        c = _CRC7_TABLE[((2 * c) ^ b) & 0xFF] & 0x7F
    return c

# ═══════════════════════════════════════════════════════════════════════
#  TEA
# ═══════════════════════════════════════════════════════════════════════
_M, _DELTA, _ROUNDS = 0xFFFFFFFF, 0x9E3779B9, 16

def _tea_enc(v0, v1, k):
    s = 0
    for _ in range(_ROUNDS):
        s = (s + _DELTA) & _M
        v0 = (v0 + ((((v1 << 4) + k[0]) & _M) ^ ((v1 + s) & _M) ^ ((v1 >> 5) + k[1]))) & _M
        v1 = (v1 + ((((v0 << 4) + k[2]) & _M) ^ ((v0 + s) & _M) ^ ((v0 >> 5) + k[3]))) & _M
    return v0, v1

def _tea_dec(v0, v1, k):
    s = (_DELTA * _ROUNDS) & _M
    for _ in range(_ROUNDS):
        v1 = (v1 - ((((v0 << 4) + k[2]) & _M) ^ ((v0 + s) & _M) ^ ((v0 >> 5) + k[3]))) & _M
        v0 = (v0 - ((((v1 << 4) + k[0]) & _M) ^ ((v1 + s) & _M) ^ ((v1 >> 5) + k[1]))) & _M
        s = (s - _DELTA) & _M
    return v0, v1

def _udp_pad(c):
    p = (8 - (len(c) + 10) % 8) % 8
    return bytes([p]) + bytes(p + 2) + c + bytes(7)

def udp_enc(content, key):
    padded = _udp_pad(content)
    k = struct.unpack("<4I", key)
    out = bytearray(len(padded))
    pc = pi = bytes(8)
    for i in range(0, len(padded), 8):
        d = bytes(a ^ b for a, b in zip(padded[i:i+8], pc))
        x = struct.pack("<2I", *_tea_enc(*struct.unpack("<2I", d), k))
        c = bytes(a ^ b for a, b in zip(x, pi))
        out[i:i+8] = c
        pc, pi = c, d
    return bytes(out)

def udp_dec(body, key):
    if not body or len(body) % 8: return b""
    k = struct.unpack("<4I", key)
    out = bytearray(len(body))
    pc = pi = bytes(8)
    for i in range(0, len(body), 8):
        x = bytes(a ^ b for a, b in zip(body[i:i+8], pi))
        d = struct.pack("<2I", *_tea_dec(*struct.unpack("<2I", x), k))
        out[i:i+8] = bytes(a ^ b for a, b in zip(d, pc))
        pc, pi = body[i:i+8], d
    return bytes(out)

def strip_tea_pad(pl):
    if len(pl) < 10: return pl
    p = pl[0]
    start = p + 3
    end = len(pl) - 7
    if start >= end: return b""
    return pl[start:end]

# ═══════════════════════════════════════════════════════════════════════
#  Protobuf
# ═══════════════════════════════════════════════════════════════════════
def _wv(v):
    if v < 0: v &= (1 << 64) - 1
    out = bytearray()
    while True:
        b = v & 0x7F; v >>= 7
        if v: b |= 0x80
        out.append(b)
        if not v: break
    return bytes(out)

def _fv(n, v): return _wv((n << 3) | 0) + _wv(int(v))
def _fs(n, s):
    b = s.encode() if isinstance(s, str) else bytes(s)
    return _wv((n << 3) | 2) + _wv(len(b)) + b
def _fb(n, b): return _wv((n << 3) | 2) + _wv(len(b)) + bytes(b)

def _serialize(d):
    out = bytearray()
    for k, v in d.items():
        if   isinstance(v, bool):               out += _fv(k, 1 if v else 0)
        elif isinstance(v, int):                out += _fv(k, v)
        elif isinstance(v, str):                out += _fs(k, v)
        elif isinstance(v, (bytes, bytearray)): out += _fb(k, v)
    return bytes(out)

def _rv(buf, off):
    r, s = 0, 0
    while True:
        b = buf[off]; off += 1
        r |= (b & 0x7F) << s
        if not (b & 0x80): return r, off
        s += 7

def _dec_len(raw, depth):
    if raw and all(32 <= b < 127 or b in (9, 10, 13) for b in raw):
        try: return raw.decode("utf-8")
        except Exception: pass
    if depth < 6 and len(raw) >= 2:
        try:
            sub, used = _parse(raw, depth + 1)
            if used == len(raw) and sub: return sub
        except Exception: pass
    try: return raw.decode("utf-8")
    except Exception: return raw

def _parse(buf, depth=0):
    out, off = {}, 0
    while off < len(buf):
        try: tag, off = _rv(buf, off)
        except Exception: break
        f, w = tag >> 3, tag & 7
        if f == 0: break
        try:
            if   w == 0: v, off = _rv(buf, off)
            elif w == 1: v = buf[off:off+8]; off += 8
            elif w == 2:
                ln, off = _rv(buf, off)
                chunk = buf[off:off+ln]; off += ln
                v = _dec_len(chunk, depth)
            elif w == 5: v = buf[off:off+4]; off += 4
            else: break
        except Exception: break
        if f in out:
            if isinstance(out[f], list): out[f].append(v)
            else: out[f] = [out[f], v]
        else: out[f] = v
    return out, off

def parse_proto(b):
    try: return _parse(b, 0)[0]
    except Exception: return {}

def _first(d, k, dflt=None):
    v = d.get(k, dflt)
    if isinstance(v, list): v = v[0] if v else dflt
    return v

def to_int(v, dflt=0):
    if isinstance(v, int): return v
    if isinstance(v, (bytes, bytearray)):
        try: return int(v.decode("ascii", "ignore"))
        except Exception: return dflt
    try: return int(v)
    except Exception: return dflt

def to_str(v, dflt=""):
    if isinstance(v, (bytes, bytearray)):
        try: return v.decode("utf-8", "ignore")
        except Exception: return dflt
    return v if isinstance(v, str) else str(v)

# ═══════════════════════════════════════════════════════════════════════
#  UDP framing
# ═══════════════════════════════════════════════════════════════════════
def non_reliable(mask):
    return "".join(c for c in mask if c != "2")

def build_pkt(cmd, content, key, mask, *, reliable=True, order_id=0,
              send_option=1, flags=1, msg_key=None, extra=b""):
    k0 = key[0]
    if extra: flags |= 4
    layout = [int(c) for c in (mask if reliable else non_reliable(mask))]
    body = udp_enc(content, key) if flags & 1 else content
    values = {0: send_option, 1: cmd, 2: order_id, 3: flags, 4: len(body)}
    head = b""
    for c in layout:
        v, n = values[c], FIELD_SIZES[c]
        head += (bytes([(v ^ k0) & 0xFF]) if n == 1
                 else struct.pack("<H", (v ^ ((k0 << 8) | k0)) & 0xFFFF))
    rest = head + bytes(b ^ k0 for b in extra) + body
    mk = msg_key if msg_key is not None else 90 + random.randrange(20)
    return bytes([mk, crc7(rest)]) + rest

def decode_header(pkt, key, mask):
    if len(pkt) < 8: return None
    k0 = key[0]
    v80 = ((k0 << 8) | k0) & 0xFFFF
    crc_ok = (pkt[1] & 0x7F) == crc7(pkt[2:])
    for m in (mask, non_reliable(mask)):
        off, f, valid = 2, {}, True
        for ch in m:
            c = int(ch); n = FIELD_SIZES[c]
            if off + n > len(pkt): valid = False; break
            if n == 1: f[FIELD_NAMES[c]] = pkt[off] ^ k0
            else: f[FIELD_NAMES[c]] = ((pkt[off] | (pkt[off+1] << 8)) ^ v80) & 0xFFFF
            off += n
        if not valid: continue
        if f["flags"] > 7 or f["sendOption"] > 7: continue
        extra = 3 if f["flags"] & 4 else 0
        if f["length"] != len(pkt) - off - extra: continue
        return {"mask": m, "header_len": off, "extra": extra,
                "crc_ok": crc_ok, "msg_key": pkt[0], "orderId": None, **f}
    return None

# ═══════════════════════════════════════════════════════════════════════
#  UDP packet builders
# ═══════════════════════════════════════════════════════════════════════
def pkt_hello(player_id, suffix, key, mask):
    data = f"{player_id}_{suffix}".encode()
    data = data.ljust(16, b"\x00")[:16]
    body = b"\x10\x00\x00\x00" + data + b"\x00" * 10
    return build_pkt(1, body, key, mask, reliable=True, order_id=0,
                     send_option=1, flags=1, msg_key=0x5E)

def pkt_ack(acked, key, mask, msg_key=0x68):
    body = struct.pack("<H", acked & 0xFFFF)
    return build_pkt(2, body, key, mask, reliable=False, order_id=0,
                     send_option=0, flags=1, msg_key=msg_key)

def pkt_auth3043(seq, account_id, match_id, key, mask, order_id, msg_key):
    body = bytes([seq & 0xFF]) + _wv(account_id) + _wv(match_id)
    return build_pkt(3043, body, key, mask, reliable=True, order_id=order_id,
                     send_option=2, flags=1, msg_key=msg_key)

def _build_auth448(acc, mid, rc, rn):
    o = bytearray()
    o += _wv(acc) + _wv(mid)
    o += bytes([0x01, 0x35])
    o += _wv(mid)
    o += bytes.fromhex("1900e907000000000102020101")
    o += _wv((len(b"IN") << 1)) + b"IN"
    o += bytes.fromhex("0002030001000004")
    o += _wv((len(CLIENT_VERSION) << 1)) + CLIENT_VERSION.encode()
    o += _wv((len(UDP_DEVICE_ID) << 1)) + UDP_DEVICE_ID.encode()
    o += b"\x00" * 8
    o += _wv((len(rc) << 1)) + rc.encode()
    o += _wv((len(rn) << 1)) + rn.encode()
    o += bytes.fromhex(
        "000000000000000000000100000000000100000000000c00a0c5c7d7ebe598df110200")
    o += _wv((len(UDP_CLIENT_IP) << 1)) + UDP_CLIENT_IP.encode()
    o += _wv((len(UDP_DEVICE_HASH) << 1)) + UDP_DEVICE_HASH.encode()
    o += b"\x00"
    return bytes(o)

def pkt_auth448(acc, mid, rc, rn, key, mask, order_id=2, msg_key=0x60):
    body = _build_auth448(acc, mid, rc, rn)
    return build_pkt(448, body, key, mask, reliable=True, order_id=order_id,
                     send_option=2, flags=1, msg_key=msg_key)

def pkt_ping(tick, key, mask, msg_key=None):
    if msg_key is None: msg_key = random.choice(PING_MSG_KEYS)
    body = _wv(tick & 0xFFFFFFFF) + b"\x00\x00\x00"
    return build_pkt(3, body, key, mask, reliable=False, order_id=0,
                     send_option=0, flags=0, msg_key=msg_key)

def pkt_idle(key, mask, msg_key=0x64):
    body = struct.pack("<I", int(time.time() * 1000) & 0xFFFFFFFF) + b"\x00" * 4
    return build_pkt(4, body, key, mask, reliable=False, order_id=0,
                     send_option=0, flags=1, msg_key=msg_key)

# ═══════════════════════════════════════════════════════════════════════
#  cmd=176 parser
# ═══════════════════════════════════════════════════════════════════════
def parse_player_sync(plain):
    if not plain: return 0, 0, (0.0, 0.0, 0.0)
    def _pos_from_bytes(b):
        if isinstance(b, (bytes, bytearray)) and len(b) >= 12:
            return struct.unpack("<fff", b[:12])
        return None
    def _pos_from_nested(b):
        if not isinstance(b, (bytes, bytearray)): return None
        try:
            pn, _ = _parse(b, 0)
            def _f(v):
                if isinstance(v, list): v = v[0] if v else b""
                if isinstance(v, (bytes, bytearray)) and len(v) >= 4:
                    return struct.unpack("<f", v[:4])[0]
                return 0.0
            return (_f(pn.get(1)), _f(pn.get(2)), _f(pn.get(3)))
        except Exception:
            return None
    try:
        node, _ = _parse(plain, 0)
        if isinstance(node, dict) and node:
            def g(f):
                v = node.get(f)
                if isinstance(v, list): return v[0] if v else None
                return v
            eid, aid, pos_raw = g(1), g(2), g(3)
            if isinstance(eid, int) and 0 < eid < 100000:
                pos = (_pos_from_bytes(pos_raw) or _pos_from_nested(pos_raw)
                       or (0.0, 0.0, 0.0))
                return int(eid), int(aid if isinstance(aid, int) else 0), pos
    except Exception:
        pass
    if len(plain) >= 24:
        try:
            eid, aid, x, y, z, _st = struct.unpack("<IIfffI", plain[:24])
            if 0 < eid < 100000:
                return int(eid), int(aid), (x, y, z)
        except Exception:
            pass
    if len(plain) >= 8:
        try:
            eid, aid = struct.unpack("<II", plain[:8])
            if 0 < eid < 100000:
                return int(eid), int(aid), (0.0, 0.0, 0.0)
        except Exception:
            pass
    return 0, 0, (0.0, 0.0, 0.0)

# ═══════════════════════════════════════════════════════════════════════
#  Recursive UDP-server finder
# ═══════════════════════════════════════════════════════════════════════
import re
_IP_PORT_RE = re.compile(rb"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{1,5})")

def _valid_ip_port(host, port):
    try:
        parts = host.split(".")
        if len(parts) != 4: return False
        if not all(0 <= int(p) <= 255 for p in parts): return False
        n = int(port)
        if not (1 <= n <= 65535): return False
        if parts[0] in ("0", "127") and parts[3] == "1": return False
        return True
    except Exception:
        return False

def find_udp_server(tree, depth=0, _seen=None):
    if _seen is None: _seen = set()
    if id(tree) in _seen: return None
    _seen.add(id(tree))
    if depth > 10: return None
    if isinstance(tree, dict):
        for v in tree.values():
            if isinstance(v, str):
                h, s, p = v.partition(":")
                if s and _valid_ip_port(h, p): return v
            r = find_udp_server(v, depth + 1, _seen)
            if r: return r
    elif isinstance(tree, list):
        for item in tree:
            r = find_udp_server(item, depth + 1, _seen)
            if r: return r
    elif isinstance(tree, (bytes, bytearray)):
        m = _IP_PORT_RE.search(bytes(tree))
        if m:
            h = m.group(1).decode(); p = m.group(2).decode()
            if _valid_ip_port(h, p): return f"{h}:{p}"
        sub = parse_proto(bytes(tree))
        if sub: return find_udp_server(sub, depth + 1, _seen)
    return None

def _resolve_mask(raw):
    if raw and len(raw) >= 5:
        cand = raw[-5:]
        if sorted(cand) == list("01234"):
            info(f"mask resolved: {raw!r} → {cand!r}")
            return cand
    if raw: warn(f"mask {raw!r} invalid → {DEFAULT_MASK}")
    return DEFAULT_MASK

# ═══════════════════════════════════════════════════════════════════════
#  DNS
# ═══════════════════════════════════════════════════════════════════════
async def resolve_host(h):
    if not h: return h
    p = h.split(".")
    if len(p) == 4 and all(x.isdigit() and 0 <= int(x) <= 255 for x in p):
        return h
    try:
        inf = await asyncio.get_running_loop().getaddrinfo(h, None,
                                                          family=socket.AF_INET)
        if inf: return inf[0][4][0]
    except Exception: pass
    return h


def _oauth(uid, pwd):
    r = requests.post(OAUTH_URL, headers={
        "User-Agent": "GarenaMSDK/4.0.19P9(SM-M526B ;Android 13;en;IN;)",
        "Content-Type": "application/x-www-form-urlencoded",
        "Connection": "close",
    }, data={
        "uid": uid, "password": pwd, "response_type": "token",
        "client_type": "2", "client_secret": CLIENT_SECRET, "client_id": APP_ID,
    }, timeout=20, verify=False)
    if r.status_code != 200:
        raise RuntimeError(f"OAuth HTTP {r.status_code}: {r.text[:200]}")
    j = r.json()
    oid, at = j.get("open_id"), j.get("access_token")
    if not oid or not at:
        raise RuntimeError(f"OAuth missing: {j}")
    return oid, at

def _build_major(open_id, at, platform="4"):
    now = str(datetime.now())[:-7]
    return _serialize({
        3: now, 4: "free fire", 5: 1, 7: CLIENT_VERSION,
        8: "Android OS 10 / API-29", 9: "Handheld",
        10: "Vi India", 11: "WIFI", 12: 1600, 13: 720, 14: "320",
        15: "ARM64 FP ASIMD AES | 2301 | 8", 16: 2799,
        17: "PowerVR Rogue GE8320", 18: "OpenGL ES 3.2 build 1.11@5425693",
        19: "Google|9f7d6b8b-b10c-454a-852d-06332cd498eb",
        20: "151.158.158.220", 21: "en", 22: open_id, 23: str(platform),
        24: "Handheld", 25: "realme RMX2189", 26: REGION,
        29: at, 30: 1, 41: "Vi India", 42: "WIFI",
        57: "1ac4b80ecf0478a44203bf8fac6120f5",
        60: 19799, 61: 2536, 62: 5056, 64: 2768,
        65: 19999, 66: 2536, 67: 19799, 73: 1,
        74: "/data/app/com.dts.freefiremax-ShI7E0dK8p1IiZ785pvuVQ==/lib/arm64",
        76: 2, 77: "apk", 78: 2, 79: 2, 81: "64", 83: "2019118527",
        86: "OpenGLES3", 87: 3071, 88: 4, 92: 67920, 93: "android_max",
        94: "KqsHT+UrR1HKqb6+1db+Ofei+NtZr2+hbiBo3yKDL8w+8E3S5qF2IgEEe1fFQFyHRzl4iyHjHp+QsfeLbjJ6+DidTiKxm0ak2uYYa6QR4nAUdlZR",
        95: 111107, 96: '{"cur_rate":null,"support_etc2":true}',
        97: 1, 98: 1, 99: str(platform), 100: str(platform), 102: "",
        104: 83812, 105: 1,
        106: "https://dl-bs.ggpolarbear.com/live/ABHotUpdates/|https://core-bs.ggpolarbear.com/live/ABHotUpdates/|a4332cb1c1a84e51dd77441e4856ed5a",
        107: "1.9393e7b8e53e8aeb",
    })

def _extract_major(raw):
    for off in (0, 64):
        if len(raw) <= off: continue
        root = parse_proto(raw[off:])
        jwt = _first(root, 8)
        if isinstance(jwt, bytes):
            try: jwt = jwt.decode("utf-8", "ignore")
            except Exception: jwt = None
        if not (isinstance(jwt, str) and jwt.count(".") == 2 and len(jwt) > 100):
            continue
        def _b(f):
            v = _first(root, f)
            if isinstance(v, (bytes, bytearray)) and len(v) == 16: return bytes(v)
            if isinstance(v, str) and len(v) == 32:
                try: return bytes.fromhex(v)
                except Exception: pass
            return None
        key, iv = _b(22), _b(23)
        if not key or not iv: continue
        acc = to_int(_first(root, 1), 0)
        kts = to_int(_first(root, 21), int(time.time()))
        return jwt, key, iv, acc, kts
    raise RuntimeError("JWT/keys not found in MajorLogin")

def _major_login(open_id, at):
    body = aes_enc(_build_major(open_id, at, "4"))
    r = requests.post(MAJOR_LOGIN_URL, data=body, headers={
        "Host": "loginbp.ppmainecoonghj.com",
        "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "Accept": "*/*", "X-GA-SV": str(int(time.time())),
        "Authorization": "Bearer ", "X-GA": "v1 1",
        "ReleaseVersion": RELEASE_VERSION,
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Unity-Version": "2018.4.12f1",
        "Content-Length": str(len(body)),
    }, timeout=30, verify=False)
    if r.status_code != 200:
        raise RuntimeError(f"MajorLogin HTTP {r.status_code}")
    return _extract_major(r.content)

def _get_login_data(jwt, open_id):
    body = aes_enc(_serialize({
        3: time.strftime("%Y-%m-%d %H:%M:%S"),
        4: "free fire", 5: 1, 7: CLIENT_VERSION,
        8: "Android OS 15 / API-35", 9: "Handheld",
        10: "airtel", 11: "CarrierDataNetwork", 12: 2400, 13: 1080, 14: "400",
        15: "ARM64 FP ASIMD AES | 4800 | 8", 16: 7461,
        17: "Adreno (TM) 710", 18: "OpenGL ES 3.2",
        19: "Google|0ea5b865-94e6-42dd-9f40-f52a63378bc7",
        20: "27.59.76.15", 21: "en", 22: open_id, 23: "4", 24: "Handheld",
        25: "motorola moto g96 5G", 26: REGION,
        29: jwt, 30: 1, 41: "airtel", 42: "4G",
        57: "1ac4b80ecf0478a44203bf8fac6120f5",
        60: 111316, 61: 73226, 62: 690, 64: 73354,
        65: 111316, 66: 73354, 67: 111316, 73: 3,
        74: "/data/app/arm64", 76: 2, 77: "apk", 78: 2, 79: 2,
        81: "64", 83: "2019118527", 85: 3,
        86: "OpenGLES3", 87: 4095, 88: 4,
        90: "New Delhi", 91: "DL", 92: 12573, 93: "android_max",
        94: "KqsHT0wswggWbev04P17TGvl/w+c875HviaAj4qL+YhymI7Psonj/aQgNTNLf+4nm1LzYE6DxxzpZH9FX0Kt119zYuGdivC/V7aWpj4/HNhVAwJy",
        95: 111107, 96: '{"cur_rate":[60,90,120,144],"support_etc2":true}',
        97: 1, 99: "0", 100: "4", 102: "",
        104: 95926, 105: 1,
        106: "https://dl.cdn.freefiremobile.com/live/ABHotUpdates/|https://dl-core.cdn.freefiremobile.com/live/ABHotUpdates/|a4332cb1c1a84e51dd77441e4856ed5a",
        107: "1.7a99d677ab872769",
    }))
    r = requests.post(f"https://{GETLOGIN_HOST}/GetLoginData", data=body, headers={
        "Host": GETLOGIN_HOST,
        "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "Accept": "*/*", "X-GA-SV": str(int(time.time())),
        "Authorization": f"Bearer {jwt}", "X-GA": "v1 1",
        "ReleaseVersion": RELEASE_VERSION,
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Unity-Version": "2018.4.12f1",
        "Content-Length": str(len(body)),
    }, timeout=20, verify=False)
    if r.status_code != 200:
        raise RuntimeError(f"GetLoginData HTTP {r.status_code}")
    raw = r.content
    for off in (0, 64):
        if len(raw) <= off: continue
        root = parse_proto(raw[off:])
        online = _first(root, 14)
        if isinstance(online, bytes):
            try: online = online.decode("utf-8", "ignore")
            except Exception: online = ""
        if isinstance(online, str) and ":" in online:
            return {"online": online,
                    "region": to_str(_first(root, 3), REGION).upper(),
                    "nickname": to_str(_first(root, 4), "")}
    raise RuntimeError("no online_ip_port in GetLoginData")

def login(uid, pwd):
    oid, at = _oauth(uid, pwd)
    jwt, key, iv, acc, kts = _major_login(oid, at)
    gld = _get_login_data(jwt, oid)
    host, port = gld["online"].rsplit(":", 1)
    return {"jwt": jwt, "key": key, "iv": iv, "acc_id": acc, "kts": kts,
            "host": host, "port": int(port),
            "region": gld["region"], "nickname": gld["nickname"]}


class FrameReader:
    def __init__(self, r):
        self.r = r; self.buf = b""
    async def _fill(self, n, t):
        if len(self.buf) >= n: return True
        end = time.time() + t
        while len(self.buf) < n:
            rem = end - time.time()
            if rem <= 0: return False
            try:
                ch = await asyncio.wait_for(self.r.read(65536), timeout=rem)
            except asyncio.TimeoutError:
                return False
            if not ch: return False
            self.buf += ch
        return True
    async def next_frame(self, timeout=1.5):
        if not await self._fill(5, timeout): return None
        op = self.buf[:2]
        ln = int.from_bytes(self.buf[2:5], "big")
        if ln <= 0 or ln > 200000:
            self.buf = self.buf[1:]; return None
        if not await self._fill(5 + ln, timeout): return None
        p = self.buf[5:5+ln]; self.buf = self.buf[5+ln:]
        return op, p

def region_byte(region):
    r = str(region).upper()
    return 0x19 if r == "BD" else 0x14 if r == "IND" else 0x15

def tcp_frame(opcode_hex, plain, key, iv):
    enc = aes_enc(plain, key, iv)
    return bytes.fromhex(opcode_hex) + len(enc).to_bytes(4, "big") + enc


async def tcp_heartbeat(writer, stop, s, interval=1.0):
    hb = tcp_frame("0e14", bytes.fromhex("080f"), s["key"], s["iv"])
    while not stop.is_set():
        try:
            if writer.is_closing(): return
            writer.write(hb); await writer.drain()
        except Exception:
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval); return
        except asyncio.TimeoutError: pass