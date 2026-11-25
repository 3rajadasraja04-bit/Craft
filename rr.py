import json
import os
import time
import requests
import urllib3
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 🎨 COLORS
NEON_CYAN = "\033[96m"
NEON_GREEN = "\033[92m"
NEON_RED = "\033[91m"
NEON_YELLOW = "\033[93m"
RESET = "\033[0m"

JWT_CACHE_FILE = "jwt.json"
KEY = b'Yg&tc%DEuh6%Zc^8'
IV  = b'6oyZDr22E3ychjM%'

# ─────────────────────── PROTO HELPERS ───────────────────────
def _enc_varint(n):
    if n < 0: return b''
    out = []
    while True:
        b = n & 0x7F; n >>= 7
        if n: b |= 0x80
        out.append(b)
        if not n: break
    return bytes(out)

def _varint_field(fn, v):
    return _enc_varint((fn << 3) | 0) + _enc_varint(v)

def _length_field(fn, v):
    d = v.encode() if isinstance(v, str) else v
    return _enc_varint((fn << 3) | 2) + _enc_varint(len(d)) + d

def _build_proto(fields):
    buf = bytearray()
    for f, v in fields.items():
        if isinstance(v, dict):
            buf.extend(_length_field(f, _build_proto(v)))
        elif isinstance(v, int):
            buf.extend(_varint_field(f, v))
        elif isinstance(v, (str, bytes)):
            buf.extend(_length_field(f, v))
    return bytes(buf)

def _aes_enc(data, key, iv):
    return AES.new(key, AES.MODE_CBC, iv).encrypt(pad(data, AES.block_size))

# ─────────────────────── LOAD JWT ───────────────────────
def load_jwt_cache():
    if not os.path.exists(JWT_CACHE_FILE):
        print(f"{NEON_RED}❌ jwt.json not found! Pehle 1.py run karke JWT generate karo.{RESET}")
        return {}
    try:
        with open(JWT_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"{NEON_RED}❌ Error reading jwt.json: {e}{RESET}")
        return {}

# ─────────────────────── MAIN TEST ───────────────────────
def main():
    print(f"\n{NEON_CYAN}====== ⭐ SUBSCRIBE TESTER ⭐ ======{RESET}")
    print("Ye tool jwt.json se JWT uthayega aur Map Subscribe karega.\n")
    
    cache = load_jwt_cache()
    if not cache:
        return

    total_accounts = len(cache)
    print(f"{NEON_GREEN}✅ {total_accounts} accounts mile jwt.json mein.{RESET}\n")

    # Target Map Code Input
    print(f"{NEON_YELLOW}▶ ENTER TARGET MAP CODE (e.g., FREEFIRE...):{RESET}")
    target_map = input("> ").strip()
    
    if not target_map:
        print(f"{NEON_RED}❌ Map Code required!{RESET}")
        return

    # 🔥 FIX: ULTIMATE CLEANING LOGIC
    clean_target = str(target_map).strip()
    
    # 1. Remove '#' if it's at the start
    if clean_target.startswith("#"): 
        clean_target = clean_target[1:]
        
    # 2. Remove 'FREEFIRE'
    clean_target = clean_target.replace("FREEFIRE", "")
    
    # 3. Remove anything after '/' (like /1)
    if "/" in clean_target: 
        clean_target = clean_target.split("/")[0]
        
    clean_target = clean_target.strip().upper()

    print(f"\n{NEON_CYAN}Parsed Map ID (sending to server): {NEON_GREEN}{clean_target}{RESET}\n")
    
    # Proto Payload Build ({1: 1, 2: clean_target, 3: 26, 4: "en"})
    proto = _build_proto({1: 1, 2: clean_target, 3: 26, 4: "en"})
    encrypted = _aes_enc(proto, KEY, IV)

    print(f"{NEON_CYAN}Test Start...{RESET}\n")

    for idx, (uid, data) in enumerate(cache.items(), 1):
        jwt = data.get("jwt", "")
        region = data.get("region", "IND").upper()
        acc_id = data.get("acc_id", "?")

        if not jwt:
            print(f"[{idx}/{total_accounts}] {NEON_RED}UID {uid} -> JWT Missing!{RESET}")
            continue

        # Region base url
        if region == "IND":
            base_url = "https://client.ind.freefiremobile.com/"
        elif region in ("BR", "US", "NA", "SAC"):
            base_url = "https://client.us.freefiremobile.com/"
        else:
            base_url = "https://clientbp.ppmainecoonghj.com/"

        url = base_url.rstrip("/") + "/SubscribeWorkshopCode"

        headers = {
            "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
            "Accept": "*/*", 
            "Accept-Encoding": "deflate, gzip",
            "X-Ga-Sv": "1795270504", 
            "Authorization": f"Bearer {jwt}",
            "X-Ga": "v1 1", 
            "ReleaseVersion": "OB55", 
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Unity-Version": "2018.4.12f1",
        }

        print(f"[{idx}/{total_accounts}] UID: {uid} | Acc_ID: {acc_id} ...", end=" ", flush=True)

        try:
            resp = requests.post(url, headers=headers, data=encrypted, verify=False, timeout=15)
            
            if resp.status_code == 200:
                if b"SUCCESS" in resp.content or len(resp.content) > 0:
                    print(f"{NEON_GREEN}✅ SUCCESS (HTTP 200){RESET}")
                else:
                    print(f"{NEON_GREEN}✅ HTTP 200 (Empty Body){RESET}")
            elif resp.status_code == 400:
                print(f"{NEON_RED}❌ HTTP 400 (Bad Request){RESET}")
                print(f"    └─ Response: {resp.content[:100]}")
            else:
                print(f"{NEON_RED}❌ HTTP {resp.status_code}{RESET}")
                print(f"    └─ Response: {resp.content[:100]}")
                
        except Exception as e:
            print(f"{NEON_RED}❌ Network Error{RESET}")
            print(f"    └─ {str(e)}")
            
        time.sleep(0.5) # Thoda delay taaki server rate limit na lagaye

    print(f"\n{NEON_CYAN}====== TEST COMPLETE ======{RESET}\n")

if __name__ == "__main__":
    main()