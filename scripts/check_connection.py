import time
import hashlib
import hmac
import urllib.request
import json
import os
import sys

def main():
    # Cari file .env di direktori backend
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(base_dir, ".env")
    
    if not os.path.exists(env_path):
        print(f"[ERROR] File .env tidak ditemukan di: {env_path}")
        sys.exit(1)

    config = {}
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                config[k.strip()] = v.strip().strip("'\"")

    client_id = config.get("TUYA_ACCESS_ID", "")
    secret = config.get("TUYA_ACCESS_KEY", "")
    endpoint = config.get("TUYA_ENDPOINT", "https://openapi-sg.iotbing.com").rstrip("/")

    print("==========================================")
    print("      TUYA CLOUD API CONNECTION CHECK     ")
    print("==========================================")
    print(f"Endpoint  : {endpoint}")
    print(f"Access ID : {client_id}")
    print("------------------------------------------")

    if not client_id or not secret:
        print("[ERROR] TUYA_ACCESS_ID atau TUYA_ACCESS_KEY belum diisi di .env!")
        sys.exit(1)

    # 1. Test Get Token
    print("[1/2] Menguji autentikasi dan pengambilan Access Token...")
    t = str(int(time.time() * 1000))
    http_method = "GET"
    content_sha256 = hashlib.sha256("".encode('utf-8')).hexdigest()
    url = "/v1.0/token?grant_type=1"
    string_to_sign = f"{http_method}\n{content_sha256}\n\n{url}"
    str_to_hash = client_id + t + string_to_sign

    sign = hmac.new(
        secret.encode('utf-8'),
        str_to_hash.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()

    headers = {
        "client_id": client_id,
        "sign": sign,
        "t": t,
        "sign_method": "HMAC-SHA256"
    }

    token_url = endpoint + url
    req = urllib.request.Request(token_url, headers=headers, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            body = response.read().decode('utf-8')
            res = json.loads(body)
            if not res.get("success"):
                print(f"[GAGAL] Error dari Tuya: {res.get('msg')} (Code: {res.get('code')})")
                return

            access_token = res["result"]["access_token"]
            expire_time = res["result"]["expire_time"]
            uid = res["result"]["uid"]
            print(f"[OK] Token berhasil diperoleh!")
            print(f"     UID          : {uid}")
            print(f"     Token        : {access_token[:10]}...")
            print(f"     Masa Berlaku : {expire_time} detik ({expire_time // 60} menit)")
    except urllib.error.HTTPError as e:
        print(f"[GAGAL] HTTP Error {e.code}: {e.read().decode('utf-8')}")
        return
    except Exception as e:
        print(f"[GAGAL] Koneksi gagal: {e}")
        return

    # 2. Test Get Devices
    print("\n[2/2] Menguji izin data center dan mengambil daftar perangkat...")
    t = str(int(time.time() * 1000))
    dev_url = f"/v1.0/users/{uid}/devices"
    string_to_sign = f"{http_method}\n{content_sha256}\n\n{dev_url}"
    str_to_hash = client_id + access_token + t + string_to_sign

    sign = hmac.new(
        secret.encode('utf-8'),
        str_to_hash.encode('utf-8'),
        hashlib.sha256
    ).hexdigest().upper()

    dev_headers = {
        "client_id": client_id,
        "access_token": access_token,
        "sign": sign,
        "t": t,
        "sign_method": "HMAC-SHA256"
    }

    req_dev = urllib.request.Request(endpoint + dev_url, headers=dev_headers, method="GET")
    try:
        with urllib.request.urlopen(req_dev, timeout=10) as response:
            body = response.read().decode('utf-8')
            dev_res = json.loads(body)
            if dev_res.get("success"):
                devices = dev_res.get("result", [])
                print(f"[BERHASIL] Layanan IoT Core aktif & terhubung!")
                print(f"           Jumlah perangkat ditemukan: {len(devices)}")
                for idx, d in enumerate(devices, 1):
                    print(f"           {idx}. [{d.get('name')}] ID: {d.get('id')} | Status: {'Online' if d.get('online') else 'Offline'}")
            else:
                print(f"[PERINGATAN] Respons Tuya: {dev_res.get('msg')} (Code: {dev_res.get('code')})")
    except urllib.error.HTTPError as e:
        print(f"[GAGAL] HTTP Error {e.code}: {e.read().decode('utf-8')}")
    except Exception as e:
        print(f"[GAGAL] Request gagal: {e}")

    print("\n==========================================")

if __name__ == "__main__":
    main()
