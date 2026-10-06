import time
import hashlib
import hmac
import urllib.request
import json
import os

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(base_dir, ".env")

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

t = str(int(time.time() * 1000))
http_method = "GET"
content_sha256 = hashlib.sha256("".encode('utf-8')).hexdigest()
url = "/v1.0/token?grant_type=1"
string_to_sign = f"{http_method}\n{content_sha256}\n\n{url}"
str_to_hash = client_id + t + string_to_sign

sign = hmac.new(secret.encode('utf-8'), str_to_hash.encode('utf-8'), hashlib.sha256).hexdigest().upper()
req = urllib.request.Request(endpoint + url, headers={
    "client_id": client_id, "sign": sign, "t": t, "sign_method": "HMAC-SHA256"
})
res = json.loads(urllib.request.urlopen(req).read().decode('utf-8'))
token = res["result"]["access_token"]

for dev_id in ["a31a5c95501212e37f2fld", "a37cb5dfb4d22c266f7wxo"]:
    ep = f"/v1.0/devices/{dev_id}"
    t = str(int(time.time() * 1000))
    string_to_sign = f"{http_method}\n{content_sha256}\n\n{ep}"
    str_to_hash = client_id + token + t + string_to_sign
    sign = hmac.new(secret.encode('utf-8'), str_to_hash.encode('utf-8'), hashlib.sha256).hexdigest().upper()
    req_dev = urllib.request.Request(endpoint + ep, headers={
        "client_id": client_id, "access_token": token, "sign": sign, "t": t, "sign_method": "HMAC-SHA256"
    })
    dev_res = json.loads(urllib.request.urlopen(req_dev).read().decode('utf-8'))

    res_data = dev_res.get("result", {})
    print(f"\n=== DEVICE {dev_id} ({res_data.get('name')}) ===")
    print(f"Online Status : {res_data.get('online')}")
    print(f"IP            : {res_data.get('ip')}")
    print("Telemetry DPS :")
    for s in res_data.get("status", []):
        if s["code"] in ["switch_1", "cur_power", "cur_voltage", "cur_current"]:
            print(f"  {s['code']}: {s['value']}")

