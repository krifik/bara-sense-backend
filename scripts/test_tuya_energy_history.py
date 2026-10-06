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

device_id = "a37cb5dfb4d22c266f7wxo"
today_str = time.strftime("%Y%m%d")
end_t = int(time.time() * 1000)
start_t = end_t - (7 * 86400 * 1000)

endpoints = [
    f"/v1.0/m/electricity/devices/{device_id}/daily?start_day=20261001&end_day={today_str}",
    f"/v1.0/m/electricity/devices/{device_id}/hours?date={today_str}",
    f"/v1.0/devices/{device_id}/electricity/daily?start_day=20261001&end_day={today_str}",
    f"/v1.0/iot-03/energy/devices/{device_id}/daily?start_day=20261001&end_day={today_str}",
    f"/v1.0/devices/{device_id}/logs?codes=add_ele,cur_power,switch_1&start_time={start_t}&end_time={end_t}&size=20",
    f"/v1.0/iot-01/associated-users/devices",
]

for ep in endpoints:
    t = str(int(time.time() * 1000))
    string_to_sign = f"{http_method}\n{content_sha256}\n\n{ep}"
    str_to_hash = client_id + token + t + string_to_sign
    sign = hmac.new(secret.encode('utf-8'), str_to_hash.encode('utf-8'), hashlib.sha256).hexdigest().upper()
    req_ep = urllib.request.Request(endpoint + ep, headers={
        "client_id": client_id, "access_token": token, "sign": sign, "t": t, "sign_method": "HMAC-SHA256"
    })
    try:
        r = json.loads(urllib.request.urlopen(req_ep).read().decode('utf-8'))
        print(f"\n[CODE {r.get('code', 'OK')}] {ep} =>")
        print(json.dumps(r, indent=2)[:500])
    except urllib.error.HTTPError as e:
        print(f"\n[HTTP ERROR {e.code}] {ep} => {e.read().decode('utf-8')[:300]}")
    except Exception as e:
        print(f"\n[ERROR] {ep} => {e}")
