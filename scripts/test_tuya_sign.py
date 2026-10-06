import time
import hashlib
import hmac
import urllib.request
import json
import os
from urllib.parse import urlparse, parse_qs

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

def tuya_request(url_path, token=None):
    t = str(int(time.time() * 1000))
    http_method = "GET"
    content_sha256 = hashlib.sha256("".encode('utf-8')).hexdigest()
    
    # Tuya OpenAPI 2.0 Signature format for URL with query params
    string_to_sign = f"{http_method}\n{content_sha256}\n\n{url_path}"
    
    str_to_hash = client_id + (token if token else "") + t + string_to_sign
    sign = hmac.new(secret.encode('utf-8'), str_to_hash.encode('utf-8'), hashlib.sha256).hexdigest().upper()
    
    headers = {
        "client_id": client_id,
        "sign": sign,
        "t": t,
        "sign_method": "HMAC-SHA256"
    }
    if token:
        headers["access_token"] = token
        
    req = urllib.request.Request(endpoint + url_path, headers=headers)
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        return {"error": str(e)}

res_token = tuya_request("/v1.0/token?grant_type=1")
token = res_token.get("result", {}).get("access_token")
print("Token acquired:", token[:10] if token else "FAILED")

device_id = "a31a5c95501212e37f2fld"
end_t = int(time.time() * 1000)
start_t = end_t - (7 * 86400 * 1000)

test_urls = [
    f"/v1.0/devices/{device_id}/status",
    f"/v1.0/devices/{device_id}/logs?start_time={start_t}&end_time={end_t}&type=7",
    f"/v1.0/iot-03/energy/{device_id}/daily?start_day=20261001&end_day=20261005",
    f"/v1.0/devices/{device_id}/energy/daily?start_day=20261001&end_day=20261005",
]

for path in test_urls:
    res = tuya_request(path, token)
    print(f"\n[{path}] =>")
    print(json.dumps(res, indent=2)[:500])
