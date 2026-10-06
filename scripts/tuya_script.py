import sys
import json
# pyrefly: ignore [missing-import]
import tinytuya

def main():
    if len(sys.argv) < 5:
        print("Usage: python tuya_script.py <device_id> <ip> <local_key> <payload_json>")
        sys.exit(1)

    device_id = sys.argv[1]
    ip = sys.argv[2]
    local_key = sys.argv[3]
    payload_raw = sys.argv[4]

    try:
        dps = json.loads(payload_raw)
    except Exception as e:
        print(f"Error parsing JSON payload: {e}")
        sys.exit(1)

    try:
        # Define the device
        d = tinytuya.OutletDevice(device_id, ip, local_key)
        d.set_version(3.3) # default version

        # Send command. Example dps: {"1": True}
        # tinytuya handles setting specific DPS values.
        # Here we just iterate over the parsed dict and set each dps
        for key, value in dps.items():
            if type(value) == bool:
                d.set_status(value, int(key))
            else:
                d.set_value(int(key), value)
                
        print("Success")
    except Exception as e:
        print(f"Error connecting to Tuya device: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
