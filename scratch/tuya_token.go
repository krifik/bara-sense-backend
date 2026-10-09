package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"os"

	"github.com/tuya/tuya-connector-go/connector"
	"github.com/tuya/tuya-connector-go/connector/env"
)

func main() {
	b, _ := ioutil.ReadFile(".env")
	content := strings.ReplaceAll(string(b), "\r", "")
	lines := strings.Split(content, "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "TUYA_ACCESS_ID=") {
			os.Setenv("TUYA_ACCESS_ID", strings.TrimPrefix(l, "TUYA_ACCESS_ID="))
		}
		if strings.HasPrefix(l, "TUYA_ACCESS_KEY=") {
			os.Setenv("TUYA_ACCESS_KEY", strings.TrimPrefix(l, "TUYA_ACCESS_KEY="))
		}
		if strings.HasPrefix(l, "TUYA_ENDPOINT=") {
			os.Setenv("TUYA_ENDPOINT", strings.TrimPrefix(l, "TUYA_ENDPOINT="))
		}
	}

	accessID := os.Getenv("TUYA_ACCESS_ID")
	accessKey := os.Getenv("TUYA_ACCESS_KEY")
	endpoint := os.Getenv("TUYA_ENDPOINT")

	if endpoint == "" {
		endpoint = "https://openapi.tuyaus.com"
	}

	connector.InitWithOptions(env.WithAccessID(accessID), env.WithAccessKey(accessKey), env.WithApiHost(endpoint), env.WithMsgHost(endpoint))

	// Test 1: /v1.0/devices/tokens
	var resp1 interface{}
	err := connector.MakePostRequest(context.Background(), connector.WithAPIUri("/v1.0/devices/tokens"), connector.WithPayload([]byte(`{}`)), connector.WithResp(&resp1))
	fmt.Printf("Test 1 (/v1.0/devices/tokens): Err=%v, Resp=%v\n", err, resp1)

	// Test 2: /v1.0/device/paring/token
	var resp2 interface{}
	err = connector.MakePostRequest(context.Background(), connector.WithAPIUri("/v1.0/device/paring/token"), connector.WithPayload([]byte(`{}`)), connector.WithResp(&resp2))
	fmt.Printf("Test 2 (/v1.0/device/paring/token): Err=%v, Resp=%v\n", err, resp2)

	// Test 3: /v1.0/iot-03/device-registration/token
	var resp3 interface{}
	err = connector.MakePostRequest(context.Background(), connector.WithAPIUri("/v1.0/iot-03/device-registration/token"), connector.WithPayload([]byte(`{"pairing_type":"BLE", "time_zone_id":"Asia/Jakarta"}`)), connector.WithResp(&resp3))
	fmt.Printf("Test 3 (/v1.0/iot-03/device-registration/token): Err=%v, Resp=%v\n", err, resp3)

}
