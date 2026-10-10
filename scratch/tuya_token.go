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

	uid := "bay17913100056154MCI"
	_ = uid
	
	ep := "https://openapi-sg.iotbing.com"
	connector.InitWithOptions(env.WithAccessID(accessID), env.WithAccessKey(accessKey), env.WithApiHost(ep), env.WithMsgHost(ep))

	// Test GET /v1.0/iot-01/associated-users/devices
	{
		var resp interface{}
		err := connector.MakeGetRequest(context.Background(), connector.WithAPIUri("/v1.0/iot-01/associated-users/devices"), connector.WithResp(&resp))
		fmt.Printf("GET /v1.0/iot-01/associated-users/devices: Err=%v, Resp=%v\n", err, resp)
	}
}



