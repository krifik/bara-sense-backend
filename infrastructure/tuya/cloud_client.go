package tuya

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/tuya/tuya-connector-go/connector"
	"github.com/tuya/tuya-connector-go/connector/env"
)

// CloudClient handles communication with Tuya devices via Tuya Cloud OpenAPI
type CloudClient struct {
	DeviceID    string
	AccessID    string
	AccessKey   string
	APIEndpoint string
}

func NewCloudClient(deviceID, accessID, accessKey, apiEndpoint string) *CloudClient {
	// Initialize Tuya Connector
	connector.InitWithOptions(
		env.WithAccessID(accessID),
		env.WithAccessKey(accessKey),
		env.WithApiHost(apiEndpoint),
		env.WithMsgHost(apiEndpoint),
	)

	return &CloudClient{
		DeviceID:    deviceID,
		AccessID:    accessID,
		AccessKey:   accessKey,
		APIEndpoint: apiEndpoint,
	}
}

// SendCommand sends a command to the Tuya device using the Tuya Cloud API
func (c *CloudClient) SendCommand(commands []map[string]interface{}) error {
	log.Printf("Connecting to Tuya Cloud for device %s...", c.DeviceID)

	payload := map[string]interface{}{
		"commands": commands,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal command payload: %v", err)
	}

	resp := &struct{}{}
	apiURI := fmt.Sprintf("/v1.0/devices/%s/commands", c.DeviceID)
	
	err = connector.MakePostRequest(
		context.Background(),
		connector.WithAPIUri(apiURI),
		connector.WithPayload(payloadBytes),
		connector.WithResp(resp),
	)

	if err != nil {
		return fmt.Errorf("failed to send command to Tuya Cloud: %v", err)
	}

	log.Println("[tuya-cloud] Command successfully sent to device.")
	return nil
}

// GetDeviceDetails fetches the status and details of the device from Tuya Cloud API
func (c *CloudClient) GetDeviceDetails() (interface{}, error) {
	log.Printf("Fetching details from Tuya Cloud for device %s...", c.DeviceID)

	var resp interface{}
	apiURI := fmt.Sprintf("/v1.0/devices/%s", c.DeviceID)
	
	err := connector.MakeGetRequest(
		context.Background(),
		connector.WithAPIUri(apiURI),
		connector.WithResp(&resp),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get device details: %v", err)
	}

	return resp, nil
}

var (
	cloudDevicesCache     interface{}
	cloudDevicesCacheTime time.Time
	cloudDevicesMutex     sync.Mutex
	quotaExhaustedUntil   time.Time
)

// GetCloudDevices fetches devices linked to the Tuya project with caching and quota protection
func (c *CloudClient) GetCloudDevices() (interface{}, error) {
	cloudDevicesMutex.Lock()
	defer cloudDevicesMutex.Unlock()

	now := time.Now()

	// 1. Circuit Breaker: Jika Tuya pernah mengembalikan pesan quota exhausted,
	// tahan pemanggilan API selama 5 menit agar tidak membanjiri request percuma
	if now.Before(quotaExhaustedUntil) {
		if cloudDevicesCache != nil {
			return cloudDevicesCache, nil
		}
		return nil, fmt.Errorf("Tuya API quota exhausted, cool down aktif hingga %s", quotaExhaustedUntil.Format("15:04:05"))
	}

	// 2. TTL Cache: Berikan data cache jika request dilakukan dalam kurun 15 detik
	if cloudDevicesCache != nil && now.Sub(cloudDevicesCacheTime) < 15*time.Second {
		return cloudDevicesCache, nil
	}

	log.Println("Fetching associated devices from Tuya Cloud (cached/throttled)...")

	var resp interface{}
	apiURI := "/v1.0/iot-01/associated-users/devices"

	err := connector.MakeGetRequest(
		context.Background(),
		connector.WithAPIUri(apiURI),
		connector.WithResp(&resp),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch devices from Tuya Cloud: %v", err)
	}

	// Cek apakah response memuat kode error Tuya Cloud (misal 28841004 quota exhausted atau 28841107 data center suspended)
	if resp != nil {
		if b, errM := json.Marshal(resp); errM == nil {
			respStr := string(b)
			if strings.Contains(respStr, "28841004") || strings.Contains(respStr, "quota is exhausted") ||
				strings.Contains(respStr, "28841107") || strings.Contains(respStr, "data center is suspended") {
				quotaExhaustedUntil = now.Add(10 * time.Minute)
				log.Println("[TUYA QUOTA PROTECTION] Tuya Cloud API dibatasi (quota exhausted / suspended). Mengaktifkan cooldown 10 menit.")
				if cloudDevicesCache != nil {
					return cloudDevicesCache, nil
				}
				// Jangan return error payload sebagai objek devices valid
				return nil, fmt.Errorf("Tuya Cloud API sedang dibatasi: %s", respStr)
			}
		}
	}

	cloudDevicesCache = resp
	cloudDevicesCacheTime = now
	return resp, nil
}

// CreatePairingToken requests a device pairing token from Tuya Cloud OpenAPI for Web Bluetooth / AP provisioning
func (c *CloudClient) CreatePairingToken(timeZone string) (interface{}, error) {
	log.Println("Requesting device pairing token from Tuya Cloud...")

	if timeZone == "" {
		timeZone = "Asia/Jakarta"
	}

	payload := map[string]interface{}{
		"time_zone": timeZone,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pairing token payload: %v", err)
	}

	var resp interface{}
	apiURI := "/v1.0/devices/tokens"

	err = connector.MakePostRequest(
		context.Background(),
		connector.WithAPIUri(apiURI),
		connector.WithPayload(payloadBytes),
		connector.WithResp(&resp),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to request pairing token from Tuya Cloud: %v", err)
	}

	return resp, nil
}

// GetPairingTokenStatus queries Tuya Cloud for devices that bound using the specified token
func (c *CloudClient) GetPairingTokenStatus(token string) (interface{}, error) {
	log.Printf("Querying status of pairing token %s from Tuya Cloud...", token)

	var resp interface{}
	apiURI := fmt.Sprintf("/v1.0/devices/tokens/%s", token)

	err := connector.MakeGetRequest(
		context.Background(),
		connector.WithAPIUri(apiURI),
		connector.WithResp(&resp),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to query pairing token status: %v", err)
	}

	return resp, nil
}

// RawGet melakukan GET request generik ke Tuya OpenAPI dan mengembalikan respons mentah
func (c *CloudClient) RawGet(apiURI string) (map[string]interface{}, error) {
	var resp map[string]interface{}
	err := connector.MakeGetRequest(
		context.Background(),
		connector.WithAPIUri(apiURI),
		connector.WithResp(&resp),
	)
	if err != nil {
		return nil, fmt.Errorf("tuya GET %s gagal: %v", apiURI, err)
	}
	return resp, nil
}

// GetDeviceDailyStatistics mengambil statistik harian (kWh per hari) dari layanan Data Statistics Tuya.
// startDay/endDay berformat yyyyMMdd.
func (c *CloudClient) GetDeviceDailyStatistics(code, startDay, endDay string) (map[string]interface{}, error) {
	uri := fmt.Sprintf("/v1.0/devices/%s/statistics/days?code=%s&start_day=%s&end_day=%s", c.DeviceID, code, startDay, endDay)
	return c.RawGet(uri)
}

// GetDeviceHourlyStatistics mengambil statistik per jam. startHour/endHour berformat yyyyMMddHH.
func (c *CloudClient) GetDeviceHourlyStatistics(code, startHour, endHour string) (map[string]interface{}, error) {
	uri := fmt.Sprintf("/v1.0/devices/%s/statistics/hours?code=%s&start_hour=%s&end_hour=%s", c.DeviceID, code, startHour, endHour)
	return c.RawGet(uri)
}
