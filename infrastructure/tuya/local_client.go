package tuya

import (
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"strings"
)

// Client handles communication with local Tuya devices
type LocalClient struct {
	DeviceID string
	IP       string
	LocalKey string
}

func NewLocalClient(deviceID, ip, localKey string) *LocalClient {
	return &LocalClient{
		DeviceID: deviceID,
		IP:       ip,
		LocalKey: localKey,
	}
}

// SendCommand sends a command to the Tuya device using tinytuya Python script
func (c *LocalClient) SendCommand(dps map[string]interface{}) error {
	log.Printf("Connecting to Tuya device %s at %s via tinytuya...", c.DeviceID, c.IP)
	
	payloadBytes, err := json.Marshal(dps)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %v", err)
	}

	// Menjalankan script Python yang memanggil modul tinytuya
	cmd := exec.Command("python", "scripts/tuya_script.py", c.DeviceID, c.IP, c.LocalKey, string(payloadBytes))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to execute tinytuya script: %v, output: %s", err, string(output))
	}

	outStr := strings.TrimSpace(string(output))
	if !strings.Contains(outStr, "Success") {
		return fmt.Errorf("tinytuya error: %s", outStr)
	}

	log.Println("[tinytuya] Command successfully sent to device.")
	return nil
}
