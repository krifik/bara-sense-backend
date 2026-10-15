package mqtt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	mqttserver "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
)

// WaterTelemetry merefleksikan payload JSON dari NodeMCU sensor air JSN-SR04T
type WaterTelemetry struct {
	Valid         bool    `json:"valid"`
	State         string  `json:"state"` // LOW | NORMAL | FULL | OVERFLOW
	WaterDepthCm  float64 `json:"water_depth_cm"`
	Percentage    float64 `json:"percentage"`
	RawDistanceCm float64 `json:"raw_distance_cm"`
	PumpAction    string  `json:"pump_action"` // START | STOP | IDLE
	RSSI          int     `json:"rssi"`
	IP            string  `json:"ip"`
	UptimeMs      int64   `json:"uptime_ms"`
	Timestamp     string  `json:"timestamp,omitempty"`
}

// ConnectedClient merefleksikan klien MQTT yang sedang aktif terhubung ke broker
type ConnectedClient struct {
	ID          string `json:"id"`
	RemoteIP    string `json:"remote_ip"`
	ConnectedAt string `json:"connected_at"`
	IsInline    bool   `json:"is_inline"`
	Username    string `json:"username,omitempty"`
}

// LogEntry menyimpan histori log pesan MQTT terbaru
type LogEntry struct {
	ID        int64  `json:"id"`
	Topic     string `json:"topic"`
	Payload   string `json:"payload"`
	ClientID  string `json:"client_id"`
	Timestamp string `json:"timestamp"`
	Direction string `json:"direction"` // "in" atau "out"
}

// TankGeometry menyimpan konfigurasi fisik tangki tandon air untuk kalkulasi telemetri
type TankGeometry struct {
	TotalDepthCm      float64 `json:"total_depth_cm"`
	SensorOffsetMinCm float64 `json:"sensor_offset_min_cm"`
	ThresholdLowPct   float64 `json:"threshold_low_pct"`
	ThresholdHighPct  float64 `json:"threshold_high_pct"`
}

// BrokerManager mengelola embedded MQTT broker, hooks, state telemetry, dan integrasi websocket
type BrokerManager struct {
	Server        *mqttserver.Server
	listenAddr    string
	wsBroadcaster func(data []byte)

	mu               sync.RWMutex
	latestWater      *WaterTelemetry
	latestState      string
	recentLogs       []LogEntry
	logCounter       int64
	clientCount      int
	lastActivityAt   time.Time
	connectedTimes   map[string]time.Time
	blockedClients   map[string]time.Time
	onWaterTelemetry func(telem *WaterTelemetry)
	tankGeometry     TankGeometry
}

var instance *BrokerManager
var once sync.Once

// NewBrokerManager membuat instance singleton BrokerManager
func NewBrokerManager(listenAddr string, wsBroadcaster func(data []byte)) *BrokerManager {
	once.Do(func() {
		if listenAddr == "" {
			listenAddr = ":1883"
		}
		instance = &BrokerManager{
			listenAddr:     listenAddr,
			wsBroadcaster:  wsBroadcaster,
			recentLogs:     make([]LogEntry, 0, 100),
			latestState:    "UNKNOWN",
			lastActivityAt: time.Now(),
			connectedTimes: make(map[string]time.Time),
			blockedClients: make(map[string]time.Time),
			tankGeometry: TankGeometry{
				TotalDepthCm:      100.0,
				SensorOffsetMinCm: 20.0,
				ThresholdLowPct:   25.0,
				ThresholdHighPct:  90.0,
			},
		}
	})
	return instance
}

// SetTankGeometry memperbarui parameter geometri tangki untuk kalkulasi kedalaman & kapasitas air
func (m *BrokerManager) SetTankGeometry(geo TankGeometry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if geo.TotalDepthCm > 0 {
		m.tankGeometry = geo
	}
}

// GetTankGeometry mengambil konfigurasi geometri tangki aktif
func (m *BrokerManager) GetTankGeometry() TankGeometry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tankGeometry
}

// SetWaterTelemetryListener mendaftarkan listener saat telemetri air baru diterima
func (m *BrokerManager) SetWaterTelemetryListener(fn func(telem *WaterTelemetry)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onWaterTelemetry = fn
}

// GetBrokerManager mengembalikan instance singleton BrokerManager
func GetBrokerManager() *BrokerManager {
	return instance
}

// Custom hook untuk menyadap OnConnectAuthenticate, OnACLCheck, OnPublish dan connect/disconnect
type telemetryHook struct {
	mqttserver.HookBase
	manager *BrokerManager
}

func (h *telemetryHook) ID() string {
	return "telemetry-interceptor"
}

// Provides mendeklarasikan metode hook yang ditangani secara eksplisit
func (h *telemetryHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqttserver.OnConnectAuthenticate,
		mqttserver.OnACLCheck,
		mqttserver.OnConnect,
		mqttserver.OnDisconnect,
		mqttserver.OnPublish,
	}, []byte{b})
}

// OnConnectAuthenticate mengizinkan koneksi dari klien IoT tanpa auth maupun dengan auth, dan memeriksa pemblokiran manual
func (h *telemetryHook) OnConnectAuthenticate(cl *mqttserver.Client, pk packets.Packet) bool {
	if cl != nil {
		h.manager.mu.RLock()
		blockUntil, blocked := h.manager.blockedClients[cl.ID]
		h.manager.mu.RUnlock()
		if blocked {
			if time.Now().Before(blockUntil) {
				log.Printf("[MQTT AUTH] Menolak koneksi klien '%s' (sedang diblokir secara manual hingga %s)", cl.ID, blockUntil.Format("15:04:05"))
				return false
			}
			// Blokir sudah kedaluwarsa
			h.manager.mu.Lock()
			delete(h.manager.blockedClients, cl.ID)
			h.manager.mu.Unlock()
		}
	}
	return true
}

// OnACLCheck mengizinkan read/write ke semua topik
func (h *telemetryHook) OnACLCheck(cl *mqttserver.Client, topic string, write bool) bool {
	return true
}

func (h *telemetryHook) OnConnect(cl *mqttserver.Client, pk packets.Packet) error {
	h.manager.mu.Lock()
	h.manager.clientCount++
	h.manager.lastActivityAt = time.Now()
	if cl != nil {
		h.manager.connectedTimes[cl.ID] = time.Now()
	}
	h.manager.mu.Unlock()

	clientName := "unknown"
	remoteAddr := "unknown"
	if cl != nil {
		clientName = cl.ID
		remoteAddr = cl.Net.Remote
	}

	log.Printf("[MQTT] Client connected: %s (IP: %s)", clientName, remoteAddr)
	h.manager.addLog("system/clients", fmt.Sprintf("Client connected: %s (%s)", clientName, remoteAddr), clientName, "in")
	h.manager.broadcastStatus()
	return nil
}

func (h *telemetryHook) OnDisconnect(cl *mqttserver.Client, err error, expire bool) {
	h.manager.mu.Lock()
	if h.manager.clientCount > 0 {
		h.manager.clientCount--
	}
	h.manager.lastActivityAt = time.Now()
	if cl != nil {
		delete(h.manager.connectedTimes, cl.ID)
	}
	h.manager.mu.Unlock()

	clientName := "unknown"
	if cl != nil {
		clientName = cl.ID
	}

	log.Printf("[MQTT] Client disconnected: %s (error: %v)", clientName, err)
	h.manager.addLog("system/clients", fmt.Sprintf("Client disconnected: %s", clientName), clientName, "in")
	h.manager.broadcastStatus()
}

func (h *telemetryHook) OnPublish(cl *mqttserver.Client, pk packets.Packet) (packets.Packet, error) {
	payloadStr := string(pk.Payload)
	topic := pk.TopicName
	clientID := "anonymous"
	if cl != nil {
		clientID = cl.ID
	}

	h.manager.mu.Lock()
	h.manager.lastActivityAt = time.Now()
	h.manager.mu.Unlock()

	// Simpan riwayat log
	h.manager.addLog(topic, payloadStr, clientID, "in")

	// Filter & Parsing Topic Khusus Telemetri Air (Bara IoT / Sensor JSN-SR04T)
	if topic == "home/water/telemetry" {
		var telem WaterTelemetry
		if err := json.Unmarshal(pk.Payload, &telem); err == nil {
			telem.Timestamp = time.Now().Format(time.RFC3339)

			// Kalkulasi dinamis berbasis konfigurasi geometri tangki aktif dari database
			h.manager.mu.RLock()
			geo := h.manager.tankGeometry
			h.manager.mu.RUnlock()

			if telem.Valid && telem.RawDistanceCm > 0 && geo.TotalDepthCm > 0 {
				effectiveMax := geo.TotalDepthCm + geo.SensorOffsetMinCm
				depth := effectiveMax - telem.RawDistanceCm
				if depth < 0 {
					depth = 0
				}
				if depth > geo.TotalDepthCm {
					depth = geo.TotalDepthCm
				}
				pct := (depth / geo.TotalDepthCm) * 100.0
				if pct < 0 {
					pct = 0
				}
				if pct > 100 {
					pct = 100
				}

				telem.WaterDepthCm = depth
				telem.Percentage = pct

				// Evaluasi state mesin telemetri
				if telem.RawDistanceCm < geo.SensorOffsetMinCm && pct >= 98.0 {
					telem.State = "OVERFLOW"
				} else if pct >= geo.ThresholdHighPct {
					telem.State = "FULL"
				} else if pct <= geo.ThresholdLowPct {
					telem.State = "LOW_LEVEL"
				} else if pct <= 5.0 {
					telem.State = "EMPTY"
				} else {
					telem.State = "NORMAL"
				}
			}

			h.manager.mu.Lock()
			h.manager.latestWater = &telem
			if telem.State != "" {
				h.manager.latestState = telem.State
			}
			h.manager.mu.Unlock()

			// Broadcast event telemetri ke WebSocket (diterima oleh frontend BARA-Sense)
			h.manager.broadcastJSON(map[string]interface{}{
				"type":      "mqtt_water_telemetry",
				"topic":     topic,
				"telemetry": telem,
			})

			// Panggil Automation Rule Engine jika listener terdaftar
			h.manager.mu.RLock()
			listener := h.manager.onWaterTelemetry
			h.manager.mu.RUnlock()
			if listener != nil {
				go listener(&telem)
			}
		}
	} else if topic == "home/water/state" {
		stateClean := strings.TrimSpace(payloadStr)
		h.manager.mu.Lock()
		h.manager.latestState = stateClean
		h.manager.mu.Unlock()

		h.manager.broadcastJSON(map[string]interface{}{
			"type":  "mqtt_water_state",
			"topic": topic,
			"state": stateClean,
		})
	}

	// Broadcast generic packet info ke realtime dashboard MQTT
	h.manager.broadcastJSON(map[string]interface{}{
		"type":      "mqtt_packet",
		"topic":     topic,
		"payload":   payloadStr,
		"client_id": clientID,
		"timestamp": time.Now().Format("15:04:05"),
	})

	return pk, nil
}

func (m *BrokerManager) addLog(topic, payload, clientID, dir string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logCounter++
	entry := LogEntry{
		ID:        m.logCounter,
		Topic:     topic,
		Payload:   payload,
		ClientID:  clientID,
		Timestamp: time.Now().Format("15:04:05"),
		Direction: dir,
	}

	m.recentLogs = append(m.recentLogs, entry)
	if len(m.recentLogs) > 50 {
		m.recentLogs = m.recentLogs[len(m.recentLogs)-50:]
	}
}

func (m *BrokerManager) broadcastJSON(v interface{}) {
	if m.wsBroadcaster == nil {
		return
	}
	b, err := json.Marshal(v)
	if err == nil {
		m.wsBroadcaster(b)
	}
}

func (m *BrokerManager) broadcastStatus() {
	status := m.GetStatus()
	m.broadcastJSON(map[string]interface{}{
		"type":   "mqtt_status",
		"status": status,
	})
}

// Start menjalankan MQTT Server Mochi di background
func (m *BrokerManager) Start() error {
	server := mqttserver.New(&mqttserver.Options{
		InlineClient: true,
	})

	hook := &telemetryHook{
		manager: m,
	}
	if err := server.AddHook(hook, nil); err != nil {
		return fmt.Errorf("failed to add telemetry hook: %w", err)
	}

	tcp := listeners.NewTCP(listeners.Config{
		ID:      "t1",
		Address: m.listenAddr,
	})
	if err := server.AddListener(tcp); err != nil {
		return fmt.Errorf("failed to add TCP listener on %s: %w", m.listenAddr, err)
	}

	m.Server = server

	go func() {
		log.Printf("[MQTT] Embedded MQTT Broker starting on TCP %s (v3.1.1 & v5 compatible)...", m.listenAddr)
		if err := server.Serve(); err != nil {
			log.Printf("[MQTT] Broker stopped: %v", err)
		}
	}()

	return nil
}

// Publish menerbitkan pesan ke broker
func (m *BrokerManager) Publish(topic string, payload []byte, retain bool) error {
	if m.Server == nil {
		return fmt.Errorf("MQTT broker is not running")
	}

	m.addLog(topic, string(payload), "bara-sense-server", "out")
	err := m.Server.Publish(topic, payload, retain, 0)
	if err == nil {
		m.broadcastJSON(map[string]interface{}{
			"type":      "mqtt_packet",
			"topic":     topic,
			"payload":   string(payload),
			"client_id": "bara-sense-server",
			"timestamp": time.Now().Format("15:04:05"),
		})
	}
	return err
}

// DisconnectClient memutus koneksi klien secara manual
func (m *BrokerManager) DisconnectClient(clientID string) error {
	if m.Server == nil || m.Server.Clients == nil {
		return fmt.Errorf("broker belum aktif")
	}

	cl, ok := m.Server.Clients.Get(clientID)
	if !ok {
		return fmt.Errorf("klien '%s' tidak ditemukan atau sudah terputus", clientID)
	}

	// Masukkan ke daftar blocked sementara selama 30 detik agar NodeMCU tidak langsung re-connect instan
	m.mu.Lock()
	m.blockedClients[clientID] = time.Now().Add(30 * time.Second)
	delete(m.connectedTimes, clientID)
	m.mu.Unlock()

	// Tutup koneksi klien (DisconnectClient mengembalikan code ErrAdministrativeAction sebagai konfirmasi penutupan)
	_ = m.Server.DisconnectClient(cl, packets.ErrAdministrativeAction)
	cl.Stop(packets.ErrAdministrativeAction)

	m.addLog("system/clients", fmt.Sprintf("Admin disconnected client: %s (cooldown 30s)", clientID), "admin", "out")
	m.broadcastStatus()
	return nil
}

// GetConnectedClients mengembalikan daftar klien yang saat ini terhubung
func (m *BrokerManager) GetConnectedClients() []ConnectedClient {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]ConnectedClient, 0)
	if m.Server == nil || m.Server.Clients == nil {
		return result
	}

	all := m.Server.Clients.GetAll()
	for id, cl := range all {
		if cl == nil {
			continue
		}
		ip := cl.Net.Remote
		if ip == "" {
			ip = "127.0.0.1 (local)"
		}

		connTime := m.connectedTimes[id]
		connTimeStr := "-"
		if !connTime.IsZero() {
			connTimeStr = connTime.Format("15:04:05")
		}

		result = append(result, ConnectedClient{
			ID:          id,
			RemoteIP:    ip,
			ConnectedAt: connTimeStr,
			IsInline:    cl.Net.Inline,
			Username:    string(cl.Properties.Username),
		})
	}
	return result
}

// GetStatus mengambil status runtime broker MQTT saat ini
func (m *BrokerManager) GetStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	logsCopy := make([]LogEntry, len(m.recentLogs))
	copy(logsCopy, m.recentLogs)

	clientsList := m.GetConnectedClients()

	return map[string]interface{}{
		"running":          m.Server != nil,
		"listen_addr":      m.listenAddr,
		"port":             1883,
		"active_clients":   len(clientsList),
		"clients":          clientsList,
		"latest_state":     m.latestState,
		"latest_water":     m.latestWater,
		"recent_logs":      logsCopy,
		"last_activity_at": m.lastActivityAt.Format(time.RFC3339),
	}
}
