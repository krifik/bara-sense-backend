package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fiqri/home-automation-backend/infrastructure/tuya"
)

// =====================================================================================
// Sinkronisasi Riwayat Energi dari Tuya Cloud (DP Report Logs)
//
// Hasil investigasi API Tuya untuk project ini:
//   - /v1.0/devices/{id}/statistics/*  -> "No permissions. This API is not subscribed."
//     (layanan Data Statistics tidak aktif pada project Tuya).
//   - /v2.0/cloud/thing/{id}/report-logs?codes=add_ele -> TERSEDIA.
//     Perangkat melaporkan DP `add_ele` kira-kira setiap 30 menit. Nilainya adalah
//     INKREMEN energi sejak laporan sebelumnya dalam satuan 0.001 kWh (Wh),
//     contoh: 34 = 0.034 kWh (cocok dengan beban PC ~61 W selama 30 menit).
//   - Sesekali perangkat mengirim nilai anomali (mis. 45934 / 46222) beberapa detik
//     setelah laporan lain (umumnya saat tengah malam / reconnect). Nilai ini adalah
//     akumulasi internal chip, bukan konsumsi riil, sehingga harus difilter.
//   - Retensi log di Tuya Cloud terbatas (beberapa hari), sehingga data di luar
//     jendela retensi tetap dipertahankan dari database lokal.
// =====================================================================================

const (
	tuyaEnergyRatePerKwh = 1444.70
	// Batas daya maksimum yang masuk akal untuk satu stop kontak (kW). Digunakan untuk
	// menolak laporan inkremen yang secara fisik mustahil terjadi dalam selang waktunya.
	tuyaMaxPlausibleKw = 3.5
	// Selang waktu default yang diasumsikan untuk laporan pertama (jam).
	tuyaDefaultReportGapHours = 0.5
)

var tuyaEnergyCodes = []string{"add_ele"}

type tuyaRawEnergyLog struct {
	Time  time.Time
	Value float64 // satuan Wh (0.001 kWh)
}

type tuyaEnergyReport struct {
	Time    time.Time
	Kwh     float64
	AvgWatt int
}

type tuyaDeviceSyncResult struct {
	DeviceID        string  `json:"device_id"`
	Name            string  `json:"name"`
	HasEnergyMeter  bool    `json:"has_energy_meter"`
	TuyaLogs        int     `json:"tuya_logs"`
	AcceptedLogs    int     `json:"accepted_logs"`
	RejectedLogs    int     `json:"rejected_logs"`
	DeletedLocal    int64   `json:"deleted_local_rows"`
	InsertedLocal   int     `json:"inserted_local_rows"`
	TuyaKwh         float64 `json:"tuya_kwh"`
	WindowStart     string  `json:"window_start,omitempty"`
	WindowEnd       string  `json:"window_end,omitempty"`
	Status          string  `json:"status"`
	Message         string  `json:"message,omitempty"`
}

// deviceHasEnergyMeter memeriksa apakah perangkat memiliki DP pengukur energi hardware.
func deviceHasEnergyMeter(dev TuyaCloudDeviceItem) bool {
	for _, s := range dev.Status {
		for _, c := range tuyaEnergyCodes {
			if strings.EqualFold(s.Code, c) {
				return true
			}
		}
	}
	return false
}

// fetchTuyaCloudDeviceList mengambil daftar perangkat beserta status DP dari Tuya Cloud.
func fetchTuyaCloudDeviceList(accessID, accessKey, endpoint string) ([]TuyaCloudDeviceItem, error) {
	client := tuya.NewCloudClient("", accessID, accessKey, endpoint)
	cloudResp, err := client.GetCloudDevices()
	if err != nil || cloudResp == nil {
		return nil, fmt.Errorf("gagal terhubung ke Tuya Cloud API: %v", err)
	}
	b, err := json.Marshal(cloudResp)
	if err != nil {
		return nil, err
	}
	var tResp TuyaCloudResponse
	if err := json.Unmarshal(b, &tResp); err != nil {
		return nil, err
	}
	if !tResp.Success {
		return nil, fmt.Errorf("Tuya Cloud mengembalikan respons tidak sukses")
	}
	return tResp.Result.Devices, nil
}

// fetchTuyaEnergyReportLogs mengambil seluruh log laporan DP add_ele dalam rentang waktu
// (dipecah per 7 hari dan dipaginasi menggunakan last_row_key).
func fetchTuyaEnergyReportLogs(accessID, accessKey, endpoint, deviceID string, start, end time.Time) ([]tuyaRawEnergyLog, error) {
	client := tuya.NewCloudClient(deviceID, accessID, accessKey, endpoint)
	seen := make(map[string]bool)
	var out []tuyaRawEnergyLog

	for chunkStart := start; chunkStart.Before(end); chunkStart = chunkStart.Add(7 * 24 * time.Hour) {
		chunkEnd := chunkStart.Add(7 * 24 * time.Hour)
		if chunkEnd.After(end) {
			chunkEnd = end
		}
		lastRowKey := ""
		for page := 0; page < 100; page++ {
			uri := fmt.Sprintf("/v2.0/cloud/thing/%s/report-logs?codes=%s&start_time=%d&end_time=%d&size=100",
				deviceID, strings.Join(tuyaEnergyCodes, ","), chunkStart.UnixMilli(), chunkEnd.UnixMilli())
			if lastRowKey != "" {
				uri += "&last_row_key=" + lastRowKey
			}
			resp, err := client.RawGet(uri)
			if err != nil {
				return nil, err
			}
			if ok, _ := resp["success"].(bool); !ok {
				return nil, fmt.Errorf("Tuya report-logs gagal: %v", resp["msg"])
			}
			result, _ := resp["result"].(map[string]interface{})
			if result == nil {
				break
			}
			logs, _ := result["logs"].([]interface{})
			for _, l := range logs {
				m, ok := l.(map[string]interface{})
				if !ok {
					continue
				}
				ts, _ := m["event_time"].(float64)
				valStr := fmt.Sprintf("%v", m["value"])
				val, errP := strconv.ParseFloat(valStr, 64)
				if errP != nil || ts <= 0 {
					continue
				}
				key := fmt.Sprintf("%d|%s", int64(ts), valStr)
				if seen[key] {
					continue // buang duplikat persis (event_time + nilai sama)
				}
				seen[key] = true
				out = append(out, tuyaRawEnergyLog{Time: time.UnixMilli(int64(ts)), Value: val})
			}
			hasMore, _ := result["has_more"].(bool)
			lastRowKey, _ = result["last_row_key"].(string)
			if !hasMore || lastRowKey == "" {
				break
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out, nil
}

// filterTuyaEnergyLogs mengubah log mentah menjadi laporan kWh yang valid dan menolak anomali
// berdasarkan batas daya fisik (kWh <= maxKw * selang waktu sejak laporan sebelumnya).
func filterTuyaEnergyLogs(raw []tuyaRawEnergyLog) (accepted []tuyaEnergyReport, rejected int) {
	var prev time.Time
	for _, r := range raw {
		if r.Value < 0 {
			rejected++
			continue
		}
		gapHours := tuyaDefaultReportGapHours
		if !prev.IsZero() {
			gapHours = r.Time.Sub(prev).Hours()
		}
		kwh := r.Value / 1000.0
		// Beri toleransi minimal 5 menit agar laporan yang berdekatan tidak salah ditolak
		effectiveGap := gapHours
		if effectiveGap < 5.0/60.0 {
			effectiveGap = 5.0 / 60.0
		}
		if kwh > tuyaMaxPlausibleKw*effectiveGap {
			rejected++
			continue // anomali: nilai akumulasi chip, bukan konsumsi riil
		}
		avgWatt := 0
		if gapHours > 0 {
			avgWatt = int(kwh / gapHours * 1000.0)
		}
		accepted = append(accepted, tuyaEnergyReport{Time: r.Time, Kwh: kwh, AvgWatt: avgWatt})
		prev = r.Time
	}
	return
}

// syncDeviceEnergyHistoryFromTuya menyamakan energy_logs lokal dengan log Tuya Cloud.
// Seluruh baris lokal di dalam jendela data Tuya [log tertua, sekarang] diganti dengan data
// Tuya, sedangkan riwayat lokal di luar jendela retensi Tuya tetap dipertahankan.
func syncDeviceEnergyHistoryFromTuya(dbConn *sql.DB, accessID, accessKey, endpoint, deviceID, name string, days int) tuyaDeviceSyncResult {
	res := tuyaDeviceSyncResult{DeviceID: deviceID, Name: name, HasEnergyMeter: true}
	end := time.Now()
	start := end.AddDate(0, 0, -days)

	raw, err := fetchTuyaEnergyReportLogs(accessID, accessKey, endpoint, deviceID, start, end)
	if err != nil {
		res.Status = "error"
		res.Message = err.Error()
		return res
	}
	res.TuyaLogs = len(raw)
	if len(raw) == 0 {
		res.Status = "skipped"
		res.Message = "Tidak ada log energi di Tuya Cloud pada rentang ini (data lokal dipertahankan)."
		return res
	}

	accepted, rejected := filterTuyaEnergyLogs(raw)
	res.AcceptedLogs = len(accepted)
	res.RejectedLogs = rejected

	// Jendela penggantian dimulai dari log Tuya tertua yang tersedia
	windowStart := raw[0].Time
	res.WindowStart = windowStart.Format(time.RFC3339)
	res.WindowEnd = end.Format(time.RFC3339)

	tx, err := dbConn.Begin()
	if err != nil {
		res.Status = "error"
		res.Message = err.Error()
		return res
	}
	defer func() { _ = tx.Rollback() }()

	delRes, err := tx.Exec("DELETE FROM energy_logs WHERE device_id = $1 AND timestamp >= $2", deviceID, windowStart)
	if err != nil {
		res.Status = "error"
		res.Message = err.Error()
		return res
	}
	res.DeletedLocal, _ = delRes.RowsAffected()

	for _, a := range accepted {
		cost := a.Kwh * tuyaEnergyRatePerKwh
		if _, err := tx.Exec("INSERT INTO energy_logs (device_id, timestamp, power_watt, kwh, cost_idr) VALUES ($1, $2, $3, $4, $5)",
			deviceID, a.Time, a.AvgWatt, a.Kwh, cost); err != nil {
			res.Status = "error"
			res.Message = err.Error()
			return res
		}
		res.InsertedLocal++
		res.TuyaKwh += a.Kwh
	}

	if err := tx.Commit(); err != nil {
		res.Status = "error"
		res.Message = err.Error()
		return res
	}
	res.TuyaKwh = roundTo(res.TuyaKwh, 4)
	res.Status = "synced"
	return res
}

// syncAllEnergyHistoryFromTuya menjalankan sinkronisasi untuk semua perangkat berpengukur energi.
func syncAllEnergyHistoryFromTuya(dbConn *sql.DB, accessID, accessKey, endpoint, onlyDeviceID string, days int) ([]tuyaDeviceSyncResult, error) {
	devices, err := fetchTuyaCloudDeviceList(accessID, accessKey, endpoint)
	if err != nil {
		return nil, err
	}
	results := make([]tuyaDeviceSyncResult, 0, len(devices))
	for _, d := range devices {
		if onlyDeviceID != "" && d.ID != onlyDeviceID {
			continue
		}
		name := d.Name
		if name == "" {
			name = d.ID
		}
		if !deviceHasEnergyMeter(d) {
			results = append(results, tuyaDeviceSyncResult{
				DeviceID: d.ID, Name: name, HasEnergyMeter: false, Status: "skipped",
				Message: "Perangkat tidak memiliki chip pengukur energi (DP add_ele).",
			})
			continue
		}
		results = append(results, syncDeviceEnergyHistoryFromTuya(dbConn, accessID, accessKey, endpoint, d.ID, name, days))
	}
	return results, nil
}

// startTuyaHistoryAutoSync menjaga data lokal tetap identik dengan Tuya secara berkala
// (menyinkronkan jendela 2 hari terakhir setiap 10 menit).
func startTuyaHistoryAutoSync(dbConn *sql.DB, wsHub *WsHub, accessID, accessKey, endpoint string) {
	if accessID == "" || accessKey == "" {
		return
	}
	go func() {
		run := func() {
			results, err := syncAllEnergyHistoryFromTuya(dbConn, accessID, accessKey, endpoint, "", 2)
			if err != nil {
				log.Printf("[TUYA HISTORY SYNC] gagal: %v", err)
				return
			}
			synced := 0
			for _, r := range results {
				if r.Status == "synced" {
					synced++
				}
			}
			log.Printf("[TUYA HISTORY SYNC] auto-sync selesai: %d perangkat tersinkron.", synced)
			broadcastAnalytics(dbConn, wsHub)
		}
		time.Sleep(15 * time.Second)
		run()
		ticker := time.NewTicker(10 * time.Minute)
		for range ticker.C {
			run()
		}
	}()
}

// sumTuyaEnergyForWindow mengambil log Tuya dan menghitung total kWh valid beserta jendela waktunya.
func sumTuyaEnergyForWindow(accessID, accessKey, endpoint, deviceID string, days int) (kwh float64, windowStart time.Time, ok bool, err error) {
	end := time.Now()
	raw, err := fetchTuyaEnergyReportLogs(accessID, accessKey, endpoint, deviceID, end.AddDate(0, 0, -days), end)
	if err != nil || len(raw) == 0 {
		return 0, time.Time{}, false, err
	}
	accepted, _ := filterTuyaEnergyLogs(raw)
	for _, a := range accepted {
		kwh += a.Kwh
	}
	return kwh, raw[0].Time, true, nil
}

func roundTo(v float64, places int) float64 {
	p := 1.0
	for i := 0; i < places; i++ {
		p *= 10
	}
	return float64(int64(v*p+0.5)) / p
}
