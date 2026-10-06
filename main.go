package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fiqri/home-automation-backend/infrastructure/db"
	"github.com/fiqri/home-automation-backend/infrastructure/tuya"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/websocket/v2"
)

// WsHub mengelola seluruh koneksi WebSocket aktif dan broadcasting data analitik
type WsHub struct {
	clients      map[*websocket.Conn]bool
	broadcast    chan []byte
	mutex        sync.Mutex
	activeTariff string
}

func NewWsHub() *WsHub {
	return &WsHub{
		clients:      make(map[*websocket.Conn]bool),
		broadcast:    make(chan []byte),
		activeTariff: "1300-2200VA",
	}
}

func (h *WsHub) Run() {
	for {
		msg := <-h.broadcast
		h.mutex.Lock()
		for client := range h.clients {
			if err := client.WriteMessage(websocket.TextMessage, msg); err != nil {
				client.Close()
				delete(h.clients, client)
			}
		}
		h.mutex.Unlock()
	}
}

func loadEnv() {
	candidates := []string{".env", "backend/.env", "../backend/.env", "./backend/.env"}
	for _, ef := range candidates {
		file, err := os.Open(ef)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}

			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				val := strings.Trim(strings.TrimSpace(parts[1]), "\"'")
				if os.Getenv(key) == "" {
					os.Setenv(key, val)
				}
			}
		}
		file.Close()
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

type TuyaCloudStatusItem struct {
	Code  string      `json:"code"`
	Value interface{} `json:"value"`
}

type TuyaCloudDeviceItem struct {
	ID          string                `json:"id"`
	Name        string                `json:"name"`
	Online      bool                  `json:"online"`
	Category    string                `json:"category"`
	ProductName string                `json:"product_name"`
	Model       string                `json:"model"`
	IP          string                `json:"ip"`
	LocalKey    string                `json:"local_key"`
	Icon        string                `json:"icon"`
	Status      []TuyaCloudStatusItem `json:"status"`
}

// Helper untuk menerjemahkan kode kategori Tuya ke label dan ikon ramah pengguna
func getTuyaCategoryInfo(cat string) (label string, icon string) {
	switch strings.ToLower(cat) {
	case "cz":
		return "Stop Kontak Pintar (Smart Socket)", "🔌"
	case "kg":
		return "Saklar Lampu Pintar (Smart Switch)", "🔘"
	case "dj":
		return "Lampu Pintar (Smart Bulb / LED)", "💡"
	case "cl":
		return "Gorden Pintar (Smart Curtain)", "🪟"
	case "fs":
		return "Kipas Angin (Smart Fan)", "💨"
	case "wk":
		return "Termostat / Pengatur Suhu", "🌡️"
	case "cs":
		return "Sensor Pintar (Smart Sensor)", "📡"
	default:
		return "Perangkat IoT Pintar", "⚡"
	}
}

type TuyaCloudResponse struct {
	Success bool `json:"success"`
	Result  struct {
		Devices []TuyaCloudDeviceItem `json:"devices"`
	} `json:"result"`
}

// Helper untuk mengekstrak telemetri realtime (status, daya watt aktual, tegangan V, arus A, total kWh hardware) dari perangkat Tuya
func parseCloudDeviceTelemetry(dev TuyaCloudDeviceItem) (status bool, powerWatt float64, voltage float64, current float64, totalKwhMeter float64) {
	for _, s := range dev.Status {
		code := strings.ToLower(s.Code)
		switch code {
		case "switch_1", "switch", "switch_led", "switch_main":
			if b, ok := s.Value.(bool); ok {
				status = b
			}
		case "cur_power", "power", "cur_power_a", "power_w", "electric", "power_consumption":
			var f float64
			switch v := s.Value.(type) {
			case float64:
				f = v
			case int:
				f = float64(v)
			case int64:
				f = float64(v)
			}
			if f > 0 {
				powerWatt = f / 10.0
			}
		case "cur_voltage", "voltage":
			var f float64
			switch v := s.Value.(type) {
			case float64:
				f = v
			case int:
				f = float64(v)
			case int64:
				f = float64(v)
			}
			if f > 0 {
				voltage = f / 10.0
			}
		case "cur_current", "current":
			var f float64
			switch v := s.Value.(type) {
			case float64:
				f = v
			case int:
				f = float64(v)
			case int64:
				f = float64(v)
			}
			if f > 0 {
				current = f / 1000.0
			}
		case "add_ele", "forward_energy", "total_forward_energy", "phase_a_energy":
			var f float64
			switch v := s.Value.(type) {
			case float64:
				f = v
			case int:
				f = float64(v)
			case int64:
				f = float64(v)
			}
			if f > 0 {
				// Standar Tuya DP add_ele adalah skala 0.001 kWh (Wh) (contoh: 45934 = 45.934 kWh)
				if f > 500000 {
					totalKwhMeter = f / 10000.0
				} else {
					totalKwhMeter = f / 1000.0
				}
			}
		}
	}

	// Jika perangkat offline di Tuya Cloud atau saklar sedang OFF (mati), konsumsi daya dan arus otomatis 0
	if !dev.Online || !status {
		status = false
		powerWatt = 0.0
		current = 0.0
		voltage = 0.0
	} else {
		if voltage == 0 {
			voltage = 220.0
		}
		if current == 0 && powerWatt > 0 {
			current = powerWatt / voltage
		}
	}
	return
}

// Helper untuk mengekstrak daya (Watt), status, dan nama otomatis dari respons OpenAPI Tuya Cloud
func extractTuyaDevicePowerAndInfo(details interface{}) (power int, status bool, name string, fetched bool) {
	if details == nil {
		return 0, false, "", false
	}

	b, err := json.Marshal(details)
	if err != nil {
		return 0, false, "", false
	}

	var root struct {
		Success bool `json:"success"`
		Result  struct {
			ID     string                `json:"id"`
			Name   string                `json:"name"`
			Online bool                  `json:"online"`
			Status []TuyaCloudStatusItem `json:"status"`
		} `json:"result"`
	}

	if err := json.Unmarshal(b, &root); err != nil || !root.Success {
		return 0, false, "", false
	}

	name = root.Result.Name
	fetched = true

	cDev := TuyaCloudDeviceItem{
		ID:     root.Result.ID,
		Name:   root.Result.Name,
		Online: root.Result.Online,
		Status: root.Result.Status,
	}

	cStatus, pWatt, _, _, _ := parseCloudDeviceTelemetry(cDev)
	status = cStatus
	power = int(math.Round(pWatt))

	return power, status, name, fetched
}

func formatIndonesianDayName(t time.Time) string {
	switch t.Weekday() {
	case time.Sunday:
		return "Minggu"
	case time.Monday:
		return "Senin"
	case time.Tuesday:
		return "Selasa"
	case time.Wednesday:
		return "Rabu"
	case time.Thursday:
		return "Kamis"
	case time.Friday:
		return "Jumat"
	case time.Saturday:
		return "Sabtu"
	default:
		return ""
	}
}

func formatIndonesianDate(t time.Time) string {
	months := []string{"Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"}
	return fmt.Sprintf("%02d %s", t.Day(), months[t.Month()-1])
}

func formatIndonesianFullDate(t time.Time) string {
	months := []string{"Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus", "September", "Oktober", "November", "Desember"}
	return fmt.Sprintf("%02d %s %d", t.Day(), months[t.Month()-1], t.Year())
}

// Global helper kalkulasi data analitik penggunaan listrik dengan pilihan Golongan Tarif PLN
func getAnalyticsData(dbConn *sql.DB, tariffCode string) (fiber.Map, error) {
	tariffOptions := []fiber.Map{
		{
			"code":           "450VA",
			"name":           "R-1 / 450 VA (Bersubsidi)",
			"power_category": "450 VA",
			"rate_per_kwh":   415.00,
			"description":    "Tarif subsidi pemerintah untuk keluarga kurang mampu.",
		},
		{
			"code":           "900VA",
			"name":           "R-1 / 900 VA RTM (Non-Subsidi)",
			"power_category": "900 VA",
			"rate_per_kwh":   1352.00,
			"description":    "Tarif Rumah Tangga Mampu (RTM) golongan 900 VA.",
		},
		{
			"code":           "1300-2200VA",
			"name":           "R-1 / 1300 - 2200 VA (Non-Subsidi)",
			"power_category": "1300 - 2200 VA",
			"rate_per_kwh":   1444.70,
			"description":    "Tarif standar rumah tangga menengah non-subsidi.",
		},
		{
			"code":           "3500VA+",
			"name":           "R-2 / 3500 - 5500 VA (Non-Subsidi)",
			"power_category": "3500 - 5500 VA",
			"rate_per_kwh":   1699.53,
			"description":    "Tarif rumah tangga besar / daya menengah ke atas.",
		},
	}

	selectedRate := 1444.70
	selectedCode := "1300-2200VA"

	for _, t := range tariffOptions {
		if strings.EqualFold(t["code"].(string), tariffCode) || strings.HasPrefix(strings.ToLower(tariffCode), strings.ToLower(t["power_category"].(string)[:3])) {
			selectedRate = t["rate_per_kwh"].(float64)
			selectedCode = t["code"].(string)
			break
		}
	}

	rows, err := dbConn.Query("SELECT id, name, status, power FROM devices")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type DeviceInfo struct {
		ID     string
		Name   string
		Status bool
		Power  int
	}
	var devices []DeviceInfo
	totalActivePower := 0

	for rows.Next() {
		var d DeviceInfo
		if err := rows.Scan(&d.ID, &d.Name, &d.Status, &d.Power); err == nil {
			if !d.Status {
				d.Power = 0
			}
			devices = append(devices, d)
			if d.Status {
				totalActivePower += d.Power
			}
		}
	}

	var deviceBreakdowns []fiber.Map
	var totalKwhToday float64 = 0

	for _, d := range devices {
		var kwh float64
		// Ambil akumulasi kWh murni untuk hari ini saja (PostgreSQL syntax)
		errLog := dbConn.QueryRow("SELECT COALESCE(SUM(kwh), 0) FROM energy_logs WHERE device_id = $1 AND timestamp >= CURRENT_DATE AND kwh >= 0", d.ID).Scan(&kwh)
		if errLog != nil || kwh < 0 {
			kwh = 0.0
		}

		cost := kwh * selectedRate
		totalKwhToday += kwh

		// Kalkulasi tegangan (V) & arus (A) realtime per perangkat
		voltageVolts := 0.0
		currentAmps := 0.0
		if d.Status {
			voltageVolts = 220.0
			currentAmps = float64(d.Power) / 220.0
		}

		// Estimasi konsumsi harian jika perangkat menyala (proyeksi 24 jam)
		dailyEstKwh := 0.0
		dailyEstCost := 0.0
		monthlyEstCost := 0.0
		if d.Status && d.Power > 0 {
			dailyEstKwh = (float64(d.Power) * 24.0) / 1000.0
			dailyEstCost = dailyEstKwh * selectedRate
			monthlyEstCost = dailyEstCost * 30.0
		}

		deviceBreakdowns = append(deviceBreakdowns, fiber.Map{
			"id":                         d.ID,
			"name":                       d.Name,
			"power_watt":                 d.Power,
			"voltage_volts":              fmt.Sprintf("%.1f", voltageVolts),
			"current_amps":               fmt.Sprintf("%.2f", currentAmps),
			"status":                     d.Status,
			"kwh_today":                  fmt.Sprintf("%.2f", kwh),
			"cost_idr":                   fmt.Sprintf("%.0f", cost),
			"cost_today_idr":             fmt.Sprintf("%.0f", cost),
			"daily_estimated_kwh":        fmt.Sprintf("%.2f", dailyEstKwh),
			"daily_estimated_cost_idr":   fmt.Sprintf("%.0f", dailyEstCost),
			"monthly_estimated_cost_idr": fmt.Sprintf("%.0f", monthlyEstCost),
			"percentage":                 0,
		})
	}

	for i := range deviceBreakdowns {
		kwhVal := 0.0
		fmt.Sscanf(deviceBreakdowns[i]["kwh_today"].(string), "%f", &kwhVal)
		if totalKwhToday > 0 {
			deviceBreakdowns[i]["percentage"] = fmt.Sprintf("%.1f", (kwhVal/totalKwhToday)*100)
		} else {
			deviceBreakdowns[i]["percentage"] = "0"
		}
	}

	totalCostToday := totalKwhToday * selectedRate

	// Prediksi bulanan dihitung dari rata-rata tren 7 hari terakhir (PostgreSQL syntax)
	var avgDailyKwhHist float64
	_ = dbConn.QueryRow(`
		SELECT COALESCE(AVG(daily_kwh), 0)
		FROM (
			SELECT SUM(kwh) as daily_kwh
			FROM energy_logs
			WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
			  AND timestamp < CURRENT_DATE
			GROUP BY date_trunc('day', timestamp)
		) sub
	`).Scan(&avgDailyKwhHist)

	predictedKwhMonth := 0.0
	if avgDailyKwhHist > 0 {
		predictedKwhMonth = avgDailyKwhHist * 30.0
	} else if totalActivePower > 0 {
		predictedKwhMonth = (float64(totalActivePower) * 12.0 * 30.0) / 1000.0
	} else {
		predictedKwhMonth = totalKwhToday * 30.0
	}
	predictedCostMonth := predictedKwhMonth * selectedRate
	estimatedCostMonth := predictedCostMonth

	efficiencyScore := 95
	if totalActivePower > 1500 {
		efficiencyScore = 65
	} else if totalActivePower > 900 {
		efficiencyScore = 82
	} else if totalActivePower > 500 {
		efficiencyScore = 90
	}

	overloadRisk := "Rendah (Aman)"
	if totalActivePower >= 1300 {
		overloadRisk = "Tinggi (Bahaya Trip/Mati Listrik)"
	} else if totalActivePower >= 900 {
		overloadRisk = "Sedang (Mendekati Batas Daya)"
	}

	var recommendations []string
	if totalActivePower > 800 {
		recommendations = append(recommendations, fmt.Sprintf("Beban daya aktif saat ini tinggi (%d Watt). Pertimbangkan mematikan saklar beban tinggi.", totalActivePower))
	} else {
		recommendations = append(recommendations, "Beban listrik saat ini tergolong aman dan efisien.")
	}

	recommendations = append(recommendations,
		fmt.Sprintf("Proyeksi tagihan bulanan untuk golongan %s (Rp %.0f/kWh) diperkirakan Rp %.0f.", selectedCode, selectedRate, predictedCostMonth),
		"Disarankan menggunakan timer saklar pada jam puncak pemakaian (18:00 - 21:00 WIB).",
	)

	// Agregasi per 3 jam dari database energy_logs untuk grafik hourlyUsage (PostgreSQL syntax)
	hours := []string{"00:00", "03:00", "06:00", "09:00", "12:00", "15:00", "18:00", "21:00"}
	var hourlyUsage []fiber.Map
	for _, h := range hours {
		var hKwh float64
		var hStartInt int
		fmt.Sscanf(h[:2], "%d", &hStartInt)
		_ = dbConn.QueryRow(`
			SELECT COALESCE(SUM(kwh), 0)
			FROM energy_logs
			WHERE timestamp >= CURRENT_DATE
			  AND EXTRACT(HOUR FROM timestamp) >= $1
			  AND EXTRACT(HOUR FROM timestamp) < ($1 + 3)
			  AND kwh >= 0
		`, hStartInt).Scan(&hKwh)

		// Fallback: If no today logs in exact 3-hour window, fetch average recent 3-hour block usage
		if hKwh == 0 {
			_ = dbConn.QueryRow(`
				SELECT COALESCE(AVG(kwh_sum), 0)
				FROM (
					SELECT SUM(kwh) as kwh_sum
					FROM energy_logs
					WHERE EXTRACT(HOUR FROM timestamp) >= $1
					  AND EXTRACT(HOUR FROM timestamp) < ($1 + 3)
					  AND kwh >= 0
					GROUP BY date_trunc('day', timestamp)
				) sub
			`, hStartInt).Scan(&hKwh)
		}

		hourlyUsage = append(hourlyUsage, fiber.Map{
			"hour": h,
			"kwh":  math.Round(hKwh*100) / 100,
			"cost": math.Round(hKwh * selectedRate),
		})
	}

	// Agregasi 7 hari terakhir dari database energy_logs (PostgreSQL syntax)
	var dailyUsage []fiber.Map
	for i := 6; i >= 0; i-- {
		targetDate := time.Now().AddDate(0, 0, -i)
		dateStr := targetDate.Format("2006-01-02")
		dayName := formatIndonesianDayName(targetDate)
		formattedDate := formatIndonesianDate(targetDate)

		var dayKwh, dayCost float64
		_ = dbConn.QueryRow(`
			SELECT COALESCE(SUM(kwh), 0), COALESCE(SUM(cost_idr), 0)
			FROM energy_logs
			WHERE timestamp >= $1::date AND timestamp < ($1::date + INTERVAL '1 day') AND kwh >= 0
		`, dateStr).Scan(&dayKwh, &dayCost)

		if i == 0 && dayKwh < totalKwhToday {
			dayKwh = totalKwhToday
			dayCost = totalCostToday
		}
		if dayCost == 0 && dayKwh > 0 {
			dayCost = dayKwh * selectedRate
		}

		dailyUsage = append(dailyUsage, fiber.Map{
			"day":      dayName,
			"date":     formattedDate,
			"date_raw": dateStr,
			"kwh":      math.Round(dayKwh*100) / 100,
			"cost":     math.Round(dayCost),
		})
	}

	return fiber.Map{
		"type":                     "analytics_update",
		"status":                   "success",
		"total_active_power_watts": totalActivePower,
		"total_kwh_today":          fmt.Sprintf("%.2f", totalKwhToday),
		"estimated_cost_today_idr": fmt.Sprintf("%.0f", totalCostToday),
		"estimated_cost_month_idr": fmt.Sprintf("%.0f", estimatedCostMonth),
		"predicted_kwh_month":      fmt.Sprintf("%.2f", predictedKwhMonth),
		"predicted_cost_month_idr": fmt.Sprintf("%.0f", predictedCostMonth),
		"efficiency_score":        efficiencyScore,
		"overload_risk":           overloadRisk,
		"peak_usage_hour":          "18:00 - 21:00 WIB",
		"selected_tariff_code":    selectedCode,
		"selected_tariff_rate":    selectedRate,
		"tariff_options":           tariffOptions,
		"recommendations":          recommendations,
		"tariff_rate_per_kwh":      selectedRate,
		"device_breakdown":         deviceBreakdowns,
		"hourly_usage":             hourlyUsage,
		"daily_usage":              dailyUsage,
	}, nil
}

// Helper penghasil riwayat pemakaian per hari lengkap beserta ringkasan KPI dan perincian perangkat
func getDailyHistoryResponse(dbConn *sql.DB, daysLimit int, filterDeviceID string, selectedRate float64) (fiber.Map, error) {
	if daysLimit <= 0 {
		daysLimit = 7
	}
	if daysLimit > 90 {
		daysLimit = 90
	}

	var historyList []fiber.Map
	var totalPeriodKwh float64 = 0
	var totalPeriodCost float64 = 0

	var highestKwh float64 = -1
	var lowestKwh float64 = 9999999
	var highestDay fiber.Map
	var lowestDay fiber.Map

	// Hitung mundur dari hari ini (i=0) sampai daysLimit
	for i := 0; i < daysLimit; i++ {
		targetDate := time.Now().AddDate(0, 0, -i)
		dateStr := targetDate.Format("2006-01-02")
		dayName := formatIndonesianDayName(targetDate)
		formattedDate := formatIndonesianDate(targetDate)
		fullDate := formatIndonesianFullDate(targetDate)
		isToday := (i == 0)

		var dayKwh, dayCost float64
		var avgWatt float64
		var peakWatt int
		var sampleCount int

		var errQ error
		if filterDeviceID != "" {
			errQ = dbConn.QueryRow(`
				SELECT COALESCE(SUM(kwh), 0), COALESCE(SUM(cost_idr), 0), COALESCE(AVG(power_watt), 0), COALESCE(MAX(power_watt), 0), COUNT(*)
				FROM energy_logs
				WHERE timestamp >= $1::date AND timestamp < ($1::date + INTERVAL '1 day') AND device_id = $2 AND kwh >= 0
			`, dateStr, filterDeviceID).Scan(&dayKwh, &dayCost, &avgWatt, &peakWatt, &sampleCount)
		} else {
			errQ = dbConn.QueryRow(`
				SELECT COALESCE(SUM(kwh), 0), COALESCE(SUM(cost_idr), 0), COALESCE(AVG(power_watt), 0), COALESCE(MAX(power_watt), 0), COUNT(*)
				FROM energy_logs
				WHERE timestamp >= $1::date AND timestamp < ($1::date + INTERVAL '1 day') AND kwh >= 0
			`, dateStr).Scan(&dayKwh, &dayCost, &avgWatt, &peakWatt, &sampleCount)
		}

		if errQ != nil {
			dayKwh = 0
			dayCost = 0
		}

		// Hitung biaya harian berdasarkan tarif PLN yang aktif
		dayCost = dayKwh * selectedRate

		// Rincian per perangkat pada hari ini (PostgreSQL syntax)
		var devList []fiber.Map
		devRows, errDev := dbConn.Query(`
			SELECT e.device_id, COALESCE(d.name, dh.name || ' (Dihapus)', e.device_id), COALESCE(SUM(e.kwh), 0), COALESCE(AVG(e.power_watt), 0), COALESCE(MAX(e.power_watt), 0)
			FROM energy_logs e
			LEFT JOIN devices d ON d.id = e.device_id
			LEFT JOIN device_name_history dh ON dh.device_id = e.device_id
			WHERE e.timestamp >= $1::date AND e.timestamp < ($1::date + INTERVAL '1 day') AND e.kwh >= 0
			GROUP BY e.device_id, d.name, dh.name
			ORDER BY SUM(e.kwh) DESC
		`, dateStr)
		if errDev == nil {
			for devRows.Next() {
				var devID, devName string
				var dKwh, dAvg float64
				var dMax int
				if errScan := devRows.Scan(&devID, &devName, &dKwh, &dAvg, &dMax); errScan == nil {
					dCost := dKwh * selectedRate
					devList = append(devList, fiber.Map{
						"device_id":       devID,
						"device_name":     devName,
						"kwh":             math.Round(dKwh*100) / 100,
						"cost_idr":        math.Round(dCost),
						"avg_power_watt":  int(math.Round(dAvg)),
						"peak_power_watt": dMax,
					})
				}
			}
			devRows.Close()
		}

		effStatus := "Hemat"
		if dayKwh > 8.0 {
			effStatus = "Tinggi"
		} else if dayKwh >= 4.0 {
			effStatus = "Wajar"
		}

		dayMap := fiber.Map{
			"date_raw":          dateStr,
			"date":              formattedDate,
			"full_date":         fullDate,
			"day":               dayName,
			"is_today":          isToday,
			"kwh":               math.Round(dayKwh*100) / 100,
			"cost":              math.Round(dayCost),
			"avg_power_watt":    int(math.Round(avgWatt)),
			"peak_power_watt":   peakWatt,
			"status_efficiency": effStatus,
			"devices":           devList,
			"device_count":      len(devList),
		}

		historyList = append(historyList, dayMap)
		totalPeriodKwh += dayKwh
		totalPeriodCost += dayCost

		if dayKwh > highestKwh {
			highestKwh = dayKwh
			highestDay = dayMap
		}
		if dayKwh < lowestKwh {
			lowestKwh = dayKwh
			lowestDay = dayMap
		}
	}

	avgKwh := 0.0
	avgCost := 0.0
	if daysLimit > 0 {
		avgKwh = math.Round((totalPeriodKwh/float64(daysLimit))*100) / 100
		avgCost = math.Round(totalPeriodCost / float64(daysLimit))
	}

	return fiber.Map{
		"status": "success",
		"days":   daysLimit,
		"summary": fiber.Map{
			"total_kwh":          math.Round(totalPeriodKwh*100) / 100,
			"total_cost_idr":     math.Round(totalPeriodCost),
			"avg_daily_kwh":      avgKwh,
			"avg_daily_cost_idr": avgCost,
			"highest_day":        highestDay,
			"lowest_day":         lowestDay,
		},
		"history": historyList,
	}, nil
}

func broadcastAnalytics(dbConn *sql.DB, wsHub *WsHub) {
	wsHub.mutex.Lock()
	tariff := wsHub.activeTariff
	wsHub.mutex.Unlock()

	data, err := getAnalyticsData(dbConn, tariff)
	if err != nil {
		return
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	wsHub.broadcast <- bytes
}

type ScheduleItem struct {
	ID         int    `json:"id"`
	DeviceID   string `json:"device_id"`
	DeviceName string `json:"device_name"`
	Action     string `json:"action"`      // "ON" or "OFF"
	TimeTarget string `json:"time_target"` // "HH:MM"
	Days       string `json:"days"`        // "ALL", "WEEKDAY", "WEEKEND"
	IsActive   bool   `json:"is_active"`
	CreatedAt  string `json:"created_at"`
}

type DeviceTimerItem struct {
	DeviceID        string `json:"device_id"`
	DeviceName      string `json:"device_name"`
	TargetAction    string `json:"target_action"`
	ExpiresAt       string `json:"expires_at"`
	DurationMinutes int    `json:"duration_minutes"`
	RemainingSec    int    `json:"remaining_seconds"`
}

func executeDeviceToggle(dbConn *sql.DB, wsHub *WsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint, deviceID string, targetStatus bool) error {
	client := tuya.NewCloudClient(deviceID, tuyaAccessID, tuyaAccessKey, tuyaEndpoint)
	commands := []map[string]interface{}{
		{
			"code":  "switch_1",
			"value": targetStatus,
		},
	}
	errCmd := client.SendCommand(commands)
	if errCmd != nil {
		log.Printf("[TOGGLE ERROR] Gagal mengirim perintah toggle ke Tuya Cloud (%s): %v", deviceID, errCmd)
		return errCmd
	}

	if !targetStatus {
		// Ketika saklar dimatikan, konsumsi daya seketika menjadi 0 Watt secara realtime
		_, err := dbConn.Exec("UPDATE devices SET status = false, power = 0 WHERE id = $1", deviceID)
		if err != nil {
			return err
		}
	} else {
		// Ketika saklar dinyalakan, coba baca telemetri aktual dari Tuya
		details, errD := client.GetDeviceDetails()
		if errD == nil && details != nil {
			cPower, _, _, fetched := extractTuyaDevicePowerAndInfo(details)
			if fetched && cPower > 0 {
				_, _ = dbConn.Exec("UPDATE devices SET status = true, power = $1 WHERE id = $2", cPower, deviceID)
			} else {
				_, _ = dbConn.Exec("UPDATE devices SET status = true WHERE id = $1", deviceID)
			}
		} else {
			_, _ = dbConn.Exec("UPDATE devices SET status = true WHERE id = $1", deviceID)
		}
	}
	broadcastAnalytics(dbConn, wsHub)
	return nil
}

// Helper matchScheduleDay mencocokkan format hari (cron, nama hari Indonesia/Inggris, atau ALL/WEEKDAY/WEEKEND)
func matchScheduleDay(days string, weekday int) bool {
	daysUpper := strings.ToUpper(strings.TrimSpace(days))
	if daysUpper == "ALL" || daysUpper == "" || daysUpper == "EVERYDAY" || daysUpper == "*" {
		return true
	}
	if daysUpper == "WEEKDAY" && weekday >= 1 && weekday <= 5 {
		return true
	}
	if daysUpper == "WEEKEND" && (weekday == 0 || weekday == 6) {
		return true
	}

	parts := strings.Split(daysUpper, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == fmt.Sprintf("%d", weekday) {
			return true
		}
		switch p {
		case "MINGGU", "SUN", "SUNDAY", "0", "7":
			if weekday == 0 {
				return true
			}
		case "SENIN", "MON", "MONDAY", "1":
			if weekday == 1 {
				return true
			}
		case "SELASA", "TUE", "TUESDAY", "2":
			if weekday == 2 {
				return true
			}
		case "RABU", "WED", "WEDNESDAY", "3":
			if weekday == 3 {
				return true
			}
		case "KAMIS", "THU", "THURSDAY", "4":
			if weekday == 4 {
				return true
			}
		case "JUMAT", "JUM'AT", "FRI", "FRIDAY", "5":
			if weekday == 5 {
				return true
			}
		case "SABTU", "SAT", "SATURDAY", "6":
			if weekday == 6 {
				return true
			}
		}
	}
	return false
}

func startSchedulerWorker(dbConn *sql.DB, wsHub *WsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint string) {
	go func() {
		// Worker berjalan independen di backend dengan presisi 1 detik (Cron & Timer Evaluation Worker)
		ticker := time.NewTicker(1 * time.Second)
		lastTriggeredMinute := make(map[int]string)

		for range ticker.C {
			now := time.Now()
			currentHM := now.Format("15:04")
			currentDateMinute := now.Format("2006-01-02 15:04")
			weekday := int(now.Weekday()) // 0=Sunday, 1=Monday... 6=Saturday

			// 1. Process Device Timers (Hitung mundur auto-off presisi 1 detik)
			timerRows, err := dbConn.Query(`
				SELECT dt.device_id, dt.target_action, dt.expires_at, COALESCE(d.name, dt.device_id)
				FROM device_timers dt
				LEFT JOIN devices d ON d.id = dt.device_id
				WHERE dt.expires_at <= CURRENT_TIMESTAMP
			`)
			if err == nil {
				var expiredIDs []string
				for timerRows.Next() {
					var devID, targetAct, expAt, devName string
					if err := timerRows.Scan(&devID, &targetAct, &expAt, &devName); err == nil {
						targetBool := strings.ToUpper(targetAct) == "ON"
						_ = executeDeviceToggle(dbConn, wsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint, devID, targetBool)
						expiredIDs = append(expiredIDs, devID)
						log.Printf("[CRON WORKER] Timer hitung mundur habis untuk '%s' (%s). Diubah ke %s.\n", devName, devID, targetAct)
					}
				}
				timerRows.Close()

				if len(expiredIDs) > 0 {
					for _, id := range expiredIDs {
						_, _ = dbConn.Exec("DELETE FROM device_timers WHERE device_id = $1", id)
					}
					broadcastAnalytics(dbConn, wsHub)
				}
			}

			// 2. Process Smart Schedules (Jadwal otomatis berbasis Cron & Time Matching)
			schedRows, err := dbConn.Query(`
				SELECT s.id, s.device_id, COALESCE(d.name, s.device_name), s.action, s.time_target, s.days
				FROM schedules s
				LEFT JOIN devices d ON d.id = s.device_id
				WHERE s.is_active = true
			`)
			if err == nil {
				triggeredAny := false
				for schedRows.Next() {
					var sID int
					var devID, devName, action, timeTarget, days string
					if err := schedRows.Scan(&sID, &devID, &devName, &action, &timeTarget, &days); err == nil {
						if timeTarget == currentHM {
							if matchScheduleDay(days, weekday) && lastTriggeredMinute[sID] != currentDateMinute {
								lastTriggeredMinute[sID] = currentDateMinute
								targetBool := strings.ToUpper(action) == "ON"
								_ = executeDeviceToggle(dbConn, wsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint, devID, targetBool)
								triggeredAny = true
								log.Printf("[CRON WORKER] Jadwal #%d aktif untuk '%s' (%s): saklar diubah ke %s pada jam %s.\n", sID, devName, devID, action, currentHM)
							}
						}
					}
				}
				schedRows.Close()
				if triggeredAny {
					broadcastAnalytics(dbConn, wsHub)
				}
			}
		}
	}()
}

type SceneAction struct {
	DeviceID string `json:"device_id"`
	Status   bool   `json:"status"`
}

type SceneItem struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Icon        string        `json:"icon"`
	Description string        `json:"description"`
	Actions     []SceneAction `json:"actions"`
	IsPreset    bool          `json:"is_preset"`
	CreatedAt   string        `json:"created_at"`
}

type PowerGuardConfig struct {
	ID                int    `json:"id"`
	MaxWattLimit      int    `json:"max_watt_limit"`
	IsEnabled         bool   `json:"is_enabled"`
	CutoffDurationSec int    `json:"cutoff_duration_sec"`
	LastTriggeredAt   string `json:"last_triggered_at"`
	CurrentTotalWatts int    `json:"current_total_watts"`
}

type BudgetSettings struct {
	MonthlyBudgetIDR      float64 `json:"monthly_budget_idr"`
	WarningThresholdPct   float64 `json:"warning_threshold_pct"`
	CurrentMonthCostIDR   float64 `json:"current_month_cost_idr"`
	CurrentMonthKwh       float64 `json:"current_month_kwh"`
	UsagePct              float64 `json:"usage_pct"`
	ProjectedMonthCostIDR float64 `json:"projected_month_cost_idr"`
}

type TelegramConfig struct {
	BotToken         string `json:"bot_token"`
	ChatID           string `json:"chat_id"`
	IsEnabled        bool   `json:"is_enabled"`
	NotifyOnOverload bool   `json:"notify_on_overload"`
	NotifyOnLeak     bool   `json:"notify_on_leak"`
	DailyDigestTime  string `json:"daily_digest_time"`
}

func sendTelegramNotification(dbConn *sql.DB, message string) {
	var token, chatID string
	var enabled bool
	err := dbConn.QueryRow("SELECT bot_token, chat_id, is_enabled FROM telegram_config WHERE id = 1").Scan(&token, &chatID, &enabled)
	if err != nil || !enabled || strings.TrimSpace(token) == "" || strings.TrimSpace(chatID) == "" {
		return
	}

	go func() {
		apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)
		payload := map[string]string{
			"chat_id":    chatID,
			"text":       message,
			"parse_mode": "Markdown",
		}
		jsonBytes, _ := json.Marshal(payload)
		resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(jsonBytes))
		if err == nil && resp != nil {
			_ = resp.Body.Close()
		}
	}()
}

func executeScene(dbConn *sql.DB, wsHub *WsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint, sceneID string) error {
	var actionsJSON string
	err := dbConn.QueryRow("SELECT actions FROM scenes WHERE id = $1", sceneID).Scan(&actionsJSON)
	if err != nil {
		return err
	}

	var actions []SceneAction
	if err := json.Unmarshal([]byte(actionsJSON), &actions); err != nil {
		return err
	}

	for _, a := range actions {
		_ = executeDeviceToggle(dbConn, wsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint, a.DeviceID, a.Status)
	}
	broadcastAnalytics(dbConn, wsHub)
	return nil
}

func startPowerGuardWorker(dbConn *sql.DB, wsHub *WsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint string) {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		consecutiveOverloadCount := 0

		for range ticker.C {
			var limit int
			var enabled bool
			err := dbConn.QueryRow("SELECT max_watt_limit, is_enabled FROM power_guard_config WHERE id = 1").Scan(&limit, &enabled)
			if err != nil || !enabled || limit <= 0 {
				consecutiveOverloadCount = 0
				continue
			}

			var currentTotalWatts int
			_ = dbConn.QueryRow("SELECT COALESCE(SUM(power), 0) FROM devices WHERE status = true").Scan(&currentTotalWatts)

			if currentTotalWatts > limit {
				consecutiveOverloadCount++
				if consecutiveOverloadCount >= 3 {
					// Cari device dengan prioritas tertinggi untuk dimatikan (priority 3 = sekunder, 2 = normal)
					var devID, devName string
					var devPower int
					errFind := dbConn.QueryRow(`
						SELECT id, name, power FROM devices 
						WHERE status = true AND COALESCE(priority, 2) >= 2 
						ORDER BY COALESCE(priority, 2) DESC, power DESC LIMIT 1
					`).Scan(&devID, &devName, &devPower)

					if errFind == nil && devID != "" {
						_ = executeDeviceToggle(dbConn, wsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint, devID, false)
						_, _ = dbConn.Exec("UPDATE power_guard_config SET last_triggered_at = CURRENT_TIMESTAMP WHERE id = 1")
						
						alertMsg := fmt.Sprintf("⚠️ *[PROTEKSI LISTRIK PLN]* Total daya terpakai (%d W) melebihi batas aman (%d W)! '%s' (%d W) otomatis dimatikan untuk mencegah MCB anjlok.", currentTotalWatts, limit, devName, devPower)
						log.Println(alertMsg)
						sendTelegramNotification(dbConn, alertMsg)
					}
					consecutiveOverloadCount = 0
				}
			} else {
				consecutiveOverloadCount = 0
			}
		}
	}()
}

func main() {
	loadEnv()

	_ = os.MkdirAll("logs", 0755)
	_ = os.MkdirAll("backend/logs", 0755)
	var logOutput io.Writer = os.Stdout
	logFilePaths := []string{"logs/server.log", "backend/logs/server.log"}
	for _, lp := range logFilePaths {
		lf, err := os.OpenFile(lp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			logOutput = io.MultiWriter(os.Stdout, lf)
			log.SetOutput(logOutput)
			break
		}
	}

	tuyaAccessID := getEnv("TUYA_ACCESS_ID", "")
	tuyaAccessKey := getEnv("TUYA_ACCESS_KEY", "")
	tuyaEndpoint := getEnv("TUYA_ENDPOINT", "https://openapi-sg.iotbing.com")
	port := getEnv("PORT", "3000")
	postgresURL := getEnv("POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/home_automation?sslmode=disable")

	if tuyaAccessID == "" || tuyaAccessKey == "" {
		log.Println("[WARNING] TUYA_ACCESS_ID atau TUYA_ACCESS_KEY belum dikonfigurasi di .env")
	}

	log.Println("Starting BARA-Sense Backend (Building Automation & Realtime Analytics) with PostgreSQL Database & Tuya Hardware Meter Sync...")

	dbConn, err := db.NewPostgresDB(postgresURL)
	if err != nil {
		log.Fatalf("[POSTGRES ERROR] Gagal terhubung ke database PostgreSQL: %v", err)
	}
	log.Println("[POSTGRES SUCCESS] Terhubung dan terinisialisasi pada PostgreSQL Database Server!")
	log.Println("Database initialized successfully.")

	wsHub := NewWsHub()
	go wsHub.Run()

	// Mulai Scheduler Worker untuk Penjadwalan Waktu & Timer Hitung Mundur
	startSchedulerWorker(dbConn, wsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint)

	// Mulai Proteksi Anti-Jeglek PLN (Smart Load Shedding & Ceiling Guard)
	startPowerGuardWorker(dbConn, wsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint)

	// CATATAN: Sinkronisasi Tuya Cloud (GetCloudDevices & History Sync) TIDAK dijalankan secara berkala
	// di background loop agar kuota API Tuya Cloud tidak terbuang / exhausted.
	// Tuya Cloud hanya diakses saat pengguna secara manual mengklik tombol "Pindai Perangkat" (/api/scan)
	// atau "Sinkronkan Riwayat" (/api/energy/sync-tuya).
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for range ticker.C {
			// Pastikan perangkat yang status = false tidak memiliki power sisa di database
			_, _ = dbConn.Exec("UPDATE devices SET power = 0 WHERE status = false")

			wsHub.mutex.Lock()
			clientCount := len(wsHub.clients)
			wsHub.mutex.Unlock()

			if clientCount > 0 {
				broadcastAnalytics(dbConn, wsHub)
			}
		}
	}()

	app := fiber.New()
	app.Use(logger.New(logger.Config{
		Output: logOutput,
	}))
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "*",
		AllowMethods: "GET,POST,HEAD,PUT,DELETE,PATCH,OPTIONS",
	}))

	app.Get("/docs/swagger.json", func(c *fiber.Ctx) error {
		candidates := []string{"./docs/swagger.json", "./backend/docs/swagger.json", "docs/swagger.json"}
		for _, cp := range candidates {
			if _, err := os.Stat(cp); err == nil {
				return c.SendFile(cp)
			}
		}
		return c.SendFile("./docs/swagger.json")
	})

	swaggerHTML := `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BARA-Sense API - Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui.css" />
  <style>
    body { margin: 0; background: #fafafa; }
    .topbar { display: none; }
  </style>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-bundle.js" crossorigin></script>
<script>
  window.onload = () => {
    window.ui = SwaggerUIBundle({
      url: '/docs/swagger.json',
      dom_id: '#swagger-ui',
      presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIBundle.SwaggerUIStandalonePreset
      ],
      layout: "BaseLayout"
    });
  };
</script>
</body>
</html>`

	app.Get("/docs", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/html")
		return c.SendString(swaggerHTML)
	})
	app.Get("/swagger", func(c *fiber.Ctx) error {
		return c.Redirect("/docs")
	})

	// WebSocket Middleware & Route for Telemetry & Realtime Analytics
	app.Use("/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.Get("/ws", websocket.New(func(c *websocket.Conn) {
		wsHub.mutex.Lock()
		wsHub.clients[c] = true
		currentTariff := wsHub.activeTariff
		wsHub.mutex.Unlock()

		log.Println("[WebSocket] Client connected:", c.RemoteAddr())

		if data, err := getAnalyticsData(dbConn, currentTariff); err == nil {
			if b, err := json.Marshal(data); err == nil {
				_ = c.WriteMessage(websocket.TextMessage, b)
			}
		}

		defer func() {
			wsHub.mutex.Lock()
			delete(wsHub.clients, c)
			wsHub.mutex.Unlock()
			c.Close()
			log.Println("[WebSocket] Client disconnected:", c.RemoteAddr())
		}()

		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				break
			}

			var payload struct {
				Type       string `json:"type"`
				DeviceID   string `json:"device_id"`
				PowerWatt  int    `json:"power_watt"`
				Status     *bool  `json:"status"`
				TariffCode string `json:"tariff_code"`
			}

			if err := json.Unmarshal(msg, &payload); err == nil {
				if payload.TariffCode != "" {
					wsHub.mutex.Lock()
					wsHub.activeTariff = payload.TariffCode
					wsHub.mutex.Unlock()
				}

				if payload.Type == "power_telemetry" || payload.Type == "power_update" {
					if payload.DeviceID != "" {
						if payload.Status != nil {
							_, _ = dbConn.Exec("UPDATE devices SET power = $1, status = $2 WHERE id = $3", payload.PowerWatt, *payload.Status, payload.DeviceID)
						} else {
							_, _ = dbConn.Exec("UPDATE devices SET power = $1 WHERE id = $2", payload.PowerWatt, payload.DeviceID)
						}

						kwhInc := (float64(payload.PowerWatt) * 0.05) / 1000.0
						costInc := kwhInc * 1444.70
						_, _ = dbConn.Exec("INSERT INTO energy_logs (device_id, power_watt, kwh, cost_idr) VALUES ($1, $2, $3, $4)", payload.DeviceID, payload.PowerWatt, kwhInc, costInc)
					}
				}
				broadcastAnalytics(dbConn, wsHub)
			}
		}
	}))

	// Middleware untuk mengecek perizinan berbasis peran (RBAC)
	requireRole := func(allowedRoles ...string) fiber.Handler {
		return func(c *fiber.Ctx) error {
			role := c.Get("X-User-Role")
			if role == "" {
				authHeader := c.Get("Authorization")
				if strings.HasPrefix(authHeader, "Bearer ") {
					token := strings.TrimPrefix(authHeader, "Bearer ")
					parts := strings.Split(token, "_")
					if len(parts) >= 2 {
						_ = dbConn.QueryRow("SELECT role FROM users WHERE id::text = $1 OR username = $2", parts[1], parts[1]).Scan(&role)
					}
				}
			}
			if role == "" {
				username := c.Get("X-Username")
				if username != "" {
					_ = dbConn.QueryRow("SELECT role FROM users WHERE username = $1", username).Scan(&role)
				}
			}
			if role == "" {
				role = "viewer" // Default jika header tidak diisi
			}

			allowed := false
			for _, r := range allowedRoles {
				if strings.EqualFold(r, role) {
					allowed = true
					break
				}
			}

			if !allowed {
				return c.Status(403).JSON(fiber.Map{
					"status":  "forbidden",
					"error":   fmt.Sprintf("Akses Ditolak: Peran '%s' tidak memiliki hak akses untuk tindakan ini.", role),
					"role":    role,
					"allowed": allowedRoles,
				})
			}
			c.Locals("user_role", role)
			return c.Next()
		}
	}

	// Group Route API Auth & RBAC User Management
	authGroup := app.Group("/api/auth")

	// API: Mendapatkan profil user aktif (Me)
	authGroup.Get("/me", func(c *fiber.Ctx) error {
		username := c.Get("X-Username")
		authHeader := c.Get("Authorization")

		if username == "" && strings.HasPrefix(authHeader, "Bearer ") {
			token := strings.TrimPrefix(authHeader, "Bearer ")
			parts := strings.Split(token, "_")
			if len(parts) >= 2 {
				username = parts[1]
			}
		}

		if username == "" {
			return c.Status(401).JSON(fiber.Map{"error": "Belum terautentikasi"})
		}

		var user struct {
			ID       int    `json:"id"`
			Username string `json:"username"`
			Name     string `json:"name"`
			Role     string `json:"role"`
		}

		err := dbConn.QueryRow("SELECT id, username, name, role FROM users WHERE username = $1 OR id::text = $2", username, username).Scan(&user.ID, &user.Username, &user.Name, &user.Role)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Pengguna tidak ditemukan"})
		}

		return c.JSON(fiber.Map{
			"status": "success",
			"user":   user,
		})
	})

	// API: Mendapatkan daftar pengguna & peran RBAC
	authGroup.Get("/users", func(c *fiber.Ctx) error {
		rows, err := dbConn.Query("SELECT id, username, name, role FROM users ORDER BY id ASC")
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		defer rows.Close()

		type UserItem struct {
			ID       int    `json:"id"`
			Username string `json:"username"`
			Name     string `json:"name"`
			Role     string `json:"role"`
		}

		users := make([]UserItem, 0)
		for rows.Next() {
			var u UserItem
			if err := rows.Scan(&u.ID, &u.Username, &u.Name, &u.Role); err == nil {
				users = append(users, u)
			}
		}

		return c.JSON(fiber.Map{
			"status": "success",
			"users":  users,
		})
	})

	// API: Mendapatkan Matriks Hak Akses / Permissions RBAC
	authGroup.Get("/permissions", func(c *fiber.Ctx) error {
		rows, err := dbConn.Query("SELECT id, feature, desc_text, admin, operator, viewer FROM permissions")
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		defer rows.Close()

		type PermItem struct {
			ID       string `json:"id"`
			Feature  string `json:"feature"`
			Desc     string `json:"desc"`
			Admin    bool   `json:"admin"`
			Operator bool   `json:"operator"`
			Viewer   bool   `json:"viewer"`
		}

		permissions := make([]PermItem, 0)
		for rows.Next() {
			var p PermItem
			if err := rows.Scan(&p.ID, &p.Feature, &p.Desc, &p.Admin, &p.Operator, &p.Viewer); err == nil {
				permissions = append(permissions, p)
			}
		}

		return c.JSON(fiber.Map{
			"status":      "success",
			"permissions": permissions,
		})
	})

	// API: Mengubah Matriks Hak Akses RBAC (Khusus Admin)
	authGroup.Put("/permissions", requireRole("admin"), func(c *fiber.Ctx) error {
		type PermUpdate struct {
			ID       string `json:"id"`
			Admin    bool   `json:"admin"`
			Operator bool   `json:"operator"`
			Viewer   bool   `json:"viewer"`
		}
		type UpdatePermReq struct {
			Permissions []PermUpdate `json:"permissions"`
		}
		var req UpdatePermReq
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
		}

		for _, p := range req.Permissions {
			_, _ = dbConn.Exec("UPDATE permissions SET admin = $1, operator = $2, viewer = $3 WHERE id = $4", p.Admin, p.Operator, p.Viewer, p.ID)
		}

		return c.JSON(fiber.Map{
			"status":  "success",
			"message": "Matriks hak akses RBAC berhasil diperbarui",
		})
	})

	// API: Autentikasi / Login User
	authGroup.Post("/login", func(c *fiber.Ctx) error {
		type LoginReq struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		var req LoginReq
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
		}

		var user struct {
			ID       int    `json:"id"`
			Username string `json:"username"`
			Name     string `json:"name"`
			Role     string `json:"role"`
			Password string `json:"-"`
		}

		err := dbConn.QueryRow("SELECT id, username, name, role, password FROM users WHERE username = $1", strings.TrimSpace(req.Username)).Scan(&user.ID, &user.Username, &user.Name, &user.Role, &user.Password)
		if err != nil || user.Password != strings.TrimSpace(req.Password) {
			return c.Status(411).JSON(fiber.Map{"error": "Username atau password salah!"})
		}

		token := fmt.Sprintf("token_%d_%s_%d", user.ID, user.Role, time.Now().Unix())

		return c.JSON(fiber.Map{
			"status":  "success",
			"message": "Login berhasil",
			"token":   token,
			"user":    user,
		})
	})

	// API: Ubah Password User Sendiri
	authGroup.Put("/change-password", func(c *fiber.Ctx) error {
		type ChangePassReq struct {
			OldPassword string `json:"old_password"`
			NewPassword string `json:"new_password"`
		}
		var req ChangePassReq
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
		}

		if strings.TrimSpace(req.NewPassword) == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Password baru tidak boleh kosong!"})
		}

		username := c.Get("X-Username")
		if username == "" {
			authHeader := c.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				parts := strings.Split(strings.TrimPrefix(authHeader, "Bearer "), "_")
				if len(parts) >= 2 {
					username = parts[1]
				}
			}
		}

		if username == "" {
			return c.Status(401).JSON(fiber.Map{"error": "Belum terautentikasi"})
		}

		var currentPass string
		var userID int
		err := dbConn.QueryRow("SELECT id, password FROM users WHERE username = $1 OR id::text = $2", username, username).Scan(&userID, &currentPass)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "User tidak ditemukan"})
		}

		if req.OldPassword != "" && currentPass != strings.TrimSpace(req.OldPassword) {
			return c.Status(400).JSON(fiber.Map{"error": "Password lama Anda salah!"})
		}

		_, err = dbConn.Exec("UPDATE users SET password = $1 WHERE id = $2", strings.TrimSpace(req.NewPassword), userID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{
			"status":  "success",
			"message": "Password berhasil diubah!",
		})
	})

	// API: Admin Mengubah/Reset Password User Lain
	authGroup.Put("/users/:id/password", requireRole("admin"), func(c *fiber.Ctx) error {
		userID := c.Params("id")
		type AdminChangePassReq struct {
			NewPassword string `json:"new_password"`
		}
		var req AdminChangePassReq
		if err := c.BodyParser(&req); err != nil || strings.TrimSpace(req.NewPassword) == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Password baru wajib diisi!"})
		}

		res, err := dbConn.Exec("UPDATE users SET password = $1 WHERE id::text = $2", strings.TrimSpace(req.NewPassword), userID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		rowsAffected, _ := res.RowsAffected()
		if rowsAffected == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "User tidak ditemukan"})
		}

		return c.JSON(fiber.Map{
			"status":  "success",
			"message": fmt.Sprintf("Password pengguna ID %s berhasil diperbarui oleh Admin!", userID),
		})
	})

	// API: Mengubah Peran User (Khusus Admin)
	authGroup.Put("/users/:id/role", requireRole("admin"), func(c *fiber.Ctx) error {
		userID := c.Params("id")
		type UpdateRoleReq struct {
			Role string `json:"role"`
		}
		var req UpdateRoleReq
		if err := c.BodyParser(&req); err != nil || strings.TrimSpace(req.Role) == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Role is required"})
		}

		validRoles := map[string]bool{"admin": true, "operator": true, "viewer": true}
		if !validRoles[strings.ToLower(req.Role)] {
			return c.Status(400).JSON(fiber.Map{"error": "Role harus salah satu dari: admin, operator, viewer"})
		}

		res, err := dbConn.Exec("UPDATE users SET role = $1 WHERE id::text = $2", strings.ToLower(req.Role), userID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		rowsAffected, _ := res.RowsAffected()
		if rowsAffected == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "User tidak ditemukan"})
		}

		return c.JSON(fiber.Map{
			"status":  "success",
			"message": fmt.Sprintf("Peran pengguna ID %s berhasil diubah menjadi %s", userID, req.Role),
		})
	})

	// API: Tambah User Baru (Khusus Admin)
	authGroup.Post("/users", requireRole("admin"), func(c *fiber.Ctx) error {
		type CreateUserReq struct {
			Username string `json:"username"`
			Name     string `json:"name"`
			Password string `json:"password"`
			Role     string `json:"role"`
		}
		var req CreateUserReq
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
		}

		req.Username = strings.TrimSpace(req.Username)
		req.Name = strings.TrimSpace(req.Name)
		req.Password = strings.TrimSpace(req.Password)
		req.Role = strings.ToLower(strings.TrimSpace(req.Role))

		if req.Username == "" || req.Name == "" || req.Password == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Username, nama lengkap, dan password wajib diisi!"})
		}

		validRoles := map[string]bool{"admin": true, "operator": true, "viewer": true}
		if !validRoles[req.Role] {
			req.Role = "viewer"
		}

		var existingCount int
		_ = dbConn.QueryRow("SELECT COUNT(*) FROM users WHERE username = $1", req.Username).Scan(&existingCount)
		if existingCount > 0 {
			return c.Status(400).JSON(fiber.Map{"error": fmt.Sprintf("Username '%s' sudah digunakan oleh pengguna lain!", req.Username)})
		}

		var lastID int
		err := dbConn.QueryRow("INSERT INTO users (username, name, password, role) VALUES ($1, $2, $3, $4) RETURNING id", req.Username, req.Name, req.Password, req.Role).Scan(&lastID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.Status(201).JSON(fiber.Map{
			"status":  "success",
			"message": "Pengguna baru berhasil ditambahkan!",
			"user": fiber.Map{
				"id":       lastID,
				"username": req.Username,
				"name":     req.Name,
				"role":     req.Role,
			},
		})
	})

	// API: Hapus User (Khusus Admin)
	authGroup.Delete("/users/:id", requireRole("admin"), func(c *fiber.Ctx) error {
		userID := c.Params("id")

		res, err := dbConn.Exec("DELETE FROM users WHERE id::text = $1", userID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		rowsAffected, _ := res.RowsAffected()
		if rowsAffected == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Pengguna tidak ditemukan"})
		}

		return c.JSON(fiber.Map{
			"status":  "success",
			"message": fmt.Sprintf("Pengguna ID %s berhasil dihapus", userID),
			"user_id": userID,
		})
	})

	api := app.Group("/api/devices")

	// 1. API: List Perangkat (Dapat diakses oleh admin, operator, viewer)
	api.Get("/", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		rows, err := dbConn.Query("SELECT id, name, status, power, COALESCE(allowed_roles, 'admin,operator'), COALESCE(priority, 2) FROM devices")
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		defer rows.Close()

		type DeviceItem struct {
			ID           string  `json:"id"`
			Name         string  `json:"name"`
			Status       bool    `json:"status"`
			Power        int     `json:"power"`
			AllowedRoles string  `json:"allowed_roles"`
			Priority     int     `json:"priority"`
			VoltageVolts float64 `json:"voltage_volts"`
			CurrentAmps  float64 `json:"current_amps"`
		}

		devices := make([]DeviceItem, 0)
		for rows.Next() {
			var d DeviceItem
			if err := rows.Scan(&d.ID, &d.Name, &d.Status, &d.Power, &d.AllowedRoles, &d.Priority); err == nil {
				if d.Status {
					d.VoltageVolts = 220.0
					d.CurrentAmps = float64(d.Power) / 220.0
				} else {
					d.Power = 0
					d.VoltageVolts = 0.0
					d.CurrentAmps = 0.0
				}
				devices = append(devices, d)
			}
		}

		return c.JSON(fiber.Map{
			"status":        "success",
			"total":         len(devices),
			"local_devices": devices,
		})
	})

	// 1b. API: Pindai Perangkat Tuya (Smart Discovery seperti aplikasi Smart Life / Tuya Smart)
	api.Get("/scan", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		if tuyaAccessID == "" || tuyaAccessKey == "" {
			return c.Status(400).JSON(fiber.Map{
				"status":  "error",
				"message": "Kredensial TUYA_ACCESS_ID atau TUYA_ACCESS_KEY belum dikonfigurasi di file .env",
			})
		}

		// Ambil daftar perangkat yang telah tersimpan di SQLite BARA-Sense
		rows, err := dbConn.Query("SELECT id, name, COALESCE(priority, 2), COALESCE(allowed_roles, 'admin,operator,viewer') FROM devices")
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		defer rows.Close()

		type RegDevInfo struct {
			Name         string
			Priority     int
			AllowedRoles string
		}
		registeredMap := make(map[string]RegDevInfo)
		for rows.Next() {
			var rID, rName, rRoles string
			var rPrio int
			if err := rows.Scan(&rID, &rName, &rPrio, &rRoles); err == nil {
				registeredMap[rID] = RegDevInfo{Name: rName, Priority: rPrio, AllowedRoles: rRoles}
			}
		}

		// Panggil Tuya Cloud OpenAPI untuk mendeteksi semua perangkat yang tertaut pada akun Smart Life / Tuya
		client := tuya.NewCloudClient("", tuyaAccessID, tuyaAccessKey, tuyaEndpoint)
		cloudResp, err := client.GetCloudDevices()
		if err != nil {
			return c.Status(502).JSON(fiber.Map{
				"status":  "error",
				"message": fmt.Sprintf("Gagal menghubungi gateway Tuya Cloud API: %v", err),
			})
		}

		type ScannedItem struct {
			ID                 string  `json:"id"`
			Name               string  `json:"name"`
			ProductName        string  `json:"product_name"`
			Model              string  `json:"model"`
			Category           string  `json:"category"`
			CategoryLabel      string  `json:"category_label"`
			CategoryIcon       string  `json:"category_icon"`
			Online             bool    `json:"online"`
			IP                 string  `json:"ip"`
			Status             bool    `json:"status"`
			PowerWatt          int     `json:"power_watt"`
			VoltageVolts       float64 `json:"voltage_volts"`
			CurrentAmps        float64 `json:"current_amps"`
			AlreadyRegistered  bool    `json:"already_registered"`
			RegisteredName     string  `json:"registered_name"`
			RegisteredPriority int     `json:"registered_priority"`
			DiscoverySource    string  `json:"discovery_source"`
		}

		scannedDevices := make([]ScannedItem, 0)
		var newDevicesCount, registeredCount int

		if cloudResp != nil {
			b, errM := json.Marshal(cloudResp)
			if errM == nil {
				var tResp TuyaCloudResponse
				if errU := json.Unmarshal(b, &tResp); errU == nil && tResp.Success {
					for _, cDev := range tResp.Result.Devices {
						cStatus, pWatt, cVolt, cCurr, _ := parseCloudDeviceTelemetry(cDev)
						catLabel, catIcon := getTuyaCategoryInfo(cDev.Category)
						prodName := cDev.Model
						if prodName == "" {
							prodName = cDev.ProductName
						}
						if prodName == "" {
							prodName = catLabel
						}

						regInfo, isReg := registeredMap[cDev.ID]
						if isReg {
							registeredCount++
						} else {
							newDevicesCount++
						}

						item := ScannedItem{
							ID:                 cDev.ID,
							Name:               cDev.Name,
							ProductName:        prodName,
							Model:              cDev.Model,
							Category:           cDev.Category,
							CategoryLabel:      catLabel,
							CategoryIcon:       catIcon,
							Online:             cDev.Online,
							IP:                 cDev.IP,
							Status:             cStatus,
							PowerWatt:          int(math.Round(pWatt)),
							VoltageVolts:       cVolt,
							CurrentAmps:        cCurr,
							AlreadyRegistered:  isReg,
							RegisteredName:     regInfo.Name,
							RegisteredPriority: regInfo.Priority,
							DiscoverySource:    "Tuya Cloud (Smart Life)",
						}
						scannedDevices = append(scannedDevices, item)
					}
				}
			}
		}

		return c.JSON(fiber.Map{
			"status":            "success",
			"cloud_connected":   true,
			"total_discovered":  len(scannedDevices),
			"new_devices_count": newDevicesCount,
			"registered_count":  registeredCount,
			"devices":           scannedDevices,
			"scanned_at":        time.Now().Format("2006-01-02 15:04:05"),
		})
	})

	// 1c. API: Import Perangkat Terpindai dari Tuya (1-Click Tambahkan atau Tambahkan Sekaligus)
	api.Post("/import", requireRole("admin"), func(c *fiber.Ctx) error {
		type ImportDeviceItem struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			Power    int    `json:"power"`
			Priority int    `json:"priority"`
		}
		type ImportReq struct {
			Devices []ImportDeviceItem `json:"devices"`
		}
		var req ImportReq
		if err := c.BodyParser(&req); err != nil || len(req.Devices) == 0 {
			return c.Status(400).JSON(fiber.Map{"error": "Daftar perangkat untuk di-import tidak boleh kosong"})
		}

		client := tuya.NewCloudClient("", tuyaAccessID, tuyaAccessKey, tuyaEndpoint)
		cloudResp, _ := client.GetCloudDevices()
		cloudMap := make(map[string]TuyaCloudDeviceItem)
		if cloudResp != nil {
			if b, errM := json.Marshal(cloudResp); errM == nil {
				var tResp TuyaCloudResponse
				if errU := json.Unmarshal(b, &tResp); errU == nil && tResp.Success {
					for _, cd := range tResp.Result.Devices {
						cloudMap[cd.ID] = cd
					}
				}
			}
		}

		importedCount := 0
		for _, dev := range req.Devices {
			devID := strings.TrimSpace(dev.ID)
			if devID == "" {
				continue
			}

			devName := strings.TrimSpace(dev.Name)
			devPower := dev.Power
			devStatus := false
			prio := dev.Priority
			if prio < 1 || prio > 3 {
				prio = 2
			}

			if cd, ok := cloudMap[devID]; ok {
				cStatus, pWatt, _, _, _ := parseCloudDeviceTelemetry(cd)
				devStatus = cStatus
				if devName == "" && cd.Name != "" {
					devName = cd.Name
				}
				if devPower <= 0 && pWatt > 0 {
					devPower = int(math.Round(pWatt))
				}
			} else {
				// Fallback untuk perangkat yang dishare ke akun (Direct Query via Device ID)
				singleClient := tuya.NewCloudClient(devID, tuyaAccessID, tuyaAccessKey, tuyaEndpoint)
				if details, errDet := singleClient.GetDeviceDetails(); errDet == nil && details != nil {
					cPower, cStatus, cName, fetched := extractTuyaDevicePowerAndInfo(details)
					if fetched {
						devStatus = cStatus
						if devName == "" && cName != "" {
							devName = cName
						}
						if devPower <= 0 && cPower > 0 {
							devPower = cPower
						}
					}
				}
			}

			if devName == "" {
				devName = "Perangkat " + devID
			}
			if devPower <= 0 {
				devPower = 60
			}

			_, err := dbConn.Exec(`
				INSERT INTO devices (id, name, status, power, priority, allowed_roles) 
				VALUES ($1, $2, $3, $4, $5, 'admin,operator,viewer') 
				ON CONFLICT (id) DO UPDATE SET 
					name = EXCLUDED.name, 
					status = EXCLUDED.status, 
					power = EXCLUDED.power,
					priority = EXCLUDED.priority
			`, devID, devName, devStatus, devPower, prio)

			if err == nil {
				importedCount++
			}
		}

		broadcastAnalytics(dbConn, wsHub)

		return c.JSON(fiber.Map{
			"status":         "success",
			"message":        fmt.Sprintf("Berhasil mendaftarkan %d perangkat dari Tuya ke BARA-Sense!", importedCount),
			"imported_count": importedCount,
		})
	})

	// 1d. API: Sinkronisasi Semua Data & Nama Perangkat dari Tuya Cloud
	api.Post("/sync-all", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		client := tuya.NewCloudClient("", tuyaAccessID, tuyaAccessKey, tuyaEndpoint)
		cloudResp, err := client.GetCloudDevices()
		if err != nil || cloudResp == nil {
			return c.Status(502).JSON(fiber.Map{"error": "Gagal menghubungi Tuya Cloud untuk sinkronisasi"})
		}

		syncedCount := 0
		b, errM := json.Marshal(cloudResp)
		if errM == nil {
			var tResp TuyaCloudResponse
			if errU := json.Unmarshal(b, &tResp); errU == nil && tResp.Success {
				for _, cd := range tResp.Result.Devices {
					cStatus, pWatt, _, _, _ := parseCloudDeviceTelemetry(cd)
					pInt := int(math.Round(pWatt))
					devName := cd.Name
					if devName == "" {
						devName = "Perangkat " + cd.ID
					}

					res, errUp := dbConn.Exec(`
						INSERT INTO devices (id, name, status, power, priority, allowed_roles)
						VALUES ($1, $2, $3, $4, 2, 'admin,operator,viewer')
						ON CONFLICT (id) DO UPDATE SET
							name = EXCLUDED.name,
							status = EXCLUDED.status,
							power = EXCLUDED.power
					`, cd.ID, devName, cStatus, pInt)

					if errUp == nil {
						if n, _ := res.RowsAffected(); n > 0 {
							syncedCount++
						}
					}
				}
			}
		}

		broadcastAnalytics(dbConn, wsHub)

		return c.JSON(fiber.Map{
			"status":       "success",
			"message":      fmt.Sprintf("Berhasil menyinkronkan %d perangkat dengan data Tuya Cloud!", syncedCount),
			"synced_count": syncedCount,
		})
	})

	// 1e. API: Request Pairing Token dari Tuya OpenAPI untuk Web Bluetooth / Direct Provisioning
	api.Post("/pairing-token", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		if tuyaAccessID == "" || tuyaAccessKey == "" {
			return c.Status(400).JSON(fiber.Map{
				"status":  "error",
				"message": "TUYA_ACCESS_ID atau TUYA_ACCESS_KEY belum dikonfigurasi di file .env",
			})
		}

		type TokenReq struct {
			TimeZone string `json:"time_zone"`
		}
		var req TokenReq
		_ = c.BodyParser(&req)

		client := tuya.NewCloudClient("", tuyaAccessID, tuyaAccessKey, tuyaEndpoint)
		tokenResp, err := client.CreatePairingToken(req.TimeZone)
		if err != nil || tokenResp == nil {
			return c.Status(502).JSON(fiber.Map{
				"status":  "error",
				"message": fmt.Sprintf("Gagal meminta token pairing dari Tuya Cloud: %v", err),
			})
		}

		return c.JSON(fiber.Map{
			"status": "success",
			"data":   tokenResp,
		})
	})

	// 1f. API: Periksa Status Pairing Token (Apakah perangkat baru sudah terikat ke Tuya Cloud)
	api.Post("/pairing-status", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		type StatusReq struct {
			Token string `json:"token"`
		}
		var req StatusReq
		_ = c.BodyParser(&req)

		if req.Token == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Parameter token wajib diisi"})
		}

		client := tuya.NewCloudClient("", tuyaAccessID, tuyaAccessKey, tuyaEndpoint)
		statusResp, err := client.GetPairingTokenStatus(req.Token)
		if err != nil {
			return c.Status(502).JSON(fiber.Map{"error": err.Error()})
		}

		// Otomatis sinkronkan jika ada perangkat baru yang sukses terikat
		cloudResp, _ := client.GetCloudDevices()
		if cloudResp != nil {
			b, errM := json.Marshal(cloudResp)
			if errM == nil {
				var tResp TuyaCloudResponse
				if errU := json.Unmarshal(b, &tResp); errU == nil && tResp.Success {
					for _, cd := range tResp.Result.Devices {
						cStatus, pWatt, _, _, _ := parseCloudDeviceTelemetry(cd)
						pInt := int(math.Round(pWatt))
						devName := cd.Name
						if devName == "" {
							devName = "Perangkat " + cd.ID
						}
						_, _ = dbConn.Exec(`
							INSERT INTO devices (id, name, status, power, priority, allowed_roles)
							VALUES ($1, $2, $3, $4, 2, 'admin,operator,viewer')
							ON CONFLICT (id) DO UPDATE SET
								name = EXCLUDED.name,
								status = EXCLUDED.status,
								power = EXCLUDED.power
						`, cd.ID, devName, cStatus, pInt)
					}
				}
			}
		}

		broadcastAnalytics(dbConn, wsHub)

		return c.JSON(fiber.Map{
			"status": "success",
			"data":   statusResp,
		})
	})

	// API: Update Prioritas Beban Perangkat (Khusus ADMIN)
	api.Put("/:id/priority", requireRole("admin"), func(c *fiber.Ctx) error {
		deviceID := c.Params("id")
		type UpdatePriorityReq struct {
			Priority int `json:"priority"`
		}
		var req UpdatePriorityReq
		if err := c.BodyParser(&req); err != nil || req.Priority < 1 || req.Priority > 3 {
			return c.Status(400).JSON(fiber.Map{"error": "Valid priority (1: Kritis, 2: Normal, 3: Sekunder) required"})
		}

		res, err := dbConn.Exec("UPDATE devices SET priority = $1 WHERE id = $2", req.Priority, deviceID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		rowsAffected, _ := res.RowsAffected()
		if rowsAffected == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Device not found"})
		}

		broadcastAnalytics(dbConn, wsHub)

		return c.JSON(fiber.Map{
			"status":    "success",
			"message":   "Prioritas proteksi perangkat berhasil diperbarui",
			"device_id": deviceID,
			"priority":  req.Priority,
		})
	})

	// API: Update Hak Akses Per Perangkat / Per-Device RBAC (Khusus ADMIN)
	api.Put("/:id/rbac", requireRole("admin"), func(c *fiber.Ctx) error {
		deviceID := c.Params("id")

		type UpdateDeviceRbacReq struct {
			AllowedRoles string `json:"allowed_roles"`
		}
		var req UpdateDeviceRbacReq
		if err := c.BodyParser(&req); err != nil || strings.TrimSpace(req.AllowedRoles) == "" {
			return c.Status(400).JSON(fiber.Map{"error": "allowed_roles mandatory!"})
		}

		res, err := dbConn.Exec("UPDATE devices SET allowed_roles = $1 WHERE id = $2", strings.TrimSpace(req.AllowedRoles), deviceID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		rowsAffected, _ := res.RowsAffected()
		if rowsAffected == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Device not found"})
		}

		broadcastAnalytics(dbConn, wsHub)

		return c.JSON(fiber.Map{
			"status":        "success",
			"message":       "Hak akses perangkat berhasil diperbarui",
			"device_id":     deviceID,
			"allowed_roles": req.AllowedRoles,
		})
	})

	// 2. API: Tambah Perangkat (Dibatasi Khusus ADMIN)
	api.Post("/", requireRole("admin"), func(c *fiber.Ctx) error {
		type CreateDeviceReq struct {
			ID    string `json:"id"`
			Name  string `json:"name"`
			Power int    `json:"power"`
		}
		var req CreateDeviceReq
		if err := c.BodyParser(&req); err != nil || strings.TrimSpace(req.ID) == "" {
			return c.Status(400).JSON(fiber.Map{"error": "id is required"})
		}

		client := tuya.NewCloudClient(req.ID, tuyaAccessID, tuyaAccessKey, tuyaEndpoint)
		details, _ := client.GetDeviceDetails()
		cloudPower, cloudStatus, cloudName, fetched := extractTuyaDevicePowerAndInfo(details)

		if fetched {
			if strings.TrimSpace(req.Name) == "" && cloudName != "" {
				req.Name = cloudName
			}
			if req.Power <= 0 && cloudPower > 0 {
				req.Power = cloudPower
			}
		}

		if strings.TrimSpace(req.Name) == "" {
			req.Name = "Perangkat " + req.ID
		}
		if req.Power <= 0 {
			req.Power = 60
		}

		_, err := dbConn.Exec("INSERT INTO devices (id, name, status, power) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, power=EXCLUDED.power", req.ID, req.Name, cloudStatus, req.Power)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		broadcastAnalytics(dbConn, wsHub)

		return c.Status(201).JSON(fiber.Map{
			"status":          "success",
			"message":         "Device successfully registered",
			"cloud_fetched":   fetched,
			"auto_power_watt": req.Power,
			"data":            req,
		})
	})

	// 3. API: Hapus Perangkat (Dibatasi Khusus ADMIN)
	api.Delete("/:id", requireRole("admin"), func(c *fiber.Ctx) error {
		deviceID := c.Params("id")
		if strings.TrimSpace(deviceID) == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Device ID is required"})
		}

		// Ambil nama perangkat terlebih dahulu sebelum dihapus agar dapat dicatat dalam riwayat
		var devName string
		_ = dbConn.QueryRow("SELECT name FROM devices WHERE id = $1", deviceID).Scan(&devName)
		if devName == "" {
			devName = deviceID
		}

		// Simpan nama ke device_name_history agar riwayat masa lalu tetap menampilkan nama perangkat
		_, _ = dbConn.Exec("INSERT INTO device_name_history (device_id, name) VALUES ($1, $2) ON CONFLICT (device_id) DO UPDATE SET name = EXCLUDED.name", deviceID, devName)

		res, err := dbConn.Exec("DELETE FROM devices WHERE id = $1", deviceID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		rowsAffected, _ := res.RowsAffected()
		if rowsAffected == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Device not found"})
		}

		// CATATAN: energy_logs SENGAJA TIDAK DIHAPUS agar seluruh riwayat konsumsi kWh,
		// biaya listrik per hari, dan statistik historis tetap tersimpan utuh di sistem.

		broadcastAnalytics(dbConn, wsHub)

		return c.JSON(fiber.Map{
			"status":    "success",
			"message":   fmt.Sprintf("Perangkat '%s' berhasil dihapus dari daftar kontrol. Seluruh riwayat penggunaan listrik dan biaya tetap tersimpan.", devName),
			"device_id": deviceID,
		})
	})

	// 4. API: Analitik Penggunaan Listrik & Biaya (Dapat diakses oleh admin, operator, viewer)
	app.Get("/api/analytics", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		tariffCode := c.Query("tariff", "1300-2200VA")
		data, err := getAnalyticsData(dbConn, tariffCode)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(data)
	})

	// 4b. API: Riwayat Penggunaan Listrik Harian (Daily History)
	app.Get("/api/analytics/history/daily", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		days := c.QueryInt("days", 7)
		deviceID := c.Query("device_id", "")
		tariffCode := c.Query("tariff", "1300-2200VA")

		rate := 1444.70
		if tariffCode == "450VA" {
			rate = 415.0
		} else if tariffCode == "900VA" {
			rate = 1352.0
		} else if tariffCode == "3500VA+" {
			rate = 1699.53
		}

		resp, err := getDailyHistoryResponse(dbConn, days, deviceID, rate)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(resp)
	})

	// 4b-2. API: Sinkronisasi Riwayat Pemakaian Listrik dengan Tuya Hardware Meter
	app.Post("/api/analytics/history/sync", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		type SyncReq struct {
			DeviceID string `json:"device_id"`
			Days     int    `json:"days"`
		}
		var req SyncReq
		_ = c.BodyParser(&req)
		days := req.Days
		if days <= 0 {
			days = 30
		}

		results, err := syncAllEnergyHistoryFromTuya(dbConn, tuyaAccessID, tuyaAccessKey, tuyaEndpoint, req.DeviceID, days)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": fmt.Sprintf("Gagal menyinkronkan dengan Tuya: %v", err)})
		}

		syncedCount := 0
		var totalKwhSynced float64
		for _, r := range results {
			if r.Status == "synced" {
				syncedCount++
				totalKwhSynced += r.TuyaKwh
			}
		}

		broadcastAnalytics(dbConn, wsHub)

		return c.JSON(fiber.Map{
			"status":           "success",
			"message":          fmt.Sprintf("Berhasil menyinkronkan riwayat pemakaian energi %d perangkat dengan Tuya Cloud (%.3f kWh)!", syncedCount, totalKwhSynced),
			"synced_count":     syncedCount,
			"total_kwh_synced": totalKwhSynced,
			"details":          results,
		})
	})

	// 4c. API: Ekspor CSV Rekapitulasi Riwayat Harian Listrik
	app.Get("/api/analytics/history/daily/export", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		days := c.QueryInt("days", 30)
		deviceID := c.Query("device_id", "")
		tariffCode := c.Query("tariff", "1300-2200VA")

		rate := 1444.70
		if tariffCode == "450VA" {
			rate = 415.0
		} else if tariffCode == "900VA" {
			rate = 1352.0
		} else if tariffCode == "3500VA+" {
			rate = 1699.53
		}

		resp, err := getDailyHistoryResponse(dbConn, days, deviceID, rate)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		history, ok := resp["history"].([]fiber.Map)
		if !ok {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to generate CSV data"})
		}

		var buf bytes.Buffer
		// UTF-8 BOM for Excel
		buf.Write([]byte{0xEF, 0xBB, 0xBF})
		buf.WriteString("Tanggal,Hari,Konsumsi (kWh),Estimasi Biaya (Rp),Beban Rata-rata (Watt),Beban Puncak (Watt),Status Efisiensi,Jumlah Perangkat\n")

		for _, item := range history {
			fullDate := fmt.Sprintf("%v", item["full_date"])
			dayName := fmt.Sprintf("%v", item["day"])
			kwhVal := item["kwh"]
			costVal := item["cost"]
			avgWatt := item["avg_power_watt"]
			peakWatt := item["peak_power_watt"]
			statusEff := fmt.Sprintf("%v", item["status_efficiency"])
			devCount := item["device_count"]

			buf.WriteString(fmt.Sprintf("\"%s\",\"%s\",%.2f,%.0f,%v,%v,\"%s\",%v\n",
				fullDate, dayName, kwhVal, costVal, avgWatt, peakWatt, statusEff, devCount))
		}

		c.Set("Content-Type", "text/csv; charset=utf-8")
		c.Set("Content-Disposition", "attachment; filename=\"rekap-harian-listrik-BARA-Sense.csv\"")
		return c.Send(buf.Bytes())
	})

	// 4d. API: Validasi Akurasi Riwayat Energi BARA-Sense dengan Hardware Meter Tuya Cloud
	app.Get("/api/analytics/validate-tuya", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		devices, err := fetchTuyaCloudDeviceList(tuyaAccessID, tuyaAccessKey, tuyaEndpoint)
		if err != nil {
			return c.Status(502).JSON(fiber.Map{"error": fmt.Sprintf("Gagal terhubung ke Tuya Cloud: %v", err)})
		}

		type DeviceValidation struct {
			DeviceID        string  `json:"device_id"`
			Name            string  `json:"name"`
			TuyaHardwareKwh float64 `json:"tuya_hardware_kwh"`
			BaraRecordedKwh float64 `json:"bara_recorded_kwh"`
			DifferenceKwh   float64 `json:"difference_kwh"`
			AccuracyPercent float64 `json:"accuracy_percent"`
			Status          string  `json:"status"`
			Online          bool    `json:"online"`
			HasMeter        bool    `json:"has_meter"`
		}

		var results []DeviceValidation
		var totalTuyaKwh, totalBaraKwh float64

		for _, cd := range devices {
			name := cd.Name
			if name == "" {
				name = cd.ID
			}

			if !deviceHasEnergyMeter(cd) {
				var baraKwh float64
				_ = dbConn.QueryRow("SELECT COALESCE(SUM(kwh), 0) FROM energy_logs WHERE device_id = $1 AND kwh >= 0", cd.ID).Scan(&baraKwh)
				results = append(results, DeviceValidation{
					DeviceID:        cd.ID,
					Name:            name,
					TuyaHardwareKwh: 0,
					BaraRecordedKwh: roundTo(baraKwh, 3),
					DifferenceKwh:   0,
					AccuracyPercent: 100.0,
					Status:          "ESTIMASI_LOKAL",
					Online:          cd.Online,
					HasMeter:        false,
				})
				continue
			}

			tuyaKwh, winStart, hasTuya, _ := sumTuyaEnergyForWindow(tuyaAccessID, tuyaAccessKey, tuyaEndpoint, cd.ID, 30)
			var baraKwh float64
			if hasTuya {
				_ = dbConn.QueryRow("SELECT COALESCE(SUM(kwh), 0) FROM energy_logs WHERE device_id = $1 AND timestamp >= $2 AND kwh >= 0", cd.ID, winStart).Scan(&baraKwh)
			} else {
				_ = dbConn.QueryRow("SELECT COALESCE(SUM(kwh), 0) FROM energy_logs WHERE device_id = $1 AND kwh >= 0", cd.ID).Scan(&baraKwh)
				tuyaKwh = baraKwh
			}

			diff := baraKwh - tuyaKwh
			acc := 100.0
			if tuyaKwh > 0 {
				acc = 100.0 - (math.Abs(diff) / tuyaKwh * 100.0)
				if acc < 0 {
					acc = 0.0
				}
			}

			statusStr := "OPTIMAL"
			if math.Abs(diff) > 0.05 && tuyaKwh > 0 {
				statusStr = "PERLU_KALIBRASI"
			}

			results = append(results, DeviceValidation{
				DeviceID:        cd.ID,
				Name:            name,
				TuyaHardwareKwh: roundTo(tuyaKwh, 3),
				BaraRecordedKwh: roundTo(baraKwh, 3),
				DifferenceKwh:   roundTo(diff, 3),
				AccuracyPercent: roundTo(acc, 1),
				Status:          statusStr,
				Online:          cd.Online,
				HasMeter:        true,
			})

			totalTuyaKwh += tuyaKwh
			totalBaraKwh += baraKwh
		}

		totalDiff := totalBaraKwh - totalTuyaKwh
		overallAcc := 100.0
		if totalTuyaKwh > 0 {
			overallAcc = 100.0 - (math.Abs(totalDiff) / totalTuyaKwh * 100.0)
			if overallAcc < 0 {
				overallAcc = 0
			}
		}

		return c.JSON(fiber.Map{
			"status":           "success",
			"validated_at":     time.Now().Format("2006-01-02 15:04:05"),
			"overall_accuracy": roundTo(overallAcc, 1),
			"total_tuya_kwh":   roundTo(totalTuyaKwh, 3),
			"total_bara_kwh":   roundTo(totalBaraKwh, 3),
			"devices":          results,
		})
	})

	// 4e. API: Kalibrasi Akumulasi Energi Lokal dengan Tuya Hardware Meter
	app.Post("/api/analytics/calibrate-tuya", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		type CalibrateReq struct {
			DeviceID string `json:"device_id"`
		}
		var req CalibrateReq
		_ = c.BodyParser(&req)

		results, err := syncAllEnergyHistoryFromTuya(dbConn, tuyaAccessID, tuyaAccessKey, tuyaEndpoint, req.DeviceID, 30)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": fmt.Sprintf("Gagal kalibrasi dari Tuya: %v", err)})
		}

		calibratedCount := 0
		for _, r := range results {
			if r.Status == "synced" {
				calibratedCount++
			}
		}

		broadcastAnalytics(dbConn, wsHub)

		return c.JSON(fiber.Map{
			"status":           "success",
			"message":          fmt.Sprintf("Berhasil melakukan kalibrasi energi untuk %d perangkat dari Tuya Hardware Meter!", calibratedCount),
			"calibrated_count": calibratedCount,
			"details":          results,
		})
	})

	// 5. API: Detail Perangkat dari Tuya Cloud
	api.Get("/:id", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		deviceID := c.Params("id")
		client := tuya.NewCloudClient(deviceID, tuyaAccessID, tuyaAccessKey, tuyaEndpoint)

		details, err := client.GetDeviceDetails()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		cloudPower, cloudStatus, cloudName, fetched := extractTuyaDevicePowerAndInfo(details)

		if fetched {
			if cloudPower > 0 {
				_, _ = dbConn.Exec("UPDATE devices SET power = $1, status = $2 WHERE id = $3", cloudPower, cloudStatus, deviceID)
			} else {
				_, _ = dbConn.Exec("UPDATE devices SET status = $1 WHERE id = $2", cloudStatus, deviceID)
			}
			broadcastAnalytics(dbConn, wsHub)
		}

		return c.JSON(fiber.Map{
			"status":          "success",
			"cloud_fetched":   fetched,
			"auto_name":       cloudName,
			"auto_power_watt": cloudPower,
			"auto_status":     cloudStatus,
			"data":            details,
		})
	})

	// 6. API: ON/OFF Perangkat (Divalidasi Berdasarkan Hak Akses Per-Device)
	api.Post("/:id/toggle", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		deviceID := c.Params("id")

		userRole := c.Get("X-User-Role")
		if userRole == "" {
			userRole = "viewer"
		}

		// Pengecekan Hak Akses Khusus Perangkat (Per-Device RBAC)
		var allowedRoles string
		err := dbConn.QueryRow("SELECT COALESCE(allowed_roles, 'admin,operator') FROM devices WHERE id = $1", deviceID).Scan(&allowedRoles)
		if err == nil && allowedRoles != "" {
			rolesList := strings.Split(allowedRoles, ",")
			roleAllowed := false
			for _, r := range rolesList {
				if strings.TrimSpace(r) == userRole {
					roleAllowed = true
					break
				}
			}
			if !roleAllowed {
				return c.Status(403).JSON(fiber.Map{
					"status": "forbidden",
					"error":  fmt.Sprintf("Akses Perangkat Ditolak: Perangkat ini dikonfigurasi khusus untuk peran (%s). Peran Anda saat ini: '%s'.", allowedRoles, userRole),
				})
			}
		}

		type ToggleRequest struct {
			Status bool `json:"status"`
		}
		var req ToggleRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
		}

		errToggle := executeDeviceToggle(dbConn, wsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint, deviceID, req.Status)
		if errToggle != nil {
			return c.Status(500).JSON(fiber.Map{"error": errToggle.Error()})
		}

		return c.JSON(fiber.Map{
			"status":         "success",
			"message":        "Device state updated",
			"device_id":      deviceID,
			"current_status": req.Status,
		})
	})

	// 7. API: Penjadwalan Otomatis (Smart Schedules)
	app.Get("/api/schedules", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		rows, err := dbConn.Query(`
			SELECT s.id, s.device_id, COALESCE(d.name, s.device_name), s.action, s.time_target, s.days, s.is_active, s.created_at
			FROM schedules s
			LEFT JOIN devices d ON d.id = s.device_id
			ORDER BY s.time_target ASC
		`)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		defer rows.Close()

		var list []ScheduleItem
		for rows.Next() {
			var item ScheduleItem
			if err := rows.Scan(&item.ID, &item.DeviceID, &item.DeviceName, &item.Action, &item.TimeTarget, &item.Days, &item.IsActive, &item.CreatedAt); err == nil {
				list = append(list, item)
			}
		}
		if list == nil {
			list = []ScheduleItem{}
		}
		return c.JSON(fiber.Map{"status": "success", "schedules": list})
	})

	app.Post("/api/schedules", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		type NewScheduleReq struct {
			DeviceID   string `json:"device_id"`
			Action     string `json:"action"`
			TimeTarget string `json:"time_target"`
			Days       string `json:"days"`
		}
		var req NewScheduleReq
		if err := c.BodyParser(&req); err != nil || strings.TrimSpace(req.DeviceID) == "" || strings.TrimSpace(req.TimeTarget) == "" {
			return c.Status(400).JSON(fiber.Map{"error": "device_id dan time_target (HH:MM) wajib diisi"})
		}
		req.Action = strings.ToUpper(strings.TrimSpace(req.Action))
		if req.Action != "ON" && req.Action != "OFF" {
			req.Action = "ON"
		}
		if strings.TrimSpace(req.Days) == "" {
			req.Days = "ALL"
		}

		var devName string
		_ = dbConn.QueryRow("SELECT name FROM devices WHERE id = $1", req.DeviceID).Scan(&devName)
		if devName == "" {
			devName = req.DeviceID
		}

		var newID int
		err := dbConn.QueryRow("INSERT INTO schedules (device_id, device_name, action, time_target, days, is_active) VALUES ($1, $2, $3, $4, $5, true) RETURNING id",
			req.DeviceID, devName, req.Action, req.TimeTarget, req.Days).Scan(&newID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.Status(201).JSON(fiber.Map{
			"status":  "success",
			"message": "Jadwal berhasil ditambahkan",
			"id":      newID,
		})
	})

	app.Put("/api/schedules/:id/toggle", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		schedID := c.Params("id")
		_, err := dbConn.Exec("UPDATE schedules SET is_active = NOT is_active WHERE id = $1", schedID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "success", "message": "Status jadwal berhasil diperbarui"})
	})

	app.Delete("/api/schedules/:id", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		schedID := c.Params("id")
		_, err := dbConn.Exec("DELETE FROM schedules WHERE id = $1", schedID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "success", "message": "Jadwal berhasil dihapus"})
	})

	// 8. API: Timer Hitung Mundur (Auto-Off Countdown)
	app.Get("/api/timers", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		rows, err := dbConn.Query(`
			SELECT dt.device_id, COALESCE(d.name, dt.device_id), dt.target_action, dt.expires_at, dt.duration_minutes,
			CAST(EXTRACT(EPOCH FROM (dt.expires_at - CURRENT_TIMESTAMP)) AS INTEGER) as rem_sec
			FROM device_timers dt
			LEFT JOIN devices d ON d.id = dt.device_id
			WHERE dt.expires_at > CURRENT_TIMESTAMP
		`)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		defer rows.Close()

		var timers []DeviceTimerItem
		for rows.Next() {
			var t DeviceTimerItem
			if err := rows.Scan(&t.DeviceID, &t.DeviceName, &t.TargetAction, &t.ExpiresAt, &t.DurationMinutes, &t.RemainingSec); err == nil {
				if t.RemainingSec > 0 {
					timers = append(timers, t)
				}
			}
		}
		if timers == nil {
			timers = []DeviceTimerItem{}
		}
		return c.JSON(fiber.Map{"status": "success", "timers": timers})
	})

	app.Post("/api/devices/:id/timer", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		deviceID := c.Params("id")
		type SetTimerReq struct {
			DurationMinutes int    `json:"duration_minutes"`
			TargetAction    string `json:"target_action"`
		}
		var req SetTimerReq
		if err := c.BodyParser(&req); err != nil || req.DurationMinutes <= 0 {
			return c.Status(400).JSON(fiber.Map{"error": "duration_minutes harus lebih besar dari 0"})
		}
		if strings.TrimSpace(req.TargetAction) == "" {
			req.TargetAction = "OFF"
		} else {
			req.TargetAction = strings.ToUpper(strings.TrimSpace(req.TargetAction))
		}

		expTime := time.Now().Add(time.Duration(req.DurationMinutes) * time.Minute).Format("2006-01-02 15:04:05")

		_, err := dbConn.Exec(`
			INSERT INTO device_timers (device_id, target_action, expires_at, duration_minutes)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (device_id) DO UPDATE SET target_action=EXCLUDED.target_action, expires_at=EXCLUDED.expires_at, duration_minutes=EXCLUDED.duration_minutes
		`, deviceID, req.TargetAction, expTime, req.DurationMinutes)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{
			"status":           "success",
			"message":          fmt.Sprintf("Timer aktif: perangkat akan %s dalam %d menit", req.TargetAction, req.DurationMinutes),
			"device_id":        deviceID,
			"expires_at":       expTime,
			"duration_minutes": req.DurationMinutes,
		})
	})

	app.Delete("/api/devices/:id/timer", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		deviceID := c.Params("id")
		_, err := dbConn.Exec("DELETE FROM device_timers WHERE device_id = $1", deviceID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{
			"status":    "success",
			"message":   "Timer berhasil dibatalkan",
			"device_id": deviceID,
		})
	})

	// 9. API: Mode Skenario Cepat (One-Touch Scenes)
	app.Get("/api/scenes", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		rows, err := dbConn.Query("SELECT id, name, icon, description, actions, is_preset, created_at FROM scenes ORDER BY is_preset DESC, name ASC")
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		defer rows.Close()

		var list []fiber.Map
		for rows.Next() {
			var id, name, icon, desc, actionsJSON, createdAt string
			var isPreset bool
			if err := rows.Scan(&id, &name, &icon, &desc, &actionsJSON, &isPreset, &createdAt); err == nil {
				var actions []SceneAction
				_ = json.Unmarshal([]byte(actionsJSON), &actions)
				list = append(list, fiber.Map{
					"id":          id,
					"name":        name,
					"icon":        icon,
					"description": desc,
					"actions":     actions,
					"is_preset":   isPreset,
					"created_at":  createdAt,
				})
			}
		}
		if list == nil {
			list = []fiber.Map{}
		}
		return c.JSON(fiber.Map{"status": "success", "scenes": list})
	})

	app.Post("/api/scenes/:id/execute", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		sceneID := c.Params("id")
		err := executeScene(dbConn, wsHub, tuyaAccessID, tuyaAccessKey, tuyaEndpoint, sceneID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		var sceneName string
		_ = dbConn.QueryRow("SELECT name FROM scenes WHERE id = $1", sceneID).Scan(&sceneName)
		return c.JSON(fiber.Map{
			"status":     "success",
			"message":    fmt.Sprintf("Skenario '%s' berhasil dijalankan", sceneName),
			"scene_id":   sceneID,
			"scene_name": sceneName,
		})
	})

	app.Post("/api/scenes", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		type NewSceneReq struct {
			Name        string        `json:"name"`
			Icon        string        `json:"icon"`
			Description string        `json:"description"`
			Actions     []SceneAction `json:"actions"`
		}
		var req NewSceneReq
		if err := c.BodyParser(&req); err != nil || strings.TrimSpace(req.Name) == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Nama skenario wajib diisi"})
		}
		if req.Icon == "" {
			req.Icon = "✨"
		}
		actionsJSON, _ := json.Marshal(req.Actions)
		newID := fmt.Sprintf("custom_%d", time.Now().Unix())

		_, err := dbConn.Exec("INSERT INTO scenes (id, name, icon, description, actions, is_preset) VALUES ($1, $2, $3, $4, $5, false)",
			newID, req.Name, req.Icon, req.Description, string(actionsJSON))
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.Status(201).JSON(fiber.Map{"status": "success", "message": "Skenario kustom berhasil disimpan", "id": newID})
	})

	app.Delete("/api/scenes/:id", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		sceneID := c.Params("id")
		var isPreset bool
		_ = dbConn.QueryRow("SELECT is_preset FROM scenes WHERE id = $1", sceneID).Scan(&isPreset)
		if isPreset {
			return c.Status(400).JSON(fiber.Map{"error": "Skenario preset bawaan tidak dapat dihapus"})
		}
		_, err := dbConn.Exec("DELETE FROM scenes WHERE id = $1", sceneID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "success", "message": "Skenario berhasil dihapus"})
	})

	// 10. API: Proteksi Anti-Jeglek PLN (Load Shedding)
	app.Get("/api/power-guard", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		var limit, duration int
		var enabled bool
		var lastTrigger sql.NullString
		err := dbConn.QueryRow("SELECT max_watt_limit, is_enabled, cutoff_duration_sec, last_triggered_at FROM power_guard_config WHERE id = 1").
			Scan(&limit, &enabled, &duration, &lastTrigger)
		if err != nil {
			limit = 1150
			enabled = true
			duration = 8
		}

		var currentTotalWatts int
		_ = dbConn.QueryRow("SELECT COALESCE(SUM(power), 0) FROM devices WHERE status = true").Scan(&currentTotalWatts)

		lastTrigStr := ""
		if lastTrigger.Valid {
			lastTrigStr = lastTrigger.String
		}

		loadPct := 0.0
		if limit > 0 {
			loadPct = float64(currentTotalWatts) / float64(limit) * 100.0
		}

		return c.JSON(fiber.Map{
			"status":              "success",
			"max_watt_limit":      limit,
			"is_enabled":          enabled,
			"cutoff_duration_sec": duration,
			"last_triggered_at":   lastTrigStr,
			"current_total_watts": currentTotalWatts,
			"is_overloaded":       currentTotalWatts > limit,
			"load_percentage":     fmt.Sprintf("%.1f", loadPct),
		})
	})

	app.Put("/api/power-guard", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		type UpdatePowerGuardReq struct {
			MaxWattLimit int  `json:"max_watt_limit"`
			IsEnabled    bool `json:"is_enabled"`
		}
		var req UpdatePowerGuardReq
		if err := c.BodyParser(&req); err != nil || req.MaxWattLimit <= 0 {
			return c.Status(400).JSON(fiber.Map{"error": "max_watt_limit harus lebih besar dari 0"})
		}
		_, err := dbConn.Exec("UPDATE power_guard_config SET max_watt_limit = $1, is_enabled = $2 WHERE id = 1", req.MaxWattLimit, req.IsEnabled)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "success", "message": "Konfigurasi Proteksi Anti-Jeglek berhasil diperbarui"})
	})

	// 11. API: Target Kuota Anggaran Bulanan & Ekspor CSV
	app.Get("/api/budget", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		var budget, warningPct float64
		err := dbConn.QueryRow("SELECT monthly_budget_idr, warning_threshold_pct FROM budget_settings WHERE id = 1").Scan(&budget, &warningPct)
		if err != nil {
			budget = 750000
			warningPct = 80
		}

		var currentMonthCost float64
		var currentMonthKwh float64
		_ = dbConn.QueryRow(`
			SELECT COALESCE(SUM(cost_idr), 0), COALESCE(SUM(kwh), 0)
			FROM energy_logs 
			WHERE date_trunc('month', timestamp) = date_trunc('month', CURRENT_TIMESTAMP) AND kwh >= 0
		`).Scan(&currentMonthCost, &currentMonthKwh)

		dayOfMonth := time.Now().Day()
		if dayOfMonth < 1 {
			dayOfMonth = 1
		}
		dailyAvg := currentMonthCost / float64(dayOfMonth)
		projectedMonthCost := dailyAvg * 30.0
		usagePct := 0.0
		if budget > 0 {
			usagePct = (currentMonthCost / budget) * 100.0
		}

		return c.JSON(fiber.Map{
			"status":                   "success",
			"monthly_budget_idr":       budget,
			"warning_threshold_pct":    warningPct,
			"current_month_cost_idr":   currentMonthCost,
			"current_month_kwh":        currentMonthKwh,
			"usage_pct":                fmt.Sprintf("%.1f", usagePct),
			"projected_month_cost_idr": projectedMonthCost,
			"is_near_limit":            usagePct >= warningPct,
			"is_exceeded":              currentMonthCost >= budget,
		})
	})

	app.Put("/api/budget", requireRole("admin", "operator"), func(c *fiber.Ctx) error {
		type UpdateBudgetReq struct {
			MonthlyBudgetIDR    float64 `json:"monthly_budget_idr"`
			WarningThresholdPct float64 `json:"warning_threshold_pct"`
		}
		var req UpdateBudgetReq
		if err := c.BodyParser(&req); err != nil || req.MonthlyBudgetIDR <= 0 {
			return c.Status(400).JSON(fiber.Map{"error": "monthly_budget_idr harus lebih dari 0"})
		}
		if req.WarningThresholdPct <= 0 {
			req.WarningThresholdPct = 80
		}
		_, err := dbConn.Exec("UPDATE budget_settings SET monthly_budget_idr = $1, warning_threshold_pct = $2 WHERE id = 1", req.MonthlyBudgetIDR, req.WarningThresholdPct)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "success", "message": "Target Anggaran Bulanan berhasil diperbarui"})
	})

	app.Get("/api/analytics/export", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		rows, err := dbConn.Query(`
			SELECT e.id, e.timestamp, COALESCE(d.name, dh.name || ' (Dihapus)', e.device_id), e.power_watt, e.kwh, e.cost_idr
			FROM energy_logs e
			LEFT JOIN devices d ON d.id = e.device_id
			LEFT JOIN device_name_history dh ON dh.device_id = e.device_id
			ORDER BY e.timestamp DESC
			LIMIT 1000
		`)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		defer rows.Close()

		var buf bytes.Buffer
		buf.WriteString("ID,Waktu,Perangkat,Daya (Watt),Konsumsi (kWh),Biaya (Rupiah)\n")
		for rows.Next() {
			var id int
			var ts, devName string
			var power int
			var kwh, cost float64
			if err := rows.Scan(&id, &ts, &devName, &power, &kwh, &cost); err == nil {
				buf.WriteString(fmt.Sprintf("%d,\"%s\",\"%s\",%d,%.4f,%.0f\n", id, ts, strings.ReplaceAll(devName, "\"", "\"\""), power, kwh, cost))
			}
		}

		c.Set("Content-Type", "text/csv; charset=utf-8")
		c.Set("Content-Disposition", "attachment; filename=\"laporan-penggunaan-listrik.csv\"")
		return c.Send(buf.Bytes())
	})

	// 12. API: Integrasi Notifikasi Bot Telegram
	app.Get("/api/telegram", requireRole("admin", "operator", "viewer"), func(c *fiber.Ctx) error {
		var token, chatID, digestTime string
		var enabled, notifyOverload, notifyLeak bool
		err := dbConn.QueryRow("SELECT bot_token, chat_id, is_enabled, notify_on_overload, notify_on_leak, daily_digest_time FROM telegram_config WHERE id = 1").
			Scan(&token, &chatID, &enabled, &notifyOverload, &notifyLeak, &digestTime)
		if err != nil {
			enabled = false
		}

		maskedToken := ""
		if len(token) > 6 {
			maskedToken = token[:6] + "****************"
		} else if token != "" {
			maskedToken = "******"
		}

		return c.JSON(fiber.Map{
			"status":             "success",
			"is_enabled":         enabled,
			"chat_id":            chatID,
			"masked_bot_token":   maskedToken,
			"has_token":          token != "",
			"notify_on_overload": notifyOverload,
			"notify_on_leak":     notifyLeak,
			"daily_digest_time":  digestTime,
		})
	})

	app.Put("/api/telegram", requireRole("admin"), func(c *fiber.Ctx) error {
		type UpdateTelegramReq struct {
			BotToken         string `json:"bot_token"`
			ChatID           string `json:"chat_id"`
			IsEnabled        bool   `json:"is_enabled"`
			NotifyOnOverload bool   `json:"notify_on_overload"`
			NotifyOnLeak     bool   `json:"notify_on_leak"`
		}
		var req UpdateTelegramReq
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
		}

		if strings.TrimSpace(req.BotToken) != "" {
			_, err := dbConn.Exec("UPDATE telegram_config SET bot_token = $1, chat_id = $2, is_enabled = $3, notify_on_overload = $4, notify_on_leak = $5 WHERE id = 1",
				req.BotToken, req.ChatID, req.IsEnabled, req.NotifyOnOverload, req.NotifyOnLeak)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{"error": err.Error()})
			}
		} else {
			_, err := dbConn.Exec("UPDATE telegram_config SET chat_id = $1, is_enabled = $2, notify_on_overload = $3, notify_on_leak = $4 WHERE id = 1",
				req.ChatID, req.IsEnabled, req.NotifyOnOverload, req.NotifyOnLeak)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{"error": err.Error()})
			}
		}

		return c.JSON(fiber.Map{"status": "success", "message": "Konfigurasi Bot Telegram berhasil disimpan"})
	})

	app.Post("/api/telegram/test", requireRole("admin"), func(c *fiber.Ctx) error {
		var token, chatID string
		_ = dbConn.QueryRow("SELECT bot_token, chat_id FROM telegram_config WHERE id = 1").Scan(&token, &chatID)
		if strings.TrimSpace(token) == "" || strings.TrimSpace(chatID) == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Bot Token dan Chat ID belum dikonfigurasi"})
		}

		testMsg := fmt.Sprintf("✅ *[BARA-Sense IoT]* Tes Notifikasi Bot Telegram Berhasil!\nSistem Building Automation & Realtime Analytics terhubung aktif pada %s.", time.Now().Format("02 Jan 2006 15:04:05 WIB"))
		apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)
		payload := map[string]string{
			"chat_id":    chatID,
			"text":       testMsg,
			"parse_mode": "Markdown",
		}
		jsonBytes, _ := json.Marshal(payload)
		resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(jsonBytes))
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": fmt.Sprintf("Gagal mengirim ke Telegram API: %v", err)})
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return c.Status(400).JSON(fiber.Map{"error": fmt.Sprintf("Telegram API mengembalikan status %d. Periksa kembali Bot Token dan Chat ID.", resp.StatusCode)})
		}

		return c.JSON(fiber.Map{"status": "success", "message": "Pesan uji coba berhasil terkirim ke Telegram Anda!"})
	})

	// Serve Static Frontend React Single Page App (dist directory)
	distPaths := []string{"../frontend/dist", "./frontend/dist", "frontend/dist", "./dist"}
	var foundDist string
	for _, p := range distPaths {
		if _, err := os.Stat(p); err == nil {
			foundDist = p
			break
		}
	}

	if foundDist != "" {
		log.Printf("Serving static frontend from %s\n", foundDist)
		app.Static("/", foundDist)
		app.Get("*", func(c *fiber.Ctx) error {
			if !strings.HasPrefix(c.Path(), "/api") && !strings.HasPrefix(c.Path(), "/ws") && !strings.HasPrefix(c.Path(), "/docs") {
				return c.SendFile(foundDist + "/index.html")
			}
			return c.Status(404).JSON(fiber.Map{"error": "Endpoint not found"})
		})
	}

	log.Printf("Server is running on port %s...\n", port)
	log.Printf("Frontend Web App available at http://localhost:%s\n", port)
	log.Printf("Swagger Documentation available at http://localhost:%s/docs\n", port)
	log.Printf("WebSocket Telemetry Stream available at ws://localhost:%s/ws\n", port)
	log.Fatal(app.Listen(fmt.Sprintf(":%s", port)))
}

