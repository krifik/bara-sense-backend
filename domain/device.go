package domain

type Device struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	IP       string `json:"ip"`
	LocalKey string `json:"local_key"`
	Status   bool   `json:"status"` // ON/OFF
	Power    int    `json:"power"`  // Watt
}

type DeviceAnalytics struct {
	TotalPowerWatts       int                   `json:"total_power_watts"`
	TotalKwhToday         float64               `json:"total_kwh_today"`
	EstimatedCostTodayIDR float64               `json:"estimated_cost_today_idr"`
	EstimatedCostMonthIDR float64               `json:"estimated_cost_month_idr"`
	PredictedKwhMonth     float64               `json:"predicted_kwh_month"`
	PredictedCostMonthIDR float64               `json:"predicted_cost_month_idr"`
	EfficiencyScore       int                   `json:"efficiency_score"`
	OverloadRisk          string                `json:"overload_risk"`
	PeakUsageHour         string                `json:"peak_usage_hour"`
	SelectedTariffCode    string                `json:"selected_tariff_code"`
	SelectedTariffRate    float64               `json:"selected_tariff_rate"`
	TariffOptions         []PlnTariffInfo       `json:"tariff_options"`
	Recommendations       []string              `json:"recommendations"`
	DeviceBreakdown       []DeviceEnergyUsage   `json:"device_breakdown"`
	HourlyUsage           []HourlyEnergyUsage   `json:"hourly_usage"`
	DailyUsage            []DailyEnergyUsage    `json:"daily_usage"`
}

type PlnTariffInfo struct {
	Code        string  `json:"code"`
	Name        string  `json:"name"`
	PowerCategory string `json:"power_category"`
	RatePerKwh  float64 `json:"rate_per_kwh"`
	Description string  `json:"description"`
}

type DeviceEnergyUsage struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	PowerWatt  int     `json:"power_watt"`
	VoltageVolts float64 `json:"voltage_volts"`
	CurrentAmps  float64 `json:"current_amps"`
	Status     bool    `json:"status"`
	KwhToday   float64 `json:"kwh_today"`
	CostIDR    float64 `json:"cost_idr"`
	Percentage float64 `json:"percentage"`
}

type HourlyEnergyUsage struct {
	Hour string  `json:"hour"`
	Kwh  float64 `json:"kwh"`
	Cost float64 `json:"cost"`
}

type DailyEnergyUsage struct {
	Day  string  `json:"day"`
	Date string  `json:"date"`
	Kwh  float64 `json:"kwh"`
	Cost float64 `json:"cost"`
}

type DeviceUsecase interface {
	AddDevice(device Device) error
	ToggleDevice(id string, status bool) error
	DeleteDevice(id string) error
	GetDevices() ([]Device, error)
	GetAnalytics() (DeviceAnalytics, error)
}

