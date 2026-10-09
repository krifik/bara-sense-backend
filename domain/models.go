package domain

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
