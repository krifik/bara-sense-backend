package db

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func NewPostgresDB(connStr string) (*sql.DB, error) {
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open Postgres connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("layanan database tidak dapat dijangkau. Periksa status dan koneksi database server")
	}

	createTablesQuery := `
	CREATE TABLE IF NOT EXISTS devices (
		id TEXT PRIMARY KEY,
		name TEXT,
		ip TEXT,
		local_key TEXT,
		status BOOLEAN DEFAULT false,
		power INTEGER DEFAULT 0,
		allowed_roles TEXT DEFAULT 'admin,operator',
		priority INTEGER DEFAULT 2
	);

	CREATE TABLE IF NOT EXISTS energy_logs (
		id SERIAL PRIMARY KEY,
		device_id TEXT,
		timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		power_watt INTEGER,
		kwh DOUBLE PRECISION,
		cost_idr DOUBLE PRECISION
	);
	CREATE INDEX IF NOT EXISTS idx_energy_logs_timestamp ON energy_logs(timestamp);
	CREATE INDEX IF NOT EXISTS idx_energy_logs_device ON energy_logs(device_id);

	CREATE TABLE IF NOT EXISTS users (
		id SERIAL PRIMARY KEY,
		username TEXT UNIQUE,
		name TEXT,
		role TEXT,
		password TEXT
	);

	CREATE TABLE IF NOT EXISTS permissions (
		id TEXT PRIMARY KEY,
		feature TEXT,
		desc_text TEXT,
		admin BOOLEAN DEFAULT true,
		operator BOOLEAN DEFAULT true,
		viewer BOOLEAN DEFAULT false
	);

	CREATE TABLE IF NOT EXISTS schedules (
		id SERIAL PRIMARY KEY,
		device_id TEXT NOT NULL,
		device_name TEXT,
		action TEXT NOT NULL DEFAULT 'ON',
		time_target TEXT NOT NULL,
		days TEXT DEFAULT 'ALL',
		is_active BOOLEAN DEFAULT true,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS device_timers (
		device_id TEXT PRIMARY KEY,
		target_action TEXT NOT NULL DEFAULT 'OFF',
		expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
		duration_minutes INTEGER NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS power_guard_config (
		id INTEGER PRIMARY KEY DEFAULT 1,
		max_watt_limit INTEGER DEFAULT 1150,
		is_enabled BOOLEAN DEFAULT true,
		cutoff_duration_sec INTEGER DEFAULT 8,
		last_triggered_at TIMESTAMP WITH TIME ZONE,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS scenes (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		icon TEXT NOT NULL,
		description TEXT,
		actions TEXT NOT NULL,
		is_preset BOOLEAN DEFAULT false,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS budget_settings (
		id INTEGER PRIMARY KEY DEFAULT 1,
		monthly_budget_idr DOUBLE PRECISION DEFAULT 750000,
		warning_threshold_pct DOUBLE PRECISION DEFAULT 80
	);

	CREATE TABLE IF NOT EXISTS telegram_config (
		id INTEGER PRIMARY KEY DEFAULT 1,
		bot_token TEXT DEFAULT '',
		chat_id TEXT DEFAULT '',
		is_enabled BOOLEAN DEFAULT false,
		notify_on_overload BOOLEAN DEFAULT true,
		notify_on_leak BOOLEAN DEFAULT true,
		daily_digest_time TEXT DEFAULT '21:00'
	);

	CREATE TABLE IF NOT EXISTS device_name_history (
		device_id TEXT PRIMARY KEY,
		name TEXT
	);
	`

	_, err = db.Exec(createTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("failed creating PostgreSQL tables: %w", err)
	}

	// Seed RBAC users if empty
	var userCount int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err == nil && userCount == 0 {
		log.Println("Seeding initial RBAC users into PostgreSQL...")
		_, _ = db.Exec(`
			INSERT INTO users (username, name, role, password) VALUES
			('admin', 'System Administrator', 'admin', 'admin'),
			('operator', 'Anggota Keluarga / Operator', 'operator', 'operator123'),
			('guest', 'Pengunjung / Tamu', 'viewer', 'guest123')
			ON CONFLICT (username) DO NOTHING;
		`)
	}

	// Seed RBAC permissions matrix if empty
	var permCount int
	err = db.QueryRow("SELECT COUNT(*) FROM permissions").Scan(&permCount)
	if err == nil && permCount == 0 {
		log.Println("Seeding initial RBAC permission matrix into PostgreSQL...")
		_, _ = db.Exec(`
			INSERT INTO permissions (id, feature, desc_text, admin, operator, viewer) VALUES
			('read_telemetry', 'Membaca Perangkat & Telemetri Analitik', 'Melihat status saklar, daya (Watt), tegangan (V), dan arus (A) secara live', true, true, true),
			('switch_control', 'Saklar Kontrol ON/OFF Perangkat', 'Mengontrol atau mematikan saklar daya fisik via API / Tuya Cloud', true, true, false),
			('change_tariff', 'Ubah Preferensi Golongan Tarif PLN', 'Mengubah tarif subsidi/non-subsidi PLN untuk estimasi biaya', true, true, false),
			('manage_schedules', 'Atur Penjadwalan & Timer Otomatis', 'Menambah, mengubah, dan menghapus jadwal atau timer saklar perangkat', true, true, false),
			('register_device', 'Daftarkan Perangkat Baru', 'Menambahkan ID perangkat baru & konfigurasinya ke sistem', true, false, false),
			('delete_device', 'Hapus Perangkat & Riwayat Daya', 'Menghapus perangkat fisik beserta riwayat log daya dari database', true, false, false),
			('manage_rbac', 'Pengaturan RBAC & Hak Akses User', 'Mengubah peran (Admin, Operator, Viewer) pada pengguna terdaftar', true, false, false)
			ON CONFLICT (id) DO NOTHING;
		`)
	}

	// Seed default configs
	_, _ = db.Exec(`
		INSERT INTO power_guard_config (id, max_watt_limit, is_enabled, cutoff_duration_sec) 
		VALUES (1, 1150, true, 8) ON CONFLICT (id) DO NOTHING;
		INSERT INTO budget_settings (id, monthly_budget_idr, warning_threshold_pct) 
		VALUES (1, 750000, 80) ON CONFLICT (id) DO NOTHING;
		INSERT INTO telegram_config (id, bot_token, chat_id, is_enabled, notify_on_overload, notify_on_leak, daily_digest_time) 
		VALUES (1, '', '', false, true, true, '21:00') ON CONFLICT (id) DO NOTHING;
	`)

	// Seed initial scenes if empty
	var sceneCount int
	err = db.QueryRow("SELECT COUNT(*) FROM scenes").Scan(&sceneCount)
	if err == nil && sceneCount == 0 {
		log.Println("Seeding initial smart scenes into PostgreSQL...")
		_, _ = db.Exec(`
			INSERT INTO scenes (id, name, icon, description, actions, is_preset) VALUES
			('away', 'Keluar Rumah', '🚶‍♂️', 'Matikan semua AC dan pompa kolam saat keluar rumah', '[{"device_id":"eb8a1b2c3d4e5f6g","status":false},{"device_id":"eb9b2c3d4e5f6g7h","status":false}]', true),
			('night', 'Tidur / Malam', '🌙', 'Matikan lampu utama dan pompa, nyalakan AC kamar', '[{"device_id":"eb8a1b2c3d4e5f6g","status":true},{"device_id":"eb9b2c3d4e5f6g7h","status":false},{"device_id":"eb0c3d4e5f6g7h8i","status":false}]', true),
			('eco', 'Hemat Daya', '🍃', 'Matikan perangkat beban sekunder untuk penghematan listrik', '[{"device_id":"eb9b2c3d4e5f6g7h","status":false}]', true),
			('normal', 'Semua Nyala', '☀️', 'Nyalakan semua perangkat utama di rumah', '[{"device_id":"eb8a1b2c3d4e5f6g","status":true},{"device_id":"eb9b2c3d4e5f6g7h","status":true},{"device_id":"eb0c3d4e5f6g7h8i","status":true}]', true)
			ON CONFLICT (id) DO NOTHING;
		`)
	}

	return db, nil
}
