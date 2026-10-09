package db

import (
	"database/sql"
	"github.com/fiqri/home-automation-backend/domain"
)

type deviceRepository struct {
	db *sql.DB
}

func NewDeviceRepository(db *sql.DB) domain.DeviceRepository {
	return &deviceRepository{db: db}
}

func (r *deviceRepository) GetDevices() ([]domain.Device, error) {
	var devices []domain.Device
	rows, err := r.db.Query("SELECT id, name, ip, local_key, status, power FROM devices ORDER BY priority ASC, name ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var dev domain.Device
		if err := rows.Scan(&dev.ID, &dev.Name, &dev.IP, &dev.LocalKey, &dev.Status, &dev.Power); err != nil {
			continue
		}
		devices = append(devices, dev)
	}
	return devices, nil
}

func (r *deviceRepository) GetDeviceByID(id string) (domain.Device, error) {
	var dev domain.Device
	err := r.db.QueryRow("SELECT id, name, ip, local_key, status, power FROM devices WHERE id = $1", id).Scan(&dev.ID, &dev.Name, &dev.IP, &dev.LocalKey, &dev.Status, &dev.Power)
	return dev, err
}

func (r *deviceRepository) UpsertDevice(dev domain.Device) error {
	_, err := r.db.Exec(`
		INSERT INTO devices (id, name, ip, local_key, status, power)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			ip = EXCLUDED.ip,
			local_key = EXCLUDED.local_key,
			status = EXCLUDED.status,
			power = EXCLUDED.power
	`, dev.ID, dev.Name, dev.IP, dev.LocalKey, dev.Status, dev.Power)
	return err
}

func (r *deviceRepository) UpdateDeviceStatus(id string, status bool, power int) error {
	_, err := r.db.Exec("UPDATE devices SET status = $1, power = $2 WHERE id = $3", status, power, id)
	return err
}

func (r *deviceRepository) DeleteDevice(id string) error {
	_, err := r.db.Exec("DELETE FROM devices WHERE id = $1", id)
	return err
}

func (r *deviceRepository) UpdateDeviceName(id, name string) error {
	_, err := r.db.Exec("UPDATE devices SET name = $1 WHERE id = $2", name, id)
	return err
}
