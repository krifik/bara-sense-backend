package domain

// DeviceRepository handles all database operations related to devices
type DeviceRepository interface {
	GetDevices() ([]Device, error)
	GetDeviceByID(id string) (Device, error)
	UpsertDevice(dev Device) error
	UpdateDeviceStatus(id string, status bool, power int) error
	DeleteDevice(id string) error
	UpdateDeviceName(id, name string) error
}
