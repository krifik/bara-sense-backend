package application

import (
	"github.com/fiqri/home-automation-backend/domain"
)

type deviceUsecase struct {
	deviceRepo domain.DeviceRepository
}

func NewDeviceUsecase(repo domain.DeviceRepository) domain.DeviceUsecase {
	return &deviceUsecase{
		deviceRepo: repo,
	}
}

func (u *deviceUsecase) AddDevice(device domain.Device) error {
	return u.deviceRepo.UpsertDevice(device)
}

func (u *deviceUsecase) ToggleDevice(id string, status bool) error {
	// Tuya cloud call would happen here before saving to DB
	// For now, this is just a skeleton for the DB operation
	return u.deviceRepo.UpdateDeviceStatus(id, status, 0)
}

func (u *deviceUsecase) DeleteDevice(id string) error {
	return u.deviceRepo.DeleteDevice(id)
}

func (u *deviceUsecase) GetDevices() ([]domain.Device, error) {
	return u.deviceRepo.GetDevices()
}

func (u *deviceUsecase) GetAnalytics() (domain.DeviceAnalytics, error) {
	// Logic to get analytics goes here
	return domain.DeviceAnalytics{}, nil
}
