package presentation

import (
	"github.com/fiqri/home-automation-backend/domain"
	"github.com/gofiber/fiber/v2"
)

type DeviceHandler struct {
	DeviceUsecase domain.DeviceUsecase
}

func NewDeviceHandler(usecase domain.DeviceUsecase) *DeviceHandler {
	return &DeviceHandler{
		DeviceUsecase: usecase,
	}
}

func (h *DeviceHandler) GetDevices(c *fiber.Ctx) error {
	devices, err := h.DeviceUsecase.GetDevices()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(devices)
}
