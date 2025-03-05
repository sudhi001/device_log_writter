# device_log_writter

// Example usage:
func main() {
	// Initialize the service
	service := NewDeviceLogService(
		"hdfs://localhost:9000",
		"/user/logs/device_logs.parquet",
		"hadoop",
	)

	// Example: Writing multiple logs
	logs := []UserDeviceLog{
		{
			UserID:    "user123",
			DeviceID:  "dev001",
			Timestamp: time.Now().Format(time.RFC3339),
			Direction: "ENTRY",
		},
		{
			UserID:    "user456",
			DeviceID:  "dev002",
			Timestamp: time.Now().Format(time.RFC3339),
			Direction: "EXIT",
		},
	}

	if err := service.WriteLogs(logs); err != nil {
		log.Fatalf("Failed to write logs: %v", err)
	}

	// Example: Adding a single entry
	if err := service.AddLogEntry("user789", "dev003", "ENTRY"); err != nil {
		log.Fatalf("Failed to add log entry: %v", err)
	}
}