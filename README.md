# device_log_writter

func main() {
	hdfsAddr := "namenode:9000"
	filePath := "/user/logs/user_device_logs.parquet"

	logger, err := NewHDFSLogger(hdfsAddr, filePath)
	if err != nil {
		log.Fatalf("Error initializing HDFS logger: %v", err)
	}

	// Sample logs
	logs := []UserDeviceLog{
		{UserID: "user_123", DeviceID: "device_A1", Timestamp: time.Now().Format(time.RFC3339), Direction: "ENTRY"},
		{UserID: "user_456", DeviceID: "device_B2", Timestamp: time.Now().Add(2 * time.Minute).Format(time.RFC3339), Direction: "ENTRY"},
		{UserID: "user_123", DeviceID: "device_A1", Timestamp: time.Now().Add(10 * time.Minute).Format(time.RFC3339), Direction: "EXIT"},
		{UserID: "user_456", DeviceID: "device_B2", Timestamp: time.Now().Add(20 * time.Minute).Format(time.RFC3339), Direction: "EXIT"},
	}

	if err := logger.WriteLogs(logs); err != nil {
		log.Fatalf("Failed to write user-device logs to HDFS: %v", err)
	}
}
