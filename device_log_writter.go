package device_log_writter

import (
	"log"
	"time"

	"github.com/xitongsys/parquet-go-source/hdfs"
	"github.com/xitongsys/parquet-go/writer"
)

// UserDeviceLog represents the structure of our device log entries
type UserDeviceLog struct {
	UserID    string `parquet:"name=user_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceID  string `parquet:"name=device_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Timestamp string `parquet:"name=timestamp, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Direction string `parquet:"name=direction, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"` // "ENTRY" or "EXIT"
}

// DeviceLogService manages device log operations
type DeviceLogService struct {
	hdfsAddr string
	filePath string
	username string
}

// NewDeviceLogService creates a new instance of DeviceLogService
func NewDeviceLogService(hdfsAddr, filePath, username string) *DeviceLogService {
	return &DeviceLogService{
		hdfsAddr: hdfsAddr,
		filePath: filePath,
		username: username,
	}
}

// WriteLogs writes a slice of device logs to HDFS as a Parquet file
func (s *DeviceLogService) WriteLogs(logs []UserDeviceLog) error {
	// Create HDFS writer
	fw, err := hdfs.NewHdfsFileWriter([]string{s.hdfsAddr}, s.username, s.filePath)
	if err != nil {
		return err
	}
	defer fw.Close()

	// Create Parquet writer
	pw, err := writer.NewParquetWriter(fw, new(UserDeviceLog), 4)
	if err != nil {
		return err
	}

	// Write the logs
	for _, logEntry := range logs {
		if err := pw.Write(logEntry); err != nil {
			log.Printf("Failed to write log entry: %v", err)
			continue
		}
	}

	// Flush and close
	if err := pw.WriteStop(); err != nil {
		return err
	}

	log.Printf("Successfully wrote %d device logs to %s", len(logs), s.filePath)
	return nil
}

// AddLogEntry creates and writes a single log entry
func (s *DeviceLogService) AddLogEntry(userID, deviceID, direction string) error {
	logEntry := UserDeviceLog{
		UserID:    userID,
		DeviceID:  deviceID,
		Timestamp: time.Now().Format(time.RFC3339),
		Direction: direction,
	}

	return s.WriteLogs([]UserDeviceLog{logEntry})
}
