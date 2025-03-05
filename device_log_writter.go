package device_log_writter

import (
	"bytes"
	"fmt"
	"log"

	"github.com/colinmarc/hdfs"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// UserDeviceLog represents the schema for user-device logs
type UserDeviceLog struct {
	UserID    string `parquet:"name=user_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceID  string `parquet:"name=device_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Timestamp string `parquet:"name=timestamp, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Direction string `parquet:"name=direction, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"` // "ENTRY" or "EXIT"
}

// HDFSLogger manages writing Parquet files and uploading them to HDFS
type HDFSLogger struct {
	client   *hdfs.Client
	filePath string
	buffer   *bytes.Buffer
}

// NewHDFSLogger initializes an HDFSLogger
func NewHDFSLogger(hdfsAddr, filePath string) (*HDFSLogger, error) {
	client, err := hdfs.New(hdfsAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to HDFS: %w", err)
	}

	return &HDFSLogger{
		client:   client,
		filePath: filePath,
		buffer:   new(bytes.Buffer),
	}, nil
}

// WriteLogs writes user-device logs to Parquet format and uploads to HDFS
func (h *HDFSLogger) WriteLogs(logs []UserDeviceLog) error {
	pw, err := writer.NewParquetWriter(h.buffer, new(UserDeviceLog), 4)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128MB row groups
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, logEntry := range logs {
		if err := pw.Write(logEntry); err != nil {
			return fmt.Errorf("failed to write log entry: %w", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to finalize Parquet file: %w", err)
	}

	if err := h.uploadToHDFS(); err != nil {
		return fmt.Errorf("failed to upload to HDFS: %w", err)
	}

	log.Printf("Successfully uploaded %d logs to HDFS at: %s\n", len(logs), h.filePath)
	return nil
}

// uploadToHDFS uploads the Parquet file to HDFS
func (h *HDFSLogger) uploadToHDFS() error {
	file, err := h.client.Create(h.filePath)
	if err != nil {
		return fmt.Errorf("failed to create file in HDFS: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(h.buffer.Bytes()); err != nil {
		return fmt.Errorf("failed to write data to HDFS file: %w", err)
	}

	return nil
}
