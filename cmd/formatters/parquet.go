package formatters

import (
	"bytes"
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// ParquetFormatter handles Parquet format output
type ParquetFormatter struct{}

// NewParquetFormatter creates a new Parquet formatter
func NewParquetFormatter() *ParquetFormatter {
	return &ParquetFormatter{}
}

// Format converts rows to Parquet format
func (f *ParquetFormatter) Format(rows []map[string]interface{}) ([]byte, error) {
	if len(rows) == 0 {
		return []byte{}, nil
	}

	var buffer bytes.Buffer

	// Create parquet writer using generic writer for maps
	writer := parquet.NewGenericWriter[map[string]any](&buffer)
	defer writer.Close()

	// Write all rows
	_, err := writer.Write(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to write parquet data: %w", err)
	}

	// Close writer to flush data
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buffer.Bytes(), nil
}

// Extension returns the file extension for Parquet files
func (f *ParquetFormatter) Extension() string {
	return ".parquet"
}

// MIMEType returns the MIME type for Parquet
func (f *ParquetFormatter) MIMEType() string {
	return "application/vnd.apache.parquet"
}
