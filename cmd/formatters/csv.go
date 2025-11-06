package formatters

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
)

// CSVFormatter handles CSV format output
type CSVFormatter struct{}

// NewCSVFormatter creates a new CSV formatter
func NewCSVFormatter() *CSVFormatter {
	return &CSVFormatter{}
}

// Format converts rows to CSV format
func (f *CSVFormatter) Format(rows []map[string]interface{}) ([]byte, error) {
	if len(rows) == 0 {
		return []byte{}, nil
	}

	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)

	// Extract column names from first row (sorted for consistency)
	columns := make([]string, 0, len(rows[0]))
	for col := range rows[0] {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Write header row
	if err := writer.Write(columns); err != nil {
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write data rows
	for _, row := range rows {
		record := make([]string, len(columns))
		for i, col := range columns {
			val := row[col]
			if val == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("CSV writer error: %w", err)
	}

	return buffer.Bytes(), nil
}

// Extension returns the file extension for CSV files
func (f *CSVFormatter) Extension() string {
	return ".csv"
}

// MIMEType returns the MIME type for CSV
func (f *CSVFormatter) MIMEType() string {
	return "text/csv"
}
