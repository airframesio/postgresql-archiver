package formatters

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
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

// CSVStreamingFormatter handles CSV format output in streaming mode
type CSVStreamingFormatter struct{}

// NewCSVStreamingFormatter creates a new CSV streaming formatter
func NewCSVStreamingFormatter() *CSVStreamingFormatter {
	return &CSVStreamingFormatter{}
}

// NewWriter creates a new CSV stream writer
func (f *CSVStreamingFormatter) NewWriter(w io.Writer, schema TableSchema) (StreamWriter, error) {
	// Extract column names from schema in sorted order for consistency
	columns := schema.GetColumns()
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.GetName()
	}
	sort.Strings(columnNames)

	// Create CSV writer
	csvWriter := csv.NewWriter(w)

	// Write header immediately
	if err := csvWriter.Write(columnNames); err != nil {
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	return &csvStreamWriter{
		writer:  csvWriter,
		columns: columnNames,
	}, nil
}

// Extension returns the file extension for CSV files
func (f *CSVStreamingFormatter) Extension() string {
	return ".csv"
}

// MIMEType returns the MIME type for CSV
func (f *CSVStreamingFormatter) MIMEType() string {
	return "text/csv"
}

// csvStreamWriter implements StreamWriter for CSV format
type csvStreamWriter struct {
	writer  *csv.Writer
	columns []string
}

// WriteChunk writes a chunk of rows in CSV format
func (w *csvStreamWriter) WriteChunk(rows []map[string]interface{}) error {
	for _, row := range rows {
		record := make([]string, len(w.columns))
		for i, col := range w.columns {
			val := row[col]
			if val == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := w.writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	return nil
}

// Close finalizes the CSV output by flushing the writer
func (w *csvStreamWriter) Close() error {
	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return fmt.Errorf("CSV writer error: %w", err)
	}
	return nil
}
