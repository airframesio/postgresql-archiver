package formatters

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"time"
)

// CSVReader reads CSV format with header detection
type CSVReader struct {
	reader   *csv.Reader
	closer   io.ReadCloser
	headers  []string
	readOnce bool
}

// NewCSVReader creates a new CSV reader
func NewCSVReader(r io.Reader) (*CSVReader, error) {
	return &CSVReader{
		reader:   csv.NewReader(r),
		closer:   nil,
		headers:  nil,
		readOnce: false,
	}, nil
}

// NewCSVReaderWithCloser creates a new CSV reader with a closable reader
func NewCSVReaderWithCloser(r io.ReadCloser) (*CSVReader, error) {
	return &CSVReader{
		reader:   csv.NewReader(r),
		closer:   r,
		headers:  nil,
		readOnce: false,
	}, nil
}

// readHeaders reads the header row if not already read
func (r *CSVReader) readHeaders() error {
	if r.readOnce {
		return nil
	}

	headers, err := r.reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	r.headers = headers
	r.readOnce = true
	return nil
}

// ReadChunk reads a chunk of rows from the CSV stream
func (r *CSVReader) ReadChunk(chunkSize int) ([]map[string]interface{}, error) {
	if err := r.readHeaders(); err != nil {
		return nil, err
	}

	var rows []map[string]interface{}

	for len(rows) < chunkSize {
		record, err := r.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV record: %w", err)
		}

		row := make(map[string]interface{})
		for i, value := range record {
			if i >= len(r.headers) {
				break // Skip extra columns
			}

			// Try to convert to appropriate type
			row[r.headers[i]] = r.convertValue(value)
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// ReadAll reads all remaining rows from the CSV stream
func (r *CSVReader) ReadAll() ([]map[string]interface{}, error) {
	if err := r.readHeaders(); err != nil {
		return nil, err
	}

	var rows []map[string]interface{}

	for {
		record, err := r.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV record: %w", err)
		}

		row := make(map[string]interface{})
		for i, value := range record {
			if i >= len(r.headers) {
				break // Skip extra columns
			}

			row[r.headers[i]] = r.convertValue(value)
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// convertValue attempts to convert a string value to an appropriate type
func (r *CSVReader) convertValue(value string) interface{} {
	if value == "" {
		return nil
	}

	// Try integer
	if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
		return intVal
	}

	// Try float
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		return floatVal
	}

	// Try boolean
	if boolVal, err := strconv.ParseBool(value); err == nil {
		return boolVal
	}

	// Try timestamp formats
	for _, layout := range []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, value); err == nil {
			return t
		}
	}

	// Default to string
	return value
}

// Close closes the underlying reader if it's closable
func (r *CSVReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}
