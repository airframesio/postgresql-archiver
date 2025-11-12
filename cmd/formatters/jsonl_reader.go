package formatters

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// JSONLReader reads JSONL format (one JSON object per line)
type JSONLReader struct {
	scanner *bufio.Scanner
	reader io.ReadCloser
}

// NewJSONLReader creates a new JSONL reader
func NewJSONLReader(r io.Reader) *JSONLReader {
	return &JSONLReader{
		scanner: bufio.NewScanner(r),
		reader:  nil, // Will be set if r is a ReadCloser
	}
}

// NewJSONLReaderWithCloser creates a new JSONL reader with a closable reader
func NewJSONLReaderWithCloser(r io.ReadCloser) *JSONLReader {
	return &JSONLReader{
		scanner: bufio.NewScanner(r),
		reader:  r,
	}
}

// ReadChunk reads a chunk of rows from the JSONL stream
func (r *JSONLReader) ReadChunk(chunkSize int) ([]map[string]interface{}, error) {
	var rows []map[string]interface{}

	for len(rows) < chunkSize && r.scanner.Scan() {
		line := r.scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines
		}

		var row map[string]interface{}
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("failed to parse JSON line: %w", err)
		}

		rows = append(rows, row)
	}

	if err := r.scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return rows, nil
}

// ReadAll reads all remaining rows from the JSONL stream
func (r *JSONLReader) ReadAll() ([]map[string]interface{}, error) {
	var rows []map[string]interface{}

	for r.scanner.Scan() {
		line := r.scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines
		}

		var row map[string]interface{}
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("failed to parse JSON line: %w", err)
		}

		rows = append(rows, row)
	}

	if err := r.scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return rows, nil
}

// Close closes the underlying reader if it's closable
func (r *JSONLReader) Close() error {
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}
