package formatters

import (
	"bytes"
	"encoding/json"
)

// JSONLFormatter handles JSONL (JSON Lines) format output
type JSONLFormatter struct{}

// NewJSONLFormatter creates a new JSONL formatter
func NewJSONLFormatter() *JSONLFormatter {
	return &JSONLFormatter{}
}

// Format converts rows to JSONL format (one JSON object per line)
func (f *JSONLFormatter) Format(rows []map[string]interface{}) ([]byte, error) {
	var buffer bytes.Buffer

	for _, row := range rows {
		// Encode each row as JSON
		jsonData, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}

		// Write JSON data followed by newline
		buffer.Write(jsonData)
		buffer.WriteByte('\n')
	}

	return buffer.Bytes(), nil
}

// Extension returns the file extension for JSONL files
func (f *JSONLFormatter) Extension() string {
	return ".jsonl"
}

// MIMEType returns the MIME type for JSONL
func (f *JSONLFormatter) MIMEType() string {
	return "application/x-ndjson"
}
