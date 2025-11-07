package formatters

import (
	"bytes"
	"encoding/json"
	"io"
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

// JSONLStreamingFormatter handles JSONL format output in streaming mode
type JSONLStreamingFormatter struct{}

// NewJSONLStreamingFormatter creates a new JSONL streaming formatter
func NewJSONLStreamingFormatter() *JSONLStreamingFormatter {
	return &JSONLStreamingFormatter{}
}

// NewWriter creates a new JSONL stream writer
func (f *JSONLStreamingFormatter) NewWriter(w io.Writer, _ TableSchema) (StreamWriter, error) {
	return &jsonlStreamWriter{
		writer: w,
	}, nil
}

// Extension returns the file extension for JSONL files
func (f *JSONLStreamingFormatter) Extension() string {
	return ".jsonl"
}

// MIMEType returns the MIME type for JSONL
func (f *JSONLStreamingFormatter) MIMEType() string {
	return "application/x-ndjson"
}

// jsonlStreamWriter implements StreamWriter for JSONL format
type jsonlStreamWriter struct {
	writer io.Writer
}

// WriteChunk writes a chunk of rows in JSONL format
func (w *jsonlStreamWriter) WriteChunk(rows []map[string]interface{}) error {
	for _, row := range rows {
		// Encode each row as JSON
		jsonData, err := json.Marshal(row)
		if err != nil {
			return err
		}

		// Write JSON data followed by newline
		if _, err := w.writer.Write(jsonData); err != nil {
			return err
		}
		if _, err := w.writer.Write([]byte{'\n'}); err != nil {
			return err
		}
	}
	return nil
}

// Close finalizes the JSONL output (no-op for JSONL)
func (w *jsonlStreamWriter) Close() error {
	// JSONL has no footer, nothing to do
	return nil
}
