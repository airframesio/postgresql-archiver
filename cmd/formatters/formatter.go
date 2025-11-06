package formatters

// Formatter defines the interface for output format handlers
type Formatter interface {
	// Format converts database rows to the target format
	Format(rows []map[string]interface{}) ([]byte, error)

	// Extension returns the file extension for this format (e.g., ".jsonl", ".csv", ".parquet")
	Extension() string

	// MIMEType returns the MIME type for this format
	MIMEType() string
}

// GetFormatter returns the appropriate formatter based on the format string
func GetFormatter(format string) Formatter {
	switch format {
	case "jsonl":
		return NewJSONLFormatter()
	case "csv":
		return NewCSVFormatter()
	case "parquet":
		return NewParquetFormatter()
	default:
		return NewJSONLFormatter() // Default to JSONL
	}
}
