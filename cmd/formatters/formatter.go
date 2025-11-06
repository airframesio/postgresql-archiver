package formatters

// Format type constants
const (
	FormatParquet = "parquet"
)

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
	case FormatParquet:
		return NewParquetFormatter()
	default:
		return NewJSONLFormatter() // Default to JSONL
	}
}

// GetFormatterWithCompression returns the appropriate formatter with compression settings
// For Parquet, this enables internal compression. For other formats, compression parameter is ignored.
func GetFormatterWithCompression(format string, compression string) Formatter {
	switch format {
	case "jsonl":
		return NewJSONLFormatter()
	case "csv":
		return NewCSVFormatter()
	case FormatParquet:
		return NewParquetFormatterWithCompression(compression)
	default:
		return NewJSONLFormatter() // Default to JSONL
	}
}

// UsesInternalCompression returns true if the format handles compression internally
func UsesInternalCompression(format string) bool {
	return format == FormatParquet
}
