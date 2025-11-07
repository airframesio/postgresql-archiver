package formatters

import "io"

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

// TableSchema represents the structure needed for streaming formatters
// It's a simplified interface to avoid circular dependencies with cmd package
type TableSchema interface {
	GetColumns() []ColumnSchema
}

// ColumnSchema represents metadata about a column
type ColumnSchema interface {
	GetName() string
	GetType() string
}

// StreamWriter handles writing rows in a streaming fashion
type StreamWriter interface {
	// WriteChunk writes a chunk of rows to the output
	WriteChunk(rows []map[string]interface{}) error

	// Close finalizes the output (writes footers, flushes buffers, etc.)
	Close() error
}

// StreamingFormatter defines the interface for streaming output format handlers
type StreamingFormatter interface {
	// NewWriter creates a new StreamWriter that writes to the given io.Writer
	// schema provides column information needed for formats like CSV (headers) and Parquet (schema)
	NewWriter(w io.Writer, schema TableSchema) (StreamWriter, error)

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

// GetStreamingFormatter returns the appropriate streaming formatter based on the format string
func GetStreamingFormatter(format string) StreamingFormatter {
	switch format {
	case "jsonl":
		return NewJSONLStreamingFormatter()
	case "csv":
		return NewCSVStreamingFormatter()
	case FormatParquet:
		return NewParquetStreamingFormatter()
	default:
		return NewJSONLStreamingFormatter() // Default to JSONL
	}
}

// UsesInternalCompression returns true if the format handles compression internally
func UsesInternalCompression(format string) bool {
	return format == FormatParquet
}
