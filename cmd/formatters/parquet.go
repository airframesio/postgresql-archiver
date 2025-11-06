package formatters

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/parquet-go/parquet-go"
)

// ParquetFormatter handles Parquet format output
type ParquetFormatter struct {
	compression string
}

// NewParquetFormatter creates a new Parquet formatter
func NewParquetFormatter() *ParquetFormatter {
	return &ParquetFormatter{
		compression: "snappy", // Default Parquet compression
	}
}

// NewParquetFormatterWithCompression creates a Parquet formatter with specified compression
func NewParquetFormatterWithCompression(compression string) *ParquetFormatter {
	return &ParquetFormatter{
		compression: compression,
	}
}

// Format converts rows to Parquet format
func (f *ParquetFormatter) Format(rows []map[string]interface{}) ([]byte, error) {
	if len(rows) == 0 {
		return []byte{}, nil
	}

	var buffer bytes.Buffer

	// Build schema from first row
	schema, columns := buildSchemaFromRow(rows[0])

	// Map compression type to parquet compression codec
	var writerOptions []parquet.WriterOption

	// Add schema as a writer option
	writerOptions = append(writerOptions, parquet.SchemaOf(schema))

	// Add compression option
	switch f.compression {
	case "zstd":
		writerOptions = append(writerOptions, parquet.Compression(&parquet.Zstd))
	case "gzip":
		writerOptions = append(writerOptions, parquet.Compression(&parquet.Gzip))
	case "lz4":
		writerOptions = append(writerOptions, parquet.Compression(&parquet.Lz4Raw))
	case "snappy":
		writerOptions = append(writerOptions, parquet.Compression(&parquet.Snappy))
	case "none":
		writerOptions = append(writerOptions, parquet.Compression(&parquet.Uncompressed))
	default:
		// Default to Snappy (standard for Parquet)
		writerOptions = append(writerOptions, parquet.Compression(&parquet.Snappy))
	}

	// Create parquet writer with options
	writer := parquet.NewWriter(&buffer, writerOptions...)
	defer writer.Close()

	// Write each row
	for _, row := range rows {
		// Convert map to parquet row format
		parquetRow := make(parquet.Row, 0, len(columns))
		for i, col := range columns {
			value := row[col]
			parquetRow = append(parquetRow, parquet.ValueOf(value).Level(0, 0, i))
		}

		_, err := writer.WriteRows([]parquet.Row{parquetRow})
		if err != nil {
			return nil, fmt.Errorf("failed to write parquet row: %w", err)
		}
	}

	// Close writer to flush data
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buffer.Bytes(), nil
}

// buildSchemaFromRow creates a Parquet schema from a sample row
func buildSchemaFromRow(row map[string]interface{}) (*parquet.Schema, []string) {
	// Get sorted column names for consistent ordering
	columns := make([]string, 0, len(row))
	for col := range row {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Build schema fields as a Group map
	fields := make(parquet.Group)
	for _, col := range columns {
		value := row[col]

		// Determine Parquet type based on Go type
		var field parquet.Node
		switch value.(type) {
		case bool:
			field = parquet.Optional(parquet.Leaf(parquet.BooleanType))
		case int, int8, int16, int32:
			field = parquet.Optional(parquet.Leaf(parquet.Int32Type))
		case int64:
			field = parquet.Optional(parquet.Leaf(parquet.Int64Type))
		case float32:
			field = parquet.Optional(parquet.Leaf(parquet.FloatType))
		case float64:
			field = parquet.Optional(parquet.Leaf(parquet.DoubleType))
		case string:
			field = parquet.Optional(parquet.String())
		case []byte:
			field = parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
		case nil:
			// For nil values, default to string (most flexible)
			field = parquet.Optional(parquet.String())
		default:
			// For unknown types, convert to string
			field = parquet.Optional(parquet.String())
		}

		fields[col] = field
	}

	schema := parquet.NewSchema("postgresql_export", fields)
	return schema, columns
}

// Extension returns the file extension for Parquet files
func (f *ParquetFormatter) Extension() string {
	return ".parquet"
}

// MIMEType returns the MIME type for Parquet
func (f *ParquetFormatter) MIMEType() string {
	return "application/vnd.apache.parquet"
}
