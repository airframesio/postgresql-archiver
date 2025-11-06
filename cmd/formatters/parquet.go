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

	// Build schema by scanning all rows to find actual types
	schema, _ := buildSchemaFromRows(rows)

	// Map compression type to parquet compression codec and create writer
	var writer *parquet.GenericWriter[map[string]any]
	switch f.compression {
	case "zstd":
		writer = parquet.NewGenericWriter[map[string]any](&buffer, schema, parquet.Compression(&parquet.Zstd))
	case "gzip":
		writer = parquet.NewGenericWriter[map[string]any](&buffer, schema, parquet.Compression(&parquet.Gzip))
	case "lz4":
		writer = parquet.NewGenericWriter[map[string]any](&buffer, schema, parquet.Compression(&parquet.Lz4Raw))
	case "snappy":
		writer = parquet.NewGenericWriter[map[string]any](&buffer, schema, parquet.Compression(&parquet.Snappy))
	case "none":
		writer = parquet.NewGenericWriter[map[string]any](&buffer, schema, parquet.Compression(&parquet.Uncompressed))
	default:
		// Default to Snappy (standard for Parquet)
		writer = parquet.NewGenericWriter[map[string]any](&buffer, schema, parquet.Compression(&parquet.Snappy))
	}
	defer writer.Close()

	// Write rows directly - GenericWriter handles the conversion
	_, err := writer.Write(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to write parquet rows: %w", err)
	}

	// Close writer to flush data
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buffer.Bytes(), nil
}

// buildSchemaFromRows creates a Parquet schema by scanning all rows to find actual types
func buildSchemaFromRows(rows []map[string]interface{}) (*parquet.Schema, []string) {
	if len(rows) == 0 {
		return parquet.NewSchema("postgresql_export", parquet.Group{}), []string{}
	}

	// Get all column names from first row
	columns := make([]string, 0, len(rows[0]))
	for col := range rows[0] {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Find the first non-nil value for each column to determine its type
	columnTypes := make(map[string]interface{})
	for _, col := range columns {
		// Scan all rows to find first non-nil value
		for _, row := range rows {
			if value := row[col]; value != nil {
				columnTypes[col] = value
				break
			}
		}
		// If all values are nil, we'll default to string
		if _, found := columnTypes[col]; !found {
			columnTypes[col] = "" // Use string as default
		}
	}

	// Build schema fields based on discovered types
	fields := make(parquet.Group)
	for _, col := range columns {
		value := columnTypes[col]

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
		default:
			// For unknown types, use string
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
