package formatters

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/parquet-go/parquet-go"
)

// ErrNoColumns is returned when a table schema has no columns
var ErrNoColumns = errors.New("table schema has no columns")

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

// ParquetStreamingFormatter handles Parquet format output in streaming mode
type ParquetStreamingFormatter struct {
	compression string
}

// NewParquetStreamingFormatter creates a new Parquet streaming formatter with default compression
func NewParquetStreamingFormatter() *ParquetStreamingFormatter {
	return &ParquetStreamingFormatter{
		compression: "snappy", // Default Parquet compression
	}
}

// NewParquetStreamingFormatterWithCompression creates a Parquet streaming formatter with specified compression
func NewParquetStreamingFormatterWithCompression(compression string) *ParquetStreamingFormatter {
	return &ParquetStreamingFormatter{
		compression: compression,
	}
}

// NewWriter creates a new Parquet stream writer
// For Parquet, we need to build the schema from the TableSchema
func (f *ParquetStreamingFormatter) NewWriter(w io.Writer, tableSchema TableSchema) (StreamWriter, error) {
	// Build Parquet schema from TableSchema
	// We need to convert the generic interface to our actual schema type
	// This is a bit of a workaround since we can't import cmd package from formatters
	// We'll build the schema from the column information

	columns := tableSchema.GetColumns()
	if len(columns) == 0 {
		return nil, ErrNoColumns
	}

	// Sort columns for consistency
	columnNames := make([]string, len(columns))
	columnMap := make(map[string]ColumnSchema)
	for i, col := range columns {
		name := col.GetName()
		columnNames[i] = name
		columnMap[name] = col
	}
	sort.Strings(columnNames)

	// Build Parquet schema fields
	fields := make(parquet.Group)
	for _, colName := range columnNames {
		col := columnMap[colName]
		field := mapPostgreSQLTypeToParquetNode(col.GetType())
		fields[colName] = field
	}

	schema := parquet.NewSchema("postgresql_export", fields)

	// Create writer with appropriate compression
	var writer *parquet.GenericWriter[map[string]any]
	switch f.compression {
	case "zstd":
		writer = parquet.NewGenericWriter[map[string]any](w, schema, parquet.Compression(&parquet.Zstd))
	case "gzip":
		writer = parquet.NewGenericWriter[map[string]any](w, schema, parquet.Compression(&parquet.Gzip))
	case "lz4":
		writer = parquet.NewGenericWriter[map[string]any](w, schema, parquet.Compression(&parquet.Lz4Raw))
	case "snappy":
		writer = parquet.NewGenericWriter[map[string]any](w, schema, parquet.Compression(&parquet.Snappy))
	case "none":
		writer = parquet.NewGenericWriter[map[string]any](w, schema, parquet.Compression(&parquet.Uncompressed))
	default:
		writer = parquet.NewGenericWriter[map[string]any](w, schema, parquet.Compression(&parquet.Snappy))
	}

	return &parquetStreamWriter{
		writer: writer,
	}, nil
}

// Extension returns the file extension for Parquet files
func (f *ParquetStreamingFormatter) Extension() string {
	return ".parquet"
}

// MIMEType returns the MIME type for Parquet
func (f *ParquetStreamingFormatter) MIMEType() string {
	return "application/vnd.apache.parquet"
}

// mapPostgreSQLTypeToParquetNode maps PostgreSQL type (UDT name) to Parquet node
// This duplicates logic from cmd/schema.go but avoids circular import
func mapPostgreSQLTypeToParquetNode(udtName string) parquet.Node {
	switch udtName {
	// Integer types
	case "int2":
		return parquet.Optional(parquet.Leaf(parquet.Int32Type))
	case "int4":
		return parquet.Optional(parquet.Leaf(parquet.Int32Type))
	case "int8":
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))

	// Floating point types
	case "float4":
		return parquet.Optional(parquet.Leaf(parquet.FloatType))
	case "float8":
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))

	// Boolean
	case "bool":
		return parquet.Optional(parquet.Leaf(parquet.BooleanType))

	// Timestamp types (store as int64 microseconds)
	case "timestamp", "timestamptz":
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))

	// Date type (store as int32 days)
	case "date":
		return parquet.Optional(parquet.Leaf(parquet.Int32Type))

	// Text/String types
	case "varchar", "text", "char", "bpchar":
		return parquet.Optional(parquet.String())

	// JSON types
	case "json", "jsonb":
		return parquet.Optional(parquet.String())

	// UUID
	case "uuid":
		return parquet.Optional(parquet.String())

	// Byte array
	case "bytea":
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType))

	// Default to string
	default:
		return parquet.Optional(parquet.String())
	}
}

// parquetStreamWriter implements StreamWriter for Parquet format
type parquetStreamWriter struct {
	writer *parquet.GenericWriter[map[string]any]
}

// WriteChunk writes a chunk of rows to the Parquet file
func (w *parquetStreamWriter) WriteChunk(rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// Write rows to Parquet writer
	_, err := w.writer.Write(rows)
	if err != nil {
		return fmt.Errorf("failed to write parquet chunk: %w", err)
	}

	return nil
}

// Close finalizes the Parquet file by closing the writer (writes footer)
func (w *parquetStreamWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}
	return nil
}
