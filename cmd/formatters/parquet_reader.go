package formatters

import (
	"bytes"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
)

// ParquetReader reads Parquet format
type ParquetReader struct {
	file   *parquet.File
	closer io.ReadCloser
}

// NewParquetReader creates a new Parquet reader
// Note: Parquet requires io.ReaderAt, so we read the entire file into memory
func NewParquetReader(r io.Reader) (*ParquetReader, error) {
	// Read entire file into memory (parquet requires ReaderAt)
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	// Use bytes.Reader which implements io.ReaderAt
	readerAt := bytes.NewReader(data)

	// Open the parquet file to read its schema
	file, err := parquet.OpenFile(readerAt, int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	return &ParquetReader{
		file:   file,
		closer: nil,
	}, nil
}

// NewParquetReaderWithCloser creates a new Parquet reader with a closable reader
// Note: Parquet requires io.ReaderAt, so we read the entire file into memory
func NewParquetReaderWithCloser(r io.ReadCloser) (*ParquetReader, error) {
	// Read entire file into memory (parquet requires ReaderAt)
	data, err := io.ReadAll(r)
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	// Use bytes.Reader which implements io.ReaderAt
	readerAt := bytes.NewReader(data)

	// Open the parquet file to read its schema
	file, err := parquet.OpenFile(readerAt, int64(len(data)))
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	return &ParquetReader{
		file:   file,
		closer: r,
	}, nil
}

// ReadChunk reads a chunk of rows from the Parquet stream
func (r *ParquetReader) ReadChunk(chunkSize int) ([]map[string]interface{}, error) {
	return r.readRows(chunkSize)
}

// ReadAll reads all remaining rows from the Parquet stream
func (r *ParquetReader) ReadAll() ([]map[string]interface{}, error) {
	return r.readRows(0) // 0 means read all
}

// readRows reads rows from the parquet file by iterating through row groups
func (r *ParquetReader) readRows(maxRows int) ([]map[string]interface{}, error) {
	var rows []map[string]interface{}

	// Get schema to know column names
	schema := r.file.Schema()
	columnPaths := schema.Columns() // Returns [][]string, each []string is a column path

	// Build column name map (flatten nested paths)
	columnNames := make([]string, len(columnPaths))
	for i, path := range columnPaths {
		// Join path components with dot for nested columns, or use last component
		if len(path) > 0 {
			columnNames[i] = path[len(path)-1] // Use last component as column name
		}
	}

	// Read all row groups
	rowCount := 0
	for _, rowGroup := range r.file.RowGroups() {
		// Limit rows if maxRows is specified
		if maxRows > 0 && rowCount >= maxRows {
			break
		}

		// Get row reader
		rowReader := rowGroup.Rows()
		defer rowReader.Close()

		// Read rows from this row group
		batchSize := 1000
		if maxRows > 0 && maxRows-rowCount < batchSize {
			batchSize = maxRows - rowCount
		}

		for {
			if maxRows > 0 && rowCount >= maxRows {
				break
			}

			// Read a batch of rows
			batch := make([]parquet.Row, batchSize)
			n, err := rowReader.ReadRows(batch)
			if err == io.EOF || n == 0 {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to read parquet rows: %w", err)
			}

			// Convert each parquet.Row to map[string]interface{}
			for rowIdx := 0; rowIdx < n; rowIdx++ {
				if maxRows > 0 && rowCount >= maxRows {
					break
				}

				parquetRow := batch[rowIdx]
				row := make(map[string]interface{})

				// Map values to column names
				// The row values are ordered by column index
				for i, val := range parquetRow {
					if i < len(columnNames) {
						if val.IsNull() {
							row[columnNames[i]] = nil
						} else {
							// Convert parquet.Value to Go value based on type
							switch {
							case val.Kind() == parquet.Boolean:
								row[columnNames[i]] = val.Boolean()
							case val.Kind() == parquet.Int32:
								row[columnNames[i]] = val.Int32()
							case val.Kind() == parquet.Int64:
								row[columnNames[i]] = val.Int64()
							case val.Kind() == parquet.Float:
								row[columnNames[i]] = val.Float()
							case val.Kind() == parquet.Double:
								row[columnNames[i]] = val.Double()
							case val.Kind() == parquet.ByteArray:
								row[columnNames[i]] = string(val.ByteArray())
							default:
								row[columnNames[i]] = string(val.ByteArray())
							}
						}
					}
				}

				rows = append(rows, row)
				rowCount++
			}

			if n < batchSize {
				break // Last batch
			}
		}
	}

	return rows, nil
}

// Close closes the underlying reader
func (r *ParquetReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}
