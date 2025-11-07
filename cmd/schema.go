package cmd

import (
	"context"
	"fmt"

	"github.com/airframesio/data-archiver/cmd/formatters"
)

// ColumnInfo represents metadata about a database column
type ColumnInfo struct {
	Name     string
	DataType string
	UDTName  string // PostgreSQL user-defined type name (e.g., int4, varchar, timestamp)
}

// GetName implements formatters.ColumnSchema
func (c *ColumnInfo) GetName() string {
	return c.Name
}

// GetType implements formatters.ColumnSchema
func (c *ColumnInfo) GetType() string {
	return c.UDTName
}

// TableSchema represents the schema of a database table
type TableSchema struct {
	TableName string
	Columns   []ColumnInfo
}

// GetColumns implements formatters.TableSchema
func (s *TableSchema) GetColumns() []formatters.ColumnSchema {
	cols := make([]formatters.ColumnSchema, len(s.Columns))
	for i := range s.Columns {
		cols[i] = &s.Columns[i]
	}
	return cols
}

// getTableSchema queries PostgreSQL information_schema to get column metadata
func (a *Archiver) getTableSchema(ctx context.Context, tableName string) (*TableSchema, error) {
	query := `
		SELECT column_name, data_type, udt_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := a.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %w", err)
	}
	defer rows.Close()

	schema := &TableSchema{
		TableName: tableName,
		Columns:   make([]ColumnInfo, 0),
	}

	for rows.Next() {
		var col ColumnInfo
		if err := rows.Scan(&col.Name, &col.DataType, &col.UDTName); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		schema.Columns = append(schema.Columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating schema rows: %w", err)
	}

	if len(schema.Columns) == 0 {
		return nil, fmt.Errorf("table %s not found or has no columns", tableName)
	}

	return schema, nil
}
