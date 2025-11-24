package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/airframesio/data-archiver/cmd/formatters"
	"github.com/lib/pq"
)

// ErrTableNotFoundOrEmpty is returned when a table is not found or has no columns
var ErrTableNotFoundOrEmpty = errors.New("table not found or has no columns")

// ErrTableNotFound is returned when a table does not exist
var ErrTableNotFound = errors.New("table not found")

// ErrTableHasNoColumns is returned when a table exists but has no columns
var ErrTableHasNoColumns = errors.New("table exists but has no columns")

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
// Uses multiple fallback methods to handle permission issues or edge cases
func (a *Archiver) getTableSchema(ctx context.Context, tableName string) (*TableSchema, error) {
	// Try information_schema.columns first (standard approach)
	query1 := `
		SELECT column_name, data_type, udt_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := a.db.QueryContext(ctx, query1, tableName)
	if err == nil {
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

		if len(schema.Columns) > 0 {
			return schema, nil
		}
	}

	// Fallback to pg_attribute if information_schema returns no rows (permissions issue)
	query2 := `
		SELECT a.attname::text,
		       pg_catalog.format_type(a.atttypid, a.atttypmod)::text,
		       t.typname::text
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
		JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
		WHERE n.nspname = 'public'
		  AND c.relname = $1
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	if a.logger != nil {
		a.logger.Debug(fmt.Sprintf("information_schema.columns returned 0 rows for %s, trying pg_attribute fallback", tableName))
	}

	rows2, err2 := a.db.QueryContext(ctx, query2, tableName)
	if err2 == nil {
		defer rows2.Close()

		schema2 := &TableSchema{
			TableName: tableName,
			Columns:   make([]ColumnInfo, 0),
		}

		for rows2.Next() {
			var col ColumnInfo
			if err := rows2.Scan(&col.Name, &col.DataType, &col.UDTName); err != nil {
				if a.logger != nil {
					a.logger.Debug(fmt.Sprintf("Error scanning pg_attribute row: %v", err))
				}
				break
			}
			schema2.Columns = append(schema2.Columns, col)
		}

		if err := rows2.Err(); err == nil && len(schema2.Columns) > 0 {
			if a.logger != nil {
				a.logger.Debug(fmt.Sprintf("Successfully got schema from pg_attribute for %s (%d columns)", tableName, len(schema2.Columns)))
			}
			return schema2, nil
		}
	}

	// Final fallback: try to query the table directly with LIMIT 0 to get column info
	// This uses the database's own metadata about the result set
	if a.logger != nil {
		a.logger.Debug(fmt.Sprintf("Trying direct SELECT query fallback for %s", tableName))
	}
	query3 := fmt.Sprintf("SELECT * FROM public.%s LIMIT 0", pq.QuoteIdentifier(tableName))
	rows3, err3 := a.db.QueryContext(ctx, query3)
	if err3 == nil {
		defer rows3.Close()
		columnTypes, err := rows3.ColumnTypes()
		if err == nil && len(columnTypes) > 0 {
			if a.logger != nil {
				a.logger.Debug(fmt.Sprintf("Successfully got schema from direct query for %s (%d columns)", tableName, len(columnTypes)))
			}
			schema3 := &TableSchema{
				TableName: tableName,
				Columns:   make([]ColumnInfo, 0),
			}
			for _, ct := range columnTypes {
				schema3.Columns = append(schema3.Columns, ColumnInfo{
					Name:     ct.Name(),
					DataType: ct.DatabaseTypeName(),
					UDTName:  ct.DatabaseTypeName(),
				})
			}
			return schema3, nil
		}
	}

	// Check if table exists at all
	var exists bool
	existsQuery := `
		SELECT EXISTS (
			SELECT 1 FROM pg_tables
			WHERE schemaname = 'public' AND tablename = $1
		)
	`
	existsErr := a.db.QueryRowContext(ctx, existsQuery, tableName).Scan(&exists)
	if existsErr == nil && exists {
		// Table exists but has no columns - return special error
		return nil, fmt.Errorf("%w: %s", ErrTableHasNoColumns, tableName)
	}

	// Table doesn't exist
	return nil, fmt.Errorf("%w: %s (tried information_schema.columns, pg_attribute, and direct query)", ErrTableNotFound, tableName)
}
