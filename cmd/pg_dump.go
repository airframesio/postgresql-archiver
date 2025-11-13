package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// PgDumpExecutor handles pg_dump operations with S3 upload
type PgDumpExecutor struct {
	config     *Config
	db         *sql.DB
	s3Client   *s3.S3
	s3Uploader *s3manager.Uploader
	logger     *slog.Logger
	ctx        context.Context
	partitions []string // Cached list of partitions to exclude for schema-only mode
}

// NewPgDumpExecutor creates a new pg_dump executor
func NewPgDumpExecutor(config *Config, logger *slog.Logger) *PgDumpExecutor {
	return &PgDumpExecutor{
		config: config,
		logger: logger,
	}
}

// Run executes pg_dump and uploads to S3
func (e *PgDumpExecutor) Run(ctx context.Context) error {
	e.ctx = ctx

	// Initialize S3 client
	if err := e.initS3(); err != nil {
		return fmt.Errorf("failed to initialize S3 client: %w", err)
	}

	// For schema-only mode, we need to discover top-level tables and partitions
	if e.config.DumpMode == "schema-only" {
		if err := e.connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to database: %w", err)
		}
		defer e.db.Close()

		// Discover partitions first (needed for both single table and all tables mode)
		partitions, err := e.discoverPartitions(ctx)
		if err != nil {
			return fmt.Errorf("failed to discover partitions: %w", err)
		}

		// Also discover tables that match partition naming patterns
		// This catches partitions that might not be in pg_inherits (e.g., messages_p*)
		patternPartitions, err := e.discoverPartitionsByPattern(ctx)
		if err != nil {
			return fmt.Errorf("failed to discover partitions by pattern: %w", err)
		}

		// Combine both lists and deduplicate
		allPartitions := make(map[string]bool)
		for _, partition := range partitions {
			allPartitions[partition] = true
		}
		for _, partition := range patternPartitions {
			allPartitions[partition] = true
		}

		// Convert back to slice
		var allPartitionsList []string
		for partition := range allPartitions {
			allPartitionsList = append(allPartitionsList, partition)
		}

		// Cache partitions for use in buildPgDumpCommand
		e.partitions = allPartitionsList

		// Create a map for fast partition lookup
		partitionMap := allPartitions

		var tablesToDump []string
		if e.config.Table == "" {
			// No table specified - discover all top-level tables
			allTables, err := e.discoverTopLevelTables(ctx)
			if err != nil {
				return fmt.Errorf("failed to discover top-level tables: %w", err)
			}

			// Filter out partitions from the top-level tables list
			for _, table := range allTables {
				if !partitionMap[table] {
					tablesToDump = append(tablesToDump, table)
				}
			}

			if len(tablesToDump) == 0 {
				e.logger.Info("No top-level tables found")
				return nil
			}

			e.logger.Info(fmt.Sprintf("Found %d top-level table(s) for schema dump (excluded %d partitions)", len(tablesToDump), len(allPartitionsList)))
			if e.config.Debug {
				for _, table := range tablesToDump {
					e.logger.Debug(fmt.Sprintf("  - %s", table))
				}
			}
		} else {
			// Single table specified
			tablesToDump = []string{e.config.Table}
		}

		if len(allPartitionsList) > 0 {
			e.logger.Debug(fmt.Sprintf("Found %d partition(s) to exclude from schema dump", len(allPartitionsList)))
			if e.config.Debug && len(allPartitionsList) <= 10 {
				for _, partition := range allPartitionsList {
					e.logger.Debug(fmt.Sprintf("  - Excluding partition: %s", partition))
				}
			} else if e.config.Debug {
				for i := 0; i < 5; i++ {
					e.logger.Debug(fmt.Sprintf("  - Excluding partition: %s", allPartitionsList[i]))
				}
				e.logger.Debug(fmt.Sprintf("  ... and %d more partitions", len(allPartitionsList)-5))
			}
		}

		// Dump each table one by one
		for i, table := range tablesToDump {
			e.logger.Info(fmt.Sprintf("Dumping schema for table %d/%d: %s", i+1, len(tablesToDump), table))
			if err := e.dumpTable(ctx, table); err != nil {
				return fmt.Errorf("failed to dump table %s: %w", table, err)
			}
		}

		e.logger.Info(fmt.Sprintf("✅ Successfully dumped schemas for %d table(s)", len(tablesToDump)))
		return nil
	}

	// For non-schema-only modes, use the original single-dump approach
	return e.dumpTable(ctx, e.config.Table)
}

// dumpTable dumps a single table and uploads to S3
func (e *PgDumpExecutor) dumpTable(ctx context.Context, tableName string) error {
	var hasPartitions bool

	// Verify table exists before attempting dump
	if e.db != nil {
		var tableExists bool
		checkQuery := `
			SELECT EXISTS (
				SELECT 1 FROM pg_tables
				WHERE schemaname = 'public' AND tablename = $1
			)
		`
		err := e.db.QueryRowContext(ctx, checkQuery, tableName).Scan(&tableExists)
		if err != nil {
			e.logger.Warn(fmt.Sprintf("Could not verify table existence: %v", err))
		} else if !tableExists {
			return fmt.Errorf("table %s does not exist in public schema", tableName)
		}

		// Check if table is a partition (child) - if so, find its parent
		var parentTableName string
		findParentQuery := `
			SELECT p.relname::text
			FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			JOIN pg_class p ON p.oid = i.inhparent
			JOIN pg_namespace n ON n.oid = p.relnamespace
			WHERE n.nspname = 'public' AND c.relname = $1
			LIMIT 1
		`
		err = e.db.QueryRowContext(ctx, findParentQuery, tableName).Scan(&parentTableName)
		if err == nil && parentTableName != "" {
			// Table is a partition (child), use parent table for schema dump
			e.logger.Info(fmt.Sprintf("Table %s is a partition. Dumping parent table %s instead (partitions inherit schema from parent)", tableName, parentTableName))
			tableName = parentTableName
		}

		// Check if it's a parent table with partitions (regardless of whether it's also a partition)
		parentCheckQuery := `
			SELECT EXISTS (
				SELECT 1 FROM pg_inherits i
				JOIN pg_class c ON c.oid = i.inhrelid
				JOIN pg_class p ON p.oid = i.inhparent
				JOIN pg_namespace n ON n.oid = p.relnamespace
				WHERE n.nspname = 'public' AND p.relname = $1
			)
		`
		err = e.db.QueryRowContext(ctx, parentCheckQuery, tableName).Scan(&hasPartitions)
		if err == nil && hasPartitions {
			e.logger.Debug(fmt.Sprintf("Table %s is a partitioned table (parent with partitions)", tableName))
			e.logger.Info(fmt.Sprintf("Using partition exclusion method for partitioned table %s", tableName))
		}
	}

	// Warn if using a connection pooler port (common PgBouncer ports) with custom format
	if e.config.Database.Port == 6432 || e.config.Database.Port == 6543 {
		e.logger.Warn(fmt.Sprintf("Warning: Port %d is typically a connection pooler (PgBouncer). Custom format dumps (-Fc) require a direct PostgreSQL connection and may not work through connection poolers.", e.config.Database.Port))
	}

	// Generate S3 object key for this table
	objectKey := e.generateObjectKey(tableName)

	// For partitioned tables, pg_dump -t doesn't work well with stdout (-f -)
	// Write to a temp file instead, then upload it
	if hasPartitions && e.config.DumpMode == "schema-only" {
		return e.dumpTableToFile(ctx, tableName, objectKey)
	}

	// Build pg_dump command for this specific table
	cmd, err := e.buildPgDumpCommand(tableName)
	if err != nil {
		return fmt.Errorf("failed to build pg_dump command: %w", err)
	}

	e.logger.Debug(fmt.Sprintf("Command: %s", strings.Join(cmd.Args, " ")))

	// Capture stderr to see any errors from pg_dump
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	// Get stdout pipe for streaming
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	// Stream output directly to S3
	e.logger.Info(fmt.Sprintf("Uploading to s3://%s/%s", e.config.S3.Bucket, objectKey))

	// Create a pipe reader/writer for streaming
	pr, pw := io.Pipe()

	// Upload in background
	uploadDone := make(chan error, 1)
	if e.config.DryRun {
		e.logger.Info("Dry run mode: skipping upload")
		// In dry run, just discard the upload
		go func() {
			defer pw.Close()
			uploadDone <- nil
		}()
	} else {
		go func() {
			// Custom format (-Fc) is always binary, regardless of dump mode
			contentType := "application/octet-stream"
			uploadInput := &s3manager.UploadInput{
				Bucket:      aws.String(e.config.S3.Bucket),
				Key:         aws.String(objectKey),
				Body:        pr,
				ContentType: aws.String(contentType),
			}
			result, uploadErr := e.s3Uploader.UploadWithContext(ctx, uploadInput)
			if uploadErr == nil && result != nil {
				e.logger.Debug(fmt.Sprintf("Successfully uploaded to S3: %s", result.Location))
			}
			uploadDone <- uploadErr
		}()
	}

	// Copy stdout to pipe in background
	// This goroutine closes pw after copying is done
	copyDone := make(chan error, 1)
	bytesCopied := make(chan int64, 1)
	go func() {
		defer func() {
			stdout.Close()
			pw.Close() // Close pipe writer after copying is done
		}()
		if e.config.DryRun {
			// In dry run, discard output
			n, copyErr := io.Copy(io.Discard, stdout)
			bytesCopied <- n
			copyDone <- copyErr
		} else {
			n, copyErr := io.Copy(pw, stdout)
			bytesCopied <- n
			copyDone <- copyErr
		}
	}()

	// Start the command
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return fmt.Errorf("failed to start pg_dump: %w", err)
	}

	// Wait for command to complete
	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()

	// Wait for all operations to complete
	var cmdErr, copyErr, uploadErr error
	var bytesCopiedCount int64
	doneCount := 0
	for doneCount < 4 {
		select {
		case cmdErr = <-cmdDone:
			doneCount++
		case copyErr = <-copyDone:
			doneCount++
		case uploadErr = <-uploadDone:
			doneCount++
		case bytesCopiedCount = <-bytesCopied:
			doneCount++
		case <-ctx.Done():
			// Cancellation requested
			cmd.Process.Kill()
			pw.Close()
			pr.Close()
			stdout.Close()
			return ctx.Err()
		}
	}

	// Log bytes copied
	e.logger.Debug(fmt.Sprintf("Copied %d bytes from pg_dump output", bytesCopiedCount))

	// Always log stderr output (might contain warnings even on success)
	stderrOutput := stderrBuf.String()
	if stderrOutput != "" {
		if e.config.Debug {
			e.logger.Debug(fmt.Sprintf("pg_dump stderr: %s", stderrOutput))
		} else {
			e.logger.Info(fmt.Sprintf("pg_dump stderr: %s", stderrOutput))
		}
	}

	// Check for errors
	if cmdErr != nil {
		if stderrOutput != "" {
			return fmt.Errorf("pg_dump failed: %w\nstderr: %s", cmdErr, stderrOutput)
		}
		return fmt.Errorf("pg_dump failed: %w", cmdErr)
	}
	if copyErr != nil {
		return fmt.Errorf("failed to copy output: %w", copyErr)
	}
	if uploadErr != nil {
		return fmt.Errorf("S3 upload failed: %w", uploadErr)
	}

	if bytesCopiedCount == 0 {
		errorMsg := "pg_dump produced no output (0 bytes)"
		if stderrOutput != "" {
			errorMsg += fmt.Sprintf("\npg_dump stderr: %s", stderrOutput)
		}
		errorMsg += "\nPossible causes: table does not exist, table has no schema, or permissions issue"
		return fmt.Errorf(errorMsg)
	}

	return nil
}

// dumpTableToFile dumps a partitioned table to a temp file, then uploads it to S3
// This is needed because pg_dump -t on partitioned tables doesn't work with stdout (-f -)
func (e *PgDumpExecutor) dumpTableToFile(ctx context.Context, tableName string, objectKey string) error {
	// Create temp file
	tempFile, err := os.CreateTemp("", "pg_dump-*.dump")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempFilePath := tempFile.Name()
	tempFile.Close() // Close immediately, pg_dump will create/write to it
	defer os.Remove(tempFilePath) // Clean up temp file

	// Build pg_dump command (similar to buildPgDumpCommand but without -f -)
	cmd := exec.CommandContext(ctx, "pg_dump")
	env := os.Environ()
	env = append(env, fmt.Sprintf("PGPASSWORD=%s", e.config.Database.Password))
	if e.config.Database.SSLMode != "" {
		env = append(env, fmt.Sprintf("PGSSLMODE=%s", e.config.Database.SSLMode))
	}
	cmd.Env = env

	cmd.Args = append(cmd.Args,
		"-h", e.config.Database.Host,
		"-p", fmt.Sprintf("%d", e.config.Database.Port),
		"-U", e.config.Database.User,
		"-d", e.config.Database.Name,
		"-Fc", "--schema-only", "-w", "-t", tableName,
		"-f", tempFilePath, // Write to file instead of stdout
	)

	e.logger.Debug(fmt.Sprintf("Command: %s", strings.Join(cmd.Args, " ")))

	// Run pg_dump (CombinedOutput captures both stdout and stderr)
	e.logger.Info(fmt.Sprintf("Dumping schema for partitioned table %s to temp file", tableName))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_dump failed: %w\noutput: %s", err, string(output))
	}

	// Log output if there's any (might contain warnings)
	if len(output) > 0 && e.config.Debug {
		e.logger.Debug(fmt.Sprintf("pg_dump output: %s", string(output)))
	}

	// Check file size
	fileInfo, err := os.Stat(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to stat temp file: %w", err)
	}
	if fileInfo.Size() == 0 {
		return fmt.Errorf("pg_dump produced empty file (0 bytes)")
	}
	e.logger.Debug(fmt.Sprintf("Dumped %d bytes to temp file", fileInfo.Size()))

	// Upload to S3
	if e.config.DryRun {
		e.logger.Info("Dry run mode: skipping upload")
		return nil
	}

	e.logger.Info(fmt.Sprintf("Uploading to s3://%s/%s", e.config.S3.Bucket, objectKey))
	file, err := os.Open(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to open temp file for upload: %w", err)
	}
	defer file.Close()

	uploadInput := &s3manager.UploadInput{
		Bucket:      aws.String(e.config.S3.Bucket),
		Key:         aws.String(objectKey),
		Body:        file,
		ContentType: aws.String("application/octet-stream"),
	}

	result, err := e.s3Uploader.UploadWithContext(ctx, uploadInput)
	if err != nil {
		return fmt.Errorf("S3 upload failed: %w", err)
	}

	if result != nil {
		e.logger.Debug(fmt.Sprintf("Successfully uploaded to S3: %s", result.Location))
	}

	return nil
}

// buildPgDumpCommand builds the pg_dump command with appropriate flags for a specific table
func (e *PgDumpExecutor) buildPgDumpCommand(tableName string) (*exec.Cmd, error) {
	// Set PGPASSWORD environment variable for password
	cmd := exec.CommandContext(e.ctx, "pg_dump")
	env := os.Environ()
	env = append(env, fmt.Sprintf("PGPASSWORD=%s", e.config.Database.Password))

	// Set SSL mode in environment if specified
	if e.config.Database.SSLMode != "" {
		env = append(env, fmt.Sprintf("PGSSLMODE=%s", e.config.Database.SSLMode))
	}
	cmd.Env = env

	// Use separate connection flags instead of connection string for better compatibility
	cmd.Args = append(cmd.Args,
		"-h", e.config.Database.Host,
		"-p", fmt.Sprintf("%d", e.config.Database.Port),
		"-U", e.config.Database.User,
		"-d", e.config.Database.Name,
	)

	// Handle dump mode and format selection
	switch e.config.DumpMode {
	case "schema-only":
		// For schema-only, use custom format (compatible with pg_restore)
		cmd.Args = append(cmd.Args, "-Fc", "--schema-only")
	case "data-only":
		// For data-only, use custom format with compression for efficiency
		cmd.Args = append(cmd.Args, "-Fc", "-Z", "9", "--data-only")
		// Set parallel jobs for data dumps (only when not dumping a specific table)
		if e.config.Workers > 1 && tableName == "" {
			cmd.Args = append(cmd.Args, "-j", fmt.Sprintf("%d", e.config.Workers))
		}
	case "schema-and-data", "":
		// Default: dump both schema and data with custom format and compression
		cmd.Args = append(cmd.Args, "-Fc", "-Z", "9")
		// Set parallel jobs for full dumps (only when not dumping a specific table)
		if e.config.Workers > 1 && tableName == "" {
			cmd.Args = append(cmd.Args, "-j", fmt.Sprintf("%d", e.config.Workers))
		}
	default:
		return nil, fmt.Errorf("invalid dump mode: %s", e.config.DumpMode)
	}

	// Add -w flag to prevent password prompts (we use PGPASSWORD env var instead)
	cmd.Args = append(cmd.Args, "-w")

	// For schema-only mode, handle table selection
	if e.config.DumpMode == "schema-only" {
		// Dump only the specified table
		if tableName != "" {
			cmd.Args = append(cmd.Args, "-t", tableName)
		}
	} else {
		// For data-only or schema-and-data modes, use table if specified
		if tableName != "" {
			cmd.Args = append(cmd.Args, "-t", tableName)
		}
	}

	// Explicitly set output to stdout (works for both plain and custom formats)
	// Note: For partitioned tables, we use dumpTableToFile instead (without -f -)
	cmd.Args = append(cmd.Args, "-f", "-")

	return cmd, nil
}

// generateObjectKey generates the S3 object key based on path template and dump mode
func (e *PgDumpExecutor) generateObjectKey(tableName string) string {
	pathTemplate := NewPathTemplate(e.config.S3.PathTemplate)

	// For schema-only dumps, use a simpler path without dates
	if e.config.DumpMode == "schema-only" {
		// Use provided table name, or database name if empty
		if tableName == "" {
			tableName = e.config.Database.Name
		}

		// Check if path template contains {table} placeholder
		hasTablePlaceholder := strings.Contains(e.config.S3.PathTemplate, "{table}")

		// Generate base path without date placeholders (replace them with empty string)
		basePath := e.config.S3.PathTemplate
		if hasTablePlaceholder {
			// Replace {table} with actual table name
			basePath = strings.ReplaceAll(basePath, "{table}", tableName)
		}
		// Remove date placeholders with their surrounding slashes
		basePath = strings.ReplaceAll(basePath, "/{YYYY}", "")
		basePath = strings.ReplaceAll(basePath, "{YYYY}/", "")
		basePath = strings.ReplaceAll(basePath, "/{MM}", "")
		basePath = strings.ReplaceAll(basePath, "{MM}/", "")
		basePath = strings.ReplaceAll(basePath, "/{DD}", "")
		basePath = strings.ReplaceAll(basePath, "{DD}/", "")
		basePath = strings.ReplaceAll(basePath, "/{HH}", "")
		basePath = strings.ReplaceAll(basePath, "{HH}/", "")
		// Also handle standalone placeholders
		basePath = strings.ReplaceAll(basePath, "{YYYY}", "")
		basePath = strings.ReplaceAll(basePath, "{MM}", "")
		basePath = strings.ReplaceAll(basePath, "{DD}", "")
		basePath = strings.ReplaceAll(basePath, "{HH}", "")
		// Clean up any double slashes
		for strings.Contains(basePath, "//") {
			basePath = strings.ReplaceAll(basePath, "//", "/")
		}
		// Remove trailing slash
		basePath = strings.TrimSuffix(basePath, "/")

		// Filename: if {table} was in path, use schema.dump; otherwise prefix with table name
		var filename string
		if hasTablePlaceholder {
			// Table is already in the path, just use schema.dump
			filename = "schema.dump"
		} else {
			// No {table} in path, prefix filename with table name
			filename = fmt.Sprintf("%s-schema.dump", tableName)
		}

		return fmt.Sprintf("%s/%s", basePath, filename)
	}

	// For data-only and schema-and-data, use date-based paths
	now := time.Now()

	// Use provided table name, or database name if empty
	if tableName == "" {
		tableName = e.config.Database.Name
	}
	basePath := pathTemplate.Generate(tableName, now)

	// Determine filename based on dump mode
	filename := tableName

	// Add dump mode suffix if not schema-and-data
	switch e.config.DumpMode {
	case "data-only":
		filename += "-data"
	}

	// Add timestamp
	filename += fmt.Sprintf("-%s.dump", now.Format("20060102-150405"))

	return fmt.Sprintf("%s/%s", basePath, filename)
}

// connect establishes a database connection
func (e *PgDumpExecutor) connect(ctx context.Context) error {
	sslMode := e.config.Database.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		e.config.Database.Host,
		e.config.Database.Port,
		e.config.Database.User,
		e.config.Database.Password,
		e.config.Database.Name,
		sslMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return err
	}

	e.db = db
	return nil
}

// discoverTopLevelTables discovers all top-level tables (non-partition tables) in the database
func (e *PgDumpExecutor) discoverTopLevelTables(ctx context.Context) ([]string, error) {
	if e.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	// Query for top-level tables (tables that are NOT partitions)
	// A table is a partition if it exists in pg_inherits as a child (inhrelid)
	query := `
		SELECT c.relname::text AS tablename
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
			AND c.relkind = 'r'
			AND NOT EXISTS (
				SELECT 1 FROM pg_inherits WHERE inhrelid = c.oid
			)
		ORDER BY c.relname;
	`

	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query top-level tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over table rows: %w", err)
	}

	return tables, nil
}

// discoverPartitions discovers all partition tables in the database
func (e *PgDumpExecutor) discoverPartitions(ctx context.Context) ([]string, error) {
	if e.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	// Query for partition tables (tables that ARE partitions)
	// This handles both:
	// 1. Table inheritance (old style) - tables in pg_inherits as children
	// 2. Declarative partitioning (new style) - tables in pg_inherits as children
	// The key is: any table that exists in pg_inherits as inhrelid is a partition
	query := `
		SELECT DISTINCT c.relname::text AS tablename
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
			AND c.relkind = 'r'
			AND EXISTS (
				SELECT 1 FROM pg_inherits WHERE inhrelid = c.oid
			)
		ORDER BY tablename;
	`

	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query partitions: %w", err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partitionName string
		if err := rows.Scan(&partitionName); err != nil {
			return nil, fmt.Errorf("failed to scan partition name: %w", err)
		}
		partitions = append(partitions, partitionName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over partition rows: %w", err)
	}

	return partitions, nil
}

// discoverPartitionsByPattern discovers partition tables by matching naming patterns
// This catches partitions that might not be registered in pg_inherits
func (e *PgDumpExecutor) discoverPartitionsByPattern(ctx context.Context) ([]string, error) {
	if e.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	// Query for tables that match common partition naming patterns
	// Patterns: *_pYYYYMMDD, *_YYYY_MM, *_YYYYMM, etc.
	query := `
		SELECT c.relname::text AS tablename
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
			AND c.relkind = 'r'
			AND (
				-- Pattern: messages_p20240101, flights_2024_01, etc.
				c.relname ~ '^[a-z_]+_p[0-9]{8}$'
				OR c.relname ~ '^[a-z_]+_[0-9]{4}_[0-9]{2}$'
				OR c.relname ~ '^[a-z_]+_[0-9]{4}_[0-9]{2}_[0-9]{2}$'
			)
		ORDER BY c.relname;
	`

	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query partitions by pattern: %w", err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partitionName string
		if err := rows.Scan(&partitionName); err != nil {
			return nil, fmt.Errorf("failed to scan partition name: %w", err)
		}
		partitions = append(partitions, partitionName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over partition rows: %w", err)
	}

	return partitions, nil
}

// initS3 initializes the S3 client and uploader
func (e *PgDumpExecutor) initS3() error {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(e.config.S3.Endpoint),
		Region:           aws.String(e.config.S3.Region),
		Credentials:      credentials.NewStaticCredentials(e.config.S3.AccessKey, e.config.S3.SecretKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create S3 session: %w", err)
	}

	e.s3Client = s3.New(sess)
	e.s3Uploader = s3manager.NewUploader(sess)

	return nil
}
