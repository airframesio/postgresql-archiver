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

	// Generate S3 object key for this table
	objectKey := e.generateObjectKey(tableName)

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
			// Set content type based on dump format
			contentType := "application/octet-stream" // Default for custom format
			if e.config.DumpMode == "schema-only" {
				contentType = "text/plain" // Plain SQL format
			}
			uploadInput := &s3manager.UploadInput{
				Bucket:      aws.String(e.config.S3.Bucket),
				Key:         aws.String(objectKey),
				Body:        pr,
				ContentType: aws.String(contentType),
			}
			_, uploadErr := e.s3Uploader.UploadWithContext(ctx, uploadInput)
			uploadDone <- uploadErr
		}()
	}

	// Copy stdout to pipe in background
	// This goroutine closes pw after copying is done
	copyDone := make(chan error, 1)
	go func() {
		defer func() {
			stdout.Close()
			pw.Close() // Close pipe writer after copying is done
		}()
		if e.config.DryRun {
			// In dry run, discard output
			_, copyErr := io.Copy(io.Discard, stdout)
			copyDone <- copyErr
		} else {
			_, copyErr := io.Copy(pw, stdout)
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
	doneCount := 0
	for doneCount < 3 {
		select {
		case cmdErr = <-cmdDone:
			doneCount++
		case copyErr = <-copyDone:
			doneCount++
		case uploadErr = <-uploadDone:
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

	// Check for errors
	if cmdErr != nil {
		stderrOutput := stderrBuf.String()
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

	// Log stderr if there's any output (might contain warnings)
	if stderrOutput := stderrBuf.String(); stderrOutput != "" && e.config.Debug {
		e.logger.Debug(fmt.Sprintf("pg_dump stderr: %s", stderrOutput))
	}

	return nil
}

// buildPgDumpCommand builds the pg_dump command with appropriate flags for a specific table
func (e *PgDumpExecutor) buildPgDumpCommand(tableName string) (*exec.Cmd, error) {
	// Set PGPASSWORD environment variable for password
	cmd := exec.CommandContext(e.ctx, "pg_dump")
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.config.Database.Password))

	// Build connection string
	connParts := []string{
		fmt.Sprintf("host=%s", e.config.Database.Host),
		fmt.Sprintf("port=%d", e.config.Database.Port),
		fmt.Sprintf("user=%s", e.config.Database.User),
		fmt.Sprintf("dbname=%s", e.config.Database.Name),
	}

	if e.config.Database.SSLMode != "" {
		connParts = append(connParts, fmt.Sprintf("sslmode=%s", e.config.Database.SSLMode))
	}

	connStr := strings.Join(connParts, " ")
	cmd.Args = append(cmd.Args, "-d", connStr)

	// Handle dump mode and format selection
	switch e.config.DumpMode {
	case "schema-only":
		// For schema-only, use plain SQL format (simpler, faster, human-readable)
		cmd.Args = append(cmd.Args, "-Fp", "--schema-only")
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

	// For schema-only mode, handle table selection
	if e.config.DumpMode == "schema-only" {
		// When dumping a single table, we don't need to exclude partitions
		// because pg_dump -t will only dump that specific table
		// Only exclude partitions when dumping multiple tables (which we don't do anymore)
		// For now, we always dump one table at a time, so no exclude needed

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
