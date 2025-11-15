package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	pq "github.com/lib/pq"
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

	// For data dumps with date ranges OR output duration, discover and dump partitions separately
	if (e.config.DumpMode == "data-only" || e.config.DumpMode == "schema-and-data") &&
		e.config.Table != "" &&
		(e.config.StartDate != "" || e.config.EndDate != "" || e.config.OutputDuration != "") {
		return e.dumpPartitionsByDateRange(ctx)
	}

	// For non-schema-only modes without date ranges or output duration, use the original single-dump approach
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

	// For schema-only dumps, pg_dump -t streaming to stdout can produce empty output for some tables.
	// Use a temp file path instead (also required for partitioned tables).
	if e.config.DumpMode == "schema-only" {
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
		return errors.New(errorMsg)
	}

	return nil
}

// dumpTableToFile dumps a partitioned table to a temp file, then uploads it to S3
// This is needed because pg_dump -t on partitioned tables doesn't work with stdout (-f -)
func (e *PgDumpExecutor) dumpTableToFile(ctx context.Context, tableName string, objectKey string) error {
	dumpTarget := e.qualifyTableName(tableName)

	// Create temp file
	tempFile, err := os.CreateTemp("", "pg_dump-*.dump")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempFilePath := tempFile.Name()
	tempFile.Close()              // Close immediately, pg_dump will create/write to it
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
		"-Fc", "--schema-only", "-w", "-t", dumpTarget,
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

// extractDateFromTableName extracts date from partition table name
func (e *PgDumpExecutor) extractDateFromTableName(tableName string) (time.Time, bool) {
	baseTableLen := len(e.config.Table)
	if len(tableName) <= baseTableLen+1 {
		return time.Time{}, false
	}

	// Remove base table name and underscore
	suffix := tableName[baseTableLen+1:]

	// Format 1: {base_table}_YYYYMMDD (8 digits)
	if len(suffix) == 8 {
		if date, err := time.Parse("20060102", suffix); err == nil {
			return date, true
		}
	}

	// Format 2: {base_table}_pYYYYMMDD (p + 8 digits)
	if len(suffix) == 9 && suffix[0] == 'p' {
		if date, err := time.Parse("20060102", suffix[1:]); err == nil {
			return date, true
		}
	}

	// Format 3: {base_table}_YYYY_MM (7 chars with underscore)
	if len(suffix) == 7 && suffix[4] == '_' {
		yearMonth := suffix[:4] + suffix[5:]
		// Use first day of the month for monthly partitions
		if date, err := time.Parse("200601", yearMonth); err == nil {
			return date, true
		}
	}

	// Format 4: {base_table}_YYYY_MM_DD (10 chars with underscores)
	if len(suffix) == 10 && suffix[4] == '_' && suffix[7] == '_' {
		dateStr := suffix[:4] + suffix[5:7] + suffix[8:]
		if date, err := time.Parse("20060102", dateStr); err == nil {
			return date, true
		}
	}

	return time.Time{}, false
}

// dumpPartitionsByDateRange discovers partitions in date range and dumps each separately
func (e *PgDumpExecutor) dumpPartitionsByDateRange(ctx context.Context) error {
	if err := e.connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer e.db.Close()

	// Parse date range
	var startDate, endDate *time.Time
	if e.config.StartDate != "" {
		if t, err := time.Parse("2006-01-02", e.config.StartDate); err == nil {
			startDate = &t
		}
	}
	if e.config.EndDate != "" {
		if t, err := time.Parse("2006-01-02", e.config.EndDate); err == nil {
			endDate = &t
		}
	}

	// Discover partitions matching the table pattern
	pattern := e.config.Table + "_%"
	query := `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public'
			AND tablename LIKE $1
		ORDER BY tablename
	`

	rows, err := e.db.QueryContext(ctx, query, pattern)
	if err != nil {
		return fmt.Errorf("failed to query partitions: %w", err)
	}
	defer rows.Close()

	var partitionsToDump []struct {
		name string
		date time.Time
	}

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}

		// Extract date from partition name
		date, ok := e.extractDateFromTableName(tableName)
		if !ok {
			e.logger.Debug(fmt.Sprintf("Skipping table %s: no valid date pattern", tableName))
			continue
		}

		// Filter by date range
		if startDate != nil && date.Before(*startDate) {
			e.logger.Debug(fmt.Sprintf("Skipping partition %s: date %s is before start date", tableName, date.Format("2006-01-02")))
			continue
		}
		if endDate != nil && date.After(*endDate) {
			e.logger.Debug(fmt.Sprintf("Skipping partition %s: date %s is after end date", tableName, date.Format("2006-01-02")))
			continue
		}

		partitionsToDump = append(partitionsToDump, struct {
			name string
			date time.Time
		}{name: tableName, date: date})
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating partitions: %w", err)
	}

	if len(partitionsToDump) == 0 {
		e.logger.Info("No partitions found in date range")
		if e.config.DateColumn == "" {
			e.logger.Info("Date column not provided; skipping data dump fallback")
			return nil
		}
		if startDate == nil || endDate == nil {
			return fmt.Errorf("date column filtering requires both start-date and end-date")
		}
		e.logger.Info(fmt.Sprintf("Using date column %s to dump %s in windows", e.config.DateColumn, e.config.Table))
		return e.dumpTableByDateColumn(ctx, *startDate, *endDate)
	}

	e.logger.Info(fmt.Sprintf("Found %d partition(s) to dump in date range", len(partitionsToDump)))

	// Group partitions by output duration
	groups := e.groupPartitionsByDuration(partitionsToDump)
	e.logger.Info(fmt.Sprintf("Grouped into %d file(s) based on output duration: %s", len(groups), e.config.OutputDuration))

	// Sort groups by date (groupKey) for consistent ordering
	type groupEntry struct {
		key        string
		partitions []struct {
			name string
			date time.Time
		}
		date time.Time
	}

	var sortedGroups []groupEntry
	for groupKey, partitions := range groups {
		// Use the first partition's date as the group date for sorting
		groupDate := partitions[0].date
		sortedGroups = append(sortedGroups, groupEntry{
			key:        groupKey,
			partitions: partitions,
			date:       groupDate,
		})
	}

	// Sort by date
	sort.Slice(sortedGroups, func(i, j int) bool {
		return sortedGroups[i].date.Before(sortedGroups[j].date)
	})

	// Dump each group in sorted order
	for i, group := range sortedGroups {
		e.logger.Info(fmt.Sprintf("Dumping group %d/%d: %s (%d partition(s))", i+1, len(sortedGroups), group.key, len(group.partitions)))
		if err := e.dumpPartitionGroup(ctx, group.partitions, group.key); err != nil {
			return fmt.Errorf("failed to dump group %s: %w", group.key, err)
		}
	}

	e.logger.Info(fmt.Sprintf("✅ Successfully dumped %d group(s) (%d total partition(s))", len(groups), len(partitionsToDump)))
	return nil
}

// groupPartitionsByDuration groups partitions by output duration
func (e *PgDumpExecutor) groupPartitionsByDuration(partitions []struct {
	name string
	date time.Time
}) map[string][]struct {
	name string
	date time.Time
} {
	groups := make(map[string][]struct {
		name string
		date time.Time
	})

	outputDuration := e.config.OutputDuration
	if outputDuration == "" {
		outputDuration = DurationDaily // Default to daily
	}

	for _, partition := range partitions {
		groupKey := e.getGroupKeyForDuration(partition.date, outputDuration)
		groups[groupKey] = append(groups[groupKey], partition)
	}

	return groups
}

// getGroupKeyForDuration returns a key for grouping partitions by duration
func (e *PgDumpExecutor) getGroupKeyForDuration(date time.Time, duration string) string {
	switch duration {
	case DurationHourly:
		return date.Format("2006-01-02-15")
	case DurationDaily:
		return date.Format("2006-01-02")
	case DurationWeekly:
		// Find Monday of the week
		weekday := int(date.Weekday())
		if weekday == 0 { // Sunday
			weekday = 7
		}
		daysToMonday := weekday - 1
		monday := date.AddDate(0, 0, -daysToMonday)
		return monday.Format("2006-01-02")
	case DurationMonthly:
		return date.Format("2006-01")
	case DurationYearly:
		return date.Format("2006")
	default:
		return date.Format("2006-01-02")
	}
}

// dumpPartitionGroup dumps a group of partitions together into a single file
func (e *PgDumpExecutor) dumpPartitionGroup(ctx context.Context, partitions []struct {
	name string
	date time.Time
}, groupKey string) error {
	if len(partitions) == 0 {
		return fmt.Errorf("no partitions in group")
	}

	// Use the first partition's date to generate the filename
	// All partitions in the group should have the same group key, so we can use any date
	groupDate := partitions[0].date

	// Generate object key with date based on output duration
	objectKey := e.generateObjectKeyWithDuration(e.config.Table, groupDate, e.config.OutputDuration)

	// Extract partition names
	partitionNames := make([]string, len(partitions))
	for i, p := range partitions {
		partitionNames[i] = p.name
	}

	// Dump all partitions in the group together
	return e.dumpMultiplePartitionsToFile(ctx, partitionNames, objectKey, groupDate)
}

type dateWindow struct {
	start time.Time
	end   time.Time
}

// dumpTableByDateColumn generates date-based windows using the configured output duration
// and dumps each window by materializing a temporary staging table that contains only rows in the window.
func (e *PgDumpExecutor) dumpTableByDateColumn(ctx context.Context, startDate, endDate time.Time) error {
	if e.db == nil {
		if err := e.connect(ctx); err != nil {
			return fmt.Errorf("failed to connect before date-column dump: %w", err)
		}
		defer e.db.Close()
	}

	if e.config.DateColumn == "" {
		return fmt.Errorf("date column is required for date-based dumping")
	}

	windows, err := e.buildDateWindows(startDate, endDate)
	if err != nil {
		return err
	}
	if len(windows) == 0 {
		e.logger.Info("No date windows generated for requested range")
		return nil
	}

	for i, window := range windows {
		stagingName := e.buildStagingTableName(i, window.start)
		rows, err := e.createStagingTable(ctx, stagingName, window)
		if err != nil {
			return fmt.Errorf("failed to create staging table %s: %w", stagingName, err)
		}

		if rows == 0 {
			e.logger.Info(fmt.Sprintf("Skipping window %d/%d (%s → %s): no rows",
				i+1, len(windows),
				window.start.Format("2006-01-02 15:04"),
				window.end.Add(-time.Second).Format("2006-01-02 15:04")))
			if dropErr := e.dropStagingTable(ctx, stagingName); dropErr != nil {
				e.logger.Warn(fmt.Sprintf("Failed to drop empty staging table %s: %v", stagingName, dropErr))
			}
			continue
		}

		objectKey := e.generateObjectKeyWithDuration(e.config.Table, window.start, e.config.OutputDuration)
		rowDesc := "unknown"
		if rows > 0 {
			rowDesc = fmt.Sprintf("%d", rows)
		}
		e.logger.Info(fmt.Sprintf("Dumping window %d/%d (%s row(s)): %s → %s",
			i+1, len(windows), rowDesc,
			window.start.Format("2006-01-02 15:04"),
			window.end.Add(-time.Second).Format("2006-01-02 15:04")))

		err = e.dumpMultiplePartitionsToFile(ctx, []string{stagingName}, objectKey, window.start)
		dropErr := e.dropStagingTable(ctx, stagingName)

		if dropErr != nil {
			e.logger.Warn(fmt.Sprintf("Failed to drop staging table %s: %v", stagingName, dropErr))
		}
		if err != nil {
			return fmt.Errorf("pg_dump failed for window starting %s: %w", window.start.Format("2006-01-02"), err)
		}
	}

	e.logger.Info(fmt.Sprintf("✅ Successfully dumped %d date window(s) using column %s", len(windows), e.config.DateColumn))
	return nil
}

func (e *PgDumpExecutor) buildDateWindows(startDate, endDate time.Time) ([]dateWindow, error) {
	start := normalizeToUTC(startDate)
	end := normalizeToUTC(endDate)
	if end.Before(start) {
		return nil, fmt.Errorf("end date %s cannot be before start date %s", end.Format("2006-01-02"), start.Format("2006-01-02"))
	}

	endExclusive := end.AddDate(0, 0, 1)
	duration := e.config.OutputDuration
	if duration == "" {
		duration = DurationDaily
	}

	var windows []dateWindow
	current := start
	for current.Before(endExclusive) {
		next := addDuration(current, duration)
		if next.After(endExclusive) {
			next = endExclusive
		}
		if !next.After(current) {
			break
		}
		windows = append(windows, dateWindow{start: current, end: next})
		current = next
	}

	return windows, nil
}

func normalizeToUTC(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
}

func addDuration(t time.Time, duration string) time.Time {
	switch duration {
	case DurationHourly:
		return t.Add(time.Hour)
	case DurationWeekly:
		return t.AddDate(0, 0, 7)
	case DurationMonthly:
		return t.AddDate(0, 1, 0)
	case DurationYearly:
		return t.AddDate(1, 0, 0)
	default: // daily
		return t.AddDate(0, 0, 1)
	}
}

func (e *PgDumpExecutor) buildStagingTableName(idx int, windowStart time.Time) string {
	base := fmt.Sprintf("_archiver_%s_%s_%d", e.config.Table, windowStart.Format("20060102"), idx)
	return truncateIdentifier(base)
}

func truncateIdentifier(identifier string) string {
	const maxLen = 63
	if len(identifier) <= maxLen {
		return identifier
	}
	return identifier[:maxLen]
}

func (e *PgDumpExecutor) createStagingTable(ctx context.Context, stagingName string, window dateWindow) (int64, error) {
	quotedStaging := fmt.Sprintf("public.%s", pq.QuoteIdentifier(stagingName))
	quotedBase := fmt.Sprintf("public.%s", pq.QuoteIdentifier(e.config.Table))
	quotedColumn := pq.QuoteIdentifier(e.config.DateColumn)

	query := fmt.Sprintf(`CREATE UNLOGGED TABLE %s AS
SELECT *
FROM %s
WHERE %s >= $1 AND %s < $2`, quotedStaging, quotedBase, quotedColumn, quotedColumn)

	result, err := e.db.ExecContext(ctx, query, window.start, window.end)
	if err != nil {
		return 0, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return -1, nil
	}
	return rows, nil
}

func (e *PgDumpExecutor) dropStagingTable(ctx context.Context, stagingName string) error {
	quotedStaging := fmt.Sprintf("public.%s", pq.QuoteIdentifier(stagingName))
	_, err := e.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedStaging))
	return err
}

// dumpMultiplePartitionsToFile dumps multiple partitions together to a temp file, then uploads it to S3
func (e *PgDumpExecutor) dumpMultiplePartitionsToFile(ctx context.Context, partitionNames []string, objectKey string, dumpDate time.Time) error {
	// Create temp file
	tempFile, err := os.CreateTemp("", "pg_dump-*.dump")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempFilePath := tempFile.Name()
	tempFile.Close()              // Close immediately, pg_dump will create/write to it
	defer os.Remove(tempFilePath) // Clean up temp file

	// Build pg_dump command
	cmd := exec.CommandContext(ctx, "pg_dump")
	env := os.Environ()
	env = append(env, fmt.Sprintf("PGPASSWORD=%s", e.config.Database.Password))
	if e.config.Database.SSLMode != "" {
		env = append(env, fmt.Sprintf("PGSSLMODE=%s", e.config.Database.SSLMode))
	}
	cmd.Env = env

	// Build command args based on dump mode
	var formatFlags []string
	switch e.config.DumpMode {
	case "data-only":
		formatFlags = []string{"-Fc", "-Z", "9", "--data-only"}
	case "schema-and-data", "":
		formatFlags = []string{"-Fc", "-Z", "9"}
	default:
		return fmt.Errorf("unsupported dump mode for date-based dumps: %s", e.config.DumpMode)
	}

	cmd.Args = append(cmd.Args,
		"-h", e.config.Database.Host,
		"-p", fmt.Sprintf("%d", e.config.Database.Port),
		"-U", e.config.Database.User,
		"-d", e.config.Database.Name,
	)
	cmd.Args = append(cmd.Args, formatFlags...)
	cmd.Args = append(cmd.Args, "-w")

	// Add -t flag for each partition
	for _, partitionName := range partitionNames {
		cmd.Args = append(cmd.Args, "-t", e.qualifyTableName(partitionName))
	}

	cmd.Args = append(cmd.Args, "-f", tempFilePath)

	e.logger.Debug(fmt.Sprintf("Command: %s", strings.Join(cmd.Args, " ")))

	// Run pg_dump
	e.logger.Debug(fmt.Sprintf("Dumping %d partition(s) to temp file", len(partitionNames)))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_dump failed: %w\noutput: %s", err, string(output))
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

// generateObjectKeyWithDuration generates the S3 object key with date placeholders filled in based on output duration
func (e *PgDumpExecutor) generateObjectKeyWithDuration(tableName string, dumpDate time.Time, duration string) string {
	pathTemplate := NewPathTemplate(e.config.S3.PathTemplate)

	// Use provided table name, or database name if empty
	if tableName == "" {
		tableName = e.config.Database.Name
	}

	// Generate path with date placeholders filled in
	basePath := pathTemplate.Generate(tableName, dumpDate)

	// Generate filename based on output duration
	var filename string
	switch duration {
	case DurationHourly:
		filename = fmt.Sprintf("%s-%s.dump", tableName, dumpDate.Format("2006-01-02-15"))
	case DurationDaily:
		filename = fmt.Sprintf("%s-%s.dump", tableName, dumpDate.Format("2006-01-02"))
	case DurationWeekly:
		// Find Monday of the week
		weekday := int(dumpDate.Weekday())
		if weekday == 0 { // Sunday
			weekday = 7
		}
		daysToMonday := weekday - 1
		monday := dumpDate.AddDate(0, 0, -daysToMonday)
		filename = fmt.Sprintf("%s-%s.dump", tableName, monday.Format("2006-01-02"))
	case DurationMonthly:
		filename = fmt.Sprintf("%s-%s.dump", tableName, dumpDate.Format("2006-01"))
	case DurationYearly:
		filename = fmt.Sprintf("%s-%s.dump", tableName, dumpDate.Format("2006"))
	default:
		filename = fmt.Sprintf("%s-%s.dump", tableName, dumpDate.Format("2006-01-02"))
	}

	return fmt.Sprintf("%s/%s", basePath, filename)
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
			cmd.Args = append(cmd.Args, "-t", e.qualifyTableName(tableName))
		}
	} else {
		// For data-only or schema-and-data modes, use table if specified
		if tableName != "" {
			cmd.Args = append(cmd.Args, "-t", e.qualifyTableName(tableName))
		}
	}

	// Explicitly set output to stdout (works for both plain and custom formats)
	// Note: For partitioned tables, we use dumpTableToFile instead (without -f -)
	cmd.Args = append(cmd.Args, "-f", "-")

	return cmd, nil
}

// qualifyTableName ensures pg_dump gets a schema-qualified table identifier
func (e *PgDumpExecutor) qualifyTableName(tableName string) string {
	if strings.Contains(tableName, ".") {
		return tableName
	}
	return fmt.Sprintf("public.%s", tableName)
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
