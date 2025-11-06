package cmd

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // MD5 used for checksums, not cryptography
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/airframesio/postgresql-archiver/cmd/compressors"
	"github.com/airframesio/postgresql-archiver/cmd/formatters"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/lib/pq"
)

// Stage constants
const (
	StageSkipped   = "Skipped"
	StageCancelled = "Cancelled"
	StageSetup     = "Setup"
)

// Error definitions
var (
	ErrInsufficientPermissions  = errors.New("insufficient permissions to read table")
	ErrPartitionNoPermissions   = errors.New("partition tables exist but you don't have SELECT permissions")
	ErrS3ClientNotInitialized   = errors.New("S3 client not initialized")
	ErrS3UploaderNotInitialized = errors.New("S3 uploader not initialized")
)

// isConnectionError checks if an error is due to a closed or broken database connection
func isConnectionError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "bad connection") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "sql: database is closed")
}

type Archiver struct {
	config       *Config
	db           *sql.DB
	s3Client     *s3.S3
	s3Uploader   *s3manager.Uploader
	progressChan chan tea.Cmd
	logger       *slog.Logger
	ctx          context.Context // Context for cancellation
}

type PartitionInfo struct {
	TableName string
	Date      time.Time
	RowCount  int64
}

type ProcessResult struct {
	Partition    PartitionInfo
	Compressed   bool
	Uploaded     bool
	Skipped      bool
	SkipReason   string
	Error        error
	BytesWritten int64
	Stage        string
	S3Key        string        // S3 object key for uploaded file
	StartTime    time.Time     // When partition processing started
	Duration     time.Duration // How long partition processing took
}

func NewArchiver(config *Config, logger *slog.Logger) *Archiver {
	return &Archiver{
		config:       config,
		progressChan: make(chan tea.Cmd, 100),
		logger:       logger,
	}
}

//nolint:gocognit // complex orchestration function
func (a *Archiver) Run(ctx context.Context) error {
	// Store context for cancellation checks during processing
	a.ctx = ctx

	// Write PID file
	if err := WritePIDFile(); err != nil {
		return fmt.Errorf("failed to write PID file: %w", err)
	}
	defer func() {
		_ = RemovePIDFile()
	}()

	// Initialize task info
	taskInfo := &TaskInfo{
		PID:         os.Getpid(),
		StartTime:   time.Now(),
		Table:       a.config.Table,
		StartDate:   a.config.StartDate,
		EndDate:     a.config.EndDate,
		CurrentTask: "Starting archiver",
	}
	_ = WriteTaskInfo(taskInfo)
	defer func() {
		_ = RemoveTaskFile()
	}()

	// Create channels for communication
	errChan := make(chan error, 1)
	resultsChan := make(chan []ProcessResult, 1)

	// In debug mode, skip the TUI and run with simple text output
	if a.config.Debug {
		a.logger.Info("Running in debug mode - TUI disabled for better log visibility")

		// Track goroutine completion separately from errors
		done := make(chan struct{})

		// Run archival process directly without TUI
		go func() {
			defer close(done)
			err := a.runArchivalProcess(ctx, nil, taskInfo)
			errChan <- err
		}()

		// Wait for completion or cancellation
		select {
		case err := <-errChan:
			// Normal completion
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			if errors.Is(err, context.Canceled) {
				a.logger.Info("⚠️  Archival process cancelled by user")
			} else {
				a.logger.Info("✅ Archival process completed")
			}
			return err
		case <-ctx.Done():
			// Cancellation detected - force close database connection immediately
			a.logger.Info("⚠️  Cancellation detected - closing database connection to abort queries...")
			if a.db != nil {
				if err := a.db.Close(); err != nil {
					a.logger.Debug(fmt.Sprintf("Error closing database: %v", err))
				}
			}

			// Wait briefly for goroutine to detect closed connection and exit gracefully
			select {
			case <-done:
				a.logger.Info("✅ Goroutine exited after connection close")
				// Get the error if available
				select {
				case err := <-errChan:
					return err
				default:
					return context.Canceled
				}
			case <-time.After(500 * time.Millisecond):
				// Goroutine didn't exit in time - force exit the process
				a.logger.Error("⚠️  Goroutine still running after 500ms - forcing process exit")
				os.Exit(130) // Standard exit code for SIGINT
			}
		}
	}

	// Start the UI with the archiver reference (normal mode)
	// Create a cancellable context so the TUI can stop operations
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	progressModel := newProgressModelWithArchiver(ctx, cancel, a.config, a, errChan, resultsChan, taskInfo)
	// CRITICAL: Disable Bubble Tea's signal handler so our custom handler can work
	program := tea.NewProgram(progressModel, tea.WithoutSignalHandler())

	// Store the program reference in the model so goroutines can send messages
	// Use a goroutine to avoid blocking before Run() starts
	go func() {
		time.Sleep(10 * time.Millisecond) // Give program.Run() time to start
		program.Send(setProgramMsg{program: program})
	}()

	// Run the TUI (this will handle everything internally)
	if _, err := program.Run(); err != nil {
		return fmt.Errorf("error running progress display: %w", err)
	}

	// Check for errors
	select {
	case err := <-errChan:
		if err != nil {
			return err
		}
		return nil // No partitions found
	default:
	}

	// Get results and print summary
	select {
	case results := <-resultsChan:
		a.printSummary(results)
	default:
		// No results (might have quit early)
	}

	// Close database connection if open
	if a.db != nil {
		a.db.Close()
	}

	return nil
}

// runArchivalProcess runs the archival process without the TUI (for debug mode)
func (a *Archiver) runArchivalProcess(ctx context.Context, _ *tea.Program, _ *TaskInfo) error {
	// Ensure database is always closed on exit
	defer func() {
		if a.db != nil {
			a.db.Close()
			a.db = nil
		}
	}()

	a.logger.Debug("Connecting to database...")
	if err := a.connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	// Only log in debug mode (when TUI is not active)
	if a.config.Debug {
		a.logger.Info("✅ Connected to database")
	}

	a.logger.Debug("Checking table permissions...")
	if err := a.checkTablePermissions(ctx); err != nil {
		return fmt.Errorf("permission check failed: %w", err)
	}
	if a.config.Debug {
		a.logger.Info("✅ Table permissions verified")
	}

	a.logger.Debug("Discovering partitions...")
	// Inline partition discovery (same logic as progress.go doDiscover)
	query := `
		SELECT c.relname::text AS tablename
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
			AND c.relname LIKE $1
			AND c.relkind = 'r'
			AND NOT EXISTS (
				SELECT 1 FROM pg_inherits WHERE inhparent = c.oid
			)
		ORDER BY c.relname;
	`

	pattern := a.config.Table + "_%"
	rows, err := a.db.QueryContext(ctx, query, pattern)
	if err != nil {
		// Check if error is due to cancellation or closed connection
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isConnectionError(err) {
			a.logger.Info("⚠️  Query cancelled or connection closed")
			return context.Canceled
		}
		return fmt.Errorf("failed to query partitions: %w", err)
	}
	defer rows.Close()

	// Check for cancellation immediately after query starts
	select {
	case <-ctx.Done():
		a.logger.Info("⚠️  Cancellation detected after query started")
		return ctx.Err()
	default:
	}

	var partitions []PartitionInfo
	for rows.Next() {
		// Check for cancellation in the loop
		select {
		case <-ctx.Done():
			a.logger.Info("⚠️  Cancellation detected during partition discovery")
			return ctx.Err()
		default:
		}

		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan partition name: %w", err)
		}

		date, ok := a.extractDateFromTableName(tableName)
		if !ok {
			a.logger.Debug(fmt.Sprintf("Skipping table %s (no valid date)", tableName))
			continue
		}

		partitions = append(partitions, PartitionInfo{
			TableName: tableName,
			Date:      date,
		})
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over partition rows: %w", err)
	}

	a.logger.Info(fmt.Sprintf("✅ Found %d partitions", len(partitions)))

	if len(partitions) == 0 {
		a.logger.Info("No partitions found to archive")
		return nil
	}

	a.logger.Debug("Processing partitions...")
	results := make([]ProcessResult, 0, len(partitions))
	for _, partition := range partitions {
		// Check if context was cancelled
		select {
		case <-ctx.Done():
			a.logger.Info("⚠️  Stopping partition processing due to cancellation")
			return ctx.Err()
		default:
			// Continue processing
		}

		a.logger.Info(fmt.Sprintf("Processing partition: %s", partition.TableName))
		result := a.ProcessPartitionWithProgress(partition, nil)
		results = append(results, result)

		if result.Error != nil {
			a.logger.Error(fmt.Sprintf("  ❌ Failed: %v", result.Error))
		} else if result.Skipped {
			a.logger.Info(fmt.Sprintf("  ⏭️  Skipped: %s", result.SkipReason))
		} else {
			a.logger.Info(fmt.Sprintf("  ✅ Success: %d bytes", result.BytesWritten))
		}
	}

	a.logger.Info("✅ All partitions processed")
	a.printSummary(results)

	return nil
}

func (a *Archiver) connect(ctx context.Context) error {
	sslMode := a.config.Database.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		a.config.Database.Host,
		a.config.Database.Port,
		a.config.Database.User,
		a.config.Database.Password,
		a.config.Database.Name,
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

	a.db = db

	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(a.config.S3.Endpoint),
		Region:           aws.String(a.config.S3.Region),
		Credentials:      credentials.NewStaticCredentials(a.config.S3.AccessKey, a.config.S3.SecretKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		db.Close()
		a.db = nil
		return fmt.Errorf("failed to create S3 session: %w", err)
	}

	a.s3Client = s3.New(sess)
	a.s3Uploader = s3manager.NewUploader(sess)

	return nil
}

func (a *Archiver) checkTablePermissions(ctx context.Context) error {
	// Use PostgreSQL's has_table_privilege function which is much faster
	// This checks SELECT permission without actually running a query

	// Check if we have permission to SELECT from the base table (if it exists)
	var hasPermission bool
	checkPermissionQuery := `
		SELECT has_table_privilege($1, 'SELECT')
		FROM pg_tables
		WHERE schemaname = 'public'
		AND tablename = $2
	`

	err := a.db.QueryRowContext(ctx, checkPermissionQuery, a.config.Table, a.config.Table).Scan(&hasPermission)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check table permissions: %w", err)
	}

	// If the base table exists and we don't have permission, fail
	if !errors.Is(err, sql.ErrNoRows) && !hasPermission {
		return fmt.Errorf("%w: %s", ErrInsufficientPermissions, a.config.Table)
	}

	// Check if we can see and access partition tables
	pattern := a.config.Table + "_%"
	partitionCheckQuery := `
		SELECT tablename 
		FROM pg_tables 
		WHERE schemaname = 'public' 
		AND tablename LIKE $1
		AND has_table_privilege(tablename, 'SELECT')
		LIMIT 1
	`

	var samplePartition string
	err = a.db.QueryRowContext(ctx, partitionCheckQuery, pattern).Scan(&samplePartition)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		// Only fail if it's not a "no rows" error
		return fmt.Errorf("failed to check partition table permissions: %w", err)
	}

	// Check if we found any partitions at all (with or without permissions)
	if errors.Is(err, sql.ErrNoRows) {
		// Let's see if partitions exist but we can't access them
		var partitionExists bool
		existsQuery := `
			SELECT EXISTS (
				SELECT 1 FROM pg_tables
				WHERE schemaname = 'public'
				AND tablename LIKE $1
			)
		`
		_ = a.db.QueryRowContext(ctx, existsQuery, pattern).Scan(&partitionExists)

		if partitionExists {
			// Partitions exist but we can't access them
			return ErrPartitionNoPermissions
		}
		// No partitions found yet, that's okay
	}

	return nil
}

// discoverPartitionsWithUI was used for UI-based discovery but is currently replaced by progress.go logic
// Keeping for potential future use
/*
func (a *Archiver) discoverPartitionsWithUI(program *tea.Program) ([]PartitionInfo, error) {
	query := `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public'
			AND tablename LIKE $1
		ORDER BY tablename;
	`

	pattern := a.config.Table + "_%"
	rows, err := a.db.Query(query, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// First, collect all matching table names
	var matchingTables []struct {
		name string
		date time.Time
	}

	var startDate, endDate time.Time
	if a.config.StartDate != "" {
		startDate, _ = time.Parse("2006-01-02", a.config.StartDate)
	}
	if a.config.EndDate != "" {
		endDate, _ = time.Parse("2006-01-02", a.config.EndDate)
	}

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}

		// Try to extract date from different partition naming formats
		date, ok := a.extractDateFromTableName(tableName)
		if !ok {
			if a.config.Debug {
				program.Send(addMessage(fmt.Sprintf("⏭️ Skipping %s: no valid date pattern", tableName)))
			}
			continue
		}

		if a.config.StartDate != "" && date.Before(startDate) {
			continue
		}
		if a.config.EndDate != "" && date.After(endDate) {
			continue
		}

		matchingTables = append(matchingTables, struct {
			name string
			date time.Time
		}{name: tableName, date: date})
	}

	if len(matchingTables) == 0 {
		return nil, nil
	}

	var partitions []PartitionInfo

	if a.config.SkipCount {
		// Skip counting for faster startup
		program.Send(addMessage("⏩ Skipping row counts (--skip-count enabled)"))
		for _, table := range matchingTables {
			partitions = append(partitions, PartitionInfo{
				TableName: table.name,
				Date:      table.date,
				RowCount:  -1, // Unknown count
			})
		}
	} else {
		// Count rows for each partition with progress feedback
		program.Send(changePhase(PhaseCounting, fmt.Sprintf("Counting rows in %d partitions...", len(matchingTables))))
		program.Send(addMessage(fmt.Sprintf("📊 Counting rows in %d partitions...", len(matchingTables))))

		for i, table := range matchingTables {
			// Update count progress
			program.Send(updateCount(i+1, len(matchingTables), table.name))

			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table.name))
			var count int64
			if err := a.db.QueryRow(countQuery).Scan(&count); err == nil {
				partitions = append(partitions, PartitionInfo{
					TableName: table.name,
					Date:      table.date,
					RowCount:  count,
				})
			} else if a.config.Debug {
				program.Send(addMessage(fmt.Sprintf("⚠️ Failed to count %s: %v", table.name, err)))
			}
		}

		program.Send(addMessage(fmt.Sprintf("✅ Counted rows in %d partitions", len(partitions))))
	}

	return partitions, nil
}
*/

// discoverPartitions was used for non-UI discovery but is currently replaced by progress.go logic
// Keeping for potential future use
/*
func (a *Archiver) discoverPartitions() ([]PartitionInfo, error) {
	query := `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public'
			AND tablename LIKE $1
		ORDER BY tablename;
	`

	pattern := a.config.Table + "_%"
	rows, err := a.db.Query(query, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// First, collect all matching table names
	var matchingTables []struct {
		name string
		date time.Time
	}

	var startDate, endDate time.Time
	if a.config.StartDate != "" {
		startDate, _ = time.Parse("2006-01-02", a.config.StartDate)
	}
	if a.config.EndDate != "" {
		endDate, _ = time.Parse("2006-01-02", a.config.EndDate)
	}

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}

		// Try to extract date from different partition naming formats
		date, ok := a.extractDateFromTableName(tableName)
		if !ok {
			if a.config.Debug {
				fmt.Println(debugStyle.Render(fmt.Sprintf("⏭️  Skipping %s: no valid date pattern found", tableName)))
			}
			continue
		}

		if a.config.StartDate != "" && date.Before(startDate) {
			continue
		}
		if a.config.EndDate != "" && date.After(endDate) {
			continue
		}

		matchingTables = append(matchingTables, struct {
			name string
			date time.Time
		}{name: tableName, date: date})
	}

	if len(matchingTables) == 0 {
		return nil, nil
	}

	var partitions []PartitionInfo

	if a.config.SkipCount {
		// Skip counting for faster startup
		fmt.Println(warningStyle.Render("⏩ Skipping row counts (--skip-count enabled)"))
		for _, table := range matchingTables {
			partitions = append(partitions, PartitionInfo{
				TableName: table.name,
				Date:      table.date,
				RowCount:  -1, // Unknown count
			})
		}
	} else {
		// Count rows for each partition with progress feedback
		fmt.Println(infoStyle.Render(fmt.Sprintf("📊 Counting rows in %d partitions...", len(matchingTables))))

		for i, table := range matchingTables {
			// Show progress spinner
			fmt.Printf("\r%s Counting rows: %d/%d - %s",
				infoStyle.Render("⏳"),
				i+1,
				len(matchingTables),
				table.name)

			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table.name))
			var count int64
			if err := a.db.QueryRow(countQuery).Scan(&count); err == nil {
				partitions = append(partitions, PartitionInfo{
					TableName: table.name,
					Date:      table.date,
					RowCount:  count,
				})
			} else if a.config.Debug {
				fmt.Println(debugStyle.Render(fmt.Sprintf("\n⚠️  Failed to count rows in %s: %v", table.name, err)))
			}
		}

		// Clear the progress line
		fmt.Printf("\r%s\r", strings.Repeat(" ", 80))
		fmt.Println(successStyle.Render(fmt.Sprintf("✅ Counted rows in %d partitions", len(partitions))))
	}

	return partitions, nil
}
*/

func (a *Archiver) extractDateFromTableName(tableName string) (time.Time, bool) {
	baseTableLen := len(a.config.Table)
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

	return time.Time{}, false
}

// processPartitionsWithProgress was used for UI-based processing but is currently replaced by progress.go logic
// Keeping for potential future use
/*
func (a *Archiver) processPartitionsWithProgress(partitions []PartitionInfo, program *tea.Program) []ProcessResult {
	results := make([]ProcessResult, len(partitions))

	// Process partitions sequentially for better progress tracking
	for i, partition := range partitions {
		result := a.ProcessPartitionWithProgress(partition, program)
		results[i] = result

		// Send completion update
		program.Send(completePartition(i, result))

		// Small delay to allow UI to update
		time.Sleep(10 * time.Millisecond)
	}

	return results
}
*/

func (a *Archiver) ProcessPartitionWithProgress(partition PartitionInfo, program *tea.Program) ProcessResult {
	// Check if we need to split this partition based on output_duration and date_column
	if a.shouldSplitPartition(partition) {
		if a.config.DateColumn == "" {
			// Need date column for splitting
			a.logger.Warn(fmt.Sprintf("⚠️  Partition %s should be split into %s files but --date-column not specified. Processing as single file.",
				partition.TableName, a.config.OutputDuration))
		} else {
			return a.processPartitionWithSplit(partition, program)
		}
	}

	// Process as a single file (original behavior)
	return a.processSinglePartition(partition, program, partition.Date)
}

// shouldSplitPartition determines if a partition needs to be split based on output_duration
func (a *Archiver) shouldSplitPartition(partition PartitionInfo) bool {
	// Determine partition period from table name
	// For now, we'll check if it's a monthly partition and output is smaller
	// Monthly partitions end with _YYYY_MM or _YYYYMM
	tableName := partition.TableName
	baseTable := a.config.Table + "_"

	if !strings.HasPrefix(tableName, baseTable) {
		return false
	}

	suffix := strings.TrimPrefix(tableName, baseTable)

	// Check for monthly partition patterns
	isMonthly := len(suffix) == 7 && suffix[4] == '_' // YYYY_MM
	if !isMonthly {
		isMonthly = len(suffix) == 6 // YYYYMM
	}

	// If it's monthly and output duration is smaller, split
	if isMonthly {
		switch a.config.OutputDuration {
		case DurationDaily, DurationWeekly, DurationHourly:
			return true
		}
	}

	// Add more cases as needed (daily -> hourly, etc.)

	return false
}

// processPartitionWithSplit splits a partition into multiple output files based on date_column
//
//nolint:gocognit // complex partition splitting logic
func (a *Archiver) processPartitionWithSplit(partition PartitionInfo, program *tea.Program) ProcessResult {
	result := ProcessResult{
		Partition: partition,
	}

	a.logger.Debug(fmt.Sprintf("Splitting partition %s by %s", partition.TableName, a.config.OutputDuration))

	// Determine partition time range (start of month, end of month)
	partitionStart := time.Date(partition.Date.Year(), partition.Date.Month(), 1, 0, 0, 0, 0, partition.Date.Location())
	partitionEnd := partitionStart.AddDate(0, 1, 0) // Start of next month

	a.logger.Debug(fmt.Sprintf("  Partition time range: %s to %s", partitionStart.Format("2006-01-02"), partitionEnd.Format("2006-01-02")))

	// Split into time ranges based on output duration
	ranges := SplitPartitionByDuration(partitionStart, partitionEnd, a.config.OutputDuration)

	// Only log in debug mode - in TUI mode this corrupts the display
	if a.config.Debug {
		a.logger.Info(fmt.Sprintf("  Splitting into %d %s files", len(ranges), a.config.OutputDuration))
	}

	// Log first few ranges for debugging
	if a.config.Debug && len(ranges) > 0 {
		for i := 0; i < len(ranges) && i < 3; i++ {
			a.logger.Debug(fmt.Sprintf("    Range %d: %s to %s", i+1, ranges[i].Start.Format("2006-01-02"), ranges[i].End.Format("2006-01-02")))
		}
		if len(ranges) > 3 {
			a.logger.Debug(fmt.Sprintf("    ... and %d more ranges", len(ranges)-3))
		}
	}

	// Process each time range separately
	totalBytes := int64(0)
	successCount := 0
	skipCount := 0
	for i, timeRange := range ranges {
		// Check for cancellation
		select {
		case <-a.ctx.Done():
			result.Error = a.ctx.Err()
			result.Stage = StageCancelled
			return result
		default:
		}

		// Send slice start message to TUI
		if program != nil {
			program.Send(sliceStartMsg{
				partitionIndex: 0, // Will be set by TUI based on current partition
				sliceIndex:     i,
				totalSlices:    len(ranges),
				sliceDate:      timeRange.Start.Format("2006-01-02"),
			})
		} else if a.config.Debug {
			// Only log in debug mode when TUI is disabled
			a.logger.Info(fmt.Sprintf("    Processing slice: %s", timeRange.Start.Format("2006-01-02")))
		}

		// Process this time slice
		sliceResult := a.processSinglePartitionSlice(partition, program, timeRange.Start, timeRange.End)

		// Send slice complete message to TUI
		if program != nil {
			program.Send(sliceCompleteMsg{
				partitionIndex: 0,
				sliceIndex:     i,
				success:        sliceResult.Error == nil && !sliceResult.Skipped,
				result:         sliceResult,
				sliceDate:      timeRange.Start.Format("2006-01-02"),
			})
		}

		if sliceResult.Error != nil {
			// Only log errors in debug mode
			if a.config.Debug {
				a.logger.Error(fmt.Sprintf("      ❌ Error processing slice %s: %v", timeRange.Start.Format("2006-01-02"), sliceResult.Error))
			}
			// Only return if it's a critical error (not "no rows")
			if !sliceResult.Skipped {
				return sliceResult
			}
		}

		if sliceResult.Skipped {
			skipCount++
			if a.config.Debug {
				a.logger.Debug(fmt.Sprintf("      No data for %s, skipping", timeRange.Start.Format("2006-01-02")))
			}
		} else {
			totalBytes += sliceResult.BytesWritten
			successCount++
		}
	}

	// Only log in debug mode - in TUI mode this corrupts the display
	if a.config.Debug {
		a.logger.Info(fmt.Sprintf("  Split partition complete: %d files created, %d skipped", successCount, skipCount))
	}

	// Handle case where all slices were skipped
	if successCount > 0 {
		result.BytesWritten = totalBytes
		result.Compressed = true
		result.Uploaded = true
	} else {
		result.BytesWritten = 0
		result.Compressed = false
		result.Uploaded = false
		result.Skipped = true
		result.SkipReason = "All slices skipped (no data in time ranges)"
	}

	return result
}

// processSinglePartition processes a partition as a single output file
func (a *Archiver) processSinglePartition(partition PartitionInfo, program *tea.Program, outputDate time.Time) ProcessResult {
	startTime := time.Now()
	result := ProcessResult{
		Partition: partition,
		StartTime: startTime,
	}

	// Helper function to update task info directly when stages change
	updateTaskStage := func(stage string) {
		// Try to read current task info and update it
		if taskInfo, err := ReadTaskInfo(); err == nil {
			taskInfo.CurrentStep = stage
			taskInfo.CurrentPartition = partition.TableName
			_ = WriteTaskInfo(taskInfo)
		}
		// Also send to program if available
		if program != nil {
			program.Send(updateProgress(stage, 0, 0))
		}
	}

	// Generate object key using path template (use outputDate for path)
	pathTemplate := NewPathTemplate(a.config.S3.PathTemplate)
	basePath := pathTemplate.Generate(a.config.Table, outputDate)

	// Get formatter with compression support
	formatter := formatters.GetFormatterWithCompression(a.config.OutputFormat, a.config.Compression)

	// For formats with internal compression (like Parquet), skip external compression
	var compressionExt string
	if formatters.UsesInternalCompression(a.config.OutputFormat) {
		// No compression extension for internally compressed formats
		compressionExt = ""
	} else {
		// Use configured compressor for other formats to get extension
		compressor, err := compressors.GetCompressor(a.config.Compression)
		if err != nil {
			result.Error = fmt.Errorf("failed to get compressor: %w", err)
			result.Stage = StageSetup
			result.Duration = time.Since(startTime)
			return result
		}
		compressionExt = compressor.Extension()
	}

	// Generate filename (use outputDate for filename)
	filename := GenerateFilename(
		a.config.Table,
		outputDate,
		a.config.OutputDuration,
		formatter.Extension(),
		compressionExt,
	)

	objectKey := basePath + "/" + filename

	// Small delay to ensure UI can update
	time.Sleep(50 * time.Millisecond)

	// Load cache
	cache, _ := loadPartitionCache(a.config.Table)

	// Check if we can skip based on cached metadata
	if shouldSkip, skipResult := a.checkCachedMetadata(partition, objectKey, cache, updateTaskStage); shouldSkip {
		skipResult.Duration = time.Since(startTime)
		return skipResult
	}

	// Check for cancellation before extracting
	select {
	case <-a.ctx.Done():
		result.Error = a.ctx.Err()
		result.Stage = StageCancelled
		result.Duration = time.Since(startTime)
		return result
	default:
	}

	// Extract data
	data, uncompressedSize, err := a.extractPartitionData(partition, program, cache, updateTaskStage)
	if err != nil {
		result.Error = err
		result.Stage = "Extracting"
		result.Duration = time.Since(startTime)
		return result
	}

	// Check for cancellation after extraction
	select {
	case <-a.ctx.Done():
		result.Error = a.ctx.Err()
		result.Stage = StageCancelled
		result.Duration = time.Since(startTime)
		return result
	default:
	}

	// Compress data
	compressed, localMD5, err := a.compressPartitionData(data, partition, program, cache, updateTaskStage)
	if err != nil {
		result.Error = err
		result.Stage = "Compressing"
		result.Duration = time.Since(startTime)
		return result
	}
	result.Compressed = true
	result.BytesWritten = int64(len(compressed))

	// Check if we can skip upload
	if shouldSkip, skipResult := a.checkExistingFile(partition, objectKey, compressed, localMD5, int64(len(compressed)), uncompressedSize, cache, updateTaskStage); shouldSkip {
		skipResult.Duration = time.Since(startTime)
		return skipResult
	}

	// Check for cancellation before upload
	select {
	case <-a.ctx.Done():
		result.Error = a.ctx.Err()
		result.Stage = StageCancelled
		result.Duration = time.Since(startTime)
		return result
	default:
	}

	// Upload to S3
	if !a.config.DryRun {
		updateTaskStage("Uploading to S3...")
		if program != nil {
			program.Send(updateProgress("Uploading to S3...", 0, 100))
		}
		result.Stage = "Uploading"
		if err := a.uploadToS3(objectKey, compressed); err != nil {
			result.Error = fmt.Errorf("upload failed: %w", err)
			result.Duration = time.Since(startTime)
			return result
		}
		result.Uploaded = true
		if program != nil {
			program.Send(updateProgress("Uploading to S3...", 100, 100))
		}

		// Save metadata to cache after successful upload
		cache.setFileMetadata(partition.TableName, objectKey, int64(len(compressed)), uncompressedSize, localMD5, true)
		_ = cache.save(a.config.Table)
		a.logger.Debug(fmt.Sprintf("  💾 Saved file metadata to cache: compressed=%d, uncompressed=%d, md5=%s", len(compressed), uncompressedSize, localMD5))
	}

	result.Stage = "Complete"
	result.S3Key = objectKey
	result.Duration = time.Since(startTime)
	return result
}

// processSinglePartitionSlice processes a time slice of a partition with date filtering
func (a *Archiver) processSinglePartitionSlice(partition PartitionInfo, _ *tea.Program, startTime, endTime time.Time) ProcessResult {
	// Slice progress is now handled by TUI messages or debug logging in parent function
	sliceStartTime := time.Now()
	result := ProcessResult{
		Partition: partition,
		StartTime: sliceStartTime,
	}

	// Generate object key using path template (use slice start time)
	pathTemplate := NewPathTemplate(a.config.S3.PathTemplate)
	basePath := pathTemplate.Generate(a.config.Table, startTime)

	// Get formatter with compression support
	formatter := formatters.GetFormatterWithCompression(a.config.OutputFormat, a.config.Compression)

	// For formats with internal compression (like Parquet), skip external compression
	var compressor compressors.Compressor
	var compressionExt string
	if formatters.UsesInternalCompression(a.config.OutputFormat) {
		// No compression extension for internally compressed formats
		var err error
		compressor, err = compressors.GetCompressor("none")
		if err != nil {
			result.Error = fmt.Errorf("failed to get compressor: %w", err)
			result.Stage = StageSetup
			result.Duration = time.Since(sliceStartTime)
			return result
		}
		compressionExt = ""
	} else {
		var err error
		compressor, err = compressors.GetCompressor(a.config.Compression)
		if err != nil {
			result.Error = fmt.Errorf("failed to get compressor: %w", err)
			result.Stage = StageSetup
			result.Duration = time.Since(sliceStartTime)
			return result
		}
		compressionExt = compressor.Extension()
	}

	// Generate filename (use slice start time)
	filename := GenerateFilename(
		a.config.Table,
		startTime,
		a.config.OutputDuration,
		formatter.Extension(),
		compressionExt,
	)

	objectKey := basePath + "/" + filename

	// Load cache
	cache, _ := loadPartitionCache(a.config.Table)

	// Helper function for stage updates (slices use debug logging only)
	updateTaskStage := func(stage string) {
		a.logger.Debug(fmt.Sprintf("      %s", stage))
	}

	// Check if we can skip based on cached metadata
	if shouldSkip, skipResult := a.checkCachedMetadata(partition, objectKey, cache, updateTaskStage); shouldSkip {
		skipResult.Duration = time.Since(sliceStartTime)
		return skipResult
	}

	// Check for cancellation
	select {
	case <-a.ctx.Done():
		result.Error = a.ctx.Err()
		result.Stage = StageCancelled
		result.Duration = time.Since(sliceStartTime)
		return result
	default:
	}

	// Extract data for this time range
	rows, err := a.extractRowsWithDateFilter(partition, startTime, endTime)
	if err != nil {
		result.Error = fmt.Errorf("failed to extract rows: %w", err)
		result.Stage = "Extracting"
		result.Duration = time.Since(sliceStartTime)
		return result
	}

	// If no rows in this slice, skip it
	if len(rows) == 0 {
		a.logger.Debug(fmt.Sprintf("      No data for %s, skipping", startTime.Format("2006-01-02")))
		result.Skipped = true
		result.SkipReason = "No data in time range"
		result.Duration = time.Since(sliceStartTime)
		return result
	}

	a.logger.Debug(fmt.Sprintf("      Extracted %d rows", len(rows)))

	// Format the data
	data, err := formatter.Format(rows)
	if err != nil {
		result.Error = fmt.Errorf("failed to format data: %w", err)
		result.Stage = "Formatting"
		result.Duration = time.Since(sliceStartTime)
		return result
	}

	uncompressedSize := int64(len(data))

	// Check for cancellation
	select {
	case <-a.ctx.Done():
		result.Error = a.ctx.Err()
		result.Stage = StageCancelled
		result.Duration = time.Since(sliceStartTime)
		return result
	default:
	}

	// Compress data
	compressed, err := compressor.Compress(data, a.config.CompressionLevel)
	if err != nil {
		result.Error = fmt.Errorf("compression failed: %w", err)
		result.Stage = "Compressing"
		result.Duration = time.Since(sliceStartTime)
		return result
	}
	result.Compressed = true
	result.BytesWritten = int64(len(compressed))

	// Calculate MD5
	localMD5 := fmt.Sprintf("%x", md5.Sum(compressed)) //nolint:gosec // MD5 used for checksums, not cryptography

	// Check if we can skip upload
	if shouldSkip, skipResult := a.checkExistingFile(partition, objectKey, compressed, localMD5, int64(len(compressed)), uncompressedSize, cache, updateTaskStage); shouldSkip {
		skipResult.Duration = time.Since(sliceStartTime)
		return skipResult
	}

	// Check for cancellation
	select {
	case <-a.ctx.Done():
		result.Error = a.ctx.Err()
		result.Stage = StageCancelled
		result.Duration = time.Since(sliceStartTime)
		return result
	default:
	}

	// Upload to S3
	if !a.config.DryRun {
		result.Stage = "Uploading"
		if err := a.uploadToS3(objectKey, compressed); err != nil {
			result.Error = fmt.Errorf("upload failed: %w", err)
			result.Duration = time.Since(sliceStartTime)
			return result
		}
		result.Uploaded = true

		// Save metadata to cache
		cache.setFileMetadata(partition.TableName, objectKey, int64(len(compressed)), uncompressedSize, localMD5, true)
		_ = cache.save(a.config.Table)

		// Only log in debug mode when TUI is disabled
		if a.config.Debug {
			a.logger.Info(fmt.Sprintf("      ✅ Uploaded %s (%d bytes)", filename, len(compressed)))
		}
	}

	result.Stage = "Complete"
	result.S3Key = objectKey
	result.Duration = time.Since(sliceStartTime)
	return result
}

// checkCachedMetadata checks if we can skip processing based on cached metadata
func (a *Archiver) checkCachedMetadata(partition PartitionInfo, objectKey string, cache *PartitionCache, updateTaskStage func(string)) (bool, ProcessResult) {
	result := ProcessResult{
		Partition: partition,
	}

	cachedSize, cachedMD5, hasCached := cache.getFileMetadata(partition.TableName, objectKey, partition.Date)
	if !hasCached {
		return false, result
	}

	// We have cached metadata, check if it matches what's in S3
	updateTaskStage("Checking cached file metadata...")

	exists, s3Size, s3ETag := a.checkObjectExists(objectKey)
	if !exists {
		a.logger.Debug("  💾 Have cached metadata but file doesn't exist in S3, will upload")
		return false, result
	}

	s3ETag = strings.Trim(s3ETag, "\"")
	isMultipart := strings.Contains(s3ETag, "-")

	a.logger.Debug(fmt.Sprintf("  💾 Using cached metadata for %s:", partition.TableName))
	a.logger.Debug(fmt.Sprintf("     Cached: size=%d, md5=%s", cachedSize, cachedMD5))
	a.logger.Debug(fmt.Sprintf("     S3: size=%d, etag=%s (multipart=%v)", s3Size, s3ETag, isMultipart))

	// Check if cached metadata matches S3
	if s3Size == cachedSize {
		if !isMultipart && s3ETag == cachedMD5 {
			// Cached metadata matches S3 - skip without extraction
			result.Skipped = true
			result.SkipReason = fmt.Sprintf("Cached metadata matches S3 (size=%d, md5=%s)", cachedSize, cachedMD5)
			result.Stage = StageSkipped
			result.BytesWritten = cachedSize
			a.logger.Debug("     ✅ Skipping based on cache: Size and MD5 match")
			return true, result
		} else if isMultipart {
			a.logger.Debug("     ℹ️  Multipart upload with matching size, proceeding with extraction to verify")
		}
	}

	return false, result
}

// extractPartitionData extracts data from the partition and formats it
func (a *Archiver) extractPartitionData(partition PartitionInfo, program *tea.Program, cache *PartitionCache, updateTaskStage func(string)) ([]byte, int64, error) {
	extractStart := time.Now()
	updateTaskStage("Extracting data...")
	if program != nil && partition.RowCount > 0 {
		program.Send(updateProgress("Extracting data...", 0, partition.RowCount))
	}

	// Extract rows as maps
	rows, err := a.extractRowsWithProgress(partition, program)
	if err != nil {
		// Save error to cache
		cache.setError(partition.TableName, fmt.Sprintf("Extraction failed: %v", err))
		_ = cache.save(a.config.Table)
		return nil, 0, fmt.Errorf("extraction failed: %w", err)
	}

	extractDuration := time.Since(extractStart)
	a.logger.Debug(fmt.Sprintf("  ⏱️  Extraction took %v for %s (%d rows)", extractDuration, partition.TableName, len(rows)))

	// Format the data using configured formatter
	updateTaskStage("Formatting data...")
	formatter := formatters.GetFormatter(a.config.OutputFormat)
	data, err := formatter.Format(rows)
	if err != nil {
		cache.setError(partition.TableName, fmt.Sprintf("Formatting failed: %v", err))
		_ = cache.save(a.config.Table)
		return nil, 0, fmt.Errorf("formatting failed: %w", err)
	}

	return data, int64(len(data)), nil
}

// compressPartitionData compresses the extracted data using configured compressor
func (a *Archiver) compressPartitionData(data []byte, partition PartitionInfo, program *tea.Program, cache *PartitionCache, updateTaskStage func(string)) ([]byte, string, error) {
	compressStart := time.Now()
	updateTaskStage("Compressing data...")
	if program != nil {
		program.Send(updateProgress("Compressing data...", 50, 100))
	}

	// Get compressor based on configuration
	compressor, err := compressors.GetCompressor(a.config.Compression)
	if err != nil {
		cache.setError(partition.TableName, fmt.Sprintf("Compressor setup failed: %v", err))
		_ = cache.save(a.config.Table)
		return nil, "", fmt.Errorf("compressor setup failed: %w", err)
	}

	// Apply compression
	compressed, err := compressor.Compress(data, a.config.CompressionLevel)
	if err != nil {
		// Save error to cache
		cache.setError(partition.TableName, fmt.Sprintf("Compression failed: %v", err))
		_ = cache.save(a.config.Table)
		return nil, "", fmt.Errorf("compression failed: %w", err)
	}

	if program != nil {
		program.Send(updateProgress("Compressing data...", 100, 100))
	}

	compressDuration := time.Since(compressStart)
	if len(compressed) < len(data) {
		a.logger.Debug(fmt.Sprintf("  ⏱️  Compression took %v for %s (%.1fx ratio)",
			compressDuration, partition.TableName, float64(len(data))/float64(len(compressed))))
	} else {
		a.logger.Debug(fmt.Sprintf("  ⏱️  Compression took %v for %s (no compression applied)",
			compressDuration, partition.TableName))
	}

	// Calculate MD5 hash of compressed data
	hasher := md5.New() //nolint:gosec // MD5 used for checksums, not cryptography
	hasher.Write(compressed)
	localMD5 := hex.EncodeToString(hasher.Sum(nil))

	return compressed, localMD5, nil
}

// checkExistingFile checks if file exists in S3 and matches local version
func (a *Archiver) checkExistingFile(partition PartitionInfo, objectKey string, compressed []byte, localMD5 string, localSize, uncompressedSize int64, cache *PartitionCache, updateTaskStage func(string)) (bool, ProcessResult) {
	result := ProcessResult{
		Partition:    partition,
		Compressed:   true,
		BytesWritten: localSize,
	}

	updateTaskStage("Checking if file exists...")
	exists, s3Size, s3ETag := a.checkObjectExists(objectKey)
	if !exists {
		a.logger.Debug(fmt.Sprintf("  📊 File does not exist in S3: %s", objectKey))
		return false, result
	}

	// Remove quotes from ETag if present
	s3ETag = strings.Trim(s3ETag, "\"")
	isMultipart := strings.Contains(s3ETag, "-")

	// Always log comparison details in debug mode
	a.logger.Debug(fmt.Sprintf("  📊 Comparing files for %s:", partition.TableName))
	a.logger.Debug(fmt.Sprintf("     S3: size=%d, etag=%s (multipart=%v)", s3Size, s3ETag, isMultipart))
	a.logger.Debug(fmt.Sprintf("     Local: size=%d, md5=%s", localSize, localMD5))

	if s3Size != localSize {
		a.logger.Debug(fmt.Sprintf("     ❌ Size mismatch: S3=%d, Local=%d", s3Size, localSize))
		return false, result
	}

	// Size matches, check hash
	if !isMultipart && s3ETag == localMD5 {
		// Single-part upload with matching MD5
		result.Skipped = true
		result.SkipReason = fmt.Sprintf("Already exists with matching size (%d bytes) and MD5 (%s)", s3Size, s3ETag)
		result.Stage = StageSkipped
		a.logger.Debug("     ✅ Skipping: Size and MD5 match")
		// Save to cache for future runs
		cache.setFileMetadata(partition.TableName, objectKey, localSize, uncompressedSize, localMD5, true)
		_ = cache.save(a.config.Table)
		return true, result
	}

	if isMultipart {
		// For multipart uploads, calculate the multipart ETag
		localMultipartETag := a.calculateMultipartETag(compressed)
		a.logger.Debug(fmt.Sprintf("     Local multipart ETag: %s", localMultipartETag))
		if s3ETag == localMultipartETag {
			result.Skipped = true
			result.SkipReason = fmt.Sprintf("Already exists with matching size (%d bytes) and multipart ETag (%s)", s3Size, s3ETag)
			result.Stage = StageSkipped
			a.logger.Debug("     ✅ Skipping: Size and multipart ETag match")
			// Save to cache for future runs
			cache.setFileMetadata(partition.TableName, objectKey, localSize, uncompressedSize, localMD5, true)
			_ = cache.save(a.config.Table)
			return true, result
		}
		a.logger.Debug(fmt.Sprintf("     ❌ Multipart ETag mismatch: S3=%s, Local=%s", s3ETag, localMultipartETag))
	} else {
		a.logger.Debug(fmt.Sprintf("     ❌ MD5 mismatch: S3=%s, Local=%s", s3ETag, localMD5))
	}

	// Size or hash doesn't match, we'll re-upload
	a.logger.Debug("     🔄 Will re-upload due to differences")

	return false, result
}

func (a *Archiver) checkObjectExists(key string) (bool, int64, string) {
	// Check if S3 client is initialized
	if a.s3Client == nil {
		a.logger.Error("S3 client not initialized")
		return false, 0, ""
	}

	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(a.config.S3.Bucket),
		Key:    aws.String(key),
	}

	result, err := a.s3Client.HeadObject(headInput)
	if err != nil {
		return false, 0, ""
	}

	var size int64
	var etag string

	if result.ContentLength != nil {
		size = *result.ContentLength
	}

	if result.ETag != nil {
		etag = *result.ETag
	}

	return true, size, etag
}

// calculateMultipartETag calculates the ETag for a multipart upload
// This matches S3's algorithm for multipart uploads
// Uses 5MB part size to match s3manager.Uploader default
func (a *Archiver) calculateMultipartETag(data []byte) string {
	const partSize = 5 * 1024 * 1024 // 5MB part size (s3manager default)

	// Calculate number of parts
	numParts := (len(data) + partSize - 1) / partSize

	// If it would be a single part, just return regular MD5
	if numParts == 1 {
		hasher := md5.New() //nolint:gosec // MD5 used for checksums, not cryptography
		hasher.Write(data)
		return hex.EncodeToString(hasher.Sum(nil))
	}

	// Calculate MD5 of each part and concatenate
	var partMD5s []byte
	for i := 0; i < numParts; i++ {
		start := i * partSize
		end := start + partSize
		if end > len(data) {
			end = len(data)
		}

		partHasher := md5.New() //nolint:gosec // MD5 used for checksums, not cryptography
		partHasher.Write(data[start:end])
		partMD5s = append(partMD5s, partHasher.Sum(nil)...)
	}

	// Calculate MD5 of concatenated MD5s
	finalHasher := md5.New() //nolint:gosec // MD5 used for checksums, not cryptography
	finalHasher.Write(partMD5s)
	finalMD5 := hex.EncodeToString(finalHasher.Sum(nil))

	// Return in S3 multipart format: MD5-numParts
	return fmt.Sprintf("%s-%d", finalMD5, numParts)
}

func (a *Archiver) uploadToS3(key string, data []byte) error {
	a.logger.Debug(fmt.Sprintf("  ☁️  Uploading to s3://%s/%s (size: %d bytes)",
		a.config.S3.Bucket, key, len(data)))

	// Use multipart upload for files larger than 100MB
	if len(data) > 100*1024*1024 {
		// Check if S3 uploader is initialized
		if a.s3Uploader == nil {
			return ErrS3UploaderNotInitialized
		}

		// Use S3 manager for automatic multipart upload handling
		uploadInput := &s3manager.UploadInput{
			Bucket:      aws.String(a.config.S3.Bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String("application/zstd"),
		}

		_, err := a.s3Uploader.Upload(uploadInput)
		return err
	}

	// Check if S3 client is initialized
	if a.s3Client == nil {
		return ErrS3ClientNotInitialized
	}

	// Use simple PutObject for smaller files
	putInput := &s3.PutObjectInput{
		Bucket:      aws.String(a.config.S3.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/zstd"),
	}

	_, err := a.s3Client.PutObject(putInput)
	return err
}

func (a *Archiver) printSummary(results []ProcessResult) {
	var successful, failed, skipped int
	var totalBytes int64

	for _, r := range results {
		if r.Error != nil {
			failed++
		} else if r.Skipped {
			skipped++
		} else {
			successful++
			totalBytes += r.BytesWritten
		}
	}

	a.logger.Info("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	a.logger.Info("📈 Summary")
	a.logger.Info(fmt.Sprintf("✅ Successful: %d", successful))
	a.logger.Info(fmt.Sprintf("⏭️  Skipped: %d", skipped))
	if failed > 0 {
		a.logger.Info(fmt.Sprintf("❌ Failed: %d", failed))
	}

	if totalBytes > 0 {
		a.logger.Info(fmt.Sprintf("💾 Total compressed: %.2f MB", float64(totalBytes)/(1024*1024)))
	}

	for _, r := range results {
		if r.Error != nil {
			a.logger.Error(fmt.Sprintf("\n❌ %s: %v",
				r.Partition.TableName,
				r.Error))
		}
	}
}

// extractRowsWithProgress extracts rows from partition as maps for formatting
//
//nolint:gocognit // complex row extraction with progress tracking
func (a *Archiver) extractRowsWithProgress(partition PartitionInfo, program *tea.Program) ([]map[string]interface{}, error) {
	quotedTable := pq.QuoteIdentifier(partition.TableName)
	query := fmt.Sprintf("SELECT row_to_json(t) FROM %s t", quotedTable) //nolint:gosec // Table name is quoted with pq.QuoteIdentifier

	rows, err := a.db.QueryContext(a.ctx, query)
	if err != nil {
		// Check if error is due to cancellation or closed connection
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isConnectionError(err) {
			a.logger.Debug("  ⚠️  Query cancelled or connection closed")
			return nil, context.Canceled
		}
		return nil, err
	}
	defer rows.Close()

	// Check for cancellation immediately after query starts
	select {
	case <-a.ctx.Done():
		a.logger.Debug("  ⚠️  Cancellation detected after query started")
		return nil, a.ctx.Err()
	default:
	}

	var result []map[string]interface{}
	rowCount := int64(0)
	var updateInterval int64 = 1000 // Default to every 1000 rows

	if partition.RowCount > 0 {
		updateInterval = partition.RowCount / 100 // Update every 1%
		if updateInterval < 1000 {
			updateInterval = 1000
		}
		// Pre-allocate slice if we know the count
		result = make([]map[string]interface{}, 0, partition.RowCount)
	}

	for rows.Next() {
		// Check for cancellation more frequently for better responsiveness
		if rowCount%100 == 0 {
			select {
			case <-a.ctx.Done():
				a.logger.Debug("  ⚠️  Cancellation detected during row extraction")
				return nil, a.ctx.Err()
			default:
			}
		}

		var jsonData json.RawMessage
		if err := rows.Scan(&jsonData); err != nil {
			return nil, err
		}

		// Unmarshal JSON into a map
		var rowData map[string]interface{}
		if err := json.Unmarshal(jsonData, &rowData); err != nil {
			return nil, err
		}

		result = append(result, rowData)
		rowCount++

		// Send progress updates
		if program != nil {
			if partition.RowCount > 0 {
				if rowCount%updateInterval == 0 || rowCount == partition.RowCount {
					program.Send(updateProgress("Extracting data...", rowCount, partition.RowCount))
				}
			} else if rowCount%10000 == 0 {
				// For unknown counts, just show the current count
				program.Send(updateProgress(fmt.Sprintf("Extracting data (%d rows so far)...", rowCount), 0, 0))
			}
		}
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Final update
	if program != nil {
		if partition.RowCount > 0 {
			program.Send(updateProgress("Extraction complete", partition.RowCount, partition.RowCount))
		} else {
			program.Send(updateProgress(fmt.Sprintf("Extraction complete (%d rows)", rowCount), 0, 0))
		}
	}

	// Save the actual row count to cache if it was unknown
	if partition.RowCount <= 0 && rowCount > 0 {
		cache, _ := loadPartitionCache(a.config.Table)
		if cache != nil {
			cache.setRowCount(partition.TableName, rowCount)
			_ = cache.save(a.config.Table)
		}
	}

	return result, nil
}

// extractRowsWithDateFilter extracts rows from partition with date range filtering
func (a *Archiver) extractRowsWithDateFilter(partition PartitionInfo, startTime, endTime time.Time) ([]map[string]interface{}, error) {
	quotedTable := pq.QuoteIdentifier(partition.TableName)
	quotedDateColumn := pq.QuoteIdentifier(a.config.DateColumn)

	// Build query with date range filter
	//nolint:gosec // Table and column names are quoted with pq.QuoteIdentifier
	query := fmt.Sprintf(
		"SELECT row_to_json(t) FROM %s t WHERE %s >= $1 AND %s < $2",
		quotedTable,
		quotedDateColumn,
		quotedDateColumn,
	)

	rows, err := a.db.QueryContext(a.ctx, query, startTime, endTime)
	if err != nil {
		// Check if error is due to cancellation or closed connection
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isConnectionError(err) {
			a.logger.Debug("  ⚠️  Filtered query cancelled or connection closed")
			return nil, context.Canceled
		}
		return nil, err
	}
	defer rows.Close()

	// Check for cancellation immediately after query starts
	select {
	case <-a.ctx.Done():
		a.logger.Debug("  ⚠️  Cancellation detected after filtered query started")
		return nil, a.ctx.Err()
	default:
	}

	var result []map[string]interface{}

	for rows.Next() {
		// Check for cancellation more frequently for better responsiveness
		if len(result)%100 == 0 {
			select {
			case <-a.ctx.Done():
				a.logger.Debug("  ⚠️  Cancellation detected during filtered row extraction")
				return nil, a.ctx.Err()
			default:
			}
		}

		var jsonData json.RawMessage
		if err := rows.Scan(&jsonData); err != nil {
			return nil, err
		}

		// Unmarshal JSON into a map
		var rowData map[string]interface{}
		if err := json.Unmarshal(jsonData, &rowData); err != nil {
			return nil, err
		}

		result = append(result, rowData)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
