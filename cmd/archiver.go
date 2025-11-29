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
	"hash"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/airframesio/data-archiver/cmd/compressors"
	"github.com/airframesio/data-archiver/cmd/formatters"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
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
	ErrInsufficientPermissions         = errors.New("insufficient permissions to read table")
	ErrPartitionNoPermissions          = errors.New("partition tables exist but you don't have SELECT permissions")
	ErrS3ClientNotInitialized          = errors.New("S3 client not initialized")
	ErrS3UploaderNotInitialized        = errors.New("S3 uploader not initialized")
	errPartitionlessDateColumnRequired = errors.New("partitionless tables require --date-column")
	errPartitionlessDateRangeRequired  = errors.New("partitionless tables require both --start-date and --end-date")
)

// Terminal styling for fmt.Println messages (not logger output)
// These are used in fmt.Println calls within this file for direct terminal output
var (
	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575")).
			Bold(true)

	warningStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFB700"))

	debugStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#666666")).
			Italic(true)
)

// init ensures style variables are always referenced to satisfy linters
func init() {
	_ = successStyle
	_ = warningStyle
	_ = debugStyle
}

// isConnectionError checks if an error is due to a closed or broken database connection
func isConnectionError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "bad connection") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "sql: database is closed")
}

// isRetryableError determines if an error should be retried
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Context deadline exceeded is retryable
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Connection errors are retryable
	if isConnectionError(err) {
		return true
	}

	// PostgreSQL specific retryable errors
	errStr := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"timeout",
		"canceling statement due to statement timeout",
		"deadline exceeded",
		"connection reset",
		"broken pipe",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// getTempDir returns the directory to use for temporary files
// Currently uses os.TempDir(). Could be made configurable via Config.TempDir if needed.
func getTempDir() string {
	return os.TempDir()
}

// createTempFile creates a temporary file with the archiver prefix
// The caller is responsible for closing and removing the file
func createTempFile() (*os.File, error) {
	return os.CreateTemp(getTempDir(), "data-archiver-*.tmp")
}

// cleanupTempFile removes a temporary file, ignoring errors if it doesn't exist
func cleanupTempFile(path string) {
	if path != "" {
		_ = os.Remove(path) // Ignore error - file might already be removed
	}
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
	TableName  string
	Date       time.Time
	RowCount   int64
	RangeStart time.Time
	RangeEnd   time.Time
}

func (p PartitionInfo) HasCustomRange() bool {
	return !p.RangeStart.IsZero() && !p.RangeEnd.IsZero()
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
	if (config.CacheScope == CacheScope{}) {
		config.CacheScope = NewCacheScope("archive", config)
	}
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
		// Use TaskInfo start time for TUI mode
		startTime := taskInfo.StartTime
		totalPartitions := len(results)
		// Try to get partition count from results if available
		if len(results) > 0 {
			// Estimate from results - this is approximate for TUI mode
			totalPartitions = len(results)
		}
		a.printSummary(results, startTime, totalPartitions)
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
	var partitions []PartitionInfo
	seenTables := make(map[string]bool) // Track tables to avoid duplicates

	// Helper function to process tables from a query result
	processTableRows := func(rows *sql.Rows, sourceType string) error {
		defer rows.Close()
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
				return fmt.Errorf("failed to scan %s name: %w", sourceType, err)
			}

			// Skip if we've already seen this table
			if seenTables[tableName] {
				continue
			}
			seenTables[tableName] = true

			date, ok := a.extractDateFromTableName(tableName)
			if !ok {
				a.logger.Debug(fmt.Sprintf("Skipping table %s (no valid date)", tableName))
				continue
			}

			// Validate that the table actually has columns before adding it
			// This prevents errors later when trying to process tables that exist
			// but have no columns or aren't valid tables
			schema, schemaErr := a.getTableSchema(ctx, tableName)
			if schemaErr != nil {
				a.logger.Debug(fmt.Sprintf("Skipping table %s (schema validation failed: %v)", tableName, schemaErr))
				continue
			}
			if schema == nil || len(schema.Columns) == 0 {
				a.logger.Debug(fmt.Sprintf("Skipping table %s (no columns found)", tableName))
				continue
			}

			// Check if we have SELECT permission on the table
			var hasPermission bool
			checkPermissionQuery := `SELECT has_table_privilege('public.' || $1, 'SELECT')`
			if err := a.db.QueryRowContext(ctx, checkPermissionQuery, tableName).Scan(&hasPermission); err != nil {
				a.logger.Debug(fmt.Sprintf("Skipping table %s (permission check failed: %v)", tableName, err))
				continue
			}
			if !hasPermission {
				a.logger.Debug(fmt.Sprintf("Skipping table %s (no SELECT permission)", tableName))
				continue
			}

			partitions = append(partitions, PartitionInfo{
				TableName: tableName,
				Date:      date,
			})
		}
		return rows.Err()
	}

	// Query actual partitions
	rows, err := a.db.QueryContext(ctx, leafPartitionListSQL, defaultTableSchema, a.config.Table)
	if err != nil {
		// Check if error is due to cancellation or closed connection
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isConnectionError(err) {
			a.logger.Info("⚠️  Query cancelled or connection closed")
			return context.Canceled
		}
		return fmt.Errorf("failed to query partitions: %w", err)
	}
	if err := processTableRows(rows, "partition"); err != nil {
		return fmt.Errorf("error iterating over partition rows: %w", err)
	}

	// If enabled, also query non-partition tables matching the pattern
	if a.config.IncludeNonPartitionTables {
		a.logger.Debug("Including non-partition tables matching pattern...")
		nonPartitionRows, err := a.db.QueryContext(ctx, nonPartitionTableListSQL, defaultTableSchema, a.config.Table)
		if err != nil {
			// Check if error is due to cancellation or closed connection
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isConnectionError(err) {
				a.logger.Info("⚠️  Query cancelled or connection closed")
				return context.Canceled
			}
			return fmt.Errorf("failed to query non-partition tables: %w", err)
		}
		if err := processTableRows(nonPartitionRows, "non-partition table"); err != nil {
			return fmt.Errorf("error iterating over non-partition table rows: %w", err)
		}
	}

	if len(partitions) == 0 {
		fallbackPartitions, fallbackErr := a.buildDateRangePartition()
		if fallbackErr != nil {
			a.logger.Info("No partitions found to archive")
			return fmt.Errorf("partitionless fallback unavailable: %w", fallbackErr)
		}
		partitions = fallbackPartitions
		inclusiveEnd := partitions[0].RangeEnd.Add(-24 * time.Hour).Format("2006-01-02")
		a.logger.Info(fmt.Sprintf("ℹ️  Table %s is not partitioned; slicing %s → %s via %s windows using %s",
			a.config.Table,
			partitions[0].RangeStart.Format("2006-01-02"),
			inclusiveEnd,
			a.config.OutputDuration,
			a.config.DateColumn))
	}

	a.logger.Info(fmt.Sprintf("✅ Found %d partitions", len(partitions)))

	a.logger.Debug("Processing partitions...")
	results := make([]ProcessResult, 0, len(partitions))
	startTime := time.Now()

	// Ensure summary is printed even on cancellation
	defer func() {
		if len(results) > 0 {
			a.printSummary(results, startTime, len(partitions))
		}
	}()

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
			a.logger.Error(fmt.Sprintf("   ❌ Failed: %v", result.Error))
		} else if result.Skipped {
			a.logger.Info(fmt.Sprintf("   ⏭️  Skipped: %s", result.SkipReason))
		} else {
			a.logger.Info(fmt.Sprintf("   ✅ Success: %d bytes", result.BytesWritten))
		}
	}

	a.logger.Info("✅ All partitions processed")
	return nil
}

func (a *Archiver) connect(ctx context.Context) error {
	sslMode := a.config.Database.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	// Build connection string with optional statement timeout
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		a.config.Database.Host,
		a.config.Database.Port,
		a.config.Database.User,
		a.config.Database.Password,
		a.config.Database.Name,
		sslMode,
	)

	// Add statement timeout if configured (convert seconds to milliseconds for PostgreSQL)
	if a.config.Database.StatementTimeout > 0 {
		timeoutMs := a.config.Database.StatementTimeout * 1000
		connStr += fmt.Sprintf(" statement_timeout=%d", timeoutMs)
		a.logger.Debug(fmt.Sprintf("  📝 Configured statement timeout: %d seconds (%d ms)",
			a.config.Database.StatementTimeout, timeoutMs))
	}

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

// queryWithRetry executes a query with retry logic for transient failures
func (a *Archiver) queryWithRetry(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	maxRetries := a.config.Database.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3 // Default to 3 retries
	}

	retryDelay := time.Duration(a.config.Database.RetryDelay) * time.Second
	if retryDelay <= 0 {
		retryDelay = 5 * time.Second // Default to 5 seconds
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		rows, err := a.db.QueryContext(ctx, query, args...)
		if err == nil {
			return rows, nil
		}

		lastErr = err

		// Check if error is retryable
		if !isRetryableError(err) {
			return nil, err
		}

		// Don't retry on the last attempt
		if attempt < maxRetries {
			a.logger.Warn(fmt.Sprintf("  ⚠️  Query failed (attempt %d/%d): %v. Retrying in %v...",
				attempt+1, maxRetries+1, err, retryDelay))

			// Wait before retrying, respecting context cancellation
			select {
			case <-time.After(retryDelay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return nil, fmt.Errorf("query failed after %d attempts: %w", maxRetries+1, lastErr)
}

func (a *Archiver) checkTablePermissions(ctx context.Context) error {
	// Use PostgreSQL's has_table_privilege function which is much faster
	// This checks SELECT permission without actually running a query

	// Check if we have permission to SELECT from the base table (if it exists)
	// First check if the table exists
	var tableExists bool
	existsQuery := `
		SELECT EXISTS (
			SELECT 1 FROM pg_tables
			WHERE schemaname = 'public'
			AND tablename = $1
		)
	`
	err := a.db.QueryRowContext(ctx, existsQuery, a.config.Table).Scan(&tableExists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	// If table exists, check permissions
	if tableExists {
		var hasPermission bool
		checkPermissionQuery := `SELECT has_table_privilege('public.' || $1, 'SELECT')`
		err = a.db.QueryRowContext(ctx, checkPermissionQuery, a.config.Table).Scan(&hasPermission)
		if err != nil {
			return fmt.Errorf("failed to check table permissions: %w", err)
		}

		if !hasPermission {
			return fmt.Errorf("%w: %s", ErrInsufficientPermissions, a.config.Table)
		}
	}

	// Check if we can see and access partition tables
	var samplePartition string
	err = a.db.QueryRowContext(ctx, leafPartitionPermissionSQL, defaultTableSchema, a.config.Table).Scan(&samplePartition)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		// Only fail if it's not a "no rows" error
		return fmt.Errorf("failed to check partition table permissions: %w", err)
	}

	// Check if we found any partitions at all (with or without permissions)
	if errors.Is(err, sql.ErrNoRows) {
		// Let's see if partitions exist but we can't access them
		var partitionExists bool
		if err := a.db.QueryRowContext(ctx, leafPartitionExistsSQL, defaultTableSchema, a.config.Table).Scan(&partitionExists); err != nil {
			return fmt.Errorf("failed to check for partition existence: %w", err)
		}

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

func (a *Archiver) buildDateRangePartition() ([]PartitionInfo, error) {
	if a.config.Table == "" {
		return nil, ErrTableNameRequired
	}
	if a.config.DateColumn == "" {
		return nil, errPartitionlessDateColumnRequired
	}
	if a.config.StartDate == "" || a.config.EndDate == "" {
		return nil, errPartitionlessDateRangeRequired
	}

	start, err := time.Parse("2006-01-02", a.config.StartDate)
	if err != nil {
		return nil, fmt.Errorf("invalid start date %s: %w", a.config.StartDate, err)
	}
	end, err := time.Parse("2006-01-02", a.config.EndDate)
	if err != nil {
		return nil, fmt.Errorf("invalid end date %s: %w", a.config.EndDate, err)
	}
	if end.Before(start) {
		return nil, fmt.Errorf("end date %s cannot be before start date %s", a.config.EndDate, a.config.StartDate)
	}

	rangeStart := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)

	partition := PartitionInfo{
		TableName:  a.config.Table,
		Date:       rangeStart,
		RowCount:   -1,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
	}

	return []PartitionInfo{partition}, nil
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
	if partition.HasCustomRange() {
		return true
	}

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

	var partitionStart time.Time
	var partitionEnd time.Time
	if partition.HasCustomRange() {
		partitionStart = partition.RangeStart
		partitionEnd = partition.RangeEnd
	} else {
		partitionStart = time.Date(partition.Date.Year(), partition.Date.Month(), 1, 0, 0, 0, 0, partition.Date.Location())
		partitionEnd = partitionStart.AddDate(0, 1, 0) // Start of next month
	}

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
	failCount := 0
	var firstError error
	var failedSliceDates []string

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
			// Track errors but continue processing other slices
			failCount++
			if firstError == nil {
				firstError = sliceResult.Error
			}
			failedSliceDates = append(failedSliceDates, timeRange.Start.Format("2006-01-02"))
			// Log errors in debug mode
			if a.config.Debug {
				a.logger.Error(fmt.Sprintf("      ❌ Error processing slice %s: %v", timeRange.Start.Format("2006-01-02"), sliceResult.Error))
			}
		} else if sliceResult.Skipped {
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
		if failCount > 0 {
			a.logger.Info(fmt.Sprintf("  Split partition complete: %d files created, %d skipped, %d failed", successCount, skipCount, failCount))
		} else {
			a.logger.Info(fmt.Sprintf("  Split partition complete: %d files created, %d skipped", successCount, skipCount))
		}
	}

	// Determine partition result based on slice outcomes
	// Count skipped slices as successful operations (they were processed successfully, just had no data)
	totalSuccessful := successCount + skipCount

	if totalSuccessful > 0 {
		// At least some slices succeeded or were skipped - mark partition as successful
		result.BytesWritten = totalBytes
		result.Compressed = successCount > 0 // Only true if we actually uploaded files
		result.Uploaded = successCount > 0   // Only true if we actually uploaded files

		// If some slices failed, log a warning but don't fail the partition
		if failCount > 0 {
			result.SkipReason = fmt.Sprintf("%d slice(s) failed: %s", failCount, strings.Join(failedSliceDates, ", "))
			if a.config.Debug {
				a.logger.Warn(fmt.Sprintf("  ⚠️  Partition %s completed with %d failed slice(s) out of %d total", partition.TableName, failCount, len(ranges)))
			}
		} else if skipCount > 0 && successCount == 0 {
			// All slices were skipped (no data)
			result.Skipped = true
			result.SkipReason = "All slices skipped (no data in time ranges)"
		}
	} else if failCount > 0 {
		// All slices failed - mark partition as failed
		result.Error = firstError
		result.BytesWritten = 0
		result.Compressed = false
		result.Uploaded = false
		result.Stage = "Failed"
	} else {
		// This shouldn't happen, but handle it gracefully
		result.BytesWritten = 0
		result.Compressed = false
		result.Uploaded = false
		result.Skipped = true
		result.SkipReason = "No slices processed"
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
	cache, _ := loadPartitionCache(a.config.CacheScope)

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

	// Extract data with streaming (includes compression and MD5 calculation)
	tempFilePath, fileSize, md5Hash, uncompressedSize, err := a.extractPartitionDataWithRetry(partition, program, cache, updateTaskStage)
	if err != nil {
		result.Error = err
		result.Stage = "Extracting"
		result.Duration = time.Since(startTime)
		return result
	}
	// Ensure temp file cleanup on error
	defer cleanupTempFile(tempFilePath)

	result.Compressed = true
	result.BytesWritten = fileSize

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
		if err := a.uploadTempFileToS3(tempFilePath, objectKey); err != nil {
			result.Error = fmt.Errorf("upload failed: %w", err)
			result.Duration = time.Since(startTime)
			return result
		}
		result.Uploaded = true
		if program != nil {
			program.Send(updateProgress("Uploading to S3...", 100, 100))
		}

		// Calculate multipart ETag if file is large enough for multipart upload
		multipartETag := ""
		if fileSize > 100*1024*1024 {
			etag, err := a.calculateMultipartETagFromFile(tempFilePath)
			if err != nil {
				a.logger.Warn(fmt.Sprintf("   ⚠️  Failed to calculate multipart ETag: %v", err))
			} else {
				multipartETag = etag
				a.logger.Debug(fmt.Sprintf("   🔐 Calculated multipart ETag: %s", multipartETag))
			}
		}

		// Save metadata to cache immediately after successful upload
		cache.setFileMetadataWithETagAndStartTime(partition.TableName, objectKey, fileSize, uncompressedSize, md5Hash, multipartETag, true, startTime)
		if err := cache.save(a.config.CacheScope); err != nil {
			a.logger.Warn(fmt.Sprintf("   ⚠️  Failed to save cache metadata: %v", err))
		} else {
			a.logger.Debug(fmt.Sprintf("   💾 Saved file metadata to cache: compressed=%d, uncompressed=%d, md5=%s, multipartETag=%s", fileSize, uncompressedSize, md5Hash, multipartETag))
		}
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
	cache, _ := loadPartitionCache(a.config.CacheScope)

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

	// Use streaming extraction to avoid loading all rows into memory
	tempFilePath, fileSize, md5Hash, uncompressedSize, extractErr := a.extractPartitionDataStreaming(partition, nil, cache, updateTaskStage, startTime, endTime)
	if extractErr != nil {
		result.Error = fmt.Errorf("failed to extract data: %w", extractErr)
		result.Stage = "Extracting"
		result.Duration = time.Since(sliceStartTime)
		return result
	}

	// Check if file was created and has content (very small files likely have no data rows)
	// Format-specific minimum sizes: CSV header ~100 bytes, Parquet footer ~1000 bytes, JSONL empty
	if tempFilePath == "" || fileSize < 100 {
		a.logger.Debug(fmt.Sprintf("      No data for %s, skipping", startTime.Format("2006-01-02")))
		result.Skipped = true
		result.SkipReason = "No data in time range"
		if tempFilePath != "" {
			cleanupTempFile(tempFilePath)
		}
		// Save cache metadata to indicate this slice was checked and had no data
		// This prevents re-checking empty slices on subsequent runs
		cache.setFileMetadataWithETagAndStartTime(partition.TableName, objectKey, 0, 0, "", "", false, sliceStartTime)
		if err := cache.save(a.config.CacheScope); err != nil {
			a.logger.Debug(fmt.Sprintf("      ⚠️  Failed to save cache metadata for empty slice: %v", err))
		}
		result.Duration = time.Since(sliceStartTime)
		return result
	}

	result.Compressed = true
	result.BytesWritten = fileSize

	// Check if we can skip upload by comparing with S3
	exists, s3Size, s3ETag := a.checkObjectExists(objectKey)
	if exists && s3Size == fileSize {
		s3ETag = strings.Trim(s3ETag, "\"")
		isMultipart := strings.Contains(s3ETag, "-")

		if !isMultipart && s3ETag == md5Hash {
			// Single-part upload matches
			result.Skipped = true
			result.SkipReason = fmt.Sprintf("Already exists with matching size (%d bytes) and MD5 (%s)", s3Size, s3ETag)
			result.Stage = StageSkipped
			// Clean up temp file
			cleanupTempFile(tempFilePath)
			// Save to cache immediately
			cache.setFileMetadataWithETagAndStartTime(partition.TableName, objectKey, fileSize, uncompressedSize, md5Hash, "", true, sliceStartTime)
			if err := cache.save(a.config.CacheScope); err != nil {
				a.logger.Warn(fmt.Sprintf("      ⚠️  Failed to save cache metadata: %v", err))
			}
			result.Duration = time.Since(sliceStartTime)
			return result
		}

		if isMultipart {
			// Calculate multipart ETag from file
			multipartETag, err := a.calculateMultipartETagFromFile(tempFilePath)
			if err == nil && s3ETag == multipartETag {
				result.Skipped = true
				result.SkipReason = fmt.Sprintf("Already exists with matching size (%d bytes) and multipart ETag (%s)", s3Size, s3ETag)
				result.Stage = StageSkipped
				// Clean up temp file
				cleanupTempFile(tempFilePath)
				// Save to cache immediately with multipart ETag
				cache.setFileMetadataWithETagAndStartTime(partition.TableName, objectKey, fileSize, uncompressedSize, md5Hash, multipartETag, true, sliceStartTime)
				if err := cache.save(a.config.CacheScope); err != nil {
					a.logger.Warn(fmt.Sprintf("      ⚠️  Failed to save cache metadata: %v", err))
				}
				result.Duration = time.Since(sliceStartTime)
				return result
			}
		}
	}

	// Check for cancellation
	select {
	case <-a.ctx.Done():
		cleanupTempFile(tempFilePath)
		result.Error = a.ctx.Err()
		result.Stage = StageCancelled
		result.Duration = time.Since(sliceStartTime)
		return result
	default:
	}

	// Upload to S3
	if !a.config.DryRun {
		result.Stage = "Uploading"
		if err := a.uploadTempFileToS3(tempFilePath, objectKey); err != nil {
			cleanupTempFile(tempFilePath)
			result.Error = fmt.Errorf("upload failed: %w", err)
			result.Duration = time.Since(sliceStartTime)
			return result
		}
		result.Uploaded = true

		// Calculate multipart ETag if file is large enough
		multipartETag := ""
		if fileSize > 100*1024*1024 {
			etag, err := a.calculateMultipartETagFromFile(tempFilePath)
			if err != nil {
				a.logger.Debug(fmt.Sprintf("      ⚠️  Failed to calculate multipart ETag: %v", err))
			} else {
				multipartETag = etag
				a.logger.Debug(fmt.Sprintf("      🔐 Calculated multipart ETag: %s", multipartETag))
			}
		}

		// Save metadata to cache immediately after successful upload
		cache.setFileMetadataWithETagAndStartTime(partition.TableName, objectKey, fileSize, uncompressedSize, md5Hash, multipartETag, true, sliceStartTime)
		if err := cache.save(a.config.CacheScope); err != nil {
			a.logger.Warn(fmt.Sprintf("      ⚠️  Failed to save cache metadata: %v", err))
		}

		// Only log in debug mode when TUI is disabled
		if a.config.Debug {
			a.logger.Info(fmt.Sprintf("      ✅ Uploaded %s (%d bytes)", filename, fileSize))
		}
	}

	// Clean up temp file
	cleanupTempFile(tempFilePath)

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

	cachedSize, cachedMD5, cachedMultipartETag, hasCached := cache.getFileMetadataWithETag(partition.TableName, objectKey, partition.Date)
	if !hasCached {
		return false, result
	}

	// We have cached metadata, check if it matches what's in S3
	updateTaskStage("Checking cached file metadata...")

	exists, s3Size, s3ETag := a.checkObjectExists(objectKey)
	if !exists {
		a.logger.Debug("💾 Have cached metadata but file doesn't exist in S3, will upload")
		return false, result
	}

	s3ETag = strings.Trim(s3ETag, "\"")
	isMultipart := strings.Contains(s3ETag, "-")

	a.logger.Debug(fmt.Sprintf("   💾 Using cached metadata for %s:", partition.TableName))
	a.logger.Debug(fmt.Sprintf("      Cached: size=%d, md5=%s, multipartETag=%s", cachedSize, cachedMD5, cachedMultipartETag))
	a.logger.Debug(fmt.Sprintf("      S3:     size=%d, etag=%s (multipart=%v)", s3Size, s3ETag, isMultipart))

	// Check if cached metadata matches S3
	if s3Size == cachedSize {
		if !isMultipart && s3ETag == cachedMD5 {
			// Cached metadata matches S3 - skip without extraction
			result.Skipped = true
			result.SkipReason = fmt.Sprintf("Cached metadata matches S3 (size=%d, md5=%s)", cachedSize, cachedMD5)
			result.Stage = StageSkipped
			result.BytesWritten = cachedSize
			a.logger.Debug("   ✅ Skipping based on cache: Size and MD5 match")
			return true, result
		} else if isMultipart {
			// For multipart uploads, compare cached multipart ETag with S3 ETag
			if cachedMultipartETag != "" && cachedMultipartETag == s3ETag {
				result.Skipped = true
				result.SkipReason = fmt.Sprintf("Cached metadata matches S3 (size=%d, multipartETag=%s)", cachedSize, cachedMultipartETag)
				result.Stage = StageSkipped
				result.BytesWritten = cachedSize
				a.logger.Debug("   ✅ Skipping based on cache: Size and multipart ETag match")
				return true, result
			}
			a.logger.Debug("   ℹ️  Multipart upload with matching size but no cached ETag or ETag mismatch, proceeding with extraction to verify")
		}
	}

	return false, result
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
		_ = cache.save(a.config.CacheScope)
		return nil, "", fmt.Errorf("compressor setup failed: %w", err)
	}

	// Apply compression
	compressed, err := compressor.Compress(data, a.config.CompressionLevel)
	if err != nil {
		// Save error to cache
		cache.setError(partition.TableName, fmt.Sprintf("Compression failed: %v", err))
		_ = cache.save(a.config.CacheScope)
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
		a.logger.Debug(fmt.Sprintf("📊 File does not exist in S3: %s", objectKey))
		return false, result
	}

	// Remove quotes from ETag if present
	s3ETag = strings.Trim(s3ETag, "\"")
	isMultipart := strings.Contains(s3ETag, "-")

	// Always log comparison details in debug mode
	a.logger.Debug(fmt.Sprintf("📊 Comparing files for %s:", partition.TableName))
	a.logger.Debug(fmt.Sprintf("   S3:     size=%d, etag=%s (multipart=%v)", s3Size, s3ETag, isMultipart))
	a.logger.Debug(fmt.Sprintf("   Local:  size=%d, md5=%s", localSize, localMD5))

	if s3Size != localSize {
		a.logger.Debug(fmt.Sprintf("   ❌ Size mismatch: S3=%d, Local=%d", s3Size, localSize))
		return false, result
	}

	// Size matches, check hash
	if !isMultipart && s3ETag == localMD5 {
		// Single-part upload with matching MD5
		result.Skipped = true
		result.SkipReason = fmt.Sprintf("Already exists with matching size (%d bytes) and MD5 (%s)", s3Size, s3ETag)
		result.Stage = StageSkipped
		a.logger.Debug("   ✅ Skipping: Size and MD5 match")
		// Save to cache immediately for future runs
		cache.setFileMetadata(partition.TableName, objectKey, localSize, uncompressedSize, localMD5, true)
		if err := cache.save(a.config.CacheScope); err != nil {
			a.logger.Warn(fmt.Sprintf("   ⚠️  Failed to save cache metadata: %v", err))
		}
		return true, result
	}

		if isMultipart {
			// For multipart uploads, calculate the multipart ETag
			localMultipartETag := a.calculateMultipartETag(compressed)
			a.logger.Debug(fmt.Sprintf("   Local multipart ETag: %s", localMultipartETag))
			if s3ETag == localMultipartETag {
				result.Skipped = true
				result.SkipReason = fmt.Sprintf("Already exists with matching size (%d bytes) and multipart ETag (%s)", s3Size, s3ETag)
				result.Stage = StageSkipped
				a.logger.Debug("   ✅ Skipping: Size and multipart ETag match")
				// Save to cache immediately for future runs with multipart ETag
				cache.setFileMetadataWithETagAndStartTime(partition.TableName, objectKey, localSize, uncompressedSize, localMD5, localMultipartETag, true, time.Time{})
				if err := cache.save(a.config.CacheScope); err != nil {
					a.logger.Warn(fmt.Sprintf("   ⚠️  Failed to save cache metadata: %v", err))
				}
				return true, result
			}
			a.logger.Debug(fmt.Sprintf("   ❌ Multipart ETag mismatch: S3=%s, Local=%s", s3ETag, localMultipartETag))
	} else {
		a.logger.Debug(fmt.Sprintf("   ❌ MD5 mismatch: S3=%s, Local=%s", s3ETag, localMD5))
	}

	// Size or hash doesn't match, we'll re-upload
	a.logger.Debug("   🔄 Will re-upload due to differences")

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

// calculateMultipartETagFromFile calculates multipart ETag from a file
func (a *Archiver) calculateMultipartETagFromFile(filePath string) (string, error) {
	const partSize = 5 * 1024 * 1024 // 5MB part size (s3manager default)

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := fileInfo.Size()

	// Calculate number of parts
	numParts := int((fileSize + partSize - 1) / partSize)

	// If it would be a single part, just return regular MD5
	if numParts == 1 {
		hasher := md5.New() //nolint:gosec // MD5 used for checksums, not cryptography
		if _, err := io.Copy(hasher, file); err != nil {
			return "", fmt.Errorf("failed to hash file: %w", err)
		}
		return hex.EncodeToString(hasher.Sum(nil)), nil
	}

	// Calculate MD5 of each part
	var partMD5s []byte
	buffer := make([]byte, partSize)

	for i := 0; i < numParts; i++ {
		partHasher := md5.New() //nolint:gosec // MD5 used for checksums, not cryptography

		// Read one part
		n, err := io.ReadFull(file, buffer)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return "", fmt.Errorf("failed to read part %d: %w", i, err)
		}

		// Hash the part data
		partHasher.Write(buffer[:n])
		partMD5s = append(partMD5s, partHasher.Sum(nil)...)
	}

	// Calculate MD5 of concatenated MD5s
	finalHasher := md5.New() //nolint:gosec // MD5 used for checksums, not cryptography
	finalHasher.Write(partMD5s)
	finalMD5 := hex.EncodeToString(finalHasher.Sum(nil))

	// Return in S3 multipart format: MD5-numParts
	return fmt.Sprintf("%s-%d", finalMD5, numParts), nil
}

func (a *Archiver) uploadToS3(key string, data []byte) error {
	a.logger.Debug(fmt.Sprintf("   ☁️  Uploading to s3://%s/%s (size: %d bytes)",
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

func (a *Archiver) printSummary(results []ProcessResult, startTime time.Time, totalPartitions int) {
	var successful, failed, skipped int
	var totalBytes int64
	var totalRows int64
	var totalDuration time.Duration
	var failedResults []ProcessResult
	var minDate, maxDate *time.Time

	for _, r := range results {
		if r.Error != nil {
			failed++
			failedResults = append(failedResults, r)
		} else if r.Skipped {
			skipped++
		} else {
			// Count as successful if processed without error and not skipped
			// This includes DryRun mode where Uploaded=false but processing succeeded
			successful++
			totalBytes += r.BytesWritten
			if r.Partition.RowCount > 0 {
				totalRows += r.Partition.RowCount
			}
			totalDuration += r.Duration
		}

		// Track date range
		if !r.Partition.Date.IsZero() {
			if minDate == nil || r.Partition.Date.Before(*minDate) {
				minDate = &r.Partition.Date
			}
			if maxDate == nil || r.Partition.Date.After(*maxDate) {
				maxDate = &r.Partition.Date
			}
		}
	}

	// Calculate total elapsed time
	totalElapsed := time.Since(startTime)

	// Calculate success rate
	totalProcessed := successful + skipped + failed
	var successRate float64
	if totalProcessed > 0 {
		successRate = float64(successful) / float64(totalProcessed) * 100
	}

	a.logger.Info("")
	a.logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	a.logger.Info("📈 Summary")
	a.logger.Info(fmt.Sprintf("   Total Partitions: %d", totalPartitions))
	a.logger.Info(fmt.Sprintf("   ✅ Successful: %d", successful))
	if skipped > 0 {
		a.logger.Info(fmt.Sprintf("   ⏭️  Skipped: %d", skipped))
	}
	if failed > 0 {
		a.logger.Info(fmt.Sprintf("   ❌ Failed: %d", failed))
	}

	// Show archive rate
	if totalProcessed > 0 {
		rateStr := fmt.Sprintf("%.1f%%", successRate)
		a.logger.Info(fmt.Sprintf("   Archive Rate: %s", rateStr))
	}

	// Show total rows transferred
	if totalRows > 0 {
		a.logger.Info(fmt.Sprintf("   Total Transferred: %s rows", formatNumberForSummary(totalRows)))
	}

	// Show total bytes if any uploads occurred
	if totalBytes > 0 {
		a.logger.Info(fmt.Sprintf("   Total Data Uploaded: %s", formatBytesForSummary(totalBytes)))
	}

	// Show duration and throughput
	if totalElapsed > 0 {
		a.logger.Info(fmt.Sprintf("   Total Duration: %s", formatDurationForSummary(totalElapsed)))

		// Calculate throughput
		if totalRows > 0 && totalElapsed.Seconds() > 0 {
			rowsPerSec := float64(totalRows) / totalElapsed.Seconds()
			a.logger.Info(fmt.Sprintf("   Throughput: %s rows/sec", formatFloatForSummary(rowsPerSec)))
			if totalBytes > 0 {
				mbPerSec := float64(totalBytes) / (1024 * 1024) / totalElapsed.Seconds()
				a.logger.Info(fmt.Sprintf("   Throughput: %s MB/sec", formatFloatForSummary(mbPerSec)))
			}
		}

		// Show average time per partition
		if successful > 0 && totalDuration > 0 {
			avgDuration := totalDuration / time.Duration(successful)
			a.logger.Info(fmt.Sprintf("   Avg Time per Partition: %s", formatDurationForSummary(avgDuration)))
		}
	}

	// Show date range
	if minDate != nil && maxDate != nil {
		if minDate.Equal(*maxDate) {
			a.logger.Info(fmt.Sprintf("   Date Range: %s", minDate.Format("2006-01-02")))
		} else {
			a.logger.Info(fmt.Sprintf("   Date Range: %s to %s", minDate.Format("2006-01-02"), maxDate.Format("2006-01-02")))
		}
	}

	// List failures with details
	if len(failedResults) > 0 {
		a.logger.Error("")
		a.logger.Error("   Failed Partitions:")
		a.logger.Error("")
		for _, result := range failedResults {
			partitionName := result.Partition.TableName
			errorMsg := result.Error.Error()
			// Truncate long error messages for better display
			if len(errorMsg) > 80 {
				errorMsg = errorMsg[:77] + "..."
			}
			a.logger.Error(fmt.Sprintf("   ❌ %s: %s", partitionName, errorMsg))
		}
	}
}

// Helper functions for formatting summary output
func formatNumberForSummary(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	str := fmt.Sprintf("%d", n)
	var result strings.Builder
	length := len(str)
	for i, char := range str {
		if i > 0 && (length-i)%3 == 0 {
			result.WriteString(",")
		}
		result.WriteRune(char)
	}
	return result.String()
}

func formatBytesForSummary(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDurationForSummary(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh %dm", hours, minutes)
}

func formatFloatForSummary(f float64) string {
	if f < 0.01 {
		return fmt.Sprintf("%.4f", f)
	}
	if f < 1 {
		return fmt.Sprintf("%.2f", f)
	}
	if f < 1000 {
		return fmt.Sprintf("%.1f", f)
	}
	if f < 1000000 {
		return fmt.Sprintf("%.0f", f)
	}
	return fmt.Sprintf("%.2f", f)
}

// extractRowsWithDateFilter extracts rows from partition with date range filtering
func (a *Archiver) extractRowsWithDateFilter(partition PartitionInfo, startTime, endTime time.Time) ([]map[string]interface{}, error) {
	quotedTable := pq.QuoteIdentifier(partition.TableName)
	quotedDateColumn := pq.QuoteIdentifier(a.config.DateColumn)

	// Build query with date range filter
	query := fmt.Sprintf(
		"SELECT row_to_json(t) FROM %s t WHERE %s >= $1 AND %s < $2",
		quotedTable,
		quotedDateColumn,
		quotedDateColumn,
	)

	// Use queryWithRetry for automatic retry on timeout/connection errors
	rows, err := a.queryWithRetry(a.ctx, query, startTime, endTime)
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

// convertPostgreSQLValue converts PostgreSQL driver values to appropriate Go types
// The pq driver returns int64 for all integer types, but formatters (especially Parquet)
// need specific types based on the actual PostgreSQL column type
func convertPostgreSQLValue(value interface{}, pgType string) interface{} {
	if value == nil {
		return nil
	}

	switch pgType {
	case "int2", "int4":
		// PostgreSQL driver returns int64, but we need int32 for Parquet INT32
		// Safe conversion: PostgreSQL int2 is -32768 to 32767, int4 is -2147483648 to 2147483647
		if v, ok := value.(int64); ok {
			return int32(v) //nolint:gosec // G115: Safe conversion, PostgreSQL int4 fits in int32
		}
	case "int8":
		// Keep as int64
		return value
	case "float4":
		// PostgreSQL driver returns float64, convert to float32
		if v, ok := value.(float64); ok {
			return float32(v)
		}
	case "float8", "numeric", "decimal":
		// Keep as float64
		return value
	case "bool":
		// Keep as bool
		return value
	case "timestamp", "timestamptz", "date":
		// Keep as time.Time
		return value
	case "bytea":
		// Keep as []byte
		return value
	default:
		// For strings and other types, keep as-is
		return value
	}

	return value
}

// calculateUncompressedRowSize estimates the uncompressed size of a single row
// based on the output format. This is used for metrics and capacity planning.
func calculateUncompressedRowSize(row map[string]interface{}, format string, columnNames []string) int64 {
	switch format {
	case formatters.FormatJSONL:
		// JSONL: JSON marshaling + newline
		jsonBytes, _ := json.Marshal(row)
		return int64(len(jsonBytes)) + 1 // +1 for newline

	case formatters.FormatCSV:
		// CSV: Estimate size accounting for commas, escaping, and newline
		// CSV writer handles escaping, so we estimate conservatively
		var size int64
		for i, col := range columnNames {
			if i > 0 {
				size++ // comma
			}
			val := row[col]
			if val == nil {
				// Empty field
				continue
			}
			// Estimate: string representation + potential escaping (double quotes + escaped quotes)
			// CSV escaping: if value contains comma, newline, or quote, it's wrapped in quotes
			// and internal quotes are doubled
			valStr := fmt.Sprintf("%v", val)
			needsEscaping := strings.Contains(valStr, ",") || strings.Contains(valStr, "\n") || strings.Contains(valStr, `"`)
			if needsEscaping {
				size += int64(len(valStr)) + 2 + int64(strings.Count(valStr, `"`)) // quotes + escaped quotes
			} else {
				size += int64(len(valStr))
			}
		}
		size++ // newline
		return size

	case formatters.FormatParquet:
		// Parquet: Binary columnar format - estimate based on data types
		// Parquet is typically more compact than JSON, but we can't easily calculate
		// without actually writing. Use a type-based estimate as approximation.
		var size int64
		for _, val := range row {
			if val == nil {
				continue // Nulls take minimal space in Parquet
			}
			switch v := val.(type) {
			case bool:
				size += 1 // 1 byte
			case int, int8, int16, int32:
				size += 4 // 4 bytes
			case int64:
				size += 8 // 8 bytes
			case float32:
				size += 4 // 4 bytes
			case float64:
				size += 8 // 8 bytes
			case string:
				// String length + overhead (dictionary encoding can reduce this, but we estimate conservatively)
				size += int64(len(v)) + 4 // length prefix + string data
			case []byte:
				size += int64(len(v)) + 4 // length prefix + bytes
			case time.Time:
				size += 8 // timestamp typically 8 bytes
			default:
				// Fallback: estimate as string representation
				size += int64(len(fmt.Sprintf("%v", v))) + 4
			}
		}
		// Add overhead for Parquet metadata (column overhead, row group overhead)
		// This is a rough estimate - actual Parquet files have additional overhead
		return size + int64(len(row)*2) // ~2 bytes overhead per column

	default:
		// Fallback: use JSON size
		jsonBytes, _ := json.Marshal(row)
		return int64(len(jsonBytes)) + 1
	}
}

// extractPartitionDataStreaming extracts partition data using streaming architecture
// This streams data in chunks to a temp file, avoiding loading everything into memory
// If startTime and endTime are provided (not zero), adds a WHERE clause to filter by date column
//
//nolint:nakedret,gocognit,gocyclo // Complex streaming function with named returns for clarity, high complexity unavoidable
func (a *Archiver) extractPartitionDataStreaming(partition PartitionInfo, program *tea.Program, cache *PartitionCache, updateTaskStage func(string), startTime, endTime time.Time) (tempFilePath string, fileSize int64, md5Hash string, uncompressedSize int64, err error) {
	extractStart := time.Now()
	updateTaskStage("Getting table schema...")

	// Get table schema for streaming formatters
	schema, schemaErr := a.getTableSchema(a.ctx, partition.TableName)
	if schemaErr != nil {
		cache.setError(partition.TableName, fmt.Sprintf("Schema query failed: %v", schemaErr))
		_ = cache.save(a.config.CacheScope)
		err = fmt.Errorf("schema query failed: %w", schemaErr)
		return
	}

	// Determine chunk size (use config or default)
	chunkSize := a.config.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 10000 // Default chunk size
	}

	// Create temp file for output
	tempFile, tempErr := createTempFile()
	if tempErr != nil {
		err = fmt.Errorf("failed to create temp file: %w", tempErr)
		return
	}
	tempFilePath = tempFile.Name()

	// Ensure cleanup on error
	defer func() {
		if err != nil {
			tempFile.Close()
			cleanupTempFile(tempFilePath)
			tempFilePath = ""
		}
	}()

	updateTaskStage("Setting up streaming pipeline...")

	// Get streaming formatter
	formatter := formatters.GetStreamingFormatter(a.config.OutputFormat)

	// Set up streaming pipeline based on format's compression handling
	var streamWriter formatters.StreamWriter
	var compressorWriter io.WriteCloser
	var hasher hash.Hash
	var multiWriter io.Writer

	if formatters.UsesInternalCompression(a.config.OutputFormat) {
		// Parquet handles compression internally
		// Pipeline: formatter → hasher → tempFile
		hasher = md5.New() //nolint:gosec // MD5 used for checksums, not cryptography
		multiWriter = io.MultiWriter(tempFile, hasher)
		streamWriter, err = formatter.NewWriter(multiWriter, schema)
		if err != nil {
			err = fmt.Errorf("failed to create streaming formatter: %w", err)
			return
		}
	} else {
		// External compression needed (JSONL, CSV)
		// Pipeline: formatter → compressor → hasher → tempFile
		compressor, compErr := compressors.GetCompressor(a.config.Compression)
		if compErr != nil {
			err = fmt.Errorf("failed to get compressor: %w", compErr)
			return
		}

		hasher = md5.New() //nolint:gosec // MD5 used for checksums, not cryptography
		multiWriter = io.MultiWriter(tempFile, hasher)
		compressorWriter = compressor.NewWriter(multiWriter, a.config.CompressionLevel)

		streamWriter, err = formatter.NewWriter(compressorWriter, schema)
		if err != nil {
			if compressorWriter != nil {
				compressorWriter.Close()
			}
			err = fmt.Errorf("failed to create streaming formatter: %w", err)
			return
		}
	}

	// Stream data in chunks
	updateTaskStage("Extracting data...")
	if program != nil && partition.RowCount > 0 {
		program.Send(updateProgress("Extracting data...", 0, partition.RowCount))
	}

	// Build column list for SELECT query
	columns := schema.GetColumns()
	columnNames := make([]string, len(columns))
	unquotedColumnNames := make([]string, len(columns))
	for i, col := range columns {
		unquotedColumnNames[i] = col.GetName()
		columnNames[i] = pq.QuoteIdentifier(col.GetName())
	}

	quotedTable := pq.QuoteIdentifier(partition.TableName)
	//nolint:gosec // G201: SQL string formatting is safe here - all identifiers are properly quoted via pq.QuoteIdentifier
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columnNames, ", "), quotedTable)
	var queryArgs []interface{}

	// Add date filtering if startTime and endTime are provided
	if !startTime.IsZero() && !endTime.IsZero() && a.config.DateColumn != "" {
		quotedDateColumn := pq.QuoteIdentifier(a.config.DateColumn)
		query += fmt.Sprintf(" WHERE %s >= $1 AND %s < $2", quotedDateColumn, quotedDateColumn)
		queryArgs = []interface{}{startTime, endTime}
	}

	// Account for CSV header row in uncompressed size
	if a.config.OutputFormat == formatters.FormatCSV {
		// CSV header: column names separated by commas + newline
		headerSize := int64(len(strings.Join(unquotedColumnNames, ",")) + 1) // commas + newline
		uncompressedSize += headerSize
	}

	var rows *sql.Rows
	var queryErr error
	if len(queryArgs) > 0 {
		rows, queryErr = a.db.QueryContext(a.ctx, query, queryArgs...)
	} else {
		rows, queryErr = a.db.QueryContext(a.ctx, query)
	}
	if queryErr != nil {
		streamWriter.Close()
		if compressorWriter != nil {
			compressorWriter.Close()
		}
		err = fmt.Errorf("query failed: %w", queryErr)
		return
	}
	defer rows.Close()

	// Prepare scan targets - use interface{} to let database/sql handle type conversion
	scanValues := make([]interface{}, len(columns))
	scanPointers := make([]interface{}, len(columns))
	for i := range scanValues {
		scanPointers[i] = &scanValues[i]
	}

	// Process rows in chunks
	chunk := make([]map[string]interface{}, 0, chunkSize)
	rowCount := int64(0)
	updateInterval := int64(1000)

	if partition.RowCount > 0 {
		updateInterval = partition.RowCount / 100
		if updateInterval < 1000 {
			updateInterval = 1000
		}
	}

	for rows.Next() {
		// Check for cancellation
		if rowCount%100 == 0 {
			select {
			case <-a.ctx.Done():
				streamWriter.Close()
				if compressorWriter != nil {
					compressorWriter.Close()
				}
				err = a.ctx.Err()
				return
			default:
			}
		}

		// Scan row columns into scanValues
		if scanErr := rows.Scan(scanPointers...); scanErr != nil {
			streamWriter.Close()
			if compressorWriter != nil {
				compressorWriter.Close()
			}
			err = fmt.Errorf("failed to scan row: %w", scanErr)
			return
		}

		// Convert to map[string]interface{} with type conversion
		rowData := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			// Convert PostgreSQL driver types to appropriate Go types for formatters
			rowData[col.GetName()] = convertPostgreSQLValue(scanValues[i], col.GetType())
		}

		chunk = append(chunk, rowData)
		rowCount++

		// Write chunk when full
		if len(chunk) >= chunkSize {
			if writeErr := streamWriter.WriteChunk(chunk); writeErr != nil {
				streamWriter.Close()
				if compressorWriter != nil {
					compressorWriter.Close()
				}
				err = fmt.Errorf("failed to write chunk: %w", writeErr)
				return
			}

			// Track uncompressed size based on output format
			for _, row := range chunk {
				uncompressedSize += calculateUncompressedRowSize(row, a.config.OutputFormat, unquotedColumnNames)
			}

			chunk = chunk[:0] // Reset slice, keeping capacity

			// Update progress
			if program != nil && partition.RowCount > 0 && rowCount%updateInterval == 0 {
				program.Send(updateProgress("Extracting data...", rowCount, partition.RowCount))
			}
		}
	}

	// Check for errors during iteration
	if rowsErr := rows.Err(); rowsErr != nil {
		streamWriter.Close()
		if compressorWriter != nil {
			compressorWriter.Close()
		}
		err = fmt.Errorf("error iterating rows: %w", rowsErr)
		return
	}

	// Write final chunk
	if len(chunk) > 0 {
		if writeErr := streamWriter.WriteChunk(chunk); writeErr != nil {
			streamWriter.Close()
			if compressorWriter != nil {
				compressorWriter.Close()
			}
			err = fmt.Errorf("failed to write final chunk: %w", writeErr)
			return
		}

		// Track uncompressed size of final chunk
		for _, row := range chunk {
			uncompressedSize += calculateUncompressedRowSize(row, a.config.OutputFormat, unquotedColumnNames)
		}
	}

	// Close stream writer (this flushes formatters and writes footers)
	if closeErr := streamWriter.Close(); closeErr != nil {
		if compressorWriter != nil {
			compressorWriter.Close()
		}
		err = fmt.Errorf("failed to close stream writer: %w", closeErr)
		return
	}

	// Close compressor (if used)
	if compressorWriter != nil {
		if closeErr := compressorWriter.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close compressor: %w", closeErr)
			return
		}
	}

	// Close temp file to flush all writes
	if closeErr := tempFile.Close(); closeErr != nil {
		err = fmt.Errorf("failed to close temp file: %w", closeErr)
		return
	}

	// Get file size
	fileInfo, statErr := os.Stat(tempFilePath)
	if statErr != nil {
		err = fmt.Errorf("failed to stat temp file: %w", statErr)
		return
	}
	fileSize = fileInfo.Size()

	// Get MD5 hash
	md5Hash = hex.EncodeToString(hasher.Sum(nil))

	extractDuration := time.Since(extractStart)
	a.logger.Debug(fmt.Sprintf("   ⏱️  Streaming extraction took %v for %s (%d rows, %d bytes)",
		extractDuration, partition.TableName, rowCount, fileSize))

	// Update progress
	if program != nil {
		if partition.RowCount > 0 {
			program.Send(updateProgress("Extraction complete", partition.RowCount, partition.RowCount))
		} else {
			program.Send(updateProgress(fmt.Sprintf("Extraction complete (%d rows)", rowCount), 0, 0))
		}
	}

	// Save row count to cache immediately if it was unknown
	if partition.RowCount <= 0 && rowCount > 0 {
		cache.setRowCount(partition.TableName, rowCount)
		if err := cache.save(a.config.CacheScope); err != nil {
			// Log warning but don't fail - row count caching is not critical
			if a.config.Debug {
				a.logger.Debug(fmt.Sprintf("   ⚠️  Failed to save row count to cache: %v", err))
			}
		}
	}

	return tempFilePath, fileSize, md5Hash, uncompressedSize, nil
}

// extractPartitionDataWithRetry wraps extractPartitionDataStreaming with retry logic
func (a *Archiver) extractPartitionDataWithRetry(partition PartitionInfo, program *tea.Program, cache *PartitionCache, updateTaskStage func(string)) (tempFilePath string, fileSize int64, md5Hash string, uncompressedSize int64, err error) {
	maxRetries := a.config.Database.MaxRetries
	retryDelay := time.Duration(a.config.Database.RetryDelay) * time.Second

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		tempPath, size, hash, uncompSize, extractErr := a.extractPartitionDataStreaming(partition, program, cache, updateTaskStage, time.Time{}, time.Time{})

		if extractErr == nil {
			return tempPath, size, hash, uncompSize, nil
		}

		lastErr = extractErr

		// Clean up failed temp file if it exists
		if tempPath != "" {
			cleanupTempFile(tempPath)
		}

		// Check if error is retryable
		if !isRetryableError(extractErr) {
			return "", 0, "", 0, extractErr
		}

		// If we've exhausted retries, return the error
		if attempt >= maxRetries {
			break
		}

		// Log retry attempt
		a.logger.Warn(fmt.Sprintf("Extraction failed for %s (attempt %d/%d): %v. Retrying in %v...",
			partition.TableName, attempt+1, maxRetries+1, extractErr, retryDelay))

		// Wait before retrying, respecting context cancellation
		select {
		case <-time.After(retryDelay):
			continue
		case <-a.ctx.Done():
			return "", 0, "", 0, a.ctx.Err()
		}
	}

	return "", 0, "", 0, fmt.Errorf("extraction failed after %d attempts: %w", maxRetries+1, lastErr)
}

// uploadTempFileToS3 uploads a temp file to S3, using multipart upload for large files
func (a *Archiver) uploadTempFileToS3(tempFilePath, objectKey string) error {
	// Open temp file for reading
	file, err := os.Open(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to open temp file: %w", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat temp file: %w", err)
	}
	fileSize := fileInfo.Size()

	a.logger.Debug(fmt.Sprintf("   ☁️  Uploading to s3://%s/%s (size: %d bytes)",
		a.config.S3.Bucket, objectKey, fileSize))

	// Use multipart upload for files larger than 100MB
	if fileSize > 100*1024*1024 {
		// Check if S3 uploader is initialized
		if a.s3Uploader == nil {
			return ErrS3UploaderNotInitialized
		}

		// Use S3 manager for automatic multipart upload handling
		uploadInput := &s3manager.UploadInput{
			Bucket:      aws.String(a.config.S3.Bucket),
			Key:         aws.String(objectKey),
			Body:        file,
			ContentType: aws.String("application/octet-stream"),
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
		Key:         aws.String(objectKey),
		Body:        file,
		ContentType: aws.String("application/octet-stream"),
	}

	_, err = a.s3Client.PutObject(putInput)
	return err
}
