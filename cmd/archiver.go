package cmd

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/klauspost/compress/zstd"
	_ "github.com/lib/pq"
)

type Archiver struct {
	config       *Config
	db           *sql.DB
	s3Client     *s3.S3
	progressChan chan tea.Cmd
}

type PartitionInfo struct {
	TableName string
	Date      time.Time
	RowCount  int64
}

type ProcessResult struct {
	Partition   PartitionInfo
	Compressed  bool
	Uploaded    bool
	Skipped     bool
	SkipReason  string
	Error       error
	BytesWritten int64
	Stage       string
}

func NewArchiver(config *Config) *Archiver {
	return &Archiver{
		config:       config,
		progressChan: make(chan tea.Cmd, 100),
	}
}

func (a *Archiver) Run() error {
	// Create channels for communication
	errChan := make(chan error, 1)
	resultsChan := make(chan []ProcessResult, 1)
	
	// Start the UI with the archiver reference
	progressModel := newProgressModelWithArchiver(a.config, a, errChan, resultsChan)
	program := tea.NewProgram(progressModel)

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

func (a *Archiver) connect() error {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		a.config.Database.Host,
		a.config.Database.Port,
		a.config.Database.User,
		a.config.Database.Password,
		a.config.Database.Name,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
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
		return fmt.Errorf("failed to create S3 session: %w", err)
	}

	a.s3Client = s3.New(sess)

	return nil
}

func (a *Archiver) checkTablePermissions() error {
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
	
	err := a.db.QueryRow(checkPermissionQuery, a.config.Table, a.config.Table).Scan(&hasPermission)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to check table permissions: %w", err)
	}
	
	// If the base table exists and we don't have permission, fail
	if err != sql.ErrNoRows && !hasPermission {
		return fmt.Errorf("insufficient permissions to read table '%s'", a.config.Table)
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
	err = a.db.QueryRow(partitionCheckQuery, pattern).Scan(&samplePartition)
	if err != nil && err != sql.ErrNoRows {
		// Only fail if it's not a "no rows" error
		return fmt.Errorf("failed to check partition table permissions: %w", err)
	}
	
	// Check if we found any partitions at all (with or without permissions)
	if err == sql.ErrNoRows {
		// Let's see if partitions exist but we can't access them
		var partitionExists bool
		existsQuery := `
			SELECT EXISTS (
				SELECT 1 FROM pg_tables 
				WHERE schemaname = 'public' 
				AND tablename LIKE $1
			)
		`
		a.db.QueryRow(existsQuery, pattern).Scan(&partitionExists)
		
		if partitionExists {
			// Partitions exist but we can't access them
			return fmt.Errorf("partition tables exist but you don't have SELECT permissions")
		}
		// No partitions found yet, that's okay
	}
	
	return nil
}

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

			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table.name)
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

			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table.name)
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

func (a *Archiver) processPartitionsWithProgress(partitions []PartitionInfo, program *tea.Program) []ProcessResult {
	results := make([]ProcessResult, len(partitions))

	// Process partitions sequentially for better progress tracking
	for i, partition := range partitions {
		result := a.ProcessPartitionWithProgress(partition, i, program)
		results[i] = result
		
		// Send completion update
		program.Send(completePartition(i, result))
		
		// Small delay to allow UI to update
		time.Sleep(10 * time.Millisecond)
	}

	return results
}

func (a *Archiver) ProcessPartitionWithProgress(partition PartitionInfo, index int, program *tea.Program) ProcessResult {
	result := ProcessResult{
		Partition: partition,
	}

	objectKey := fmt.Sprintf("export/%s/%s/%s.jsonl.zst",
		a.config.Table,
		partition.Date.Format("2006/01"),
		partition.Date.Format("2006-01-02"),
	)

	// Small delay to ensure UI can update
	time.Sleep(50 * time.Millisecond)
	
	// Check if already exists
	if program != nil {
		program.Send(updateProgress("Checking if file exists...", 0, 0))
	}
	if exists, size := a.checkObjectExists(objectKey); exists {
		result.Skipped = true
		result.SkipReason = fmt.Sprintf("Already exists with size %d", size)
		result.Stage = "Skipped"
		return result
	}

	// Extract data with progress
	if program != nil {
		if partition.RowCount > 0 {
			program.Send(updateProgress("Extracting data...", 0, partition.RowCount))
		} else {
			program.Send(updateProgress("Extracting data...", 0, 0))
		}
	}
	result.Stage = "Extracting"
	data, err := a.extractDataWithProgress(partition, program)
	if err != nil {
		result.Error = fmt.Errorf("extraction failed: %w", err)
		return result
	}

	// Compress data
	if program != nil {
		program.Send(updateProgress("Compressing data...", 50, 100))
	}
	result.Stage = "Compressing"
	compressed, err := a.compressData(data)
	if err != nil {
		result.Error = fmt.Errorf("compression failed: %w", err)
		return result
	}
	result.Compressed = true
	result.BytesWritten = int64(len(compressed))
	if program != nil {
		program.Send(updateProgress("Compressing data...", 100, 100))
	}

	// Upload to S3
	if !a.config.DryRun {
		if program != nil {
			program.Send(updateProgress("Uploading to S3...", 0, 100))
		}
		result.Stage = "Uploading"
		if err := a.uploadToS3(objectKey, compressed); err != nil {
			result.Error = fmt.Errorf("upload failed: %w", err)
			return result
		}
		result.Uploaded = true
		if program != nil {
			program.Send(updateProgress("Uploading to S3...", 100, 100))
		}
	}

	result.Stage = "Complete"
	return result
}

func (a *Archiver) extractDataWithProgress(partition PartitionInfo, program *tea.Program) ([]byte, error) {
	query := fmt.Sprintf("SELECT row_to_json(t) FROM %s t", partition.TableName)
	
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)

	rowCount := int64(0)
	var updateInterval int64 = 1000 // Default to every 1000 rows
	
	if partition.RowCount > 0 {
		updateInterval = partition.RowCount / 100 // Update every 1%
		if updateInterval < 1000 {
			updateInterval = 1000
		}
	}

	for rows.Next() {
		var jsonData json.RawMessage
		if err := rows.Scan(&jsonData); err != nil {
			return nil, err
		}

		if err := encoder.Encode(jsonData); err != nil {
			return nil, err
		}
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

	// Final update
	if program != nil {
		if partition.RowCount > 0 {
			program.Send(updateProgress("Extraction complete", partition.RowCount, partition.RowCount))
		} else {
			program.Send(updateProgress(fmt.Sprintf("Extraction complete (%d rows)", rowCount), 0, 0))
		}
	}

	return buffer.Bytes(), nil
}

func (a *Archiver) compressData(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	
	encoder, err := zstd.NewWriter(&buffer, 
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithEncoderConcurrency(a.config.Workers))
	if err != nil {
		return nil, err
	}

	if _, err := encoder.Write(data); err != nil {
		encoder.Close()
		return nil, err
	}

	if err := encoder.Close(); err != nil {
		return nil, err
	}

	compressionRatio := float64(len(data)) / float64(buffer.Len())
	if a.config.Debug {
		fmt.Println(debugStyle.Render(fmt.Sprintf("  🗜️  Compressed: %d → %d bytes (%.1fx ratio)",
			len(data), buffer.Len(), compressionRatio)))
	}

	return buffer.Bytes(), nil
}

func (a *Archiver) checkObjectExists(key string) (bool, int64) {
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(a.config.S3.Bucket),
		Key:    aws.String(key),
	}

	result, err := a.s3Client.HeadObject(headInput)
	if err != nil {
		return false, 0
	}

	if result.ContentLength != nil {
		return true, *result.ContentLength
	}

	return true, 0
}

func (a *Archiver) uploadToS3(key string, data []byte) error {
	putInput := &s3.PutObjectInput{
		Bucket:      aws.String(a.config.S3.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/zstd"),
	}

	if a.config.Debug {
		fmt.Println(debugStyle.Render(fmt.Sprintf("  ☁️  Uploading to s3://%s/%s",
			a.config.S3.Bucket, key)))
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

	fmt.Println(infoStyle.Render("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"))
	fmt.Println(titleStyle.Render("📈 Summary"))
	fmt.Printf("%s %d\n", successStyle.Render("✅ Successful:"), successful)
	fmt.Printf("%s %d\n", warningStyle.Render("⏭️  Skipped:"), skipped)
	if failed > 0 {
		fmt.Printf("%s %d\n", lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000")).Render("❌ Failed:"), failed)
	}
	
	if totalBytes > 0 {
		fmt.Printf("%s %.2f MB\n", infoStyle.Render("💾 Total compressed:"), float64(totalBytes)/(1024*1024))
	}

	for _, r := range results {
		if r.Error != nil {
			fmt.Printf("\n%s %s: %v\n", 
				lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000")).Render("❌"),
				r.Partition.TableName,
				r.Error)
		}
	}
}

