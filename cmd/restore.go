package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/airframesio/data-archiver/cmd/compressors"
	"github.com/airframesio/data-archiver/cmd/formatters"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	restoreTable                  string
	restorePathTemplate           string
	restoreStartDate              string
	restoreEndDate                string
	restoreTablePartitionRange    string
	restoreTablePartitionTemplate string
	restoreDateColumn             string
	restoreOutputFormat           string
	restoreCompression            string
)

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore database tables from S3 archives",
	Long:  `Restore database tables from S3 archives. Downloads files, decompresses, parses formats, and inserts data into PostgreSQL tables with automatic table/partition creation.`,
	Run: func(cmd *cobra.Command, _ []string) {
		runRestore(cmd)
	},
}

func init() {
	rootCmd.AddCommand(restoreCmd)

	// Database flags
	restoreCmd.Flags().StringVar(&dbHost, "db-host", "localhost", "PostgreSQL host")
	restoreCmd.Flags().IntVar(&dbPort, "db-port", 5432, "PostgreSQL port")
	restoreCmd.Flags().StringVar(&dbUser, "db-user", "", "PostgreSQL user")
	restoreCmd.Flags().StringVar(&dbPassword, "db-password", "", "PostgreSQL password")
	restoreCmd.Flags().StringVar(&dbName, "db-name", "", "PostgreSQL database name")
	restoreCmd.Flags().StringVar(&dbSSLMode, "db-sslmode", "disable", "PostgreSQL SSL mode (disable, require, verify-ca, verify-full)")
	restoreCmd.Flags().IntVar(&dbStatementTimeout, "db-statement-timeout", 300, "PostgreSQL statement timeout in seconds (0 = no timeout)")
	restoreCmd.Flags().IntVar(&dbMaxRetries, "db-max-retries", 3, "Maximum number of retry attempts for failed queries")
	restoreCmd.Flags().IntVar(&dbRetryDelay, "db-retry-delay", 5, "Delay in seconds between retry attempts")

	// S3 flags
	restoreCmd.Flags().StringVar(&s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint URL")
	restoreCmd.Flags().StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket name")
	restoreCmd.Flags().StringVar(&s3AccessKey, "s3-access-key", "", "S3 access key")
	restoreCmd.Flags().StringVar(&s3SecretKey, "s3-secret-key", "", "S3 secret key")
	restoreCmd.Flags().StringVar(&s3Region, "s3-region", "auto", "S3 region")

	// Restore-specific flags
	restoreCmd.Flags().StringVar(&restoreTable, "table", "", "base table name (required)")
	restoreCmd.Flags().StringVar(&restorePathTemplate, "path-template", "", "S3 path template with placeholders: {table}, {YYYY}, {MM}, {DD}, {HH} (required)")
	restoreCmd.Flags().StringVar(&restoreStartDate, "start-date", "", "start date (YYYY-MM-DD)")
	restoreCmd.Flags().StringVar(&restoreEndDate, "end-date", "", "end date (YYYY-MM-DD)")
	restoreCmd.Flags().StringVar(&restoreTablePartitionRange, "table-partition-range", "", "partition range: hourly, daily, monthly, quarterly, yearly")
	restoreCmd.Flags().StringVar(&restoreTablePartitionTemplate, "table-partition-template", "", "partition name template with placeholders: {table}, {YYYY}, {MM}, {DD}, {HH}, {Q} (quarter)")
	restoreCmd.Flags().StringVar(&restoreDateColumn, "date-column", "", "timestamp column name for splitting rows into partitions (required for hourly partitioning of daily files)")
	restoreCmd.Flags().StringVar(&restoreOutputFormat, "output-format", "", "override format detection (jsonl, csv, parquet)")
	restoreCmd.Flags().StringVar(&restoreCompression, "compression", "", "override compression detection (zstd, lz4, gzip, none)")

	// Bind database flags to viper
	_ = viper.BindPFlag("db.host", restoreCmd.Flags().Lookup("db-host"))
	_ = viper.BindPFlag("db.port", restoreCmd.Flags().Lookup("db-port"))
	_ = viper.BindPFlag("db.user", restoreCmd.Flags().Lookup("db-user"))
	_ = viper.BindPFlag("db.password", restoreCmd.Flags().Lookup("db-password"))
	_ = viper.BindPFlag("db.name", restoreCmd.Flags().Lookup("db-name"))
	_ = viper.BindPFlag("db.sslmode", restoreCmd.Flags().Lookup("db-sslmode"))
	_ = viper.BindPFlag("db.statement_timeout", restoreCmd.Flags().Lookup("db-statement-timeout"))
	_ = viper.BindPFlag("db.max_retries", restoreCmd.Flags().Lookup("db-max-retries"))
	_ = viper.BindPFlag("db.retry_delay", restoreCmd.Flags().Lookup("db-retry-delay"))

	// Bind S3 flags to viper
	_ = viper.BindPFlag("s3.endpoint", restoreCmd.Flags().Lookup("s3-endpoint"))
	_ = viper.BindPFlag("s3.bucket", restoreCmd.Flags().Lookup("s3-bucket"))
	_ = viper.BindPFlag("s3.access_key", restoreCmd.Flags().Lookup("s3-access-key"))
	_ = viper.BindPFlag("s3.secret_key", restoreCmd.Flags().Lookup("s3-secret-key"))
	_ = viper.BindPFlag("s3.region", restoreCmd.Flags().Lookup("s3-region"))

	// Bind restore-specific flags to viper
	_ = viper.BindPFlag("restore.table", restoreCmd.Flags().Lookup("table"))
	_ = viper.BindPFlag("restore.path_template", restoreCmd.Flags().Lookup("path-template"))
	_ = viper.BindPFlag("restore.start_date", restoreCmd.Flags().Lookup("start-date"))
	_ = viper.BindPFlag("restore.end_date", restoreCmd.Flags().Lookup("end-date"))
	_ = viper.BindPFlag("restore.table_partition_range", restoreCmd.Flags().Lookup("table-partition-range"))
	_ = viper.BindPFlag("restore.table_partition_template", restoreCmd.Flags().Lookup("table-partition-template"))
	_ = viper.BindPFlag("restore.date_column", restoreCmd.Flags().Lookup("date-column"))
	_ = viper.BindPFlag("restore.output_format", restoreCmd.Flags().Lookup("output-format"))
	_ = viper.BindPFlag("restore.compression", restoreCmd.Flags().Lookup("compression"))
}

// S3File represents a file found in S3
type S3File struct {
	Key                 string
	Size                int64
	LastModified        time.Time
	DetectedFormat      string
	DetectedCompression string
	Date                time.Time // Extracted from filename
}

// Restorer handles restoration of tables from S3
type Restorer struct {
	config       *Config
	db           *sql.DB
	s3Client     *s3.S3
	s3Downloader *s3manager.Downloader
	logger       *slog.Logger
	ctx          context.Context
}

// NewRestorer creates a new Restorer instance
func NewRestorer(config *Config, logger *slog.Logger) *Restorer {
	return &Restorer{
		config: config,
		logger: logger,
	}
}

func runRestore(cmd *cobra.Command) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "\n❌ PANIC: %v\n", r)
			os.Exit(1)
		}
	}()

	// Helper function to get config value: use flag if set, otherwise use viper
	getStringConfig := func(flagValue string, flagName string, viperKey string) string {
		if flag := cmd.Flags().Lookup(flagName); flag != nil && flag.Changed {
			return flagValue
		}
		return viper.GetString(viperKey)
	}
	getIntConfig := func(flagValue int, flagName string, viperKey string) int {
		if flag := cmd.Flags().Lookup(flagName); flag != nil && flag.Changed {
			return flagValue
		}
		return viper.GetInt(viperKey)
	}

	config := &Config{
		Debug:     viper.GetBool("debug"),
		LogFormat: viper.GetString("log_format"),
		DryRun:    viper.GetBool("dry_run"),
		Database: DatabaseConfig{
			Host:             getStringConfig(dbHost, "db-host", "db.host"),
			Port:             getIntConfig(dbPort, "db-port", "db.port"),
			User:             getStringConfig(dbUser, "db-user", "db.user"),
			Password:         getStringConfig(dbPassword, "db-password", "db.password"),
			Name:             getStringConfig(dbName, "db-name", "db.name"),
			SSLMode:          getStringConfig(dbSSLMode, "db-sslmode", "db.sslmode"),
			StatementTimeout: getIntConfig(dbStatementTimeout, "db-statement-timeout", "db.statement_timeout"),
			MaxRetries:       getIntConfig(dbMaxRetries, "db-max-retries", "db.max_retries"),
			RetryDelay:       getIntConfig(dbRetryDelay, "db-retry-delay", "db.retry_delay"),
		},
		S3: S3Config{
			Endpoint:     getStringConfig(s3Endpoint, "s3-endpoint", "s3.endpoint"),
			Bucket:       getStringConfig(s3Bucket, "s3-bucket", "s3.bucket"),
			AccessKey:    getStringConfig(s3AccessKey, "s3-access-key", "s3.access_key"),
			SecretKey:    getStringConfig(s3SecretKey, "s3-secret-key", "s3.secret_key"),
			Region:       getStringConfig(s3Region, "s3-region", "s3.region"),
			PathTemplate: getStringConfig(restorePathTemplate, "path-template", "restore.path_template"),
		},
		Table:     getStringConfig(restoreTable, "table", "restore.table"),
		StartDate: getStringConfig(restoreStartDate, "start-date", "restore.start_date"),
		EndDate:   getStringConfig(restoreEndDate, "end-date", "restore.end_date"),
	}

	// Get restore-specific config (check flags first)
	restoreTablePartitionRangeVal := restoreTablePartitionRange
	if flag := cmd.Flags().Lookup("table-partition-range"); flag == nil || !flag.Changed {
		restoreTablePartitionRangeVal = viper.GetString("restore.table_partition_range")
	}
	restoreTablePartitionTemplateVal := restoreTablePartitionTemplate
	if flag := cmd.Flags().Lookup("table-partition-template"); flag == nil || !flag.Changed {
		restoreTablePartitionTemplateVal = viper.GetString("restore.table_partition_template")
	}
	restoreDateColumnVal := restoreDateColumn
	if flag := cmd.Flags().Lookup("date-column"); flag == nil || !flag.Changed {
		restoreDateColumnVal = viper.GetString("restore.date_column")
	}
	restoreOutputFormatVal := restoreOutputFormat
	if flag := cmd.Flags().Lookup("output-format"); flag == nil || !flag.Changed {
		restoreOutputFormatVal = viper.GetString("restore.output_format")
	}
	restoreCompressionVal := restoreCompression
	if flag := cmd.Flags().Lookup("compression"); flag == nil || !flag.Changed {
		restoreCompressionVal = viper.GetString("restore.compression")
	}

	// Initialize logger
	initLogger(config.Debug, config.LogFormat)

	logger.Info("")
	logger.Info(fmt.Sprintf("🔄 Data Restorer v%s", Version))
	logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// Print configuration table in debug mode
	if config.Debug {
		printRestoreConfig(config, restoreTablePartitionRangeVal, restoreTablePartitionTemplateVal, restoreDateColumnVal, restoreOutputFormatVal, restoreCompressionVal)
	}

	logger.Debug("Validating configuration...")
	if err := validateRestoreConfig(config, restoreTablePartitionRangeVal); err != nil {
		logger.Error(fmt.Sprintf("❌ Configuration error: %s", err.Error()))
		os.Exit(1)
	}
	logger.Debug("Configuration validated successfully")

	ctx := signalContext
	if ctx == nil {
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
	}

	restorer := NewRestorer(config, logger)

	// Store restore-specific config in a way we can access it
	restoreConfig := map[string]string{
		"table_partition_range":    restoreTablePartitionRangeVal,
		"table_partition_template": restoreTablePartitionTemplateVal,
		"date_column":              restoreDateColumnVal,
		"output_format":            restoreOutputFormatVal,
		"compression":              restoreCompressionVal,
	}

	err := restorer.Run(ctx, restoreConfig)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("")
			logger.Info("⚠️  Restore cancelled by user")
			os.Exit(130)
		}
		logger.Error(fmt.Sprintf("❌ Restore failed: %s", err.Error()))
		os.Exit(1)
	}

	logger.Info("")
	logger.Info("✅ Restore completed successfully!")
}

// printRestoreConfig prints a table of configuration information in debug mode
func printRestoreConfig(config *Config, partitionRange, tablePartitionTemplate, dateColumn, outputFormat, compression string) {
	logger.Debug("")
	logger.Debug("📋 Configuration:")
	logger.Debug("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// Database configuration
	logger.Debug("  Database:")
	logger.Debug(fmt.Sprintf("    Host:             %s", config.Database.Host))
	logger.Debug(fmt.Sprintf("    Port:             %d", config.Database.Port))
	logger.Debug(fmt.Sprintf("    User:             %s", maskString(config.Database.User)))
	logger.Debug(fmt.Sprintf("    Database:          %s", config.Database.Name))
	logger.Debug(fmt.Sprintf("    SSL Mode:          %s", config.Database.SSLMode))
	logger.Debug(fmt.Sprintf("    Statement Timeout: %d seconds", config.Database.StatementTimeout))
	logger.Debug(fmt.Sprintf("    Max Retries:       %d", config.Database.MaxRetries))
	logger.Debug(fmt.Sprintf("    Retry Delay:       %d seconds", config.Database.RetryDelay))

	// S3 configuration
	logger.Debug("  S3:")
	logger.Debug(fmt.Sprintf("    Endpoint:          %s", config.S3.Endpoint))
	logger.Debug(fmt.Sprintf("    Bucket:            %s", config.S3.Bucket))
	logger.Debug(fmt.Sprintf("    Access Key:        %s", maskString(config.S3.AccessKey)))
	logger.Debug(fmt.Sprintf("    Region:            %s", config.S3.Region))
	logger.Debug(fmt.Sprintf("    Path Template:     %s", config.S3.PathTemplate))

	// Restore configuration
	logger.Debug("  Restore:")
	logger.Debug(fmt.Sprintf("    Table:             %s", config.Table))
	if config.StartDate != "" {
		logger.Debug(fmt.Sprintf("    Start Date:        %s", config.StartDate))
	}
	if config.EndDate != "" {
		logger.Debug(fmt.Sprintf("    End Date:          %s", config.EndDate))
	}
	if partitionRange != "" {
		logger.Debug(fmt.Sprintf("    Partition Range:   %s", partitionRange))
	}
	if tablePartitionTemplate != "" {
		logger.Debug(fmt.Sprintf("    Table Partition Template: %s", tablePartitionTemplate))
	}
	if dateColumn != "" {
		logger.Debug(fmt.Sprintf("    Date Column:          %s", dateColumn))
	}
	if outputFormat != "" {
		logger.Debug(fmt.Sprintf("    Output Format:     %s (override)", outputFormat))
	} else {
		logger.Debug("    Output Format:     auto-detect")
	}
	if compression != "" {
		logger.Debug(fmt.Sprintf("    Compression:       %s (override)", compression))
	} else {
		logger.Debug("    Compression:       auto-detect")
	}

	// General settings
	logger.Debug("  Settings:")
	logger.Debug(fmt.Sprintf("    Dry Run:           %v", config.DryRun))
	logger.Debug(fmt.Sprintf("    Debug:             %v", config.Debug))
	logger.Debug(fmt.Sprintf("    Log Format:        %s", config.LogFormat))
	logger.Debug("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Debug("")
}

// maskString masks sensitive strings (shows first 4 chars, rest as *)
func maskString(s string) string {
	if s == "" {
		return "(not set)"
	}
	if len(s) <= 4 {
		return "****"
	}
	return s[:4] + strings.Repeat("*", len(s)-4)
}

// validateRestoreConfig validates restore-specific configuration
func validateRestoreConfig(config *Config, partitionRange string) error {
	if config.Table == "" {
		return errors.New("table name is required for restore")
	}
	if config.S3.PathTemplate == "" {
		return errors.New("path template is required for restore")
	}
	if !isValidTableName(config.Table) {
		return fmt.Errorf("%w: '%s'", ErrTableNameInvalid, config.Table)
	}
	if partitionRange != "" {
		validRanges := map[string]bool{
			"hourly":    true,
			"daily":     true,
			"monthly":   true,
			"quarterly": true,
			"yearly":    true,
		}
		if !validRanges[partitionRange] {
			return fmt.Errorf("invalid partition range: %s (must be: hourly, daily, monthly, quarterly, yearly)", partitionRange)
		}
	}
	return nil
}

// connect connects to database and S3
func (r *Restorer) connect(ctx context.Context) error {
	sslMode := r.config.Database.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		r.config.Database.Host,
		r.config.Database.Port,
		r.config.Database.User,
		r.config.Database.Password,
		r.config.Database.Name,
		sslMode,
	)

	if r.config.Database.StatementTimeout > 0 {
		timeoutMs := r.config.Database.StatementTimeout * 1000
		connStr += fmt.Sprintf(" statement_timeout=%d", timeoutMs)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return err
	}

	r.db = db

	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(r.config.S3.Endpoint),
		Region:           aws.String(r.config.S3.Region),
		Credentials:      credentials.NewStaticCredentials(r.config.S3.AccessKey, r.config.S3.SecretKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		db.Close()
		r.db = nil
		return fmt.Errorf("failed to create S3 session: %w", err)
	}

	r.s3Client = s3.New(sess)
	r.s3Downloader = s3manager.NewDownloader(sess)

	return nil
}

// detectFormatAndCompression detects format and compression from filename
func detectFormatAndCompression(filename string, overrideFormat, overrideCompression string) (format string, compression string, err error) {
	// Use overrides if provided
	if overrideFormat != "" {
		format = overrideFormat
	} else {
		// Detect format from extension
		ext := filepath.Ext(filename)
		switch ext {
		case ".jsonl":
			format = "jsonl"
		case ".csv":
			format = "csv"
		case ".parquet":
			format = "parquet"
		default:
			// Try without compression extension
			baseExt := strings.TrimSuffix(filename, ".zst")
			baseExt = strings.TrimSuffix(baseExt, ".lz4")
			baseExt = strings.TrimSuffix(baseExt, ".gz")
			baseExt = filepath.Ext(baseExt)
			switch baseExt {
			case ".jsonl":
				format = "jsonl"
			case ".csv":
				format = "csv"
			case ".parquet":
				format = "parquet"
			default:
				return "", "", fmt.Errorf("unable to detect format from filename: %s", filename)
			}
		}
	}

	if overrideCompression != "" {
		compression = overrideCompression
	} else {
		// Detect compression from extension
		lowerFilename := strings.ToLower(filename)
		if strings.HasSuffix(lowerFilename, ".zst") {
			compression = "zstd"
		} else if strings.HasSuffix(lowerFilename, ".lz4") {
			compression = "lz4"
		} else if strings.HasSuffix(lowerFilename, ".gz") {
			compression = "gzip"
		} else {
			compression = "none"
		}
	}

	return format, compression, nil
}

// extractDateFromFilename extracts date from filename patterns
func extractDateFromFilename(filename string) (time.Time, bool) {
	// Pattern 1: table-YYYY-MM-DD or table-YYYY-MM-DD-HH
	re1 := regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})(?:-(\d{2}))?`)
	matches := re1.FindStringSubmatch(filename)
	if len(matches) >= 4 {
		year := matches[1]
		month := matches[2]
		day := matches[3]
		hour := "00"
		if len(matches) >= 5 && matches[4] != "" {
			hour = matches[4]
		}
		dateStr := fmt.Sprintf("%s-%s-%sT%s:00:00Z", year, month, day, hour)
		if t, err := time.Parse(time.RFC3339, dateStr); err == nil {
			return t, true
		}
	}

	// Pattern 2: table-YYYYMMDD
	re2 := regexp.MustCompile(`(\d{8})`)
	matches = re2.FindStringSubmatch(filename)
	if len(matches) >= 2 {
		if t, err := time.Parse("20060102", matches[1]); err == nil {
			return t, true
		}
	}

	// Pattern 3: table-YYYY-MM (monthly)
	re3 := regexp.MustCompile(`(\d{4})-(\d{2})(?:-(\d{2}))?`)
	matches = re3.FindStringSubmatch(filename)
	if len(matches) >= 3 {
		year := matches[1]
		month := matches[2]
		day := "01"
		if len(matches) >= 4 && matches[3] != "" {
			day = matches[3]
		}
		dateStr := fmt.Sprintf("%s-%s-%sT00:00:00Z", year, month, day)
		if t, err := time.Parse(time.RFC3339, dateStr); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

// discoverS3Files discovers files in S3 matching the path template and date range
func (r *Restorer) discoverS3Files(ctx context.Context, tableName string, startDate, endDate *time.Time) ([]S3File, error) {
	// Build base path from template (replace {table} placeholder)
	basePath := strings.ReplaceAll(r.config.S3.PathTemplate, "{table}", tableName)

	// Remove date placeholders for listing (we'll match files by pattern)
	listPrefix := basePath
	// Remove date placeholders to get a prefix for listing
	listPrefix = regexp.MustCompile(`\{YYYY\}|\{MM\}|\{DD\}|\{HH\}`).ReplaceAllString(listPrefix, "")

	// Clean up double slashes and ensure proper prefix format
	listPrefix = regexp.MustCompile(`/+`).ReplaceAllString(listPrefix, "/")
	// Remove trailing slash if present (S3 listing works better without it for recursive listing)
	listPrefix = strings.TrimSuffix(listPrefix, "/")

	r.logger.Debug(fmt.Sprintf("Discovering files in S3 with prefix: %s", listPrefix))

	var files []S3File
	var continuationToken *string

	for {
		listInput := &s3.ListObjectsV2Input{
			Bucket:            aws.String(r.config.S3.Bucket),
			Prefix:            aws.String(listPrefix),
			ContinuationToken: continuationToken,
		}

		result, err := r.s3Client.ListObjectsV2WithContext(ctx, listInput)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		r.logger.Debug(fmt.Sprintf("ListObjectsV2 returned %d objects", len(result.Contents)))

		for _, obj := range result.Contents {
			key := aws.StringValue(obj.Key)

			r.logger.Debug(fmt.Sprintf("Found S3 object: %s", key))

			// Skip directories
			if strings.HasSuffix(key, "/") {
				r.logger.Debug(fmt.Sprintf("Skipping directory: %s", key))
				continue
			}

			// Extract date from filename
			filename := filepath.Base(key)
			fileDate, ok := extractDateFromFilename(filename)
			if !ok {
				r.logger.Debug(fmt.Sprintf("Skipping file %s: unable to extract date from filename %s", key, filename))
				continue
			}

			r.logger.Debug(fmt.Sprintf("Extracted date %s from filename %s", fileDate.Format("2006-01-02"), filename))

			// Filter by date range
			if startDate != nil && fileDate.Before(*startDate) {
				r.logger.Debug(fmt.Sprintf("Skipping file %s: date %s is before start date %s", key, fileDate.Format("2006-01-02"), startDate.Format("2006-01-02")))
				continue
			}
			if endDate != nil && fileDate.After(*endDate) {
				r.logger.Debug(fmt.Sprintf("Skipping file %s: date %s is after end date %s", key, fileDate.Format("2006-01-02"), endDate.Format("2006-01-02")))
				continue
			}

			// Detect format and compression
			format, compression, err := detectFormatAndCompression(filename, "", "")
			if err != nil {
				r.logger.Debug(fmt.Sprintf("Skipping file %s: %v", key, err))
				continue
			}

			files = append(files, S3File{
				Key:                 key,
				Size:                aws.Int64Value(obj.Size),
				LastModified:        aws.TimeValue(obj.LastModified),
				DetectedFormat:      format,
				DetectedCompression: compression,
				Date:                fileDate,
			})
		}

		if !aws.BoolValue(result.IsTruncated) {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	r.logger.Info(fmt.Sprintf("Found %d files to restore", len(files)))
	return files, nil
}

// inferTableSchema infers table schema from sample rows
func (r *Restorer) inferTableSchema(ctx context.Context, rows []map[string]interface{}) (*TableSchema, error) {
	if len(rows) == 0 {
		return nil, errors.New("cannot infer schema from empty rows")
	}

	// Get all column names from first row
	columnMap := make(map[string]bool)
	for _, row := range rows {
		for col := range row {
			columnMap[col] = true
		}
	}

	// Build column info
	columns := make([]ColumnInfo, 0, len(columnMap))
	for colName := range columnMap {
		// Determine type from sample values
		var pgType string
		var foundValue bool

		for _, row := range rows {
			if val, ok := row[colName]; ok && val != nil {
				foundValue = true
				pgType = inferPostgreSQLType(val)
				break
			}
		}

		if !foundValue {
			pgType = "text" // Default to text if all nulls
		}

		columns = append(columns, ColumnInfo{
			Name:     colName,
			DataType: pgType,
			UDTName:  pgType,
		})
	}

	// Sort columns for consistency
	sortColumns(columns)

	return &TableSchema{
		TableName: r.config.Table,
		Columns:   columns,
	}, nil
}

// inferPostgreSQLType infers PostgreSQL type from Go value
func inferPostgreSQLType(value interface{}) string {
	switch value.(type) {
	case bool:
		return "bool"
	case int, int8, int16, int32:
		return "int4"
	case int64:
		return "int8"
	case float32:
		return "float4"
	case float64:
		return "float8"
	case string:
		// Try to parse as timestamp
		if str, ok := value.(string); ok {
			if _, err := time.Parse(time.RFC3339, str); err == nil {
				return "timestamptz"
			}
		}
		return "text"
	case []byte:
		return "bytea"
	case time.Time:
		return "timestamptz"
	default:
		return "text"
	}
}

// sortColumns sorts columns by name
func sortColumns(columns []ColumnInfo) {
	// Simple bubble sort (columns should be small)
	for i := 0; i < len(columns); i++ {
		for j := i + 1; j < len(columns); j++ {
			if columns[i].Name > columns[j].Name {
				columns[i], columns[j] = columns[j], columns[i]
			}
		}
	}
}

// ensureTableExists ensures the table exists, creating it if necessary
func (r *Restorer) ensureTableExists(ctx context.Context, tableName string, schema *TableSchema) error {
	// Check if table exists
	var exists bool
	checkQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)
	`
	err := r.db.QueryRowContext(ctx, checkQuery, tableName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists {
		r.logger.Debug(fmt.Sprintf("Table %s already exists", tableName))
		return nil
	}

	// Create table
	r.logger.Info(fmt.Sprintf("Creating table %s", tableName))

	var columnDefs []string
	for _, col := range schema.Columns {
		colDef := fmt.Sprintf("%s %s", pq.QuoteIdentifier(col.Name), mapPostgreSQLTypeToSQLType(col.UDTName))
		columnDefs = append(columnDefs, colDef)
	}

	createQuery := fmt.Sprintf(
		"CREATE TABLE %s (%s)",
		pq.QuoteIdentifier(tableName),
		strings.Join(columnDefs, ", "),
	)

	if r.config.DryRun {
		r.logger.Info(fmt.Sprintf("[DRY RUN] Would execute: %s", createQuery))
		return nil
	}

	_, err = r.db.ExecContext(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	r.logger.Info(fmt.Sprintf("✅ Created table %s", tableName))
	return nil
}

// mapPostgreSQLTypeToSQLType maps PostgreSQL UDT name to SQL type
func mapPostgreSQLTypeToSQLType(udtName string) string {
	switch udtName {
	case "int2":
		return "SMALLINT"
	case "int4":
		return "INTEGER"
	case "int8":
		return "BIGINT"
	case "float4":
		return "REAL"
	case "float8", "numeric", "decimal":
		return "DOUBLE PRECISION"
	case "bool":
		return "BOOLEAN"
	case "timestamp", "timestamptz":
		return "TIMESTAMP WITH TIME ZONE"
	case "date":
		return "DATE"
	case "text", "varchar", "char", "bpchar":
		return "TEXT"
	case "json", "jsonb":
		return "JSONB"
	case "uuid":
		return "UUID"
	case "bytea":
		return "BYTEA"
	default:
		return "TEXT"
	}
}

// generatePartitionName generates a partition name from a template or default pattern
func generatePartitionName(baseTable string, partitionDate time.Time, partitionRange string, template string) string {
	// If template is provided, use it
	if template != "" {
		result := template
		result = strings.ReplaceAll(result, "{table}", baseTable)
		result = strings.ReplaceAll(result, "{YYYY}", partitionDate.Format("2006"))
		result = strings.ReplaceAll(result, "{MM}", partitionDate.Format("01"))
		result = strings.ReplaceAll(result, "{DD}", partitionDate.Format("02"))
		result = strings.ReplaceAll(result, "{HH}", partitionDate.Format("15"))

		// Calculate quarter
		quarter := (int(partitionDate.Month())-1)/3 + 1
		result = strings.ReplaceAll(result, "{Q}", fmt.Sprintf("%d", quarter))

		return result
	}

	// Otherwise use default pattern based on range
	switch partitionRange {
	case "hourly":
		return fmt.Sprintf("%s_%s", baseTable, partitionDate.Format("2006010215"))
	case "daily":
		return fmt.Sprintf("%s_%s", baseTable, partitionDate.Format("20060102"))
	case "monthly":
		return fmt.Sprintf("%s_%s", baseTable, partitionDate.Format("200601"))
	case "quarterly":
		quarter := (int(partitionDate.Month())-1)/3 + 1
		return fmt.Sprintf("%s_%dQ%d", baseTable, partitionDate.Year(), quarter)
	case "yearly":
		return fmt.Sprintf("%s_%d", baseTable, partitionDate.Year())
	default:
		// Default to daily if range is unknown
		return fmt.Sprintf("%s_%s", baseTable, partitionDate.Format("20060102"))
	}
}

// ensurePartitionExists ensures a partition exists for the given date and range
func (r *Restorer) ensurePartitionExists(ctx context.Context, baseTable string, partitionDate time.Time, partitionRange string, partitionTemplate string) error {
	if partitionRange == "" {
		// No partitioning, use base table
		return nil
	}

	// Generate partition name
	partitionName := generatePartitionName(baseTable, partitionDate, partitionRange, partitionTemplate)

	// Check if partition exists
	var exists bool
	checkQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)
	`
	err := r.db.QueryRowContext(ctx, checkQuery, partitionName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if partition exists: %w", err)
	}

	if exists {
		r.logger.Debug(fmt.Sprintf("Partition %s already exists", partitionName))
		return nil
	}

	// Get base table schema
	schema, err := r.getTableSchema(ctx, baseTable)
	if err != nil {
		return fmt.Errorf("failed to get base table schema: %w", err)
	}

	// Create partition table
	r.logger.Info(fmt.Sprintf("Creating partition %s", partitionName))

	var columnDefs []string
	for _, col := range schema.Columns {
		colDef := fmt.Sprintf("%s %s", pq.QuoteIdentifier(col.Name), mapPostgreSQLTypeToSQLType(col.UDTName))
		columnDefs = append(columnDefs, colDef)
	}

	// Use PostgreSQL inheritance for partitions
	createQuery := fmt.Sprintf(
		"CREATE TABLE %s (LIKE %s INCLUDING ALL)",
		pq.QuoteIdentifier(partitionName),
		pq.QuoteIdentifier(baseTable),
	)

	if r.config.DryRun {
		r.logger.Info(fmt.Sprintf("[DRY RUN] Would execute: %s", createQuery))
		return nil
	}

	_, err = r.db.ExecContext(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("failed to create partition: %w", err)
	}

	r.logger.Info(fmt.Sprintf("✅ Created partition %s", partitionName))
	return nil
}

// getTableSchema gets schema for an existing table
func (r *Restorer) getTableSchema(ctx context.Context, tableName string) (*TableSchema, error) {
	query := `
		SELECT column_name, data_type, udt_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := r.db.QueryContext(ctx, query, tableName)
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
		return nil, fmt.Errorf("%w: %s", ErrTableNotFoundOrEmpty, tableName)
	}

	return schema, nil
}

// insertRowsByHour splits rows by timestamp and inserts into hourly partitions
func (r *Restorer) insertRowsByHour(ctx context.Context, baseTable string, rows []map[string]interface{}, schema *TableSchema, partitionRange string, partitionTemplate string, dateColumn string) error {
	// Group rows by hour
	rowsByHour := make(map[time.Time][]map[string]interface{})

	for _, row := range rows {
		// Extract timestamp from date column
		dateValue, ok := row[dateColumn]
		if !ok {
			r.logger.Debug(fmt.Sprintf("Row missing date column %s, skipping", dateColumn))
			continue
		}

		// Convert to time.Time
		var rowTime time.Time
		switch v := dateValue.(type) {
		case time.Time:
			rowTime = v
		case string:
			// Try parsing common timestamp formats
			parsed, err := time.Parse(time.RFC3339, v)
			if err != nil {
				parsed, err = time.Parse("2006-01-02 15:04:05", v)
				if err != nil {
					parsed, err = time.Parse("2006-01-02T15:04:05", v)
					if err != nil {
						parsed, err = time.Parse("2006-01-02T15:04:05Z", v)
						if err != nil {
							r.logger.Debug(fmt.Sprintf("Unable to parse timestamp %v, skipping row", v))
							continue
						}
					}
				}
			}
			rowTime = parsed
		case int64:
			// Could be Unix timestamp (seconds) or Parquet timestamp (microseconds)
			// Parquet timestamps are typically microseconds, but check magnitude
			// If > 1e12, it's likely microseconds (year 2001+), otherwise seconds
			if v > 1e12 {
				// Parquet timestamp: microseconds since Unix epoch
				rowTime = time.Unix(0, v*1000) // Convert microseconds to nanoseconds
			} else {
				// Unix timestamp: seconds since epoch
				rowTime = time.Unix(v, 0)
			}
		case int32:
			// Could be Unix timestamp (seconds) or Parquet date (days since epoch)
			// If < 1e9, it's likely days since epoch (Parquet date)
			if v < 1000000 {
				// Parquet date: days since Unix epoch (1970-01-01)
				rowTime = time.Unix(int64(v)*86400, 0)
			} else {
				// Unix timestamp: seconds since epoch
				rowTime = time.Unix(int64(v), 0)
			}
		case float64:
			// Unix timestamp as float (seconds) or Parquet timestamp (microseconds)
			if v > 1e12 {
				// Parquet timestamp: microseconds
				rowTime = time.Unix(0, int64(v)*1000)
			} else {
				// Unix timestamp: seconds
				rowTime = time.Unix(int64(v), 0)
			}
		default:
			r.logger.Debug(fmt.Sprintf("Unknown timestamp type %T for value %v, skipping row", v, v))
			continue
		}

		// Round down to hour
		hourStart := time.Date(rowTime.Year(), rowTime.Month(), rowTime.Day(), rowTime.Hour(), 0, 0, 0, rowTime.Location())
		rowsByHour[hourStart] = append(rowsByHour[hourStart], row)
	}

	// Insert rows into each hourly partition
	for hourStart, hourRows := range rowsByHour {
		// Ensure partition exists
		if err := r.ensurePartitionExists(ctx, baseTable, hourStart, partitionRange, partitionTemplate); err != nil {
			return fmt.Errorf("failed to ensure partition exists for %s: %w", hourStart.Format("2006-01-02 15:04"), err)
		}

		// Generate partition name
		targetTable := generatePartitionName(baseTable, hourStart, partitionRange, partitionTemplate)

		// Insert rows
		r.logger.Info(fmt.Sprintf("Inserting %d rows into %s (hour: %s)", len(hourRows), targetTable, hourStart.Format("2006-01-02 15:04")))
		if err := r.insertRows(ctx, targetTable, hourRows, schema); err != nil {
			return fmt.Errorf("failed to insert rows into %s: %w", targetTable, err)
		}
	}

	return nil
}

// insertRows inserts rows into the table with ON CONFLICT DO NOTHING
func (r *Restorer) insertRows(ctx context.Context, tableName string, rows []map[string]interface{}, schema *TableSchema) error {
	if len(rows) == 0 {
		return nil
	}

	// Build column names
	columnNames := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		columnNames[i] = pq.QuoteIdentifier(col.Name)
	}

	// Build INSERT statement with ON CONFLICT DO NOTHING
	// Note: This assumes there's a primary key or unique constraint
	// For now, we'll use a simple approach and let PostgreSQL handle conflicts
	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	insertQuery := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		pq.QuoteIdentifier(tableName),
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "),
	)

	if r.config.DryRun {
		r.logger.Info(fmt.Sprintf("[DRY RUN] Would insert %d rows into %s", len(rows), tableName))
		return nil
	}

	// Use batch inserts for performance
	batchSize := 1000
	totalInserted := 0

	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[i:end]
		inserted := 0

		for _, row := range batch {
			values := make([]interface{}, len(columnNames))
			for j, col := range schema.Columns {
				val := row[col.Name]
				values[j] = convertValueForPostgreSQL(val, col.UDTName)
			}

			result, err := r.db.ExecContext(ctx, insertQuery, values...)
			if err != nil {
				return fmt.Errorf("failed to insert row: %w", err)
			}

			rowsAffected, _ := result.RowsAffected()
			inserted += int(rowsAffected)
		}

		totalInserted += inserted
	}

	r.logger.Debug(fmt.Sprintf("Inserted %d/%d rows into %s", totalInserted, len(rows), tableName))
	return nil
}

// convertValueForPostgreSQL converts a Go value to PostgreSQL-compatible type
func convertValueForPostgreSQL(value interface{}, pgType string) interface{} {
	if value == nil {
		return nil
	}

	// Handle time.Time
	if t, ok := value.(time.Time); ok {
		return t
	}

	// Handle string timestamps
	if str, ok := value.(string); ok {
		if pgType == "timestamptz" || pgType == "timestamp" {
			if t, err := time.Parse(time.RFC3339, str); err == nil {
				return t
			}
		}
	}

	return value
}

// Run executes the restore process
func (r *Restorer) Run(ctx context.Context, restoreConfig map[string]string) error {
	r.ctx = ctx

	// Connect to database and S3
	r.logger.Debug("Connecting to database and S3...")
	if err := r.connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer func() {
		if r.db != nil {
			r.db.Close()
		}
	}()

	r.logger.Info("✅ Connected to database and S3")

	// Parse date range
	var startDate, endDate *time.Time
	if r.config.StartDate != "" {
		if t, err := time.Parse("2006-01-02", r.config.StartDate); err == nil {
			startDate = &t
		}
	}
	if r.config.EndDate != "" {
		if t, err := time.Parse("2006-01-02", r.config.EndDate); err == nil {
			endDate = &t
		}
	}

	// Discover S3 files
	r.logger.Info("Discovering files in S3...")
	files, err := r.discoverS3Files(ctx, r.config.Table, startDate, endDate)
	if err != nil {
		return fmt.Errorf("failed to discover files: %w", err)
	}

	if len(files) == 0 {
		r.logger.Info("No files found to restore")
		return nil
	}

	// Process files sequentially
	partitionRange := restoreConfig["table_partition_range"]
	overrideFormat := restoreConfig["output_format"]
	overrideCompression := restoreConfig["compression"]

	var inferredSchema *TableSchema

	for i, file := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		r.logger.Info(fmt.Sprintf("Processing file %d/%d: %s", i+1, len(files), file.Key))

		// Download file
		r.logger.Debug(fmt.Sprintf("Downloading %s", file.Key))
		tempFile, err := os.CreateTemp("", "restore-*.tmp")
		if err != nil {
			r.logger.Error(fmt.Sprintf("Failed to create temp file: %v", err))
			continue
		}
		tempPath := tempFile.Name()
		defer os.Remove(tempPath)
		defer tempFile.Close()

		_, err = r.s3Downloader.DownloadWithContext(ctx, tempFile, &s3.GetObjectInput{
			Bucket: aws.String(r.config.S3.Bucket),
			Key:    aws.String(file.Key),
		})
		if err != nil {
			r.logger.Error(fmt.Sprintf("Failed to download %s: %v", file.Key, err))
			continue
		}

		// Reopen file for reading
		tempFile.Close()
		fileReader, err := os.Open(tempPath)
		if err != nil {
			r.logger.Error(fmt.Sprintf("Failed to open temp file: %v", err))
			continue
		}
		defer fileReader.Close()

		// Detect format/compression (use detected or override)
		format := file.DetectedFormat
		compression := file.DetectedCompression
		if overrideFormat != "" {
			format = overrideFormat
		}
		if overrideCompression != "" {
			compression = overrideCompression
		}

		// Decompress
		compressor, err := compressors.GetCompressor(compression)
		if err != nil {
			r.logger.Error(fmt.Sprintf("Failed to get compressor: %v", err))
			continue
		}

		decompressedReader, err := compressor.NewReader(fileReader)
		if err != nil {
			r.logger.Error(fmt.Sprintf("Failed to create decompression reader: %v", err))
			continue
		}
		defer decompressedReader.Close()

		// Parse format
		var rows []map[string]interface{}
		switch format {
		case "jsonl":
			reader := formatters.NewJSONLReaderWithCloser(decompressedReader)
			rows, err = reader.ReadAll()
			if err != nil {
				r.logger.Error(fmt.Sprintf("Failed to read JSONL: %v", err))
				continue
			}
		case "csv":
			reader, err := formatters.NewCSVReaderWithCloser(decompressedReader)
			if err != nil {
				r.logger.Error(fmt.Sprintf("Failed to create CSV reader: %v", err))
				continue
			}
			rows, err = reader.ReadAll()
			if err != nil {
				r.logger.Error(fmt.Sprintf("Failed to read CSV: %v", err))
				continue
			}
		case "parquet":
			reader, err := formatters.NewParquetReaderWithCloser(decompressedReader)
			if err != nil {
				r.logger.Error(fmt.Sprintf("Failed to create Parquet reader: %v", err))
				continue
			}
			rows, err = reader.ReadAll()
			if err != nil {
				r.logger.Error(fmt.Sprintf("Failed to read Parquet: %v", err))
				continue
			}
		default:
			r.logger.Error(fmt.Sprintf("Unsupported format: %s", format))
			continue
		}

		if len(rows) == 0 {
			r.logger.Debug(fmt.Sprintf("No rows in file %s", file.Key))
			continue
		}

		// Infer schema if first file
		if inferredSchema == nil {
			r.logger.Debug("Inferring table schema from data...")
			inferredSchema, err = r.inferTableSchema(ctx, rows)
			if err != nil {
				r.logger.Error(fmt.Sprintf("Failed to infer schema: %v", err))
				continue
			}

			// Ensure base table exists
			if err := r.ensureTableExists(ctx, r.config.Table, inferredSchema); err != nil {
				r.logger.Error(fmt.Sprintf("Failed to ensure table exists: %v", err))
				continue
			}
		}

		// Determine target table (base or partition)
		dateColumn := restoreConfig["date_column"]
		if partitionRange != "" && dateColumn != "" && partitionRange == "hourly" {
			// Split rows by timestamp into hourly partitions
			if err := r.insertRowsByHour(ctx, r.config.Table, rows, inferredSchema, partitionRange, restoreConfig["table_partition_template"], dateColumn); err != nil {
				r.logger.Error(fmt.Sprintf("Failed to insert rows by hour: %v", err))
				continue
			}
			r.logger.Info(fmt.Sprintf("✅ Processed %s (%d rows)", file.Key, len(rows)))
		} else {
			// Single partition or no date column - insert all rows into one partition
			targetTable := r.config.Table
			if partitionRange != "" {
				partitionTemplate := restoreConfig["table_partition_template"]
				// Ensure partition exists
				if err := r.ensurePartitionExists(ctx, r.config.Table, file.Date, partitionRange, partitionTemplate); err != nil {
					r.logger.Error(fmt.Sprintf("Failed to ensure partition exists: %v", err))
					continue
				}
				// Generate partition name using template or default
				targetTable = generatePartitionName(r.config.Table, file.Date, partitionRange, partitionTemplate)
			}

			// Insert rows
			r.logger.Info(fmt.Sprintf("Inserting %d rows into %s", len(rows), targetTable))
			if err := r.insertRows(ctx, targetTable, rows, inferredSchema); err != nil {
				r.logger.Error(fmt.Sprintf("Failed to insert rows: %v", err))
				continue
			}

			r.logger.Info(fmt.Sprintf("✅ Processed %s (%d rows)", file.Key, len(rows)))
		}
	}

	r.logger.Info(fmt.Sprintf("✅ Restored %d files", len(files)))
	return nil
}
