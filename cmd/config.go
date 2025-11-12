package cmd

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Static errors for configuration validation
var (
	ErrDatabaseUserRequired    = errors.New("database user is required")
	ErrDatabaseNameRequired    = errors.New("database name is required")
	ErrDatabasePortInvalid     = errors.New("database port must be between 1 and 65535")
	ErrStatementTimeoutInvalid = errors.New("database statement timeout must be >= 0")
	ErrMaxRetriesInvalid       = errors.New("database max retries must be >= 0")
	ErrRetryDelayInvalid       = errors.New("database retry delay must be >= 0")
	ErrS3EndpointRequired      = errors.New("S3 endpoint is required")
	ErrS3BucketRequired        = errors.New("S3 bucket is required")
	ErrS3AccessKeyRequired     = errors.New("S3 access key is required")
	ErrS3SecretKeyRequired     = errors.New("S3 secret key is required")
	ErrS3RegionInvalid         = errors.New("S3 region contains invalid characters or is too long")
	ErrTableNameRequired       = errors.New("table name is required")
	ErrTableNameInvalid        = errors.New("table name is invalid: must be 1-63 characters, start with a letter or underscore, and contain only letters, numbers, and underscores")
	ErrStartDateFormatInvalid  = errors.New("invalid start date format")
	ErrEndDateFormatInvalid    = errors.New("invalid end date format")
	ErrWorkersMinimum          = errors.New("workers must be at least 1")
	ErrWorkersMaximum          = errors.New("workers must not exceed 1000")
	ErrChunkSizeMinimum        = errors.New("chunk size must be at least 100")
	ErrChunkSizeMaximum        = errors.New("chunk size must not exceed 1000000")
	ErrPathTemplateRequired    = errors.New("path template is required")
	ErrPathTemplateInvalid     = errors.New("path template must contain {table} placeholder")
	ErrOutputDurationInvalid   = errors.New("output duration must be one of: hourly, daily, weekly, monthly, yearly")
	ErrOutputFormatInvalid     = errors.New("output format must be one of: jsonl, csv, parquet")
	ErrCompressionInvalid      = errors.New("compression must be one of: zstd, lz4, gzip, none")
	ErrCompressionLevelInvalid = errors.New("compression level must be between 1 and 22 (zstd), 1-9 (lz4/gzip)")
	ErrDateColumnInvalid       = errors.New("date column is invalid: must start with a letter or underscore, and contain only letters, numbers, and underscores")
	ErrDumpModeInvalid         = errors.New("dump mode must be one of: schema-only, data-only, schema-and-data")
)

const regionAuto = "auto"

type Config struct {
	Debug            bool
	LogFormat        string
	DryRun           bool
	Workers          int
	SkipCount        bool
	CacheViewer      bool
	ViewerPort       int
	ChunkSize        int // Number of rows to process in each chunk (streaming mode)
	Database         DatabaseConfig
	S3               S3Config
	Table            string
	StartDate        string
	EndDate          string
	OutputDuration   string
	OutputFormat     string
	Compression      string
	CompressionLevel int
	DateColumn       string
	DumpMode         string // pg_dump mode: schema-only, data-only, schema-and-data
}

type DatabaseConfig struct {
	Host             string
	Port             int
	User             string
	Password         string
	Name             string
	SSLMode          string
	StatementTimeout int // Statement timeout in seconds (0 = no timeout, default 300)
	MaxRetries       int // Maximum number of retry attempts for failed queries (default 3)
	RetryDelay       int // Delay in seconds between retry attempts (default 5)
}

type S3Config struct {
	Endpoint     string
	Bucket       string
	AccessKey    string
	SecretKey    string
	Region       string
	PathTemplate string
}

// validPostgreSQLIdentifier checks if a string is a valid PostgreSQL identifier
// to prevent SQL injection attacks
var validPostgreSQLIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// isValidTableName validates that a table name is safe to use in SQL queries
func isValidTableName(name string) bool {
	// Check for empty or excessively long names
	if name == "" || len(name) > 63 {
		return false
	}

	// Must match PostgreSQL identifier rules
	return validPostgreSQLIdentifier.MatchString(name)
}

// isValidRegion validates that an S3 region is reasonable
func isValidRegion(region string) bool {
	// Empty region is not valid (except for regionAuto which is handled separately)
	if region == "" {
		return false
	}

	// Region should be reasonable length
	if len(region) > 50 {
		return false
	}

	// Region should only contain alphanumeric, dash, and underscore
	matched, _ := regexp.MatchString(`^[a-zA-Z0-9_-]+$`, region)
	return matched
}

// isValidPathTemplate validates that a path template contains required placeholders
func isValidPathTemplate(template string) bool {
	if template == "" {
		return false
	}
	// Must contain {table} placeholder
	return regexp.MustCompile(`\{table\}`).MatchString(template)
}

// isValidOutputDuration validates the output duration
func isValidOutputDuration(duration string) bool {
	validDurations := map[string]bool{
		"hourly":  true,
		"daily":   true,
		"weekly":  true,
		"monthly": true,
		"yearly":  true,
	}
	return validDurations[duration]
}

// isValidOutputFormat validates the output format
func isValidOutputFormat(format string) bool {
	validFormats := map[string]bool{
		"jsonl":   true,
		"csv":     true,
		"parquet": true,
	}
	return validFormats[format]
}

// isValidCompression validates the compression type
func isValidCompression(compression string) bool {
	validCompressions := map[string]bool{
		"zstd": true,
		"lz4":  true,
		"gzip": true,
		"none": true,
	}
	return validCompressions[compression]
}

// isValidCompressionLevel validates compression level based on compression type
func isValidCompressionLevel(compression string, level int) bool {
	switch compression {
	case "zstd":
		return level >= 1 && level <= 22
	case "lz4", "gzip":
		return level >= 1 && level <= 9
	case "none":
		return level == 0 // no compression, level should be 0
	default:
		return false
	}
}

// isValidDumpMode validates the dump mode
func isValidDumpMode(mode string) bool {
	validModes := map[string]bool{
		"schema-only":       true,
		"data-only":         true,
		"schema-and-data":   true,
	}
	return validModes[mode]
}

func (c *Config) Validate() error {
	// Validate database configuration
	if c.Database.User == "" {
		return ErrDatabaseUserRequired
	}
	if c.Database.Name == "" {
		return ErrDatabaseNameRequired
	}

	// Validate database port
	if c.Database.Port < 1 || c.Database.Port > 65535 {
		return fmt.Errorf("%w, got %d", ErrDatabasePortInvalid, c.Database.Port)
	}

	// Validate database statement timeout (if set, must be positive)
	if c.Database.StatementTimeout < 0 {
		return fmt.Errorf("%w, got %d", ErrStatementTimeoutInvalid, c.Database.StatementTimeout)
	}

	// Validate database max retries (if set, must be >= 0)
	if c.Database.MaxRetries < 0 {
		return fmt.Errorf("%w, got %d", ErrMaxRetriesInvalid, c.Database.MaxRetries)
	}

	// Validate database retry delay (if set, must be positive)
	if c.Database.RetryDelay < 0 {
		return fmt.Errorf("%w, got %d", ErrRetryDelayInvalid, c.Database.RetryDelay)
	}

	// Validate S3 configuration
	if c.S3.Endpoint == "" {
		return ErrS3EndpointRequired
	}
	if c.S3.Bucket == "" {
		return ErrS3BucketRequired
	}
	if c.S3.AccessKey == "" {
		return ErrS3AccessKeyRequired
	}
	if c.S3.SecretKey == "" {
		return ErrS3SecretKeyRequired
	}

	// Validate S3 region
	if c.S3.Region != "" && c.S3.Region != regionAuto {
		if !isValidRegion(c.S3.Region) {
			return fmt.Errorf("%w: %s", ErrS3RegionInvalid, c.S3.Region)
		}
	}

	// Check if we're in dump mode
	isDumpMode := c.DumpMode != ""

	// Validate and sanitize table name to prevent SQL injection
	// For dump mode, table is optional (can dump all top-level tables)
	// For archive mode, table is required
	if !isDumpMode {
		if c.Table == "" {
			return ErrTableNameRequired
		}
	}
	// If table is provided (in either mode), validate it
	if c.Table != "" && !isValidTableName(c.Table) {
		return fmt.Errorf("%w: '%s'", ErrTableNameInvalid, c.Table)
	}

	// Archive-specific validations (skip for dump mode)
	if !isDumpMode {
		// Validate date formats
		if c.StartDate != "" {
			if _, err := time.Parse("2006-01-02", c.StartDate); err != nil {
				return fmt.Errorf("%w: %w", ErrStartDateFormatInvalid, err)
			}
		}
		if c.EndDate != "" {
			if _, err := time.Parse("2006-01-02", c.EndDate); err != nil {
				return fmt.Errorf("%w: %w", ErrEndDateFormatInvalid, err)
			}
		}

		// Validate chunk size (if set)
		if c.ChunkSize > 0 {
			if c.ChunkSize < 100 {
				return fmt.Errorf("%w, got %d", ErrChunkSizeMinimum, c.ChunkSize)
			}
			if c.ChunkSize > 1000000 {
				return fmt.Errorf("%w, got %d", ErrChunkSizeMaximum, c.ChunkSize)
			}
		}

		// Validate output duration
		if !isValidOutputDuration(c.OutputDuration) {
			return fmt.Errorf("%w: '%s'", ErrOutputDurationInvalid, c.OutputDuration)
		}

		// Validate output format
		if !isValidOutputFormat(c.OutputFormat) {
			return fmt.Errorf("%w: '%s'", ErrOutputFormatInvalid, c.OutputFormat)
		}

		// Validate compression
		if !isValidCompression(c.Compression) {
			return fmt.Errorf("%w: '%s'", ErrCompressionInvalid, c.Compression)
		}

		// Validate compression level
		if !isValidCompressionLevel(c.Compression, c.CompressionLevel) {
			return fmt.Errorf("%w for compression %s: got %d", ErrCompressionLevelInvalid, c.Compression, c.CompressionLevel)
		}

		// Validate date column (if provided, must be valid identifier)
		if c.DateColumn != "" && !validPostgreSQLIdentifier.MatchString(c.DateColumn) {
			return fmt.Errorf("%w: '%s'", ErrDateColumnInvalid, c.DateColumn)
		}
	}

	// Common validations for both modes
	// Validate workers count
	if c.Workers < 1 {
		return ErrWorkersMinimum
	}
	// Prevent integer overflow and excessive resource usage
	// More than 1000 workers is unreasonable and could cause issues
	if c.Workers > 1000 {
		return fmt.Errorf("%w, got %d", ErrWorkersMaximum, c.Workers)
	}

	// Validate path template (required for both modes)
	if c.S3.PathTemplate == "" {
		return ErrPathTemplateRequired
	}
	// For schema-only mode, {table} placeholder is optional (table name goes in filename)
	// For other modes, {table} placeholder is required
	if c.DumpMode != "schema-only" {
		if !isValidPathTemplate(c.S3.PathTemplate) {
			return fmt.Errorf("%w: '%s'", ErrPathTemplateInvalid, c.S3.PathTemplate)
		}
	}

	// Validate dump mode (if provided)
	if c.DumpMode != "" && !isValidDumpMode(c.DumpMode) {
		return fmt.Errorf("%w: '%s'", ErrDumpModeInvalid, c.DumpMode)
	}

	return nil
}
