package cmd

import (
	"fmt"
	"regexp"
	"time"
)

type Config struct {
	Debug       bool
	DryRun      bool
	Workers     int
	SkipCount   bool
	CacheViewer bool
	ViewerPort  int
	Database    DatabaseConfig
	S3          S3Config
	Table       string
	StartDate   string
	EndDate     string
}

type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
	SSLMode  string
}

type S3Config struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
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
	// Empty region is not valid (except for "auto" which is handled separately)
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

func (c *Config) Validate() error {
	// Validate database configuration
	if c.Database.User == "" {
		return fmt.Errorf("database user is required")
	}
	if c.Database.Name == "" {
		return fmt.Errorf("database name is required")
	}

	// Validate database port
	if c.Database.Port < 1 || c.Database.Port > 65535 {
		return fmt.Errorf("database port must be between 1 and 65535, got %d", c.Database.Port)
	}

	// Validate S3 configuration
	if c.S3.Endpoint == "" {
		return fmt.Errorf("S3 endpoint is required")
	}
	if c.S3.Bucket == "" {
		return fmt.Errorf("S3 bucket is required")
	}
	if c.S3.AccessKey == "" {
		return fmt.Errorf("S3 access key is required")
	}
	if c.S3.SecretKey == "" {
		return fmt.Errorf("S3 secret key is required")
	}

	// Validate S3 region
	if c.S3.Region != "" && c.S3.Region != "auto" {
		if !isValidRegion(c.S3.Region) {
			return fmt.Errorf("S3 region contains invalid characters or is too long: %s", c.S3.Region)
		}
	}

	// Validate and sanitize table name to prevent SQL injection
	if c.Table == "" {
		return fmt.Errorf("table name is required")
	}
	if !isValidTableName(c.Table) {
		return fmt.Errorf("table name '%s' is invalid: must be 1-63 characters, start with a letter or underscore, and contain only letters, numbers, and underscores", c.Table)
	}

	// Validate date formats
	if c.StartDate != "" {
		if _, err := time.Parse("2006-01-02", c.StartDate); err != nil {
			return fmt.Errorf("invalid start date format: %v", err)
		}
	}
	if c.EndDate != "" {
		if _, err := time.Parse("2006-01-02", c.EndDate); err != nil {
			return fmt.Errorf("invalid end date format: %v", err)
		}
	}

	// Validate workers count
	if c.Workers < 1 {
		return fmt.Errorf("workers must be at least 1")
	}
	// Prevent integer overflow and excessive resource usage
	// More than 1000 workers is unreasonable and could cause issues
	if c.Workers > 1000 {
		return fmt.Errorf("workers must not exceed 1000, got %d", c.Workers)
	}

	return nil
}
