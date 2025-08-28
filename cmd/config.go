package cmd

import (
	"fmt"
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
}

type S3Config struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
}

func (c *Config) Validate() error {
	if c.Database.User == "" {
		return fmt.Errorf("database user is required")
	}
	if c.Database.Name == "" {
		return fmt.Errorf("database name is required")
	}
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
	if c.Table == "" {
		return fmt.Errorf("table name is required")
	}

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

	if c.Workers < 1 {
		return fmt.Errorf("workers must be at least 1")
	}

	return nil
}