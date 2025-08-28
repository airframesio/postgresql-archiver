package cmd

import (
	"testing"
)

func TestConfigValidation(t *testing.T) {
	t.Run("ValidConfig", func(t *testing.T) {
		config := &Config{
			Database: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				Name:     "testdb",
			},
			S3: S3Config{
				Endpoint:  "https://s3.example.com",
				Bucket:    "test-bucket",
				AccessKey: "access123",
				SecretKey: "secret456",
				Region:    "us-east-1",
			},
			Table:     "test_table",
			StartDate: "2024-01-01",
			EndDate:   "2024-01-31",
			Workers:   4,
		}

		err := config.Validate()
		if err != nil {
			t.Fatalf("valid config should not return error: %v", err)
		}
	})

	t.Run("MissingDatabaseUser", func(t *testing.T) {
		config := &Config{
			Database: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				Password: "testpass",
				Name:     "testdb",
				// User is missing
			},
			S3: S3Config{
				Endpoint:  "https://s3.example.com",
				Bucket:    "test-bucket",
				AccessKey: "access123",
				SecretKey: "secret456",
				Region:    "us-east-1",
			},
			Table: "test_table",
		}

		err := config.Validate()
		if err == nil {
			t.Fatal("should return error for missing database user")
		}
		if err.Error() != "database user is required" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("MissingDatabaseName", func(t *testing.T) {
		config := &Config{
			Database: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				// Name is missing
			},
			S3: S3Config{
				Endpoint:  "https://s3.example.com",
				Bucket:    "test-bucket",
				AccessKey: "access123",
				SecretKey: "secret456",
				Region:    "us-east-1",
			},
			Table: "test_table",
		}

		err := config.Validate()
		if err == nil {
			t.Fatal("should return error for missing database name")
		}
		if err.Error() != "database name is required" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("MissingS3Endpoint", func(t *testing.T) {
		config := &Config{
			Database: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				Name:     "testdb",
			},
			S3: S3Config{
				// Endpoint is missing
				Bucket:    "test-bucket",
				AccessKey: "access123",
				SecretKey: "secret456",
				Region:    "us-east-1",
			},
			Table: "test_table",
		}

		err := config.Validate()
		if err == nil {
			t.Fatal("should return error for missing S3 endpoint")
		}
		if err.Error() != "S3 endpoint is required" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("MissingS3Bucket", func(t *testing.T) {
		config := &Config{
			Database: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				Name:     "testdb",
			},
			S3: S3Config{
				Endpoint:  "https://s3.example.com",
				// Bucket is missing
				AccessKey: "access123",
				SecretKey: "secret456",
				Region:    "us-east-1",
			},
			Table: "test_table",
		}

		err := config.Validate()
		if err == nil {
			t.Fatal("should return error for missing S3 bucket")
		}
		if err.Error() != "S3 bucket is required" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("MissingS3Credentials", func(t *testing.T) {
		config := &Config{
			Database: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				Name:     "testdb",
			},
			S3: S3Config{
				Endpoint: "https://s3.example.com",
				Bucket:   "test-bucket",
				// AccessKey and SecretKey are missing
				Region: "us-east-1",
			},
			Table: "test_table",
		}

		err := config.Validate()
		if err == nil {
			t.Fatal("should return error for missing S3 access key")
		}
		if err.Error() != "S3 access key is required" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("MissingTable", func(t *testing.T) {
		config := &Config{
			Database: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				Name:     "testdb",
			},
			S3: S3Config{
				Endpoint:  "https://s3.example.com",
				Bucket:    "test-bucket",
				AccessKey: "access123",
				SecretKey: "secret456",
				Region:    "us-east-1",
			},
			// Table is missing
		}

		err := config.Validate()
		if err == nil {
			t.Fatal("should return error for missing table")
		}
		if err.Error() != "table name is required" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("InvalidDateFormat", func(t *testing.T) {
		config := &Config{
			Database: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				Name:     "testdb",
			},
			S3: S3Config{
				Endpoint:  "https://s3.example.com",
				Bucket:    "test-bucket",
				AccessKey: "access123",
				SecretKey: "secret456",
				Region:    "us-east-1",
			},
			Table:     "test_table",
			StartDate: "01/01/2024", // Invalid format
		}

		err := config.Validate()
		if err == nil {
			t.Fatal("should return error for invalid date format")
		}
	})

	t.Run("DefaultValues", func(t *testing.T) {
		config := &Config{
			Database: DatabaseConfig{
				// Port should default to 5432
				User:     "testuser",
				Password: "testpass",
				Name:     "testdb",
			},
			S3: S3Config{
				Endpoint:  "https://s3.example.com",
				Bucket:    "test-bucket",
				AccessKey: "access123",
				SecretKey: "secret456",
				// Region should default to "auto"
			},
			Table: "test_table",
			// Workers should default to 4
		}

		// Simulate default values (would be set by viper in real usage)
		if config.Database.Port == 0 {
			config.Database.Port = 5432
		}
		if config.Database.Host == "" {
			config.Database.Host = "localhost"
		}
		if config.S3.Region == "" {
			config.S3.Region = "auto"
		}
		if config.Workers == 0 {
			config.Workers = 4
		}

		err := config.Validate()
		if err != nil {
			t.Fatalf("config with defaults should be valid: %v", err)
		}

		if config.Database.Port != 5432 {
			t.Fatalf("expected default port 5432, got %d", config.Database.Port)
		}
		if config.Database.Host != "localhost" {
			t.Fatalf("expected default host localhost, got %s", config.Database.Host)
		}
		if config.S3.Region != "auto" {
			t.Fatalf("expected default region auto, got %s", config.S3.Region)
		}
		if config.Workers != 4 {
			t.Fatalf("expected default workers 4, got %d", config.Workers)
		}
	})

	t.Run("CacheViewerConfig", func(t *testing.T) {
		config := &Config{
			Database: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				Name:     "testdb",
			},
			S3: S3Config{
				Endpoint:  "https://s3.example.com",
				Bucket:    "test-bucket",
				AccessKey: "access123",
				SecretKey: "secret456",
				Region:    "us-east-1",
			},
			Table:       "test_table",
			Workers:     4,
			CacheViewer: true,
			ViewerPort:  8080,
		}

		err := config.Validate()
		if err != nil {
			t.Fatalf("valid config with cache viewer should not return error: %v", err)
		}

		if !config.CacheViewer {
			t.Fatal("cache viewer should be enabled")
		}
		if config.ViewerPort != 8080 {
			t.Fatalf("expected viewer port 8080, got %d", config.ViewerPort)
		}
	})
}