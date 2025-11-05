package cmd

import (
	"fmt"
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
				Endpoint: "https://s3.example.com",
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

	t.Run("InvalidDatabasePort", func(t *testing.T) {
		testCases := []struct {
			name string
			port int
		}{
			{"zero port", 0},
			{"negative port", -1},
			{"port too large", 65536},
			{"very large port", 100000},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				config := &Config{
					Database: DatabaseConfig{
						Host:     "localhost",
						Port:     tc.port,
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
					Table:   "test_table",
					Workers: 4,
				}

				err := config.Validate()
				if err == nil {
					t.Fatalf("should return error for invalid port %d", tc.port)
				}
			})
		}
	})

	t.Run("InvalidS3Region", func(t *testing.T) {
		testCases := []struct {
			name   string
			region string
		}{
			{"region with spaces", "us east 1"},
			{"region with special chars", "us-east-1!"},
			{"region too long", "this-is-a-very-long-region-name-that-exceeds-the-maximum-allowed-length"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
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
						Region:    tc.region,
					},
					Table:   "test_table",
					Workers: 4,
				}

				err := config.Validate()
				if err == nil {
					t.Fatalf("should return error for invalid region '%s'", tc.region)
				}
			})
		}
	})

	t.Run("ValidS3Regions", func(t *testing.T) {
		testCases := []string{
			"auto",
			"us-east-1",
			"us-west-2",
			"eu-central-1",
			"ap-southeast-1",
		}

		for _, region := range testCases {
			t.Run(region, func(t *testing.T) {
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
						Region:    region,
					},
					Table:   "test_table",
					Workers: 4,
				}

				err := config.Validate()
				if err != nil {
					t.Fatalf("valid region '%s' should not return error: %v", region, err)
				}
			})
		}
	})

	t.Run("InvalidTableNames", func(t *testing.T) {
		testCases := []struct {
			name      string
			tableName string
		}{
			{"starts with number", "1table"},
			{"contains special chars", "table-name"},
			{"contains spaces", "table name"},
			{"SQL injection attempt", "table'; DROP TABLE users--"},
			{"too long", "this_is_a_very_long_table_name_that_exceeds_the_maximum_allowed_length_of_63_characters"},
			{"contains quotes", "table'name"},
			{"contains semicolon", "table;name"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
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
					Table:   tc.tableName,
					Workers: 4,
				}

				err := config.Validate()
				if err == nil {
					t.Fatalf("should return error for invalid table name '%s'", tc.tableName)
				}
			})
		}
	})

	t.Run("ValidTableNames", func(t *testing.T) {
		testCases := []string{
			"test_table",
			"_private_table",
			"TableName",
			"table123",
			"a",
			"_",
		}

		for _, tableName := range testCases {
			t.Run(tableName, func(t *testing.T) {
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
					Table:   tableName,
					Workers: 4,
				}

				err := config.Validate()
				if err != nil {
					t.Fatalf("valid table name '%s' should not return error: %v", tableName, err)
				}
			})
		}
	})

	t.Run("InvalidWorkersCount", func(t *testing.T) {
		testCases := []struct {
			name    string
			workers int
		}{
			{"zero workers", 0},
			{"negative workers", -1},
			{"too many workers", 1001},
			{"excessive workers", 10000},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
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
					Table:   "test_table",
					Workers: tc.workers,
				}

				err := config.Validate()
				if err == nil {
					t.Fatalf("should return error for invalid workers count %d", tc.workers)
				}
			})
		}
	})

	t.Run("ValidWorkersCount", func(t *testing.T) {
		testCases := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000}

		for _, workers := range testCases {
			t.Run(fmt.Sprintf("%d workers", workers), func(t *testing.T) {
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
					Table:   "test_table",
					Workers: workers,
				}

				err := config.Validate()
				if err != nil {
					t.Fatalf("valid workers count %d should not return error: %v", workers, err)
				}
			})
		}
	})
}

func TestTableNameValidation(t *testing.T) {
	t.Run("ValidTableNames", func(t *testing.T) {
		validNames := []string{
			"test_table",
			"_underscore_prefix",
			"CamelCase",
			"table123",
			"a",
			"_",
			"table_with_multiple_underscores",
		}

		for _, name := range validNames {
			if !isValidTableName(name) {
				t.Errorf("table name '%s' should be valid", name)
			}
		}
	})

	t.Run("InvalidTableNames", func(t *testing.T) {
		invalidNames := []string{
			"",
			"123table",
			"table-name",
			"table name",
			"table;drop",
			"table'name",
			"table\"name",
			"table.name",
			string(make([]byte, 64)), // 64 characters - too long
		}

		for _, name := range invalidNames {
			if isValidTableName(name) {
				t.Errorf("table name '%s' should be invalid", name)
			}
		}
	})
}

func TestRegionValidation(t *testing.T) {
	t.Run("ValidRegions", func(t *testing.T) {
		validRegions := []string{
			"us-east-1",
			"us-west-2",
			"eu-central-1",
			"ap-southeast-1",
			"custom_region",
			"region-123",
		}

		for _, region := range validRegions {
			if !isValidRegion(region) {
				t.Errorf("region '%s' should be valid", region)
			}
		}
	})

	t.Run("InvalidRegions", func(t *testing.T) {
		invalidRegions := []string{
			"",
			"us east 1",
			"us-east-1!",
			"region@test",
			string(make([]byte, 51)), // 51 characters - too long
		}

		for _, region := range invalidRegions {
			if isValidRegion(region) {
				t.Errorf("region '%s' should be invalid", region)
			}
		}
	})
}
