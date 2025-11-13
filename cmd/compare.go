package cmd

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
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
	// Source 1 flags
	compareSource1Type        string
	compareSource1DbHost      string
	compareSource1DbPort      int
	compareSource1DbUser      string
	compareSource1DbPassword  string
	compareSource1DbName      string
	compareSource1DbSSLMode   string
	compareSource1S3Endpoint   string
	compareSource1S3Bucket     string
	compareSource1S3AccessKey string
	compareSource1S3SecretKey string
	compareSource1S3Region    string
	compareSource1SchemaPath  string
	compareSource1DataPath    string
	compareSource1SchemaSource string // pg_dump, inferred, auto

	// Source 2 flags
	compareSource2Type        string
	compareSource2DbHost      string
	compareSource2DbPort      int
	compareSource2DbUser      string
	compareSource2DbPassword  string
	compareSource2DbName      string
	compareSource2DbSSLMode   string
	compareSource2S3Endpoint   string
	compareSource2S3Bucket     string
	compareSource2S3AccessKey string
	compareSource2S3SecretKey string
	compareSource2S3Region    string
	compareSource2SchemaPath  string
	compareSource2DataPath    string
	compareSource2SchemaSource string // pg_dump, inferred, auto

	// Comparison flags
	compareMode        string // schema-only, data-only, schema-and-data
	dataCompareType    string // row-count, row-by-row, sample
	sampleSize         int
	compareTables      string // comma-separated table names

	// Output flags
	compareOutputFormat string // text, json
	compareOutputFile   string
)

var compareCmd = &cobra.Command{
	Use:   "compare",
	Short: "Compare schemas and data between databases or S3",
	Long:  `Compare schemas and data between two PostgreSQL databases, or between a database and S3 content. Supports schema-only, data-only, or schema-and-data comparisons.`,
	Run: func(cmd *cobra.Command, _ []string) {
		runCompare(cmd)
	},
}

func init() {
	rootCmd.AddCommand(compareCmd)

	// Source 1 flags
	compareCmd.Flags().StringVar(&compareSource1Type, "source1-type", "", "Type of source1: db or s3 (required)")
	compareCmd.Flags().StringVar(&compareSource1DbHost, "source1-db-host", "localhost", "Source1 PostgreSQL host")
	compareCmd.Flags().IntVar(&compareSource1DbPort, "source1-db-port", 5432, "Source1 PostgreSQL port")
	compareCmd.Flags().StringVar(&compareSource1DbUser, "source1-db-user", "", "Source1 PostgreSQL user")
	compareCmd.Flags().StringVar(&compareSource1DbPassword, "source1-db-password", "", "Source1 PostgreSQL password")
	compareCmd.Flags().StringVar(&compareSource1DbName, "source1-db-name", "", "Source1 PostgreSQL database name")
	compareCmd.Flags().StringVar(&compareSource1DbSSLMode, "source1-db-sslmode", "disable", "Source1 PostgreSQL SSL mode")
	compareCmd.Flags().StringVar(&compareSource1S3Endpoint, "source1-s3-endpoint", "", "Source1 S3 endpoint")
	compareCmd.Flags().StringVar(&compareSource1S3Bucket, "source1-s3-bucket", "", "Source1 S3 bucket")
	compareCmd.Flags().StringVar(&compareSource1S3AccessKey, "source1-s3-access-key", "", "Source1 S3 access key")
	compareCmd.Flags().StringVar(&compareSource1S3SecretKey, "source1-s3-secret-key", "", "Source1 S3 secret key")
	compareCmd.Flags().StringVar(&compareSource1S3Region, "source1-s3-region", "auto", "Source1 S3 region")
	compareCmd.Flags().StringVar(&compareSource1SchemaPath, "source1-schema-path", "", "Source1 S3 path for schemas")
	compareCmd.Flags().StringVar(&compareSource1DataPath, "source1-data-path", "", "Source1 S3 path for data")
	compareCmd.Flags().StringVar(&compareSource1SchemaSource, "source1-schema-source", "auto", "Source1 S3 schema source: pg_dump, inferred, auto")

	// Source 2 flags
	compareCmd.Flags().StringVar(&compareSource2Type, "source2-type", "", "Type of source2: db or s3 (required)")
	compareCmd.Flags().StringVar(&compareSource2DbHost, "source2-db-host", "localhost", "Source2 PostgreSQL host")
	compareCmd.Flags().IntVar(&compareSource2DbPort, "source2-db-port", 5432, "Source2 PostgreSQL port")
	compareCmd.Flags().StringVar(&compareSource2DbUser, "source2-db-user", "", "Source2 PostgreSQL user")
	compareCmd.Flags().StringVar(&compareSource2DbPassword, "source2-db-password", "", "Source2 PostgreSQL password")
	compareCmd.Flags().StringVar(&compareSource2DbName, "source2-db-name", "", "Source2 PostgreSQL database name")
	compareCmd.Flags().StringVar(&compareSource2DbSSLMode, "source2-db-sslmode", "disable", "Source2 PostgreSQL SSL mode")
	compareCmd.Flags().StringVar(&compareSource2S3Endpoint, "source2-s3-endpoint", "", "Source2 S3 endpoint")
	compareCmd.Flags().StringVar(&compareSource2S3Bucket, "source2-s3-bucket", "", "Source2 S3 bucket")
	compareCmd.Flags().StringVar(&compareSource2S3AccessKey, "source2-s3-access-key", "", "Source2 S3 access key")
	compareCmd.Flags().StringVar(&compareSource2S3SecretKey, "source2-s3-secret-key", "", "Source2 S3 secret key")
	compareCmd.Flags().StringVar(&compareSource2S3Region, "source2-s3-region", "auto", "Source2 S3 region")
	compareCmd.Flags().StringVar(&compareSource2SchemaPath, "source2-schema-path", "", "Source2 S3 path for schemas")
	compareCmd.Flags().StringVar(&compareSource2DataPath, "source2-data-path", "", "Source2 S3 path for data")
	compareCmd.Flags().StringVar(&compareSource2SchemaSource, "source2-schema-source", "auto", "Source2 S3 schema source: pg_dump, inferred, auto")

	// Comparison flags
	compareCmd.Flags().StringVar(&compareMode, "compare-mode", "schema-and-data", "Comparison mode: schema-only, data-only, schema-and-data")
	compareCmd.Flags().StringVar(&dataCompareType, "data-compare-type", "row-count", "Data comparison type: row-count, row-by-row, sample")
	compareCmd.Flags().IntVar(&sampleSize, "sample-size", 100, "Number of rows for sample comparison")
	compareCmd.Flags().StringVar(&compareTables, "tables", "", "Comma-separated table names to compare (empty = all tables)")

	// Output flags
	compareCmd.Flags().StringVar(&compareOutputFormat, "output-format", "text", "Output format: text, json")
	compareCmd.Flags().StringVar(&compareOutputFile, "output-file", "", "Output file path (default: stdout)")
}

// ComparisonSource represents a source for comparison (database or S3)
type ComparisonSource struct {
	Type        string // "db" or "s3"
	Database    DatabaseConfig
	S3          S3Config
	SchemaPath  string // S3 path for schemas
	DataPath    string // S3 path for data
	SchemaSource string // pg_dump, inferred, auto
}

// ComparisonResult contains the results of a comparison
type ComparisonResult struct {
	Schema *SchemaComparisonResult `json:"schema,omitempty"`
	Data   *DataComparisonResult   `json:"data,omitempty"`
}

// SchemaComparisonResult contains schema comparison results
type SchemaComparisonResult struct {
	TablesOnlyInSource1 []string                    `json:"tables_only_in_source1"`
	TablesOnlyInSource2 []string                    `json:"tables_only_in_source2"`
	TableDiffs          map[string]*TableSchemaDiff `json:"table_diffs"`
}

// TableSchemaDiff contains differences for a single table
type TableSchemaDiff struct {
	ColumnsOnlyInSource1 []ColumnInfo            `json:"columns_only_in_source1"`
	ColumnsOnlyInSource2 []ColumnInfo            `json:"columns_only_in_source2"`
	TypeMismatches       []ColumnTypeMismatch    `json:"type_mismatches"`
}

// ColumnTypeMismatch represents a column type difference
type ColumnTypeMismatch struct {
	ColumnName string `json:"column_name"`
	Source1Type string `json:"source1_type"`
	Source2Type string `json:"source2_type"`
}

// DataComparisonResult contains data comparison results
type DataComparisonResult struct {
	RowCountDiffs map[string]*RowCountDiff `json:"row_count_diffs,omitempty"`
	RowByRowDiffs map[string]*RowByRowDiff `json:"row_by_row_diffs,omitempty"`
	SampleDiffs   map[string]*SampleDiff   `json:"sample_diffs,omitempty"`
}

// RowCountDiff contains row count differences
type RowCountDiff struct {
	Source1Count int64 `json:"source1_count"`
	Source2Count int64 `json:"source2_count"`
	Difference   int64 `json:"difference"`
}

// RowByRowDiff contains row-by-row comparison results
type RowByRowDiff struct {
	Source1TotalRows int64   `json:"source1_total_rows"`
	Source2TotalRows int64   `json:"source2_total_rows"`
	MatchingRows     int64   `json:"matching_rows"`
	MissingInSource2 int64   `json:"missing_in_source2"`
	ExtraInSource2   int64   `json:"extra_in_source2"`
}

// SampleDiff contains sample comparison results
type SampleDiff struct {
	Source1Sample []map[string]interface{} `json:"source1_sample,omitempty"`
	Source2Sample []map[string]interface{} `json:"source2_sample,omitempty"`
	Differences   []string                  `json:"differences"`
}

// Comparer handles comparison operations
type Comparer struct {
	source1      *ComparisonSource
	source2      *ComparisonSource
	config       *CompareConfig
	logger       *slog.Logger
	ctx          context.Context
	db1          *sql.DB
	db2          *sql.DB
	s3Client1    *s3.S3
	s3Client2    *s3.S3
	s3Downloader1 *s3manager.Downloader
	s3Downloader2 *s3manager.Downloader
}

// CompareConfig contains comparison configuration
type CompareConfig struct {
	Mode           string   // schema-only, data-only, schema-and-data
	DataCompareType string   // row-count, row-by-row, sample
	SampleSize     int
	Tables         []string // Empty = all tables
	OutputFormat   string   // text, json
	OutputFile     string
	Debug          bool
	DryRun         bool
}

// NewComparer creates a new Comparer instance
func NewComparer(source1, source2 *ComparisonSource, config *CompareConfig, logger *slog.Logger) *Comparer {
	return &Comparer{
		source1: source1,
		source2: source2,
		config:  config,
		logger:  logger,
	}
}

func runCompare(cmd *cobra.Command) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "\n❌ PANIC: %v\n", r)
			os.Exit(1)
		}
	}()

	// Helper function to get config value: use flag if set, otherwise use viper, fallback to flag default
	getStringConfig := func(flagValue string, flagName string, viperKey string) string {
		if flag := cmd.Flags().Lookup(flagName); flag != nil && flag.Changed {
			return flagValue
		}
		if viperValue := viper.GetString(viperKey); viperValue != "" {
			return viperValue
		}
		// Fallback to flag default value
		return flagValue
	}
	getIntConfig := func(flagValue int, flagName string, viperKey string) int {
		if flag := cmd.Flags().Lookup(flagName); flag != nil && flag.Changed {
			return flagValue
		}
		if viperValue := viper.GetInt(viperKey); viperValue != 0 {
			return viperValue
		}
		// Fallback to flag default value
		return flagValue
	}

	// Build source configurations
	source1 := &ComparisonSource{
		Type: getStringConfig(compareSource1Type, "source1-type", "compare.source1.type"),
		Database: DatabaseConfig{
			Host:    getStringConfig(compareSource1DbHost, "source1-db-host", "compare.source1.db.host"),
			Port:    getIntConfig(compareSource1DbPort, "source1-db-port", "compare.source1.db.port"),
			User:    getStringConfig(compareSource1DbUser, "source1-db-user", "compare.source1.db.user"),
			Password: getStringConfig(compareSource1DbPassword, "source1-db-password", "compare.source1.db.password"),
			Name:    getStringConfig(compareSource1DbName, "source1-db-name", "compare.source1.db.name"),
			SSLMode: getStringConfig(compareSource1DbSSLMode, "source1-db-sslmode", "compare.source1.db.sslmode"),
		},
		S3: S3Config{
			Endpoint:  getStringConfig(compareSource1S3Endpoint, "source1-s3-endpoint", "compare.source1.s3.endpoint"),
			Bucket:    getStringConfig(compareSource1S3Bucket, "source1-s3-bucket", "compare.source1.s3.bucket"),
			AccessKey: getStringConfig(compareSource1S3AccessKey, "source1-s3-access-key", "compare.source1.s3.access_key"),
			SecretKey: getStringConfig(compareSource1S3SecretKey, "source1-s3-secret-key", "compare.source1.s3.secret_key"),
			Region:    getStringConfig(compareSource1S3Region, "source1-s3-region", "compare.source1.s3.region"),
		},
		SchemaPath:   getStringConfig(compareSource1SchemaPath, "source1-schema-path", "compare.source1.schema_path"),
		DataPath:     getStringConfig(compareSource1DataPath, "source1-data-path", "compare.source1.data_path"),
		SchemaSource: getStringConfig(compareSource1SchemaSource, "source1-schema-source", "compare.source1.schema_source"),
	}

	source2 := &ComparisonSource{
		Type: getStringConfig(compareSource2Type, "source2-type", "compare.source2.type"),
		Database: DatabaseConfig{
			Host:    getStringConfig(compareSource2DbHost, "source2-db-host", "compare.source2.db.host"),
			Port:    getIntConfig(compareSource2DbPort, "source2-db-port", "compare.source2.db.port"),
			User:    getStringConfig(compareSource2DbUser, "source2-db-user", "compare.source2.db.user"),
			Password: getStringConfig(compareSource2DbPassword, "source2-db-password", "compare.source2.db.password"),
			Name:    getStringConfig(compareSource2DbName, "source2-db-name", "compare.source2.db.name"),
			SSLMode: getStringConfig(compareSource2DbSSLMode, "source2-db-sslmode", "compare.source2.db.sslmode"),
		},
		S3: S3Config{
			Endpoint:  getStringConfig(compareSource2S3Endpoint, "source2-s3-endpoint", "compare.source2.s3.endpoint"),
			Bucket:    getStringConfig(compareSource2S3Bucket, "source2-s3-bucket", "compare.source2.s3.bucket"),
			AccessKey: getStringConfig(compareSource2S3AccessKey, "source2-s3-access-key", "compare.source2.s3.access_key"),
			SecretKey: getStringConfig(compareSource2S3SecretKey, "source2-s3-secret-key", "compare.source2.s3.secret_key"),
			Region:    getStringConfig(compareSource2S3Region, "source2-s3-region", "compare.source2.s3.region"),
		},
		SchemaPath:   getStringConfig(compareSource2SchemaPath, "source2-schema-path", "compare.source2.schema_path"),
		DataPath:     getStringConfig(compareSource2DataPath, "source2-data-path", "compare.source2.data_path"),
		SchemaSource: getStringConfig(compareSource2SchemaSource, "source2-schema-source", "compare.source2.schema_source"),
	}

	// Parse tables
	var tables []string
	if compareTables != "" {
		tables = strings.Split(compareTables, ",")
		for i := range tables {
			tables[i] = strings.TrimSpace(tables[i])
		}
	}

	config := &CompareConfig{
		Mode:           getStringConfig(compareMode, "compare-mode", "compare.mode"),
		DataCompareType: getStringConfig(dataCompareType, "data-compare-type", "compare.data_compare_type"),
		SampleSize:     getIntConfig(sampleSize, "sample-size", "compare.sample_size"),
		Tables:         tables,
		OutputFormat:   getStringConfig(compareOutputFormat, "output-format", "compare.output_format"),
		OutputFile:     getStringConfig(compareOutputFile, "output-file", "compare.output_file"),
		Debug:          viper.GetBool("debug"),
		DryRun:         viper.GetBool("dry_run"),
	}

	// Initialize logger
	initLogger(config.Debug, viper.GetString("log_format"))

	logger.Info("")
	logger.Info(fmt.Sprintf("🔍 Data Comparer v%s", Version))
	logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// Print configuration table
	printCompareConfig(source1, source2, config)

	// Validate configuration
	if err := validateCompareConfig(source1, source2, config); err != nil {
		logger.Error(fmt.Sprintf("❌ Configuration error: %s", err.Error()))
		os.Exit(1)
	}

	ctx := signalContext
	if ctx == nil {
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
	}

	comparer := NewComparer(source1, source2, config, logger)

	err := comparer.Run(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("")
			logger.Info("⚠️  Comparison cancelled by user")
			os.Exit(130)
		}
		logger.Error(fmt.Sprintf("❌ Comparison failed: %s", err.Error()))
		os.Exit(1)
	}

	logger.Info("")
	logger.Info("✅ Comparison completed successfully!")
}

// printCompareConfig prints a table of configuration information
func printCompareConfig(source1, source2 *ComparisonSource, config *CompareConfig) {
	logger.Info("")
	logger.Info("📋 Configuration:")
	logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// Source 1 configuration
	logger.Info("  Source 1:")
	logger.Info(fmt.Sprintf("    Type:              %s", source1.Type))
	if source1.Type == "db" {
		logger.Info(fmt.Sprintf("    Host:              %s", source1.Database.Host))
		logger.Info(fmt.Sprintf("    Port:              %d", source1.Database.Port))
		logger.Info(fmt.Sprintf("    User:              %s", maskString(source1.Database.User)))
		logger.Info(fmt.Sprintf("    Database:          %s", source1.Database.Name))
		logger.Info(fmt.Sprintf("    SSL Mode:          %s", source1.Database.SSLMode))
	} else {
		logger.Info(fmt.Sprintf("    Endpoint:          %s", source1.S3.Endpoint))
		logger.Info(fmt.Sprintf("    Bucket:            %s", source1.S3.Bucket))
		logger.Info(fmt.Sprintf("    Access Key:        %s", maskString(source1.S3.AccessKey)))
		logger.Info(fmt.Sprintf("    Region:            %s", source1.S3.Region))
		if source1.SchemaPath != "" {
			logger.Info(fmt.Sprintf("    Schema Path:       %s", source1.SchemaPath))
		}
		if source1.DataPath != "" {
			logger.Info(fmt.Sprintf("    Data Path:         %s", source1.DataPath))
		}
		logger.Info(fmt.Sprintf("    Schema Source:     %s", source1.SchemaSource))
	}

	// Source 2 configuration
	logger.Info("  Source 2:")
	logger.Info(fmt.Sprintf("    Type:              %s", source2.Type))
	if source2.Type == "db" {
		logger.Info(fmt.Sprintf("    Host:              %s", source2.Database.Host))
		logger.Info(fmt.Sprintf("    Port:              %d", source2.Database.Port))
		logger.Info(fmt.Sprintf("    User:              %s", maskString(source2.Database.User)))
		logger.Info(fmt.Sprintf("    Database:          %s", source2.Database.Name))
		logger.Info(fmt.Sprintf("    SSL Mode:          %s", source2.Database.SSLMode))
	} else {
		logger.Info(fmt.Sprintf("    Endpoint:          %s", source2.S3.Endpoint))
		logger.Info(fmt.Sprintf("    Bucket:            %s", source2.S3.Bucket))
		logger.Info(fmt.Sprintf("    Access Key:        %s", maskString(source2.S3.AccessKey)))
		logger.Info(fmt.Sprintf("    Region:            %s", source2.S3.Region))
		if source2.SchemaPath != "" {
			logger.Info(fmt.Sprintf("    Schema Path:       %s", source2.SchemaPath))
		}
		if source2.DataPath != "" {
			logger.Info(fmt.Sprintf("    Data Path:         %s", source2.DataPath))
		}
		logger.Info(fmt.Sprintf("    Schema Source:     %s", source2.SchemaSource))
	}

	// Comparison configuration
	logger.Info("  Comparison:")
	logger.Info(fmt.Sprintf("    Mode:              %s", config.Mode))
	if config.Mode == "data-only" || config.Mode == "schema-and-data" {
		logger.Info(fmt.Sprintf("    Data Compare Type: %s", config.DataCompareType))
		if config.DataCompareType == "sample" {
			logger.Info(fmt.Sprintf("    Sample Size:       %d", config.SampleSize))
		}
	}
	if len(config.Tables) > 0 {
		logger.Info(fmt.Sprintf("    Tables:            %s", strings.Join(config.Tables, ", ")))
	} else {
		logger.Info("    Tables:            (all tables)")
	}

	// Output configuration
	logger.Info("  Output:")
	logger.Info(fmt.Sprintf("    Format:            %s", config.OutputFormat))
	if config.OutputFile != "" {
		logger.Info(fmt.Sprintf("    File:              %s", config.OutputFile))
	} else {
		logger.Info("    File:              stdout")
	}

	// General settings
	logger.Info("  Settings:")
	logger.Info(fmt.Sprintf("    Dry Run:           %v", config.DryRun))
	logger.Info(fmt.Sprintf("    Debug:             %v", config.Debug))
	logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Info("")
}

// validateCompareConfig validates compare configuration
func validateCompareConfig(source1, source2 *ComparisonSource, config *CompareConfig) error {
	if source1.Type == "" {
		return errors.New("source1-type is required")
	}
	if source2.Type == "" {
		return errors.New("source2-type is required")
	}
	if source1.Type != "db" && source1.Type != "s3" {
		return fmt.Errorf("invalid source1-type: %s (must be 'db' or 's3')", source1.Type)
	}
	if source2.Type != "db" && source2.Type != "s3" {
		return fmt.Errorf("invalid source2-type: %s (must be 'db' or 's3')", source2.Type)
	}

	if source1.Type == "db" {
		if source1.Database.User == "" {
			return errors.New("source1-db-user is required when source1-type is db")
		}
		if source1.Database.Name == "" {
			return errors.New("source1-db-name is required when source1-type is db")
		}
	}
	if source2.Type == "db" {
		if source2.Database.User == "" {
			return errors.New("source2-db-user is required when source2-type is db")
		}
		if source2.Database.Name == "" {
			return errors.New("source2-db-name is required when source2-type is db")
		}
	}

	if source1.Type == "s3" {
		if source1.S3.Endpoint == "" {
			return errors.New("source1-s3-endpoint is required when source1-type is s3")
		}
		if source1.S3.Bucket == "" {
			return errors.New("source1-s3-bucket is required when source1-type is s3")
		}
		if source1.S3.AccessKey == "" {
			return errors.New("source1-s3-access-key is required when source1-type is s3")
		}
		if source1.S3.SecretKey == "" {
			return errors.New("source1-s3-secret-key is required when source1-type is s3")
		}
	}
	if source2.Type == "s3" {
		if source2.S3.Endpoint == "" {
			return errors.New("source2-s3-endpoint is required when source2-type is s3")
		}
		if source2.S3.Bucket == "" {
			return errors.New("source2-s3-bucket is required when source2-type is s3")
		}
		if source2.S3.AccessKey == "" {
			return errors.New("source2-s3-access-key is required when source2-type is s3")
		}
		if source2.S3.SecretKey == "" {
			return errors.New("source2-s3-secret-key is required when source2-type is s3")
		}
	}

	validModes := map[string]bool{
		"schema-only":     true,
		"data-only":       true,
		"schema-and-data": true,
	}
	if !validModes[config.Mode] {
		return fmt.Errorf("invalid compare-mode: %s (must be schema-only, data-only, or schema-and-data)", config.Mode)
	}

	// Only validate data comparison settings if mode includes data comparison
	if config.Mode == "data-only" || config.Mode == "schema-and-data" {
		validDataCompareTypes := map[string]bool{
			"row-count":   true,
			"row-by-row": true,
			"sample":     true,
		}
		if !validDataCompareTypes[config.DataCompareType] {
			return fmt.Errorf("invalid data-compare-type: %s (must be row-count, row-by-row, or sample)", config.DataCompareType)
		}

		if config.DataCompareType == "sample" && config.SampleSize < 1 {
			return errors.New("sample-size must be at least 1")
		}
	}

	validOutputFormats := map[string]bool{
		"text": true,
		"json": true,
	}
	if !validOutputFormats[config.OutputFormat] {
		return fmt.Errorf("invalid output-format: %s (must be text or json)", config.OutputFormat)
	}

	return nil
}

// Run executes the comparison
func (c *Comparer) Run(ctx context.Context) error {
	c.ctx = ctx

	// Connect to sources
	if err := c.connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer c.cleanup()

	result := &ComparisonResult{}

	// Compare schemas if needed
	if c.config.Mode == "schema-only" || c.config.Mode == "schema-and-data" {
		c.logger.Info("Comparing schemas...")
		schemaResult, err := c.compareSchemas(ctx)
		if err != nil {
			return fmt.Errorf("schema comparison failed: %w", err)
		}
		result.Schema = schemaResult
	}

	// Compare data if needed
	if c.config.Mode == "data-only" || c.config.Mode == "schema-and-data" {
		c.logger.Info("Comparing data...")
		dataResult, err := c.compareData(ctx)
		if err != nil {
			return fmt.Errorf("data comparison failed: %w", err)
		}
		result.Data = dataResult
	}

	// Output results
	return c.outputResults(result)
}

// connect connects to databases and/or S3 as needed
func (c *Comparer) connect(ctx context.Context) error {
	if c.source1.Type == "db" {
		if err := c.connectDatabase(ctx, c.source1, &c.db1); err != nil {
			return fmt.Errorf("failed to connect to source1 database: %w", err)
		}
	}
	if c.source2.Type == "db" {
		if err := c.connectDatabase(ctx, c.source2, &c.db2); err != nil {
			return fmt.Errorf("failed to connect to source2 database: %w", err)
		}
	}

	if c.source1.Type == "s3" {
		if err := c.connectS3(c.source1, &c.s3Client1, &c.s3Downloader1); err != nil {
			return fmt.Errorf("failed to connect to source1 S3: %w", err)
		}
	}
	if c.source2.Type == "s3" {
		if err := c.connectS3(c.source2, &c.s3Client2, &c.s3Downloader2); err != nil {
			return fmt.Errorf("failed to connect to source2 S3: %w", err)
		}
	}

	return nil
}

// connectDatabase connects to a PostgreSQL database
func (c *Comparer) connectDatabase(ctx context.Context, source *ComparisonSource, db **sql.DB) error {
	sslMode := source.Database.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	// Build connection string
	// Note: lib/pq handles password escaping internally, so we don't need URL encoding
	// Set search_path to ensure we can find tables in the public schema
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s search_path=public",
		source.Database.Host,
		source.Database.Port,
		source.Database.User,
		source.Database.Password,
		source.Database.Name,
		sslMode,
	)

	if c.config.Debug {
		passwordMasked := "***"
		if source.Database.Password != "" {
			passwordMasked = "***"
		}
		c.logger.Debug(fmt.Sprintf("Connecting to database: host=%s port=%d user=%s password=%s dbname=%s sslmode=%s search_path=public",
			source.Database.Host,
			source.Database.Port,
			source.Database.User,
			passwordMasked,
			source.Database.Name,
			sslMode,
		))
		c.logger.Debug(fmt.Sprintf("Connection string (password masked): host=%s port=%d user=%s password=*** dbname=%s sslmode=%s search_path=public",
			source.Database.Host,
			source.Database.Port,
			source.Database.User,
			source.Database.Name,
			sslMode,
		))
	}

	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Verify we're connected to the correct database
	var currentDB string
	if err := conn.QueryRowContext(ctx, "SELECT current_database()").Scan(&currentDB); err == nil {
		if c.config.Debug {
			c.logger.Debug(fmt.Sprintf("Connected to database: %s (requested: %s)", currentDB, source.Database.Name))
		}
		if currentDB != source.Database.Name {
			conn.Close()
			return fmt.Errorf("connected to database '%s' but expected '%s'. The user '%s' may not have permission to connect to '%s', or the database doesn't exist. Please check database permissions.", currentDB, source.Database.Name, source.Database.User, source.Database.Name)
		}
	}

	*db = conn
	return nil
}

// connectS3 connects to S3
func (c *Comparer) connectS3(source *ComparisonSource, client **s3.S3, downloader **s3manager.Downloader) error {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(source.S3.Endpoint),
		Region:           aws.String(source.S3.Region),
		Credentials:      credentials.NewStaticCredentials(source.S3.AccessKey, source.S3.SecretKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create S3 session: %w", err)
	}

	*client = s3.New(sess)
	*downloader = s3manager.NewDownloader(sess)
	return nil
}

// cleanup closes all connections
func (c *Comparer) cleanup() {
	if c.db1 != nil {
		c.db1.Close()
	}
	if c.db2 != nil {
		c.db2.Close()
	}
}

// compareSchemas compares schemas between sources
func (c *Comparer) compareSchemas(ctx context.Context) (*SchemaComparisonResult, error) {
	// Extract schemas from both sources
	schemas1, err := c.extractSchemas(ctx, c.source1)
	if err != nil {
		return nil, fmt.Errorf("failed to extract schemas from source1: %w", err)
	}

	schemas2, err := c.extractSchemas(ctx, c.source2)
	if err != nil {
		return nil, fmt.Errorf("failed to extract schemas from source2: %w", err)
	}

	// Filter tables if specified
	if len(c.config.Tables) > 0 {
		schemas1 = c.filterSchemas(schemas1, c.config.Tables)
		schemas2 = c.filterSchemas(schemas2, c.config.Tables)
	}

	// Compare schemas
	result := &SchemaComparisonResult{
		TablesOnlyInSource1: []string{},
		TablesOnlyInSource2: []string{},
		TableDiffs:          make(map[string]*TableSchemaDiff),
	}

	// Build table name maps
	tableMap1 := make(map[string]*TableSchema)
	tableMap2 := make(map[string]*TableSchema)
	for _, schema := range schemas1 {
		tableMap1[schema.TableName] = schema
	}
	for _, schema := range schemas2 {
		tableMap2[schema.TableName] = schema
	}

	// Find tables only in source1
	for tableName := range tableMap1 {
		if _, exists := tableMap2[tableName]; !exists {
			result.TablesOnlyInSource1 = append(result.TablesOnlyInSource1, tableName)
		}
	}

	// Find tables only in source2
	for tableName := range tableMap2 {
		if _, exists := tableMap1[tableName]; !exists {
			result.TablesOnlyInSource2 = append(result.TablesOnlyInSource2, tableName)
		}
	}

	// Compare common tables
	for tableName, schema1 := range tableMap1 {
		schema2, exists := tableMap2[tableName]
		if !exists {
			continue
		}

		diff := c.compareTableSchemas(schema1, schema2)
		if diff != nil {
			result.TableDiffs[tableName] = diff
		}
	}

	return result, nil
}

// extractSchemas extracts schemas from a source (database or S3)
func (c *Comparer) extractSchemas(ctx context.Context, source *ComparisonSource) (map[string]*TableSchema, error) {
	if source.Type == "db" {
		return c.extractSchemasFromDatabase(ctx, source)
	}
	return c.extractSchemasFromS3(ctx, source)
}

// extractSchemasFromDatabase extracts schemas from a PostgreSQL database
func (c *Comparer) extractSchemasFromDatabase(ctx context.Context, source *ComparisonSource) (map[string]*TableSchema, error) {
	var db *sql.DB
	if source == c.source1 {
		db = c.db1
	} else {
		db = c.db2
	}

	if db == nil {
		return nil, errors.New("database connection not established")
	}

	// Get list of tables
	tables, err := c.listTables(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	sourceLabel := "source1"
	if source == c.source2 {
		sourceLabel = "source2"
	}

	if c.config.Debug {
		c.logger.Debug(fmt.Sprintf("Found %d tables in %s: %v", len(tables), sourceLabel, tables))
	}

	// If no tables found but we have specific tables requested, log a warning
	if len(tables) == 0 && len(c.config.Tables) > 0 {
		c.logger.Warn(fmt.Sprintf("No tables found in %s database, but will attempt to check requested tables: %v", sourceLabel, c.config.Tables))
	}

	// Determine which tables to process
	var tablesToProcess []string
	if len(c.config.Tables) > 0 {
		// If specific tables are requested, process those (even if they don't exist)
		tablesToProcess = c.config.Tables
	} else {
		// Otherwise, process all tables
		tablesToProcess = tables
	}

	schemas := make(map[string]*TableSchema)
	for _, tableName := range tablesToProcess {
		// First check if table exists
		exists, err := c.tableExists(ctx, db, tableName)
		if err != nil {
			c.logger.Warn(fmt.Sprintf("Failed to check if table %s exists in %s: %v", tableName, sourceLabel, err))
			continue
		}

		if !exists {
			// Table doesn't exist - check if it exists in other schemas
			schemaName, err := c.findTableSchema(ctx, db, tableName)
			if err == nil && schemaName != "" {
				c.logger.Warn(fmt.Sprintf("Table %s exists in schema '%s' in %s, but only 'public' schema is supported", tableName, schemaName, sourceLabel))
			}
			// Don't add to schemas map - will be reported as "only in other source"
			continue
		}

		// Table exists, try to get schema
		schema, err := c.getTableSchema(ctx, db, tableName)
		if err != nil {
			// Check if it's a "table has no columns" error
			if errors.Is(err, ErrTableHasNoColumns) {
				// Create a schema with no columns - this is a valid schema difference
				schema = &TableSchema{
					TableName: tableName,
					Columns:   []ColumnInfo{},
				}
				schemas[tableName] = schema
				if c.config.Debug {
					c.logger.Debug(fmt.Sprintf("Table %s exists in %s but has no columns", tableName, sourceLabel))
				}
			} else if errors.Is(err, ErrTableNotFound) {
				// Shouldn't happen since we checked exists above, but handle it
				continue
			} else {
				c.logger.Warn(fmt.Sprintf("Failed to get schema for table %s from %s: %v", tableName, sourceLabel, err))
				continue
			}
		} else {
			schemas[tableName] = schema
			if c.config.Debug {
				c.logger.Debug(fmt.Sprintf("Successfully extracted schema for table %s from %s (%d columns)", tableName, sourceLabel, len(schema.Columns)))
			}
		}
	}

	return schemas, nil
}

// listTables lists all tables in a database
func (c *Comparer) listTables(ctx context.Context, db *sql.DB) ([]string, error) {
	// Use pg_tables like other commands for consistency
	query := `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public'
		ORDER BY tablename
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
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
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return tables, nil
}

// tableExists checks if a table exists in the public schema
func (c *Comparer) tableExists(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	// Try multiple methods to check if table exists
	// Method 1: pg_class + pg_namespace (most direct)
	query1 := `
		SELECT EXISTS (
			SELECT 1 FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'public' AND c.relname = $1 AND c.relkind = 'r'
		)
	`
	var exists bool
	err := db.QueryRowContext(ctx, query1, tableName).Scan(&exists)
	if err == nil && exists {
		return true, nil
	}

	// Method 2: pg_tables view
	query2 := `
		SELECT EXISTS (
			SELECT 1 FROM pg_tables
			WHERE schemaname = 'public' AND tablename = $1
		)
	`
	err = db.QueryRowContext(ctx, query2, tableName).Scan(&exists)
	if err == nil && exists {
		return true, nil
	}

	// Method 3: information_schema
	query3 := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)
	`
	err = db.QueryRowContext(ctx, query3, tableName).Scan(&exists)
	return exists, err
}

// findTableSchema finds which schema a table exists in (if not in public)
func (c *Comparer) findTableSchema(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	query := `
		SELECT table_schema
		FROM information_schema.tables
		WHERE table_name = $1
		LIMIT 1
	`
	var schemaName string
	err := db.QueryRowContext(ctx, query, tableName).Scan(&schemaName)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return schemaName, err
}

// getTableSchema gets schema for a table from database
func (c *Comparer) getTableSchema(ctx context.Context, db *sql.DB, tableName string) (*TableSchema, error) {
	// Try information_schema.columns first (standard approach)
	query1 := `
		SELECT column_name, data_type, udt_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := db.QueryContext(ctx, query1, tableName)
	if err == nil {
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

		if len(schema.Columns) > 0 {
			return schema, nil
		}
	}

	// Fallback to pg_attribute if information_schema returns no rows (permissions issue)
	query2 := `
		SELECT a.attname::text,
		       pg_catalog.format_type(a.atttypid, a.atttypmod)::text,
		       t.typname::text
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
		JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
		WHERE n.nspname = 'public'
		  AND c.relname = $1
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	// Fallback to pg_attribute if information_schema returns no rows (permissions issue)
	if c.config.Debug {
		c.logger.Debug(fmt.Sprintf("information_schema.columns returned 0 rows for %s, trying pg_attribute fallback", tableName))
	}

	rows2, err2 := db.QueryContext(ctx, query2, tableName)
	if err2 == nil {
		defer rows2.Close()

		schema2 := &TableSchema{
			TableName: tableName,
			Columns:   make([]ColumnInfo, 0),
		}

		for rows2.Next() {
			var col ColumnInfo
			if err := rows2.Scan(&col.Name, &col.DataType, &col.UDTName); err != nil {
				if c.config.Debug {
					c.logger.Debug(fmt.Sprintf("Error scanning pg_attribute row: %v", err))
				}
				break
			}
			schema2.Columns = append(schema2.Columns, col)
		}

		if err := rows2.Err(); err == nil && len(schema2.Columns) > 0 {
			if c.config.Debug {
				c.logger.Debug(fmt.Sprintf("Successfully got schema from pg_attribute for %s (%d columns)", tableName, len(schema2.Columns)))
			}
			return schema2, nil
		}
		if c.config.Debug {
			c.logger.Debug(fmt.Sprintf("pg_attribute returned %d rows for %s", len(schema2.Columns), tableName))
		}
	} else if c.config.Debug {
		c.logger.Debug(fmt.Sprintf("pg_attribute query failed for %s: %v", tableName, err2))
	}

	// Final fallback: try to query the table directly with LIMIT 0 to get column info
	// This uses the database's own metadata about the result set
	// Use fully qualified name to avoid search_path issues
	if c.config.Debug {
		c.logger.Debug(fmt.Sprintf("Trying direct SELECT query fallback for %s", tableName))
	}
	query3 := fmt.Sprintf("SELECT * FROM public.%s LIMIT 0", pq.QuoteIdentifier(tableName))
	rows3, err3 := db.QueryContext(ctx, query3)
	if err3 == nil {
		defer rows3.Close()
		columnTypes, err := rows3.ColumnTypes()
		if err != nil {
			if c.config.Debug {
				c.logger.Debug(fmt.Sprintf("Error getting column types from direct query for %s: %v", tableName, err))
			}
		} else if len(columnTypes) > 0 {
			if c.config.Debug {
				c.logger.Debug(fmt.Sprintf("Successfully got schema from direct query for %s (%d columns)", tableName, len(columnTypes)))
			}
			schema3 := &TableSchema{
				TableName: tableName,
				Columns:   make([]ColumnInfo, 0),
			}
			for _, ct := range columnTypes {
				schema3.Columns = append(schema3.Columns, ColumnInfo{
					Name:     ct.Name(),
					DataType: ct.DatabaseTypeName(),
					UDTName:  ct.DatabaseTypeName(),
				})
			}
			return schema3, nil
		} else {
			if c.config.Debug {
				c.logger.Debug(fmt.Sprintf("Direct query succeeded but returned %d column types for %s (table may have no columns or permissions issue)", len(columnTypes), tableName))
			}
		}
	} else if c.config.Debug {
		c.logger.Debug(fmt.Sprintf("Direct SELECT query failed for %s: %v", tableName, err3))
	}

	// Check if table exists at all
	exists, existsErr := c.tableExists(ctx, db, tableName)
	if existsErr == nil && exists {
		// Table exists but has no columns - return special error
		return nil, fmt.Errorf("%w: %s", ErrTableHasNoColumns, tableName)
	}

	// Table doesn't exist
	return nil, fmt.Errorf("%w: %s (tried information_schema.columns, pg_attribute, and direct query)", ErrTableNotFound, tableName)
}

// extractSchemasFromS3 extracts schemas from S3
func (c *Comparer) extractSchemasFromS3(ctx context.Context, source *ComparisonSource) (map[string]*TableSchema, error) {
	var client *s3.S3
	var downloader *s3manager.Downloader
	if source == c.source1 {
		client = c.s3Client1
		downloader = c.s3Downloader1
	} else {
		client = c.s3Client2
		downloader = c.s3Downloader2
	}

	if client == nil {
		return nil, errors.New("S3 client not established")
	}

	schemaSource := source.SchemaSource
	if schemaSource == "" {
		schemaSource = "auto"
	}

	// Try pg_dump first if auto or pg_dump
	if schemaSource == "auto" || schemaSource == "pg_dump" {
		schemas, err := c.extractSchemasFromPgDump(ctx, source, client, downloader)
		if err == nil && len(schemas) > 0 {
			return schemas, nil
		}
		if schemaSource == "pg_dump" {
			return nil, fmt.Errorf("failed to extract schemas from pg_dump: %w", err)
		}
		// Fall through to inferred if auto mode
	}

	// Try inferred schema
	if schemaSource == "auto" || schemaSource == "inferred" {
		schemas, err := c.extractSchemasFromDataFiles(ctx, source, client, downloader)
		if err != nil {
			return nil, fmt.Errorf("failed to extract schemas from data files: %w", err)
		}
		return schemas, nil
	}

	return nil, fmt.Errorf("unknown schema source: %s", schemaSource)
}

// extractSchemasFromPgDump extracts schemas from pg_dump files in S3
func (c *Comparer) extractSchemasFromPgDump(ctx context.Context, source *ComparisonSource, client *s3.S3, downloader *s3manager.Downloader) (map[string]*TableSchema, error) {
	schemaPath := source.SchemaPath
	if schemaPath == "" {
		schemaPath = source.S3.PathTemplate
	}

	// List S3 objects matching schema path
	prefix := strings.TrimSuffix(schemaPath, "/")
	if !strings.HasSuffix(prefix, "*") {
		prefix = prefix + "/"
	}

	// Look for schema dump files
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(source.S3.Bucket),
		Prefix: aws.String(prefix),
	}

	result, err := client.ListObjectsV2WithContext(ctx, listInput)
	if err != nil {
		return nil, fmt.Errorf("failed to list S3 objects: %w", err)
	}

	schemas := make(map[string]*TableSchema)

	for _, obj := range result.Contents {
		key := aws.StringValue(obj.Key)
		if !strings.Contains(key, "schema") || !strings.HasSuffix(key, ".dump") {
			continue
		}

		// Download and parse pg_dump file
		schema, err := c.parsePgDumpSchema(ctx, source, key, client, downloader)
		if err != nil {
			c.logger.Warn(fmt.Sprintf("Failed to parse pg_dump file %s: %v", key, err))
			continue
		}

		if schema != nil {
			schemas[schema.TableName] = schema
		}
	}

	return schemas, nil
}

// parsePgDumpSchema parses a pg_dump schema file
func (c *Comparer) parsePgDumpSchema(ctx context.Context, source *ComparisonSource, key string, client *s3.S3, downloader *s3manager.Downloader) (*TableSchema, error) {
	// Download file to temp location
	tempFile, err := os.CreateTemp("", "pg_dump-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	_, err = downloader.DownloadWithContext(ctx, tempFile, &s3.GetObjectInput{
		Bucket: aws.String(source.S3.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %w", err)
	}

	tempFile.Close()

	// Use pg_restore -l to list schema objects
	cmd := exec.CommandContext(ctx, "pg_restore", "-l", tempFile.Name())
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to run pg_restore -l: %w", err)
	}

	// Parse output to extract table schemas
	// This is a simplified parser - pg_restore -l outputs lines like:
	// "1234; 1259 16384 TABLE public table_name user"
	// We'll extract table names and then use pg_restore to get DDL
	return c.parsePgRestoreList(string(output), tempFile.Name())
}

// parsePgRestoreList parses pg_restore -l output
func (c *Comparer) parsePgRestoreList(output, dumpFile string) (*TableSchema, error) {
	// For now, return nil - full pg_dump parsing is complex
	// We'll focus on inferred schemas for now
	// TODO: Implement full pg_dump schema parsing
	return nil, errors.New("pg_dump schema parsing not fully implemented")
}

// extractSchemasFromDataFiles extracts schemas by inferring from data files
func (c *Comparer) extractSchemasFromDataFiles(ctx context.Context, source *ComparisonSource, client *s3.S3, downloader *s3manager.Downloader) (map[string]*TableSchema, error) {
	dataPath := source.DataPath
	if dataPath == "" {
		dataPath = source.S3.PathTemplate
	}

	// Discover data files
	files, err := c.discoverS3DataFiles(ctx, source, client, dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to discover data files: %w", err)
	}

	if len(files) == 0 {
		return make(map[string]*TableSchema), nil
	}

	// Group files by table name
	tableFiles := make(map[string][]S3File)
	for _, file := range files {
		tableName := c.extractTableNameFromPath(file.Key, dataPath)
		if tableName != "" {
			tableFiles[tableName] = append(tableFiles[tableName], file)
		}
	}

	// Filter tables if specified
	if len(c.config.Tables) > 0 {
		tableSet := make(map[string]bool)
		for _, t := range c.config.Tables {
			tableSet[t] = true
		}
		filtered := make(map[string][]S3File)
		for tableName, files := range tableFiles {
			if tableSet[tableName] {
				filtered[tableName] = files
			}
		}
		tableFiles = filtered
	}

	// Infer schema from first file of each table
	schemas := make(map[string]*TableSchema)
	for tableName, files := range tableFiles {
		if len(files) == 0 {
			continue
		}

		// Use first file to infer schema
		schema, err := c.inferSchemaFromS3File(ctx, source, files[0], client, downloader, tableName)
		if err != nil {
			c.logger.Warn(fmt.Sprintf("Failed to infer schema for table %s: %v", tableName, err))
			continue
		}

		schemas[tableName] = schema
	}

	return schemas, nil
}

// discoverS3DataFiles discovers data files in S3
func (c *Comparer) discoverS3DataFiles(ctx context.Context, source *ComparisonSource, client *s3.S3, dataPath string) ([]S3File, error) {
	// Remove date placeholders for listing
	listPrefix := regexp.MustCompile(`\{YYYY\}|\{MM\}|\{DD\}|\{HH\}|\{table\}`).ReplaceAllString(dataPath, "")
	listPrefix = regexp.MustCompile(`/+`).ReplaceAllString(listPrefix, "/")
	listPrefix = strings.TrimSuffix(listPrefix, "/")

	var files []S3File
	var continuationToken *string

	for {
		listInput := &s3.ListObjectsV2Input{
			Bucket:            aws.String(source.S3.Bucket),
			Prefix:            aws.String(listPrefix),
			ContinuationToken: continuationToken,
		}

		result, err := client.ListObjectsV2WithContext(ctx, listInput)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range result.Contents {
			key := aws.StringValue(obj.Key)
			if strings.HasSuffix(key, "/") {
				continue
			}

			// Skip schema files
			if strings.Contains(key, "schema") && strings.HasSuffix(key, ".dump") {
				continue
			}

			filename := filepath.Base(key)
			format, compression, err := detectFormatAndCompression(filename, "", "")
			if err != nil {
				continue
			}

			fileDate, ok := extractDateFromFilename(filename)
			if !ok || fileDate.IsZero() {
				fileDate = aws.TimeValue(obj.LastModified)
			}
			_ = time.Time{} // Ensure time package is used (S3File.Date is time.Time)

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

	return files, nil
}

// extractTableNameFromPath extracts table name from S3 path
func (c *Comparer) extractTableNameFromPath(key, pathTemplate string) string {
	// Try to extract table name from path
	// If path template has {table}, try to match it
	if strings.Contains(pathTemplate, "{table}") {
		// Replace placeholders with regex patterns
		pattern := regexp.MustCompile(`\{table\}`).ReplaceAllString(regexp.QuoteMeta(pathTemplate), `([^/]+)`)
		pattern = regexp.MustCompile(`\{YYYY\}`).ReplaceAllString(pattern, `\d{4}`)
		pattern = regexp.MustCompile(`\{MM\}`).ReplaceAllString(pattern, `\d{2}`)
		pattern = regexp.MustCompile(`\{DD\}`).ReplaceAllString(pattern, `\d{2}`)
		pattern = regexp.MustCompile(`\{HH\}`).ReplaceAllString(pattern, `\d{2}`)

		re := regexp.MustCompile(pattern)
		matches := re.FindStringSubmatch(key)
		if len(matches) > 1 {
			return matches[1]
		}
	}

	// Fallback: extract from filename
	filename := filepath.Base(key)
	// Remove extensions
	filename = strings.TrimSuffix(filename, ".zst")
	filename = strings.TrimSuffix(filename, ".lz4")
	filename = strings.TrimSuffix(filename, ".gz")
	filename = strings.TrimSuffix(filename, ".jsonl")
	filename = strings.TrimSuffix(filename, ".csv")
	filename = strings.TrimSuffix(filename, ".parquet")

	// Remove date patterns
	filename = regexp.MustCompile(`_\d{4}-\d{2}-\d{2}`).ReplaceAllString(filename, "")
	filename = regexp.MustCompile(`_\d{8}`).ReplaceAllString(filename, "")

	return filename
}

// inferSchemaFromS3File infers schema from an S3 data file
func (c *Comparer) inferSchemaFromS3File(ctx context.Context, source *ComparisonSource, file S3File, client *s3.S3, downloader *s3manager.Downloader, tableName string) (*TableSchema, error) {
	// Download file
	tempFile, err := os.CreateTemp("", "compare-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	_, err = downloader.DownloadWithContext(ctx, tempFile, &s3.GetObjectInput{
		Bucket: aws.String(source.S3.Bucket),
		Key:    aws.String(file.Key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %w", err)
	}

	tempFile.Close()

	// Reopen for reading
	fileReader, err := os.Open(tempFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to open temp file: %w", err)
	}
	defer fileReader.Close()

	// Decompress
	compressor, err := compressors.GetCompressor(file.DetectedCompression)
	if err != nil {
		return nil, fmt.Errorf("failed to get compressor: %w", err)
	}

	decompressedReader, err := compressor.NewReader(fileReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create decompression reader: %w", err)
	}
	defer decompressedReader.Close()

	// Read sample rows
	var rows []map[string]interface{}
	switch file.DetectedFormat {
	case "jsonl":
		reader := formatters.NewJSONLReaderWithCloser(decompressedReader)
		rows, err = reader.ReadAll()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read JSONL: %w", err)
		}
	case "csv":
		reader, err := formatters.NewCSVReaderWithCloser(decompressedReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create CSV reader: %w", err)
		}
		rows, err = reader.ReadAll()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read CSV: %w", err)
		}
	case "parquet":
		reader, err := formatters.NewParquetReaderWithCloser(decompressedReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create Parquet reader: %w", err)
		}
		rows, err = reader.ReadAll()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read Parquet: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported format: %s", file.DetectedFormat)
	}

	if len(rows) == 0 {
		return nil, errors.New("no rows found in file")
	}

	// Infer schema from rows
	return c.inferTableSchema(rows, tableName)
}

// inferTableSchema infers table schema from sample rows
func (c *Comparer) inferTableSchema(rows []map[string]interface{}, tableName string) (*TableSchema, error) {
	if len(rows) == 0 {
		return nil, errors.New("cannot infer schema from empty rows")
	}

	// Get all column names from all rows
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
		TableName: tableName,
		Columns:   columns,
	}, nil
}

// filterSchemas filters schemas by table names
func (c *Comparer) filterSchemas(schemas map[string]*TableSchema, tableNames []string) map[string]*TableSchema {
	tableSet := make(map[string]bool)
	for _, name := range tableNames {
		tableSet[name] = true
	}

	filtered := make(map[string]*TableSchema)
	for name, schema := range schemas {
		if tableSet[name] {
			filtered[name] = schema
		}
	}

	return filtered
}

// compareTableSchemas compares two table schemas
func (c *Comparer) compareTableSchemas(schema1, schema2 *TableSchema) *TableSchemaDiff {
	diff := &TableSchemaDiff{
		ColumnsOnlyInSource1: []ColumnInfo{},
		ColumnsOnlyInSource2: []ColumnInfo{},
		TypeMismatches:       []ColumnTypeMismatch{},
	}

	// Special case: if one table has no columns and the other has columns, this is a difference
	if len(schema1.Columns) == 0 && len(schema2.Columns) > 0 {
		// Source1 has no columns, source2 has columns - all source2 columns are differences
		diff.ColumnsOnlyInSource2 = schema2.Columns
		return diff
	}
	if len(schema1.Columns) > 0 && len(schema2.Columns) == 0 {
		// Source1 has columns, source2 has no columns - all source1 columns are differences
		diff.ColumnsOnlyInSource1 = schema1.Columns
		return diff
	}

	// Build column maps
	colMap1 := make(map[string]ColumnInfo)
	colMap2 := make(map[string]ColumnInfo)
	for _, col := range schema1.Columns {
		colMap1[col.Name] = col
	}
	for _, col := range schema2.Columns {
		colMap2[col.Name] = col
	}

	// Find columns only in source1
	for name, col := range colMap1 {
		if _, exists := colMap2[name]; !exists {
			diff.ColumnsOnlyInSource1 = append(diff.ColumnsOnlyInSource1, col)
		}
	}

	// Find columns only in source2
	for name, col := range colMap2 {
		if _, exists := colMap1[name]; !exists {
			diff.ColumnsOnlyInSource2 = append(diff.ColumnsOnlyInSource2, col)
		}
	}

	// Find type mismatches
	for name, col1 := range colMap1 {
		col2, exists := colMap2[name]
		if !exists {
			continue
		}
		if col1.UDTName != col2.UDTName {
			diff.TypeMismatches = append(diff.TypeMismatches, ColumnTypeMismatch{
				ColumnName:  name,
				Source1Type: col1.UDTName,
				Source2Type: col2.UDTName,
			})
		}
	}

	// Return nil if no differences
	if len(diff.ColumnsOnlyInSource1) == 0 && len(diff.ColumnsOnlyInSource2) == 0 && len(diff.TypeMismatches) == 0 {
		return nil
	}

	return diff
}

// compareData compares data between sources
func (c *Comparer) compareData(ctx context.Context) (*DataComparisonResult, error) {
	result := &DataComparisonResult{
		RowCountDiffs: make(map[string]*RowCountDiff),
		RowByRowDiffs: make(map[string]*RowByRowDiff),
		SampleDiffs:   make(map[string]*SampleDiff),
	}

	// Get list of tables to compare
	tables, err := c.getTablesToCompare(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables to compare: %w", err)
	}

	// Filter tables if specified
	if len(c.config.Tables) > 0 {
		tableSet := make(map[string]bool)
		for _, t := range c.config.Tables {
			tableSet[t] = true
		}
		filtered := []string{}
		for _, t := range tables {
			if tableSet[t] {
				filtered = append(filtered, t)
			}
		}
		tables = filtered
	}

	// Compare each table
	for _, tableName := range tables {
		c.logger.Info(fmt.Sprintf("Comparing data for table: %s", tableName))

		switch c.config.DataCompareType {
		case "row-count":
			diff, err := c.compareRowCounts(ctx, tableName)
			if err != nil {
				c.logger.Warn(fmt.Sprintf("Failed to compare row counts for %s: %v", tableName, err))
				continue
			}
			if diff != nil {
				result.RowCountDiffs[tableName] = diff
			}

		case "row-by-row":
			diff, err := c.compareRowByRow(ctx, tableName)
			if err != nil {
				c.logger.Warn(fmt.Sprintf("Failed to compare row-by-row for %s: %v", tableName, err))
				continue
			}
			if diff != nil {
				result.RowByRowDiffs[tableName] = diff
			}

		case "sample":
			diff, err := c.compareSamples(ctx, tableName)
			if err != nil {
				c.logger.Warn(fmt.Sprintf("Failed to compare samples for %s: %v", tableName, err))
				continue
			}
			if diff != nil {
				result.SampleDiffs[tableName] = diff
			}
		}
	}

	return result, nil
}

// getTablesToCompare gets the list of tables to compare from both sources
func (c *Comparer) getTablesToCompare(ctx context.Context) ([]string, error) {
	var tables1, tables2 []string
	var err error

	if c.source1.Type == "db" {
		if c.db1 != nil {
			tables1, err = c.listTables(ctx, c.db1)
			if err != nil {
				return nil, fmt.Errorf("failed to list tables from source1: %w", err)
			}
		}
	} else {
		// For S3, we need to discover tables from files
		tables1, err = c.listTablesFromS3(ctx, c.source1)
		if err != nil {
			return nil, fmt.Errorf("failed to list tables from source1 S3: %w", err)
		}
	}

	if c.source2.Type == "db" {
		if c.db2 != nil {
			tables2, err = c.listTables(ctx, c.db2)
			if err != nil {
				return nil, fmt.Errorf("failed to list tables from source2: %w", err)
			}
		}
	} else {
		tables2, err = c.listTablesFromS3(ctx, c.source2)
		if err != nil {
			return nil, fmt.Errorf("failed to list tables from source2 S3: %w", err)
		}
	}

	// Combine and deduplicate
	tableSet := make(map[string]bool)
	for _, t := range tables1 {
		tableSet[t] = true
	}
	for _, t := range tables2 {
		tableSet[t] = true
	}

	tables := make([]string, 0, len(tableSet))
	for t := range tableSet {
		tables = append(tables, t)
	}
	sort.Strings(tables)

	return tables, nil
}

// listTablesFromS3 lists tables from S3 by discovering data files
func (c *Comparer) listTablesFromS3(ctx context.Context, source *ComparisonSource) ([]string, error) {
	var client *s3.S3
	if source == c.source1 {
		client = c.s3Client1
	} else {
		client = c.s3Client2
	}

	if client == nil {
		return nil, errors.New("S3 client not established")
	}

	dataPath := source.DataPath
	if dataPath == "" {
		dataPath = source.S3.PathTemplate
	}

	files, err := c.discoverS3DataFiles(ctx, source, client, dataPath)
	if err != nil {
		return nil, err
	}

	tableSet := make(map[string]bool)
	for _, file := range files {
		tableName := c.extractTableNameFromPath(file.Key, dataPath)
		if tableName != "" {
			tableSet[tableName] = true
		}
	}

	tables := make([]string, 0, len(tableSet))
	for t := range tableSet {
		tables = append(tables, t)
	}
	sort.Strings(tables)

	return tables, nil
}

// compareRowCounts compares row counts between sources
func (c *Comparer) compareRowCounts(ctx context.Context, tableName string) (*RowCountDiff, error) {
	var count1, count2 int64
	var err error

	if c.source1.Type == "db" {
		count1, err = c.getRowCountFromDatabase(ctx, c.db1, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get row count from source1: %w", err)
		}
	} else {
		count1, err = c.getRowCountFromS3(ctx, c.source1, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get row count from source1 S3: %w", err)
		}
	}

	if c.source2.Type == "db" {
		count2, err = c.getRowCountFromDatabase(ctx, c.db2, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get row count from source2: %w", err)
		}
	} else {
		count2, err = c.getRowCountFromS3(ctx, c.source2, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get row count from source2 S3: %w", err)
		}
	}

	if count1 == count2 {
		return nil, nil // No difference
	}

	return &RowCountDiff{
		Source1Count: count1,
		Source2Count: count2,
		Difference:   count1 - count2,
	}, nil
}

// getRowCountFromDatabase gets row count from a database table
func (c *Comparer) getRowCountFromDatabase(ctx context.Context, db *sql.DB, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(tableName))
	var count int64
	err := db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// getRowCountFromS3 gets row count from S3 data files
func (c *Comparer) getRowCountFromS3(ctx context.Context, source *ComparisonSource, tableName string) (int64, error) {
	var client *s3.S3
	var downloader *s3manager.Downloader
	if source == c.source1 {
		client = c.s3Client1
		downloader = c.s3Downloader1
	} else {
		client = c.s3Client2
		downloader = c.s3Downloader2
	}

	dataPath := source.DataPath
	if dataPath == "" {
		dataPath = source.S3.PathTemplate
	}

	// Replace {table} placeholder
	dataPath = strings.ReplaceAll(dataPath, "{table}", tableName)

	// Discover files for this table
	files, err := c.discoverS3DataFiles(ctx, source, client, dataPath)
	if err != nil {
		return 0, err
	}

	var totalCount int64
	for _, file := range files {
		count, err := c.countRowsInS3File(ctx, source, file, client, downloader)
		if err != nil {
			c.logger.Warn(fmt.Sprintf("Failed to count rows in %s: %v", file.Key, err))
			continue
		}
		totalCount += count
	}

	return totalCount, nil
}

// countRowsInS3File counts rows in an S3 data file
func (c *Comparer) countRowsInS3File(ctx context.Context, source *ComparisonSource, file S3File, client *s3.S3, downloader *s3manager.Downloader) (int64, error) {
	// Download file
	tempFile, err := os.CreateTemp("", "compare-count-*.tmp")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	_, err = downloader.DownloadWithContext(ctx, tempFile, &s3.GetObjectInput{
		Bucket: aws.String(source.S3.Bucket),
		Key:    aws.String(file.Key),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to download file: %w", err)
	}

	tempFile.Close()

	// Reopen for reading
	fileReader, err := os.Open(tempFile.Name())
	if err != nil {
		return 0, fmt.Errorf("failed to open temp file: %w", err)
	}
	defer fileReader.Close()

	// Decompress
	compressor, err := compressors.GetCompressor(file.DetectedCompression)
	if err != nil {
		return 0, fmt.Errorf("failed to get compressor: %w", err)
	}

	decompressedReader, err := compressor.NewReader(fileReader)
	if err != nil {
		return 0, fmt.Errorf("failed to create decompression reader: %w", err)
	}
	defer decompressedReader.Close()

	// Count rows
	var count int64
	switch file.DetectedFormat {
	case "jsonl":
		reader := formatters.NewJSONLReaderWithCloser(decompressedReader)
		for {
			chunk, err := reader.ReadChunk(1000)
			if err != nil && err != io.EOF {
				return 0, err
			}
			if len(chunk) == 0 {
				break
			}
			count += int64(len(chunk))
			if err == io.EOF {
				break
			}
		}
	case "csv":
		reader, err := formatters.NewCSVReaderWithCloser(decompressedReader)
		if err != nil {
			return 0, fmt.Errorf("failed to create CSV reader: %w", err)
		}
		for {
			chunk, err := reader.ReadChunk(1000)
			if err != nil && err != io.EOF {
				return 0, err
			}
			if len(chunk) == 0 {
				break
			}
			count += int64(len(chunk))
			if err == io.EOF {
				break
			}
		}
	case "parquet":
		reader, err := formatters.NewParquetReaderWithCloser(decompressedReader)
		if err != nil {
			return 0, fmt.Errorf("failed to create Parquet reader: %w", err)
		}
		rows, err := reader.ReadAll()
		if err != nil && err != io.EOF {
			return 0, fmt.Errorf("failed to read Parquet: %w", err)
		}
		count = int64(len(rows))
	default:
		return 0, fmt.Errorf("unsupported format: %s", file.DetectedFormat)
	}

	return count, nil
}

// compareRowByRow compares rows using checksums
func (c *Comparer) compareRowByRow(ctx context.Context, tableName string) (*RowByRowDiff, error) {
	hashes1, err := c.getRowHashes(ctx, c.source1, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get row hashes from source1: %w", err)
	}

	hashes2, err := c.getRowHashes(ctx, c.source2, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get row hashes from source2: %w", err)
	}

	// Count matches and differences
	matching := int64(0)
	missingInSource2 := int64(0)
	extraInSource2 := int64(0)

	hashSet2 := make(map[string]bool)
	for hash := range hashes2 {
		hashSet2[hash] = true
	}

	for hash := range hashes1 {
		if hashSet2[hash] {
			matching++
		} else {
			missingInSource2++
		}
	}

	for hash := range hashes2 {
		if !hashes1[hash] {
			extraInSource2++
		}
	}

	return &RowByRowDiff{
		Source1TotalRows: int64(len(hashes1)),
		Source2TotalRows: int64(len(hashes2)),
		MatchingRows:     matching,
		MissingInSource2: missingInSource2,
		ExtraInSource2:   extraInSource2,
	}, nil
}

// getRowHashes gets hashes for all rows from a source
func (c *Comparer) getRowHashes(ctx context.Context, source *ComparisonSource, tableName string) (map[string]bool, error) {
	hashes := make(map[string]bool)

	if source.Type == "db" {
		var db *sql.DB
		if source == c.source1 {
			db = c.db1
		} else {
			db = c.db2
		}

		rows, err := c.getRowsFromDatabase(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			hash := hashRow(row)
			hashes[hash] = true
		}
	} else {
		rows, err := c.getRowsFromS3(ctx, source, tableName)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			hash := hashRow(row)
			hashes[hash] = true
		}
	}

	return hashes, nil
}

// getRowsFromDatabase gets all rows from a database table
func (c *Comparer) getRowsFromDatabase(ctx context.Context, db *sql.DB, tableName string) ([]map[string]interface{}, error) {
	// Get schema first
	schema, err := c.getTableSchema(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	// Build SELECT query
	columns := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		columns[i] = pq.QuoteIdentifier(col.Name)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), pq.QuoteIdentifier(tableName))
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	columnNames := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		columnNames[i] = col.Name
	}

	values := make([]interface{}, len(columnNames))
	valuePtrs := make([]interface{}, len(columnNames))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columnNames {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	return result, rows.Err()
}

// getRowsFromS3 gets all rows from S3 data files
func (c *Comparer) getRowsFromS3(ctx context.Context, source *ComparisonSource, tableName string) ([]map[string]interface{}, error) {
	var client *s3.S3
	var downloader *s3manager.Downloader
	if source == c.source1 {
		client = c.s3Client1
		downloader = c.s3Downloader1
	} else {
		client = c.s3Client2
		downloader = c.s3Downloader2
	}

	dataPath := source.DataPath
	if dataPath == "" {
		dataPath = source.S3.PathTemplate
	}

	// Replace {table} placeholder
	dataPath = strings.ReplaceAll(dataPath, "{table}", tableName)

	// Discover files for this table
	files, err := c.discoverS3DataFiles(ctx, source, client, dataPath)
	if err != nil {
		return nil, err
	}

	var allRows []map[string]interface{}
	for _, file := range files {
		rows, err := c.readRowsFromS3File(ctx, source, file, client, downloader)
		if err != nil {
			c.logger.Warn(fmt.Sprintf("Failed to read rows from %s: %v", file.Key, err))
			continue
		}
		allRows = append(allRows, rows...)
	}

	return allRows, nil
}

// readRowsFromS3File reads all rows from an S3 data file
func (c *Comparer) readRowsFromS3File(ctx context.Context, source *ComparisonSource, file S3File, client *s3.S3, downloader *s3manager.Downloader) ([]map[string]interface{}, error) {
	// Download file
	tempFile, err := os.CreateTemp("", "compare-read-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	_, err = downloader.DownloadWithContext(ctx, tempFile, &s3.GetObjectInput{
		Bucket: aws.String(source.S3.Bucket),
		Key:    aws.String(file.Key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %w", err)
	}

	tempFile.Close()

	// Reopen for reading
	fileReader, err := os.Open(tempFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to open temp file: %w", err)
	}
	defer fileReader.Close()

	// Decompress
	compressor, err := compressors.GetCompressor(file.DetectedCompression)
	if err != nil {
		return nil, fmt.Errorf("failed to get compressor: %w", err)
	}

	decompressedReader, err := compressor.NewReader(fileReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create decompression reader: %w", err)
	}
	defer decompressedReader.Close()

	// Read rows
	var rows []map[string]interface{}
	switch file.DetectedFormat {
	case "jsonl":
		reader := formatters.NewJSONLReaderWithCloser(decompressedReader)
		rows, err = reader.ReadAll()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read JSONL: %w", err)
		}
	case "csv":
		reader, err := formatters.NewCSVReaderWithCloser(decompressedReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create CSV reader: %w", err)
		}
		rows, err = reader.ReadAll()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read CSV: %w", err)
		}
	case "parquet":
		reader, err := formatters.NewParquetReaderWithCloser(decompressedReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create Parquet reader: %w", err)
		}
		rows, err = reader.ReadAll()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read Parquet: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported format: %s", file.DetectedFormat)
	}

	return rows, nil
}

// compareSamples compares sample rows between sources
func (c *Comparer) compareSamples(ctx context.Context, tableName string) (*SampleDiff, error) {
	sample1, err := c.getSampleRows(ctx, c.source1, tableName, c.config.SampleSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get sample from source1: %w", err)
	}

	sample2, err := c.getSampleRows(ctx, c.source2, tableName, c.config.SampleSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get sample from source2: %w", err)
	}

	diff := &SampleDiff{
		Source1Sample: sample1,
		Source2Sample: sample2,
		Differences:   []string{},
	}

	// Compare samples
	if len(sample1) != len(sample2) {
		diff.Differences = append(diff.Differences, fmt.Sprintf("Sample sizes differ: source1=%d, source2=%d", len(sample1), len(sample2)))
	}

	minLen := len(sample1)
	if len(sample2) < minLen {
		minLen = len(sample2)
	}

	for i := 0; i < minLen; i++ {
		if !rowsEqual(sample1[i], sample2[i]) {
			diff.Differences = append(diff.Differences, fmt.Sprintf("Row %d differs", i))
		}
	}

	if len(diff.Differences) == 0 && len(sample1) == len(sample2) {
		return nil, nil // No differences
	}

	return diff, nil
}

// getSampleRows gets sample rows from a source
func (c *Comparer) getSampleRows(ctx context.Context, source *ComparisonSource, tableName string, sampleSize int) ([]map[string]interface{}, error) {
	var allRows []map[string]interface{}
	var err error

	if source.Type == "db" {
		var db *sql.DB
		if source == c.source1 {
			db = c.db1
		} else {
			db = c.db2
		}

		allRows, err = c.getRowsFromDatabase(ctx, db, tableName)
		if err != nil {
			return nil, err
		}
	} else {
		allRows, err = c.getRowsFromS3(ctx, source, tableName)
		if err != nil {
			return nil, err
		}
	}

	// Take first N rows
	if len(allRows) > sampleSize {
		return allRows[:sampleSize], nil
	}
	return allRows, nil
}

// rowsEqual checks if two rows are equal
func rowsEqual(row1, row2 map[string]interface{}) bool {
	if len(row1) != len(row2) {
		return false
	}

	for k, v1 := range row1 {
		v2, exists := row2[k]
		if !exists {
			return false
		}
		if v1 != v2 {
			return false
		}
	}

	return true
}

// outputResults outputs comparison results
func (c *Comparer) outputResults(result *ComparisonResult) error {
	var output io.Writer = os.Stdout
	if c.config.OutputFile != "" {
		file, err := os.Create(c.config.OutputFile)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer file.Close()
		output = file
	}

	if c.config.OutputFormat == "json" {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	// Human-readable format
	return c.outputTextFormat(result, output)
}

// outputTextFormat outputs results in human-readable text format
func (c *Comparer) outputTextFormat(result *ComparisonResult, w io.Writer) error {
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Fprintf(w, "COMPARISON RESULTS\n")
	fmt.Fprintf(w, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Fprintf(w, "\n")

	// Schema comparison results
	if result.Schema != nil {
		fmt.Fprintf(w, "SCHEMA COMPARISON\n")
		fmt.Fprintf(w, "─────────────────────────────────\n")

		if len(result.Schema.TablesOnlyInSource1) > 0 {
			fmt.Fprintf(w, "\n⚠️  Tables only in Source 1:\n")
			for _, table := range result.Schema.TablesOnlyInSource1 {
				fmt.Fprintf(w, "  • %s\n", table)
			}
		}

		if len(result.Schema.TablesOnlyInSource2) > 0 {
			fmt.Fprintf(w, "\n⚠️  Tables only in Source 2:\n")
			for _, table := range result.Schema.TablesOnlyInSource2 {
				fmt.Fprintf(w, "  • %s\n", table)
			}
		}

		if len(result.Schema.TableDiffs) > 0 {
			fmt.Fprintf(w, "\n⚠️  Table Schema Differences:\n")
			for tableName, diff := range result.Schema.TableDiffs {
				fmt.Fprintf(w, "\n  Table: %s\n", tableName)

		// Check if one table has no columns
		// If all columns from one source are "only in" that source and the other has none,
		// it means the other table has no columns
		if len(diff.ColumnsOnlyInSource1) > 0 && len(diff.ColumnsOnlyInSource2) == 0 {
			// All columns are only in source1, and none in source2 - source2 table has no columns
			fmt.Fprintf(w, "    ⚠️  Source 2 table has NO COLUMNS (Source 1 has %d columns)\n", len(diff.ColumnsOnlyInSource1))
		} else if len(diff.ColumnsOnlyInSource2) > 0 && len(diff.ColumnsOnlyInSource1) == 0 {
			// All columns are only in source2, and none in source1 - source1 table has no columns
			fmt.Fprintf(w, "    ⚠️  Source 1 table has NO COLUMNS (Source 2 has %d columns)\n", len(diff.ColumnsOnlyInSource2))
		}

				if len(diff.ColumnsOnlyInSource1) > 0 {
					fmt.Fprintf(w, "    Columns only in Source 1:\n")
					for _, col := range diff.ColumnsOnlyInSource1 {
						fmt.Fprintf(w, "      • %s (%s)\n", col.Name, col.UDTName)
					}
				}

				if len(diff.ColumnsOnlyInSource2) > 0 {
					fmt.Fprintf(w, "    Columns only in Source 2:\n")
					for _, col := range diff.ColumnsOnlyInSource2 {
						fmt.Fprintf(w, "      • %s (%s)\n", col.Name, col.UDTName)
					}
				}

				if len(diff.TypeMismatches) > 0 {
					fmt.Fprintf(w, "    Type Mismatches:\n")
					for _, mismatch := range diff.TypeMismatches {
						fmt.Fprintf(w, "      • %s: Source1=%s, Source2=%s\n",
							mismatch.ColumnName, mismatch.Source1Type, mismatch.Source2Type)
					}
				}
			}
		}

		if len(result.Schema.TablesOnlyInSource1) == 0 &&
			len(result.Schema.TablesOnlyInSource2) == 0 &&
			len(result.Schema.TableDiffs) == 0 {
			fmt.Fprintf(w, "✅ No schema differences found\n")
		}

		fmt.Fprintf(w, "\n")
	}

	// Data comparison results
	if result.Data != nil {
		fmt.Fprintf(w, "DATA COMPARISON\n")
		fmt.Fprintf(w, "─────────────────────────────────\n")

		// Row count differences
		if len(result.Data.RowCountDiffs) > 0 {
			fmt.Fprintf(w, "\n⚠️  Row Count Differences:\n")
			for tableName, diff := range result.Data.RowCountDiffs {
				fmt.Fprintf(w, "  Table: %s\n", tableName)
				fmt.Fprintf(w, "    Source 1: %d rows\n", diff.Source1Count)
				fmt.Fprintf(w, "    Source 2: %d rows\n", diff.Source2Count)
				if diff.Difference > 0 {
					fmt.Fprintf(w, "    Difference: +%d rows in Source 1\n", diff.Difference)
				} else {
					fmt.Fprintf(w, "    Difference: %d rows (Source 2 has more)\n", diff.Difference)
				}
				fmt.Fprintf(w, "\n")
			}
		}

		// Row-by-row differences
		if len(result.Data.RowByRowDiffs) > 0 {
			fmt.Fprintf(w, "\n⚠️  Row-by-Row Differences:\n")
			for tableName, diff := range result.Data.RowByRowDiffs {
				fmt.Fprintf(w, "  Table: %s\n", tableName)
				fmt.Fprintf(w, "    Source 1 total rows: %d\n", diff.Source1TotalRows)
				fmt.Fprintf(w, "    Source 2 total rows: %d\n", diff.Source2TotalRows)
				fmt.Fprintf(w, "    Matching rows: %d\n", diff.MatchingRows)
				fmt.Fprintf(w, "    Missing in Source 2: %d\n", diff.MissingInSource2)
				fmt.Fprintf(w, "    Extra in Source 2: %d\n", diff.ExtraInSource2)
				fmt.Fprintf(w, "\n")
			}
		}

		// Sample differences
		if len(result.Data.SampleDiffs) > 0 {
			fmt.Fprintf(w, "\n⚠️  Sample Differences:\n")
			for tableName, diff := range result.Data.SampleDiffs {
				fmt.Fprintf(w, "  Table: %s\n", tableName)
				if len(diff.Differences) > 0 {
					fmt.Fprintf(w, "    Differences:\n")
					for _, d := range diff.Differences {
						fmt.Fprintf(w, "      • %s\n", d)
					}
				}
				fmt.Fprintf(w, "\n")
			}
		}

		// Check if no differences
		hasDifferences := len(result.Data.RowCountDiffs) > 0 ||
			len(result.Data.RowByRowDiffs) > 0 ||
			len(result.Data.SampleDiffs) > 0

		if !hasDifferences {
			fmt.Fprintf(w, "✅ No data differences found\n")
		}

		fmt.Fprintf(w, "\n")
	}

	fmt.Fprintf(w, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	return nil
}

// hashRow generates an MD5 hash for a row
func hashRow(row map[string]interface{}) string {
	// Sort keys for consistency
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build a string representation
	var parts []string
	for _, k := range keys {
		val := row[k]
		parts = append(parts, fmt.Sprintf("%s=%v", k, val))
	}

	hash := md5.Sum([]byte(strings.Join(parts, "|")))
	return fmt.Sprintf("%x", hash)
}
