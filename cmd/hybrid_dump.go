package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/viper"
)

var (
	errHybridDateRangeRequired = errors.New("hybrid dump requires --start-date and/or --end-date")
	errHybridTableRequired     = errors.New("hybrid dump requires --table to be specified")
)

func runHybridDump() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "\n❌ PANIC: %v\n", r)
			os.Exit(1)
		}
	}()

	config := &Config{
		Debug:     viper.GetBool("debug"),
		LogFormat: viper.GetString("log_format"),
		DryRun:    viper.GetBool("dry_run"),
		Workers:   viper.GetInt("workers"),
		Database: DatabaseConfig{
			Host:     viper.GetString("db.host"),
			Port:     viper.GetInt("db.port"),
			User:     viper.GetString("db.user"),
			Password: viper.GetString("db.password"),
			Name:     viper.GetString("db.name"),
			SSLMode:  viper.GetString("db.sslmode"),
		},
		S3: S3Config{
			Endpoint:     viper.GetString("s3.endpoint"),
			Bucket:       viper.GetString("s3.bucket"),
			AccessKey:    viper.GetString("s3.access_key"),
			SecretKey:    viper.GetString("s3.secret_key"),
			Region:       viper.GetString("s3.region"),
			PathTemplate: viper.GetString("s3.path_template"),
		},
		Table:          viper.GetString("table"),
		StartDate:      viper.GetString("start_date"),
		EndDate:        viper.GetString("end_date"),
		OutputDuration: viper.GetString("output_duration"),
		DateColumn:     viper.GetString("date_column"),
	}

	if config.OutputDuration == "" {
		config.OutputDuration = DurationDaily
	}

	initLogger(config.Debug, config.LogFormat)

	logger.Info("")
	logger.Info(fmt.Sprintf("🚀 Data Archiver v%s - hybrid pg_dump", Version))
	logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if err := validateHybridConfig(config); err != nil {
		logger.Error(fmt.Sprintf("❌ Configuration error: %s", err.Error()))
		os.Exit(1)
	}

	schemaConfig := *config
	schemaConfig.DumpMode = "schema-only"
	if err := schemaConfig.Validate(); err != nil {
		logger.Error(fmt.Sprintf("❌ Schema dump configuration error: %s", err.Error()))
		os.Exit(1)
	}

	dataConfig := *config
	dataConfig.DumpMode = "data-only"
	if err := dataConfig.Validate(); err != nil {
		logger.Error(fmt.Sprintf("❌ Data dump configuration error: %s", err.Error()))
		os.Exit(1)
	}

	ctx := signalContext
	if ctx == nil {
		logger.Warn("Signal context not set, creating fallback...")
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
	}

	logger.Info("Step 1/2: dumping schema (excluding partitions)...")
	schemaExecutor := NewPgDumpExecutor(&schemaConfig, logger)
	if err := schemaExecutor.Run(ctx); err != nil {
		handleHybridError("schema dump", err)
	}

	logger.Info("")
	logger.Info("Step 2/2: dumping data partitions by date range...")
	dataExecutor := NewPgDumpExecutor(&dataConfig, logger)
	if err := dataExecutor.Run(ctx); err != nil {
		handleHybridError("data dump", err)
	}

	logger.Info("")
	logger.Info("✅ Hybrid dump completed successfully!")
}

func validateHybridConfig(config *Config) error {
	if config.Table == "" {
		return errHybridTableRequired
	}
	if !isValidTableName(config.Table) {
		return fmt.Errorf("%w: '%s'", ErrTableNameInvalid, config.Table)
	}

	if config.S3.PathTemplate == "" {
		return ErrPathTemplateRequired
	}

	if config.StartDate == "" && config.EndDate == "" {
		return errHybridDateRangeRequired
	}

	var (
		startTime *time.Time
		endTime   *time.Time
	)

	if config.StartDate != "" {
		t, err := time.Parse("2006-01-02", config.StartDate)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrStartDateFormatInvalid, err)
		}
		startTime = &t
	}

	if config.EndDate != "" {
		t, err := time.Parse("2006-01-02", config.EndDate)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrEndDateFormatInvalid, err)
		}
		endTime = &t
	}

	if startTime != nil && endTime != nil && startTime.After(*endTime) {
		return fmt.Errorf("start date %s cannot be after end date %s", config.StartDate, config.EndDate)
	}

	if config.OutputDuration == "" {
		config.OutputDuration = DurationDaily
	}

	if !isValidOutputDuration(config.OutputDuration) {
		return fmt.Errorf("%w: '%s'", ErrOutputDurationInvalid, config.OutputDuration)
	}

	return nil
}

func handleHybridError(phase string, err error) {
	if errors.Is(err, context.Canceled) {
		logger.Info("")
		logger.Info(fmt.Sprintf("⚠️  %s cancelled by user", phase))
		os.Exit(130)
	}

	logger.Error(fmt.Sprintf("❌ %s failed: %s", phase, err.Error()))
	os.Exit(1)
}
