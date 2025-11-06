package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	// signalContext is set by main() before Cobra initialization
	// This ensures signal handling is set up before any library can interfere
	signalContext context.Context
	stopFilePath  string

	cfgFile          string
	debug            bool
	dbHost           string
	dbPort           int
	dbUser           string
	dbPassword       string
	dbName           string
	dbSSLMode        string
	s3Endpoint       string
	s3Bucket         string
	s3AccessKey      string
	s3SecretKey      string
	s3Region         string
	baseTable        string
	startDate        string
	endDate          string
	workers          int
	dryRun           bool
	skipCount        bool
	cacheViewer      bool
	viewerPort       int
	pathTemplate     string
	outputDuration   string
	outputFormat     string
	compression      string
	compressionLevel int
	dateColumn       string

	titleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7D56F4")).
			Bold(true).
			Underline(true)

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575")).
			Bold(true)

	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00D9FF"))

	warningStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFB700"))

	debugStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#666666")).
			Italic(true)

	logger *slog.Logger
)

// SetSignalContext stores the signal-aware context created in main()
// This must be called before Execute() to ensure proper signal handling
func SetSignalContext(ctx context.Context, stopFile string) {
	signalContext = ctx
	stopFilePath = stopFile
}

// initLogger initializes the slog logger based on debug flag
func initLogger(isDebug bool) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	if isDebug {
		opts.Level = slog.LevelDebug
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	logger = slog.New(handler)
}

var rootCmd = &cobra.Command{
	Use:   "postgresql-archiver",
	Short: "📦 Archive PostgreSQL partition data to object storage",
	Long: titleStyle.Render("PostgreSQL Archiver") + `

A CLI tool to efficiently archive PostgreSQL partitioned table data to object storage.
Extracts data by day, converts to JSONL, compresses with zstd, and uploads to S3-compatible storage.`,
	Run: func(_ *cobra.Command, _ []string) {
		runArchive()
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.postgresql-archiver.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debug output")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "perform a dry run without uploading")

	rootCmd.Flags().StringVar(&dbHost, "db-host", "localhost", "PostgreSQL host")
	rootCmd.Flags().IntVar(&dbPort, "db-port", 5432, "PostgreSQL port")
	rootCmd.Flags().StringVar(&dbUser, "db-user", "", "PostgreSQL user")
	rootCmd.Flags().StringVar(&dbPassword, "db-password", "", "PostgreSQL password")
	rootCmd.Flags().StringVar(&dbName, "db-name", "", "PostgreSQL database name")
	rootCmd.Flags().StringVar(&dbSSLMode, "db-sslmode", "disable", "PostgreSQL SSL mode (disable, require, verify-ca, verify-full)")

	rootCmd.Flags().StringVar(&s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint URL")
	rootCmd.Flags().StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket name")
	rootCmd.Flags().StringVar(&s3AccessKey, "s3-access-key", "", "S3 access key")
	rootCmd.Flags().StringVar(&s3SecretKey, "s3-secret-key", "", "S3 secret key")
	rootCmd.Flags().StringVar(&s3Region, "s3-region", "auto", "S3 region")

	rootCmd.Flags().StringVar(&baseTable, "table", "", "base table name (required)")
	rootCmd.Flags().StringVar(&startDate, "start-date", "", "start date (YYYY-MM-DD)")
	rootCmd.Flags().StringVar(&endDate, "end-date", time.Now().Format("2006-01-02"), "end date (YYYY-MM-DD)")
	rootCmd.Flags().IntVar(&workers, "workers", 4, "number of parallel workers")
	rootCmd.Flags().BoolVar(&skipCount, "skip-count", false, "skip counting rows (faster startup, no progress bars)")
	rootCmd.Flags().BoolVar(&cacheViewer, "cache-viewer", false, "start embedded cache viewer web server")
	rootCmd.Flags().IntVar(&viewerPort, "viewer-port", 8080, "port for cache viewer web server")

	// Output configuration flags
	rootCmd.Flags().StringVar(&pathTemplate, "path-template", "", "S3 path template with placeholders: {table}, {YYYY}, {MM}, {DD}, {HH} (required)")
	rootCmd.Flags().StringVar(&outputDuration, "output-duration", "daily", "output file duration: hourly, daily, weekly, monthly, yearly")
	rootCmd.Flags().StringVar(&outputFormat, "output-format", "jsonl", "output format: jsonl, csv, parquet")
	rootCmd.Flags().StringVar(&compression, "compression", "zstd", "compression type: zstd, lz4, gzip, none")
	rootCmd.Flags().IntVar(&compressionLevel, "compression-level", 3, "compression level (zstd: 1-22, lz4/gzip: 1-9, none: 0)")
	rootCmd.Flags().StringVar(&dateColumn, "date-column", "", "timestamp column name for duration-based splitting (optional)")

	// Note: We don't use MarkFlagRequired because it checks before viper loads the config file.
	// Instead, validation happens in config.Validate() which runs after all config sources are loaded.

	_ = viper.BindPFlag("debug", rootCmd.PersistentFlags().Lookup("debug"))
	_ = viper.BindPFlag("db.host", rootCmd.Flags().Lookup("db-host"))
	_ = viper.BindPFlag("db.port", rootCmd.Flags().Lookup("db-port"))
	_ = viper.BindPFlag("db.user", rootCmd.Flags().Lookup("db-user"))
	_ = viper.BindPFlag("cache_viewer", rootCmd.Flags().Lookup("cache-viewer"))
	_ = viper.BindPFlag("viewer_port", rootCmd.Flags().Lookup("viewer-port"))
	_ = viper.BindPFlag("db.password", rootCmd.Flags().Lookup("db-password"))
	_ = viper.BindPFlag("db.name", rootCmd.Flags().Lookup("db-name"))
	_ = viper.BindPFlag("db.sslmode", rootCmd.Flags().Lookup("db-sslmode"))
	_ = viper.BindPFlag("s3.endpoint", rootCmd.Flags().Lookup("s3-endpoint"))
	_ = viper.BindPFlag("s3.bucket", rootCmd.Flags().Lookup("s3-bucket"))
	_ = viper.BindPFlag("s3.access_key", rootCmd.Flags().Lookup("s3-access-key"))
	_ = viper.BindPFlag("s3.secret_key", rootCmd.Flags().Lookup("s3-secret-key"))
	_ = viper.BindPFlag("s3.region", rootCmd.Flags().Lookup("s3-region"))
	_ = viper.BindPFlag("table", rootCmd.Flags().Lookup("table"))
	_ = viper.BindPFlag("start_date", rootCmd.Flags().Lookup("start-date"))
	_ = viper.BindPFlag("end_date", rootCmd.Flags().Lookup("end-date"))
	_ = viper.BindPFlag("workers", rootCmd.Flags().Lookup("workers"))
	_ = viper.BindPFlag("dry_run", rootCmd.Flags().Lookup("dry-run"))
	_ = viper.BindPFlag("skip_count", rootCmd.Flags().Lookup("skip-count"))
	_ = viper.BindPFlag("s3.path_template", rootCmd.Flags().Lookup("path-template"))
	_ = viper.BindPFlag("output_duration", rootCmd.Flags().Lookup("output-duration"))
	_ = viper.BindPFlag("output_format", rootCmd.Flags().Lookup("output-format"))
	_ = viper.BindPFlag("compression", rootCmd.Flags().Lookup("compression"))
	_ = viper.BindPFlag("compression_level", rootCmd.Flags().Lookup("compression-level"))
	_ = viper.BindPFlag("date_column", rootCmd.Flags().Lookup("date-column"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".postgresql-archiver")
	}

	viper.SetEnvPrefix("ARCHIVE")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil && debug {
		// Initialize logger early if reading config in debug mode
		if logger == nil {
			initLogger(debug)
		}
		logger.Debug(debugStyle.Render("📄 Using config file: " + viper.ConfigFileUsed()))
	}
}

func runArchive() {
	// Add panic recovery to catch any unexpected crashes
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "\n❌ PANIC: %v\n", r)
			os.Exit(1)
		}
	}()

	config := &Config{
		Debug:       viper.GetBool("debug"),
		DryRun:      viper.GetBool("dry_run"),
		Workers:     viper.GetInt("workers"),
		SkipCount:   viper.GetBool("skip_count"),
		CacheViewer: viper.GetBool("cache_viewer"),
		ViewerPort:  viper.GetInt("viewer_port"),
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
		Table:            viper.GetString("table"),
		StartDate:        viper.GetString("start_date"),
		EndDate:          viper.GetString("end_date"),
		OutputDuration:   viper.GetString("output_duration"),
		OutputFormat:     viper.GetString("output_format"),
		Compression:      viper.GetString("compression"),
		CompressionLevel: viper.GetInt("compression_level"),
		DateColumn:       viper.GetString("date_column"),
	}

	// Initialize logger
	initLogger(config.Debug)

	// Log startup banner
	logger.Info(titleStyle.Render("\n🚀 PostgreSQL Archiver"))
	logger.Info(infoStyle.Render("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"))

	// Display stop instructions (for Warp terminal compatibility) - only in debug mode
	// In TUI mode, printing to stderr corrupts the display
	if config.Debug && stopFilePath != "" {
		fmt.Fprintln(os.Stderr, "\n"+infoStyle.Render("💡 To stop archiver: Press CTRL-C, or run:"))
		fmt.Fprintf(os.Stderr, "   "+infoStyle.Render("touch %s")+"\n\n", stopFilePath)
	}

	logger.Debug("Validating configuration...")
	if err := config.Validate(); err != nil {
		logger.Error(warningStyle.Render("❌ Configuration error: " + err.Error()))
		os.Exit(1)
	}
	logger.Debug("Configuration validated successfully")

	// Use the signal context created in main() before Cobra initialization
	// This ensures signals were registered before any library interference
	ctx := signalContext
	if ctx == nil {
		// Fallback if SetSignalContext wasn't called (shouldn't happen)
		logger.Warn("Signal context not set, creating fallback...")
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
	}

	// Set up a goroutine to force-exit if graceful shutdown takes too long
	exited := make(chan struct{})
	go func() {
		<-ctx.Done()
		logger.Info("\n⚠️  Interrupt signal received, shutting down...")

		// Wait for graceful shutdown, but force exit after 2 seconds
		select {
		case <-exited:
			// Graceful shutdown completed
			return
		case <-time.After(2 * time.Second):
			logger.Error("⚠️  Graceful shutdown timed out, forcing exit...")
			os.Exit(130)
		}
	}()

	logger.Debug("Creating archiver...")
	archiver := NewArchiver(config, logger)
	logger.Debug("Starting archival process...")

	err := archiver.Run(ctx)
	close(exited) // Signal that the archival process has exited

	if err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("\n⚠️  Archival cancelled by user")
			os.Exit(130)
		}
		logger.Error(warningStyle.Render("❌ Archive failed: " + err.Error()))
		os.Exit(1)
	}

	logger.Info(successStyle.Render("\n✅ Archive completed successfully!"))
}
