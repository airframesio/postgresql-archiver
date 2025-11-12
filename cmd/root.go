package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	// Version information - set via ldflags during build
	// Example: go build -ldflags "-X github.com/airframesio/data-archiver/cmd.Version=1.2.3"
	Version = "dev" // Default to "dev" if not set during build

	// signalContext is set by main() before Cobra initialization
	// This ensures signal handling is set up before any library can interfere
	signalContext context.Context
	stopFilePath  string

	// versionCheckResult stores the result of the background version check
	// This is shared between the startup check and TUI display
	versionCheckResult *VersionCheckResult

	cfgFile            string
	debug              bool
	logFormat          string
	dbHost             string
	dbPort             int
	dbUser             string
	dbPassword         string
	dbName             string
	dbSSLMode          string
	dbStatementTimeout int
	dbMaxRetries       int
	dbRetryDelay       int
	s3Endpoint         string
	s3Bucket           string
	s3AccessKey        string
	s3SecretKey        string
	s3Region           string
	baseTable          string
	startDate          string
	endDate            string
	workers            int
	dryRun             bool
	skipCount          bool
	cacheViewer        bool
	viewerPort         int
	chunkSize          int
	pathTemplate       string
	outputDuration     string
	outputFormat       string
	compression        string
	compressionLevel   int
	dateColumn         string

	titleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7D56F4")).
			Bold(true).
			Underline(true)

	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00D9FF"))

	logger *slog.Logger
)

// SetSignalContext stores the signal-aware context created in main()
// This must be called before Execute() to ensure proper signal handling
func SetSignalContext(ctx context.Context, stopFile string) {
	signalContext = ctx
	stopFilePath = stopFile
}

// broadcastLogHandler wraps a slog handler and broadcasts logs to WebSocket clients
type broadcastLogHandler struct {
	handler slog.Handler
}

func newBroadcastLogHandler(handler slog.Handler) *broadcastLogHandler {
	return &broadcastLogHandler{handler: handler}
}

func (h *broadcastLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *broadcastLogHandler) Handle(ctx context.Context, r slog.Record) error {
	// Broadcast log to WebSocket clients (non-blocking)
	// Only broadcast if logBroadcast channel is available (cache viewer mode)
	// Note: logBroadcast is initialized in cache_server.go at package level
	// Since both files are in the same package, it's accessible here
	if logBroadcast != nil {
		logMsg := LogMessage{
			Timestamp: r.Time.Format("2006-01-02 15:04:05"),
			Level:     r.Level.String(),
			Message:   r.Message,
		}
		select {
		case logBroadcast <- logMsg:
			// Successfully sent to broadcast channel
		default:
			// Channel full, skip broadcast to avoid blocking
			// This shouldn't happen often with a 1000 buffer
		}
	}

	// Always write to original handler (this ensures logs still appear in console)
	return h.handler.Handle(ctx, r)
}

func (h *broadcastLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &broadcastLogHandler{handler: h.handler.WithAttrs(attrs)}
}

func (h *broadcastLogHandler) WithGroup(name string) slog.Handler {
	return &broadcastLogHandler{handler: h.handler.WithGroup(name)}
}

// textOnlyHandler is a custom slog handler that outputs human-readable text
// without key=value pairs, suitable for interactive terminal usage
type textOnlyHandler struct {
	opts   slog.HandlerOptions
	writer io.Writer
}

func newTextOnlyHandler(w io.Writer, opts *slog.HandlerOptions) *textOnlyHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	return &textOnlyHandler{
		opts:   *opts,
		writer: w,
	}
}

func (h *textOnlyHandler) Enabled(_ context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return level >= minLevel
}

func (h *textOnlyHandler) Handle(_ context.Context, r slog.Record) error {
	// Format: YYYY-MM-DD HH:MM:SS LEVEL message
	timestamp := r.Time.Format("2006-01-02 15:04:05")
	level := r.Level.String()

	// Write the log entry
	_, err := fmt.Fprintf(h.writer, "%s %s %s\n", timestamp, level, r.Message)
	return err
}

func (h *textOnlyHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	// For simplicity, we ignore attributes in text-only mode
	return h
}

func (h *textOnlyHandler) WithGroup(_ string) slog.Handler {
	// For simplicity, we ignore groups in text-only mode
	return h
}

// initLogger initializes the slog logger based on debug flag and log format
func initLogger(isDebug bool, format string) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	if isDebug {
		opts.Level = slog.LevelDebug
	}

	var handler slog.Handler
	switch format {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	case "logfmt":
		// logfmt uses slog.TextHandler which outputs key=value pairs
		handler = slog.NewTextHandler(os.Stdout, opts)
	default: // "text" or anything else
		// For human-readable text output, we'll use a custom handler
		// that formats messages more naturally without key=value pairs
		handler = newTextOnlyHandler(os.Stdout, opts)
	}

	// Wrap handler to broadcast logs if logBroadcast channel exists (cache viewer mode)
	// Note: logBroadcast is only initialized in cache viewer mode
	// We'll check for it at runtime in the handler
	handler = newBroadcastLogHandler(handler)

	logger = slog.New(handler)
}

var rootCmd = &cobra.Command{
	Use:     "data-archiver",
	Version: Version,
	Short:   "📦 Archive database data to object storage (currently PostgreSQL → S3)",
	Long: titleStyle.Render("Data Archiver") + `

A CLI tool to efficiently archive database data to object storage.
Currently supports PostgreSQL input (partitioned tables) and S3-compatible storage output.
Extracts data by day, converts to JSONL/CSV/Parquet, compresses with zstd/lz4/gzip, and uploads.`,
	Run: func(_ *cobra.Command, _ []string) {
		runArchive()
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.data-archiver.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debug output")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "log format (text, logfmt, json)")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "perform a dry run without uploading")

	rootCmd.Flags().StringVar(&dbHost, "db-host", "localhost", "PostgreSQL host")
	rootCmd.Flags().IntVar(&dbPort, "db-port", 5432, "PostgreSQL port")
	rootCmd.Flags().StringVar(&dbUser, "db-user", "", "PostgreSQL user")
	rootCmd.Flags().StringVar(&dbPassword, "db-password", "", "PostgreSQL password")
	rootCmd.Flags().StringVar(&dbName, "db-name", "", "PostgreSQL database name")
	rootCmd.Flags().StringVar(&dbSSLMode, "db-sslmode", "disable", "PostgreSQL SSL mode (disable, require, verify-ca, verify-full)")
	rootCmd.Flags().IntVar(&dbStatementTimeout, "db-statement-timeout", 300, "PostgreSQL statement timeout in seconds (0 = no timeout)")
	rootCmd.Flags().IntVar(&dbMaxRetries, "db-max-retries", 3, "Maximum number of retry attempts for failed queries")
	rootCmd.Flags().IntVar(&dbRetryDelay, "db-retry-delay", 5, "Delay in seconds between retry attempts")

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
	rootCmd.Flags().BoolVar(&cacheViewer, "viewer", false, "start embedded cache viewer web server")
	rootCmd.Flags().IntVar(&viewerPort, "viewer-port", 8080, "port for cache viewer web server")
	rootCmd.Flags().IntVar(&chunkSize, "chunk-size", 10000, "number of rows to process in each chunk (streaming mode, 0 = auto)")

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
	_ = viper.BindPFlag("log_format", rootCmd.PersistentFlags().Lookup("log-format"))
	_ = viper.BindPFlag("db.host", rootCmd.Flags().Lookup("db-host"))
	_ = viper.BindPFlag("db.port", rootCmd.Flags().Lookup("db-port"))
	_ = viper.BindPFlag("db.user", rootCmd.Flags().Lookup("db-user"))
	_ = viper.BindPFlag("viewer", rootCmd.Flags().Lookup("viewer"))
	_ = viper.BindPFlag("viewer_port", rootCmd.Flags().Lookup("viewer-port"))
	_ = viper.BindPFlag("chunk_size", rootCmd.Flags().Lookup("chunk-size"))
	_ = viper.BindPFlag("db.password", rootCmd.Flags().Lookup("db-password"))
	_ = viper.BindPFlag("db.name", rootCmd.Flags().Lookup("db-name"))
	_ = viper.BindPFlag("db.sslmode", rootCmd.Flags().Lookup("db-sslmode"))
	_ = viper.BindPFlag("db.statement_timeout", rootCmd.Flags().Lookup("db-statement-timeout"))
	_ = viper.BindPFlag("db.max_retries", rootCmd.Flags().Lookup("db-max-retries"))
	_ = viper.BindPFlag("db.retry_delay", rootCmd.Flags().Lookup("db-retry-delay"))
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
		viper.SetConfigName(".data-archiver")
	}

	viper.SetEnvPrefix("ARCHIVE")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil && debug {
		// Initialize logger early if reading config in debug mode
		if logger == nil {
			initLogger(debug, logFormat)
		}
		logger.Debug(fmt.Sprintf("📄 Using config file: %s", viper.ConfigFileUsed()))
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
		LogFormat:   viper.GetString("log_format"),
		DryRun:      viper.GetBool("dry_run"),
		Workers:     viper.GetInt("workers"),
		SkipCount:   viper.GetBool("skip_count"),
		CacheViewer: viper.GetBool("viewer"),
		ViewerPort:  viper.GetInt("viewer_port"),
		ChunkSize:   viper.GetInt("chunk_size"),
		Database: DatabaseConfig{
			Host:             viper.GetString("db.host"),
			Port:             viper.GetInt("db.port"),
			User:             viper.GetString("db.user"),
			Password:         viper.GetString("db.password"),
			Name:             viper.GetString("db.name"),
			SSLMode:          viper.GetString("db.sslmode"),
			StatementTimeout: viper.GetInt("db.statement_timeout"),
			MaxRetries:       viper.GetInt("db.max_retries"),
			RetryDelay:       viper.GetInt("db.retry_delay"),
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
	initLogger(config.Debug, config.LogFormat)

	// Log startup banner
	logger.Info("")
	logger.Info(fmt.Sprintf("🚀 Data Archiver v%s", Version))
	logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// Display stop instructions (for Warp terminal compatibility) - only in debug mode
	// In TUI mode, printing to stderr corrupts the display
	if config.Debug && stopFilePath != "" {
		fmt.Fprintln(os.Stderr, "\n"+infoStyle.Render("💡 To stop archiver: Press CTRL-C, or run:"))
		fmt.Fprintf(os.Stderr, "   "+infoStyle.Render("touch %s")+"\n\n", stopFilePath)
	}

	logger.Debug("Validating configuration...")
	if err := config.Validate(); err != nil {
		logger.Error(fmt.Sprintf("❌ Configuration error: %s", err.Error()))
		os.Exit(1)
	}
	logger.Debug("Configuration validated successfully")

	// Check for updates in background (non-blocking)
	updateCheckDone := make(chan struct{})
	go func() {
		defer close(updateCheckDone)
		result := checkForUpdates(context.Background(), Version)
		versionCheckResult = &result

		if result.UpdateAvailable {
			logger.Info("")
			logger.Info(fmt.Sprintf("💡 %s", formatUpdateMessage(result)))
		} else if result.Error != nil && config.Debug {
			logger.Debug(fmt.Sprintf("Version check failed: %v", result.Error))
		}
	}()

	// Give version check a short time to complete, but don't block startup
	select {
	case <-updateCheckDone:
		// Version check completed quickly
	case <-time.After(2 * time.Second):
		// Continue without waiting further
		logger.Debug("Version check taking longer than expected, continuing...")
	}

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
		logger.Info("")
		logger.Info("⚠️  Interrupt signal received, shutting down...")

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
			logger.Info("")
			logger.Info("⚠️  Archival cancelled by user")
			os.Exit(130)
		}
		logger.Error(fmt.Sprintf("❌ Archive failed: %s", err.Error()))
		os.Exit(1)
	}

	logger.Info("")
	logger.Info("✅ Archive completed successfully!")
}
