package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile     string
	debug       bool
	dbHost      string
	dbPort      int
	dbUser      string
	dbPassword  string
	dbName      string
	s3Endpoint  string
	s3Bucket    string
	s3AccessKey string
	s3SecretKey string
	s3Region    string
	baseTable   string
	startDate   string
	endDate     string
	workers     int
	dryRun      bool
	skipCount   bool
	cacheViewer bool
	viewerPort  int

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
)

var rootCmd = &cobra.Command{
	Use:   "postgresql-archiver",
	Short: "📦 Archive PostgreSQL partition data to object storage",
	Long: titleStyle.Render("PostgreSQL Archiver") + `

A CLI tool to efficiently archive PostgreSQL partitioned table data to object storage.
Extracts data by day, converts to JSONL, compresses with zstd, and uploads to S3-compatible storage.`,
	Run: func(cmd *cobra.Command, args []string) {
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

	_ = rootCmd.MarkFlagRequired("table")

	_ = viper.BindPFlag("debug", rootCmd.PersistentFlags().Lookup("debug"))
	_ = viper.BindPFlag("db.host", rootCmd.Flags().Lookup("db-host"))
	_ = viper.BindPFlag("db.port", rootCmd.Flags().Lookup("db-port"))
	_ = viper.BindPFlag("db.user", rootCmd.Flags().Lookup("db-user"))
	_ = viper.BindPFlag("cache_viewer", rootCmd.Flags().Lookup("cache-viewer"))
	_ = viper.BindPFlag("viewer_port", rootCmd.Flags().Lookup("viewer-port"))
	_ = viper.BindPFlag("db.password", rootCmd.Flags().Lookup("db-password"))
	_ = viper.BindPFlag("db.name", rootCmd.Flags().Lookup("db-name"))
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
		fmt.Println(debugStyle.Render("📄 Using config file: " + viper.ConfigFileUsed()))
	}
}

func runArchive() {
	fmt.Println(titleStyle.Render("\n🚀 PostgreSQL Archiver"))
	fmt.Println(infoStyle.Render("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"))

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
		},
		S3: S3Config{
			Endpoint:  viper.GetString("s3.endpoint"),
			Bucket:    viper.GetString("s3.bucket"),
			AccessKey: viper.GetString("s3.access_key"),
			SecretKey: viper.GetString("s3.secret_key"),
			Region:    viper.GetString("s3.region"),
		},
		Table:     viper.GetString("table"),
		StartDate: viper.GetString("start_date"),
		EndDate:   viper.GetString("end_date"),
	}

	if err := config.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, warningStyle.Render("❌ Configuration error: "+err.Error()))
		os.Exit(1)
	}

	archiver := NewArchiver(config)
	if err := archiver.Run(); err != nil {
		fmt.Fprintln(os.Stderr, warningStyle.Render("❌ Archive failed: "+err.Error()))
		os.Exit(1)
	}

	fmt.Println(successStyle.Render("\n✅ Archive completed successfully!"))
}
