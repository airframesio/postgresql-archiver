package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/lib/pq"
)

type Phase int

const (
	PhaseConnecting Phase = iota
	PhaseCheckingPermissions
	PhaseDiscovering
	PhaseCounting
	PhaseProcessing
	PhaseComplete
)

// sliceResultEntry represents a single slice processing result with its date
type sliceResultEntry struct {
	date   string
	result ProcessResult
}

// safeSliceResults is a thread-safe wrapper for storing and retrieving slice processing results.
// It uses a read-write mutex to allow concurrent reads while ensuring exclusive writes.
// The structure automatically limits storage to the most recent maxSliceResults entries to prevent unbounded memory growth.
type safeSliceResults struct {
	mu      sync.RWMutex
	results []sliceResultEntry
}

// maxSliceResults is the maximum number of slice results to retain in memory
const maxSliceResults = 10

// newSafeSliceResults creates a new thread-safe slice results container
func newSafeSliceResults() *safeSliceResults {
	return &safeSliceResults{
		results: make([]sliceResultEntry, 0, maxSliceResults),
	}
}

// append adds a new slice result to the container.
// If the container exceeds maxSliceResults entries, the oldest entries are discarded.
// This method is safe for concurrent use.
func (s *safeSliceResults) append(date string, result ProcessResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.results = append(s.results, sliceResultEntry{
		date:   date,
		result: result,
	})

	// Keep only the last maxSliceResults to avoid memory growth
	if len(s.results) > maxSliceResults {
		s.results = s.results[len(s.results)-maxSliceResults:]
	}
}

// clear removes all slice results from the container.
// This method is safe for concurrent use.
func (s *safeSliceResults) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = s.results[:0] // Preserve capacity
}

// len returns the current number of slice results in the container.
// This method is safe for concurrent use.
func (s *safeSliceResults) len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.results)
}

// getRecent returns the n most recent slice results.
// If fewer than n results exist, all results are returned.
// The returned slice is a defensive copy and safe to use without holding the lock.
// This method is safe for concurrent use.
func (s *safeSliceResults) getRecent(n int) []sliceResultEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if n <= 0 {
		return nil
	}

	startIndex := 0
	if len(s.results) > n {
		startIndex = len(s.results) - n
	}

	// Make a defensive copy to avoid holding the lock during rendering
	resultsCopy := make([]sliceResultEntry, len(s.results[startIndex:]))
	copy(resultsCopy, s.results[startIndex:])
	return resultsCopy
}

type progressModel struct {
	phase           Phase
	partitions      []PartitionInfo
	currentIndex    int
	currentProgress progress.Model
	overallProgress progress.Model
	currentSpinner  spinner.Model
	currentStage    string
	currentRows     int64
	totalRows       int64
	done            bool
	width           int
	height          int
	results         []ProcessResult
	startTime       time.Time
	messages        []string
	countProgress   int
	countTotal      int
	config          *Config
	archiver        *Archiver
	errChan         chan<- error
	resultsChan     chan<- []ProcessResult
	initialized     bool
	ctx             context.Context
	cancel          context.CancelFunc // Cancel function to stop all operations
	pendingTables   []struct {
		name string
		date time.Time
	}
	countedPartitions   []PartitionInfo
	currentCountIndex   int
	partitionCache      *PartitionCache
	processingStartTime time.Time
	taskInfo            *TaskInfo
	program             *tea.Program // Reference for sending messages from goroutines
	// Slice progress tracking
	currentSliceIndex    int
	totalSlices          int
	currentSliceDate     string
	sliceProgress        progress.Model
	sliceResults         *safeSliceResults
	totalSlicesProcessed int // Track total slices processed across all partitions
}

type progressMsg struct {
	stage   string
	current int64
	total   int64
}

type partitionCompleteMsg struct {
	index  int
	result ProcessResult
}

type allCompleteMsg struct{}

type phaseMsg struct {
	phase   Phase
	message string
}

type partitionsFoundMsg struct {
	partitions []PartitionInfo
}

type discoveredTablesMsg struct {
	tables []struct {
		name string
		date time.Time
	}
}

type countProgressMsg struct {
	current   int
	total     int
	tableName string
}

type tableCountedMsg struct {
	table struct {
		name string
		date time.Time
	}
	count     int64
	fromCache bool
}

type messageMsg string

type stageUpdateMsg struct {
	stage string
}

type stageTickMsg time.Time

type connectedMsg struct {
	host string
}

type setProgramMsg struct {
	program *tea.Program
}

type sliceStartMsg struct {
	partitionIndex int
	sliceIndex     int
	totalSlices    int
	sliceDate      string
}

type sliceCompleteMsg struct {
	partitionIndex int
	sliceIndex     int
	success        bool
	result         ProcessResult
	sliceDate      string
}

var (
	helpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#626262")).
			Margin(0, 2)

	stageStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575")).
			Margin(0, 2)

	tableHeaderStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFAA00")).
				Bold(true).
				Margin(0, 2)

	progressInfoStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#888888")).
				Margin(0, 2)
)

// updateTaskInfo updates the task info file with current progress
func (m *progressModel) updateTaskInfo() {
	if m.taskInfo != nil {
		m.taskInfo.CurrentTask = m.currentStage

		// Set current partition and step info
		if m.currentIndex < len(m.partitions) {
			m.taskInfo.CurrentPartition = m.partitions[m.currentIndex].TableName
			m.taskInfo.CurrentStep = m.currentStage
		} else {
			m.taskInfo.CurrentPartition = ""
			m.taskInfo.CurrentStep = ""
		}

		// Update partition statistics
		if len(m.partitions) > 0 {
			m.taskInfo.TotalItems = len(m.partitions)
			m.taskInfo.CompletedItems = m.currentIndex
			m.taskInfo.Progress = float64(m.currentIndex) / float64(len(m.partitions))
			m.taskInfo.TotalPartitions = len(m.partitions)
			m.taskInfo.PartitionsProcessed = m.currentIndex
		}

		// Update counting statistics
		if m.countTotal > 0 {
			m.taskInfo.PartitionsCounted = m.countProgress
		}

		// Update slice tracking fields if slicing is active
		if m.totalSlices > 0 {
			m.taskInfo.CurrentSliceIndex = m.currentSliceIndex
			m.taskInfo.TotalSlices = m.totalSlices
			m.taskInfo.CurrentSliceDate = m.currentSliceDate
		} else {
			// Clear slice fields when not slicing
			m.taskInfo.CurrentSliceIndex = 0
			m.taskInfo.TotalSlices = 0
			m.taskInfo.CurrentSliceDate = ""
		}

		// Update total slices processed
		m.taskInfo.SlicesProcessed = m.totalSlicesProcessed

		_ = WriteTaskInfo(m.taskInfo)
	}
}

func newProgressModelWithArchiver(ctx context.Context, cancel context.CancelFunc, config *Config, archiver *Archiver, errChan chan<- error, resultsChan chan<- []ProcessResult, taskInfo *TaskInfo) progressModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	currentProg := progress.New(
		progress.WithDefaultGradient(),
		progress.WithWidth(60),
	)

	overallProg := progress.New(
		progress.WithScaledGradient("#FF7CCB", "#FDFF8C"),
		progress.WithWidth(60),
	)

	sliceProg := progress.New(
		progress.WithScaledGradient("#9B59B6", "#3498DB"),
		progress.WithWidth(50),
	)

	// Load cache for row counts
	cache, _ := loadPartitionCache(config.CacheScope)
	if cache == nil {
		cache = &PartitionCache{
			Entries: make(map[string]PartitionCacheEntry),
		}
	}

	return progressModel{
		phase:           PhaseConnecting,
		currentProgress: currentProg,
		overallProgress: overallProg,
		sliceProgress:   sliceProg,
		currentSpinner:  s,
		currentStage:    "Initializing...",
		results:         make([]ProcessResult, 0),
		messages:        make([]string, 0),
		startTime:       time.Now(),
		config:          config,
		archiver:        archiver,
		errChan:         errChan,
		resultsChan:     resultsChan,
		initialized:     false,
		ctx:             ctx,
		cancel:          cancel,
		partitionCache:  cache,
		taskInfo:        taskInfo,
		sliceResults:    newSafeSliceResults(),
	}
}

func (m progressModel) Init() tea.Cmd {
	return tea.Batch(
		m.currentSpinner.Tick,
		tea.EnterAltScreen,
		m.startArchiving(),
	)
}

func (m *progressModel) startArchiving() tea.Cmd {
	return func() tea.Msg {
		// Return a message to start the process
		return messageMsg("🚀 Starting archive process...")
	}
}

// runArchiving was replaced by direct processing logic
/*
func (m *progressModel) runArchiving() tea.Cmd {
	return func() tea.Msg {
		// Start connecting
		return phaseMsg{
			phase:   PhaseConnecting,
			message: "Connecting to database...",
		}
	}
}
*/

func (m *progressModel) doConnect() tea.Cmd {
	return func() tea.Msg {
		if m.archiver == nil {
			return messageMsg("❌ Archiver not initialized")
		}

		// Connect to database
		if err := m.archiver.connect(m.ctx); err != nil {
			if m.errChan != nil {
				m.errChan <- err
			}
			return messageMsg(fmt.Sprintf("❌ Failed to connect: %v", err))
		}

		// Connection successful - return a composite message
		return connectedMsg{
			host: m.config.Database.Host,
		}
	}
}

func (m *progressModel) doCheckPermissions() tea.Cmd {
	return func() tea.Msg {
		if m.archiver == nil {
			return messageMsg("❌ Archiver not initialized")
		}

		// Check table permissions
		if err := m.archiver.checkTablePermissions(m.ctx); err != nil {
			if m.errChan != nil {
				m.errChan <- fmt.Errorf("permission check failed: %w", err)
			}
			// Return error message - will need to handle exit separately
			return messageMsg(fmt.Sprintf("❌ Permission check failed: %v", err))
		}

		// Permissions verified - return success message
		return messageMsg("✅ Table permissions verified")
	}
}

func (m *progressModel) doDiscover() tea.Cmd {
	return func() tea.Msg {
		if m.archiver == nil || m.archiver.db == nil {
			return messageMsg("❌ Database not connected")
		}

		// First, collect all matching table names
		var matchingTables []struct {
			name string
			date time.Time
		}

		var startDate, endDate time.Time
		if m.config.StartDate != "" {
			startDate, _ = time.Parse("2006-01-02", m.config.StartDate)
		}
		if m.config.EndDate != "" {
			endDate, _ = time.Parse("2006-01-02", m.config.EndDate)
		}

		discoveredCount := 0
		skippedCount := 0
		seenTables := make(map[string]bool) // Track tables to avoid duplicates

		// Helper function to process tables from a query result
		processTableRows := func(rows *sql.Rows, sourceType string) error {
			defer rows.Close()
			for rows.Next() {
				var tableName string
				if err := rows.Scan(&tableName); err != nil {
					return fmt.Errorf("failed to scan %s name: %w", sourceType, err)
				}

				// Skip if we've already seen this table
				if seenTables[tableName] {
					continue
				}
				seenTables[tableName] = true

				// Try to extract date from different partition naming formats
				date, ok := m.archiver.extractDateFromTableName(tableName)
				if !ok {
					skippedCount++
					continue
				}

				if m.config.StartDate != "" && date.Before(startDate) {
					skippedCount++
					continue
				}
				if m.config.EndDate != "" && date.After(endDate) {
					skippedCount++
					continue
				}

				// Validate that the table actually has columns before adding it
				// This prevents errors later when trying to process tables that exist
				// but have no columns or aren't valid tables
				schema, schemaErr := m.archiver.getTableSchema(context.Background(), tableName)
				if schemaErr != nil {
					skippedCount++
					continue
				}
				if schema == nil || len(schema.Columns) == 0 {
					skippedCount++
					continue
				}

				// Check if we have SELECT permission on the table
				var hasPermission bool
				checkPermissionQuery := `SELECT has_table_privilege('public.' || $1, 'SELECT')`
				if err := m.archiver.db.QueryRow(checkPermissionQuery, tableName).Scan(&hasPermission); err != nil {
					skippedCount++
					continue
				}
				if !hasPermission {
					skippedCount++
					continue
				}

				discoveredCount++
				matchingTables = append(matchingTables, struct {
					name string
					date time.Time
				}{name: tableName, date: date})
			}
			return rows.Err()
		}

		// Query for leaf partitions only (not intermediate parent partitions)
		// This handles hierarchical partitioning like: flights -> flights_2024 -> flights_2024_01 -> flights_2024_01_01
		rows, err := m.archiver.db.Query(leafPartitionListSQL, defaultTableSchema, m.config.Table)
		if err != nil {
			return messageMsg(fmt.Sprintf("❌ Failed to query partitions: %v", err))
		}
		if err := processTableRows(rows, "partition"); err != nil {
			return messageMsg(fmt.Sprintf("❌ Failed to scan partitions: %v", err))
		}

		// If enabled, also query non-partition tables matching the pattern
		if m.config.IncludeNonPartitionTables {
			nonPartitionRows, err := m.archiver.db.Query(nonPartitionTableListSQL, defaultTableSchema, m.config.Table)
			if err != nil {
				return messageMsg(fmt.Sprintf("❌ Failed to query non-partition tables: %v", err))
			}
			if err := processTableRows(nonPartitionRows, "non-partition table"); err != nil {
				return messageMsg(fmt.Sprintf("❌ Failed to scan non-partition tables: %v", err))
			}
		}

		if len(matchingTables) == 0 {
			partitions, err := m.archiver.buildDateRangePartition()
			if err != nil {
				return messageMsg(fmt.Sprintf("❌ %v", err))
			}
			return partitionsFoundMsg{partitions: partitions}
		}

		return discoveredTablesMsg{
			tables: matchingTables,
		}
	}
}

// startCounting was replaced by countNextTable logic
/*
func (m *progressModel) startCounting(tables []struct {
	name string
	date time.Time
}) tea.Cmd {
	// Instead of doing all counting in one go, just start with the first one
	if len(tables) == 0 {
		return func() tea.Msg {
			return partitionsFoundMsg{partitions: []PartitionInfo{}}
		}
	}

	// Store the tables to count
	m.pendingTables = tables
	m.countedPartitions = make([]PartitionInfo, 0, len(tables))
	m.currentCountIndex = 0

	// Start counting the first table
	return m.countNextTable()
}
*/

func (m *progressModel) countNextTable() tea.Cmd {
	if m.currentCountIndex >= len(m.pendingTables) {
		// Done counting all tables - clean expired entries and save cache
		if m.partitionCache != nil && m.config != nil {
			m.partitionCache.cleanExpired()
			_ = m.partitionCache.save(m.config.CacheScope)
		}
		return func() tea.Msg {
			return partitionsFoundMsg{partitions: m.countedPartitions}
		}
	}

	// Get the current table to count
	table := m.pendingTables[m.currentCountIndex]

	return func() tea.Msg {
		var count int64
		var fromCache bool

		// Try to get from cache first
		if m.partitionCache != nil {
			if cachedCount, ok := m.partitionCache.getRowCount(table.name, table.date); ok {
				count = cachedCount
				fromCache = true
			}
		}

		// If not in cache or expired, count from database
		if !fromCache {
			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table.name))
			if err := m.archiver.db.QueryRow(countQuery).Scan(&count); err == nil {
				// Save to cache (preserving existing metadata)
				if m.partitionCache != nil {
					m.partitionCache.setRowCount(table.name, count)
					// Save cache immediately after updating
					_ = m.partitionCache.save(m.config.CacheScope)
				}
			} else {
				// Even on error, continue to next table
				count = -1
			}
		}

		// Return a message with the count result
		return tableCountedMsg{
			table:     table,
			count:     count,
			fromCache: fromCache,
		}
	}
}

func (m *progressModel) processNext() tea.Cmd {
	if m.currentIndex >= len(m.partitions) {
		// Done processing all partitions
		if m.resultsChan != nil {
			m.resultsChan <- m.results
		}
		return func() tea.Msg {
			return allCompleteMsg{}
		}
	}

	// Get current partition
	partition := m.partitions[m.currentIndex]
	index := m.currentIndex

	// Set initial stages for display and record start time
	m.currentStage = "Checking if file exists in S3"
	m.processingStartTime = time.Now()
	m.updateTaskInfo() // Update task info with initial stage

	return func() tea.Msg {
		// Actually process the partition using the archiver
		if m.archiver != nil && m.archiver.db != nil {
			result := m.archiver.ProcessPartitionWithProgress(partition, m.program)

			// Debug: log any errors
			if result.Error != nil && m.config.Debug {
				fmt.Fprintf(os.Stderr, "Error processing partition %s: %v\n", partition.TableName, result.Error)
			}

			return partitionCompleteMsg{
				index:  index,
				result: result,
			}
		}

		// Fallback if archiver not available
		return partitionCompleteMsg{
			index: index,
			result: ProcessResult{
				Partition:  partition,
				Skipped:    true,
				SkipReason: "Archiver not initialized",
			},
		}
	}
}

func (m progressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKeyMsg(msg)
	case tea.WindowSizeMsg:
		return m.handleWindowSizeMsg(msg)
	case spinner.TickMsg:
		return m.handleSpinnerTickMsg(msg)
	case progress.FrameMsg:
		return m.handleProgressFrameMsg(msg)
	case phaseMsg:
		return m.handlePhaseMsg(msg)
	case messageMsg:
		return m.handleMessageMsg(msg)
	case connectedMsg:
		return m.handleConnectedMsg(msg)
	case setProgramMsg:
		m.program = msg.program
		return m, nil
	case discoveredTablesMsg:
		return m.handleDiscoveredTablesMsg(msg)
	case partitionsFoundMsg:
		return m.handlePartitionsFoundMsg(msg)
	case tableCountedMsg:
		return m.handleTableCountedMsg(msg)
	case countProgressMsg:
		return m.handleCountProgressMsg(msg)
	case progressMsg:
		return m.handleProgressMsg(msg)
	case partitionCompleteMsg:
		return m.handlePartitionCompleteMsg(msg)
	case allCompleteMsg:
		return m.handleAllCompleteMsg(msg)
	case stageUpdateMsg:
		return m.handleStageUpdateMsg(msg)
	case stageTickMsg:
		return m.handleStageTickMsg(msg)
	case sliceStartMsg:
		return m.handleSliceStartMsg(msg)
	case sliceCompleteMsg:
		return m.handleSliceCompleteMsg(msg)
	}
	return m, nil
}

func (m progressModel) handleKeyMsg(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "ctrl+c" || msg.String() == "q" {
		m.done = true
		// Cancel the context to stop all archival operations
		if m.cancel != nil {
			m.cancel()
		}
		return m, tea.Quit
	}
	return m, nil
}

func (m progressModel) handleWindowSizeMsg(msg tea.WindowSizeMsg) (tea.Model, tea.Cmd) {
	m.width = msg.Width
	m.height = msg.Height
	m.currentProgress.Width = msg.Width - 10
	m.overallProgress.Width = msg.Width - 10
	return m, nil
}

func (m progressModel) handleSpinnerTickMsg(msg spinner.TickMsg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	m.currentSpinner, cmd = m.currentSpinner.Update(msg)
	return m, cmd
}

func (m progressModel) handleProgressFrameMsg(msg progress.FrameMsg) (tea.Model, tea.Cmd) {
	progressModel, cmd := m.currentProgress.Update(msg)
	if pm, ok := progressModel.(progress.Model); ok {
		m.currentProgress = pm
	}

	overallModel, cmd2 := m.overallProgress.Update(msg)
	if om, ok := overallModel.(progress.Model); ok {
		m.overallProgress = om
	}

	sliceModel, cmd3 := m.sliceProgress.Update(msg)
	if sm, ok := sliceModel.(progress.Model); ok {
		m.sliceProgress = sm
	}

	return m, tea.Batch(cmd, cmd2, cmd3)
}

func (m progressModel) handlePhaseMsg(msg phaseMsg) (tea.Model, tea.Cmd) {
	m.phase = msg.phase
	m.currentStage = msg.message

	// Handle phase transitions
	switch msg.phase { //nolint:exhaustive // PhaseComplete is terminal
	case PhaseConnecting:
		return m, m.doConnect()
	case PhaseCheckingPermissions:
		return m, m.doCheckPermissions()
	case PhaseDiscovering:
		return m, m.doDiscover()
	case PhaseCounting:
		if len(m.pendingTables) > 0 {
			if m.currentCountIndex == 0 && len(m.pendingTables) > 0 {
				m.currentStage = fmt.Sprintf("Counting rows: 0/%d - %s", len(m.pendingTables), m.pendingTables[0].name)
			}
			return m, m.countNextTable()
		}
	case PhaseProcessing:
		if len(m.partitions) > 0 {
			return m, tea.Batch(
				m.processNext(),
				tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
					return stageTickMsg(t)
				}),
			)
		}
	}
	return m, nil
}

func (m progressModel) handleMessageMsg(msg messageMsg) (tea.Model, tea.Cmd) {
	m.messages = append(m.messages, string(msg))
	if len(m.messages) > 10 {
		m.messages = m.messages[len(m.messages)-10:]
	}

	msgStr := string(msg)

	if strings.Contains(msgStr, "Starting archive process") {
		m.phase = PhaseConnecting
		m.currentStage = "Connecting to database..."
		m.updateTaskInfo()
		return m, m.doConnect()
	}

	if strings.Contains(msgStr, "✅ Table permissions verified") && m.phase == PhaseCheckingPermissions {
		m.messages = append(m.messages, fmt.Sprintf("✅ Connected to S3 at s3://%s", m.config.S3.Bucket))
		if len(m.messages) > 10 {
			m.messages = m.messages[len(m.messages)-10:]
		}
		m.phase = PhaseDiscovering
		m.currentStage = "Discovering partitions..."
		m.updateTaskInfo()
		return m, m.doDiscover()
	}

	if strings.Contains(msgStr, "❌ Permission check failed") {
		return m, tea.Sequence(tea.ExitAltScreen, tea.Quit)
	}

	return m, nil
}

func (m progressModel) handleConnectedMsg(msg connectedMsg) (tea.Model, tea.Cmd) {
	m.messages = append(m.messages, fmt.Sprintf("✅ Connected to PostgreSQL at %s", msg.host))

	if m.config.CacheViewer {
		startBackgroundServices()
		go m.startCacheViewerServer()
		m.messages = append(m.messages, fmt.Sprintf("🌐 Cache viewer started at http://localhost:%d", m.config.ViewerPort))
	}

	if len(m.messages) > 10 {
		m.messages = m.messages[len(m.messages)-10:]
	}

	m.phase = PhaseCheckingPermissions
	m.currentStage = "Checking table permissions..."
	m.updateTaskInfo()
	return m, m.doCheckPermissions()
}

func (m progressModel) startCacheViewerServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", serveCacheViewer)
	mux.HandleFunc("/api/cache", serveCacheData)
	mux.HandleFunc("/api/status", serveStatusData)
	mux.HandleFunc("/ws", handleWebSocket)

	addr := fmt.Sprintf(":%d", m.config.ViewerPort)
	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		Handler:           mux,
	}

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		m.archiver.logger.Error(fmt.Sprintf("Cache viewer server error: %v", err))
	}
}

func (m progressModel) handleDiscoveredTablesMsg(msg discoveredTablesMsg) (tea.Model, tea.Cmd) {
	m.messages = append(m.messages, fmt.Sprintf("📊 Found %d partitions to process", len(msg.tables)))
	if len(m.messages) > 10 {
		m.messages = m.messages[len(m.messages)-10:]
	}

	// Update task info with total partitions discovered
	if m.taskInfo != nil {
		m.taskInfo.TotalPartitions = len(msg.tables)
		_ = WriteTaskInfo(m.taskInfo)
	}

	if m.config.SkipCount {
		partitions := make([]PartitionInfo, len(msg.tables))
		for i, table := range msg.tables {
			partitions[i] = PartitionInfo{
				TableName: table.name,
				Date:      table.date,
				RowCount:  -1,
			}
		}
		return m, func() tea.Msg {
			return partitionsFoundMsg{partitions: partitions}
		}
	}

	m.countTotal = len(msg.tables)
	m.countProgress = 0
	m.pendingTables = msg.tables
	m.countedPartitions = make([]PartitionInfo, 0, len(msg.tables))
	m.currentCountIndex = 0

	return m, func() tea.Msg {
		return phaseMsg{
			phase:   PhaseCounting,
			message: fmt.Sprintf("Counting rows in %d partitions...", len(msg.tables)),
		}
	}
}

func (m progressModel) handlePartitionsFoundMsg(msg partitionsFoundMsg) (tea.Model, tea.Cmd) {
	if m.phase == PhaseCounting {
		m.countProgress = m.countTotal
		m.messages = append(m.messages, fmt.Sprintf("✅ Finished counting rows in %d partitions", len(msg.partitions)))
		if len(m.messages) > 10 {
			m.messages = m.messages[len(m.messages)-10:]
		}
	}

	m.partitions = msg.partitions
	m.phase = PhaseProcessing
	m.results = make([]ProcessResult, 0, len(msg.partitions))
	m.currentIndex = 0
	m.currentStage = ""

	if m.taskInfo != nil {
		m.taskInfo.TotalPartitions = len(msg.partitions)
		_ = WriteTaskInfo(m.taskInfo)
	}

	m.messages = append(m.messages, fmt.Sprintf("🚀 Starting to process %d partitions", len(msg.partitions)))
	if len(m.messages) > 10 {
		m.messages = m.messages[len(m.messages)-10:]
	}

	if len(msg.partitions) == 1 && msg.partitions[0].HasCustomRange() {
		inclusiveEnd := msg.partitions[0].RangeEnd.Add(-24 * time.Hour).Format("2006-01-02")
		m.messages = append(m.messages, fmt.Sprintf("📆 %s is not partitioned; archiving rows from %s to %s via %s windows",
			msg.partitions[0].TableName,
			msg.partitions[0].RangeStart.Format("2006-01-02"),
			inclusiveEnd,
			m.config.OutputDuration))
		if len(m.messages) > 10 {
			m.messages = m.messages[len(m.messages)-10:]
		}
	}

	if len(msg.partitions) > 0 {
		return m, tea.Sequence(
			tea.Tick(200*time.Millisecond, func(time.Time) tea.Msg {
				return nil
			}),
			m.processNext(),
		)
	}
	return m, nil
}

func (m progressModel) handleTableCountedMsg(msg tableCountedMsg) (tea.Model, tea.Cmd) {
	if msg.count >= 0 {
		m.countedPartitions = append(m.countedPartitions, PartitionInfo{
			TableName: msg.table.name,
			Date:      msg.table.date,
			RowCount:  msg.count,
		})
	}

	m.currentCountIndex++
	m.countProgress = m.currentCountIndex
	m.currentStage = fmt.Sprintf("Counting rows: %d/%d - %s", m.currentCountIndex, m.countTotal, msg.table.name)

	return m, m.countNextTable()
}

func (m progressModel) handleCountProgressMsg(msg countProgressMsg) (tea.Model, tea.Cmd) {
	m.countProgress = msg.current
	m.countTotal = msg.total
	m.currentStage = fmt.Sprintf("Counting rows: %d/%d - %s", msg.current, msg.total, msg.tableName)
	return m, nil
}

func (m progressModel) handleProgressMsg(msg progressMsg) (tea.Model, tea.Cmd) {
	m.currentStage = msg.stage
	m.currentRows = msg.current
	m.totalRows = msg.total
	m.updateTaskInfo()

	if msg.total > 0 {
		percent := float64(msg.current) / float64(msg.total)
		cmd := m.currentProgress.SetPercent(percent)
		return m, cmd
	}
	return m, nil
}

func (m progressModel) handlePartitionCompleteMsg(msg partitionCompleteMsg) (tea.Model, tea.Cmd) {
	m.results = append(m.results, msg.result)
	m.currentIndex = msg.index + 1
	m.updateTaskInfo()

	m.currentStage = ""
	m.processingStartTime = time.Time{}
	// Reset slice progress tracking
	m.currentSliceIndex = 0
	m.totalSlices = 0
	m.currentSliceDate = ""
	m.sliceResults.clear() // Clear slice results for next partition

	if len(m.partitions) > 0 {
		overallPercent := float64(m.currentIndex) / float64(len(m.partitions))
		return m, tea.Batch(
			m.overallProgress.SetPercent(overallPercent),
			m.processNext(),
			tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
				return stageTickMsg(t)
			}),
		)
	}
	return m, nil
}

func (m progressModel) handleAllCompleteMsg(_ allCompleteMsg) (tea.Model, tea.Cmd) {
	m.phase = PhaseComplete
	m.done = true
	// Don't use ExitAltScreen so the completion summary stays visible
	return m, tea.Quit
}

func (m progressModel) handleStageUpdateMsg(msg stageUpdateMsg) (tea.Model, tea.Cmd) {
	m.currentStage = msg.stage
	m.updateTaskInfo()
	return m, nil
}

func (m progressModel) handleStageTickMsg(_ stageTickMsg) (tea.Model, tea.Cmd) {
	if m.phase == PhaseProcessing && m.currentIndex < len(m.partitions) && !m.processingStartTime.IsZero() {
		elapsed := time.Since(m.processingStartTime)

		if elapsed < 1*time.Second {
			m.currentStage = "Checking if file exists in S3"
		} else if elapsed < 3*time.Second {
			m.currentStage = "Extracting data"
		} else if elapsed < 5*time.Second {
			m.currentStage = "Compressing data"
		} else if elapsed < 7*time.Second {
			m.currentStage = "Uploading to S3"
		} else {
			m.currentStage = "Finalizing"
		}
		m.updateTaskInfo()

		return m, tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
			return stageTickMsg(t)
		})
	}
	return m, nil
}

func (m progressModel) handleSliceStartMsg(msg sliceStartMsg) (tea.Model, tea.Cmd) {
	m.currentSliceIndex = msg.sliceIndex
	m.totalSlices = msg.totalSlices
	m.currentSliceDate = msg.sliceDate
	return m, nil
}

func (m progressModel) handleSliceCompleteMsg(msg sliceCompleteMsg) (tea.Model, tea.Cmd) {
	// Store slice result for display in Recent Results
	m.sliceResults.append(msg.sliceDate, msg.result)

	// Increment total slices processed if slice was successfully processed
	if msg.success {
		m.totalSlicesProcessed++
		m.updateTaskInfo()
	}

	return m, nil
}

// renderBanner renders the ASCII banner with logo
func (m progressModel) renderBanner() []string {
	var sections []string

	titleStyle1 := lipgloss.NewStyle().Foreground(lipgloss.Color("#FF7CCB")).Bold(true)
	titleStyle2 := lipgloss.NewStyle().Foreground(lipgloss.Color("#FDFF8C")).Bold(true)
	authorStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#999999"))

	const boxWidth = 66
	const indent = "   "

	makeLine := func(content string) string {
		visibleWidth := lipgloss.Width(content)
		targetWidth := boxWidth - 4
		padding := targetWidth - visibleWidth
		if padding < 0 {
			padding = 0
		}
		return fmt.Sprintf("%s║  %s%s║", indent, content, strings.Repeat(" ", padding))
	}

	topBorder := indent + "╔" + strings.Repeat("═", boxWidth-2) + "╗"
	bottomBorder := indent + "╚" + strings.Repeat("═", boxWidth-2) + "╝"

	sections = append(sections, "")
	sections = append(sections, topBorder)
	sections = append(sections, makeLine(""))
	sections = append(sections, makeLine("                        "+titleStyle1.Render("PostgreSQL")))
	sections = append(sections, makeLine(""))
	sections = append(sections, makeLine(titleStyle1.Render("█████╗ ██████╗  ██████╗██╗  ██╗██╗██╗   ██╗███████╗██████╗")))
	sections = append(sections, makeLine(titleStyle1.Render("██╔══██╗██╔══██╗██╔════╝██║  ██║██║██║   ██║██╔════╝██╔══██╗")))
	sections = append(sections, makeLine(titleStyle2.Render("███████║██████╔╝██║     ███████║██║██║   ██║█████╗  ██████╔╝")))
	sections = append(sections, makeLine(titleStyle2.Render("██╔══██║██╔══██╗██║     ██╔══██║██║╚██╗ ██╔╝██╔══╝  ██╔══██╗")))
	sections = append(sections, makeLine(titleStyle2.Render("██║  ██║██║  ██║╚██████╗██║  ██║██║ ╚████╔╝ ███████╗██║  ██║")))
	sections = append(sections, makeLine(titleStyle2.Render("╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝")))
	sections = append(sections, makeLine(""))
	sections = append(sections, makeLine("        "+authorStyle.Render("Created by Airframes <hello@airframes.io>")))
	sections = append(sections, makeLine("     "+authorStyle.Render("https://github.com/airframesio/postgresql-archiver")))
	sections = append(sections, makeLine("                          "+authorStyle.Render(fmt.Sprintf("Version %s", Version))))

	// Display update notification if available
	if versionCheckResult != nil && versionCheckResult.UpdateAvailable {
		updateStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFB700")).
			Bold(true)
		updateMsg := fmt.Sprintf("💡 Update available: v%s → v%s",
			versionCheckResult.CurrentVersion,
			versionCheckResult.LatestVersion)
		sections = append(sections, makeLine(""))
		sections = append(sections, makeLine("              "+updateStyle.Render(updateMsg)))
	}

	sections = append(sections, makeLine(""))
	sections = append(sections, bottomBorder)
	sections = append(sections, "")

	return sections
}

// renderMessages renders the message log section
func (m progressModel) renderMessages() []string {
	var sections []string
	sections = append(sections, helpStyle.Render("   Log:"))
	if len(m.messages) == 0 {
		sections = append(sections, "     (waiting for operations...)")
	} else {
		for _, msg := range m.messages {
			sections = append(sections, "     "+msg)
		}
	}
	return sections
}

// renderSeparator renders a horizontal separator
func (m progressModel) renderSeparator() []string {
	separatorWidth := 80
	if m.width > 0 && m.width < 200 {
		separatorWidth = m.width - 6
	}
	separator := "   " + strings.Repeat("─", separatorWidth)
	return []string{"", lipgloss.NewStyle().Foreground(lipgloss.Color("#444")).Render(separator), ""}
}

// renderInitialPhase renders connecting/checking/discovering phases
func (m progressModel) renderInitialPhase() []string {
	var sections []string
	if m.currentStage != "" {
		stageInfo := fmt.Sprintf("   %s %s", m.currentSpinner.View(), m.currentStage)
		sections = append(sections, stageStyle.Render(stageInfo))
	} else {
		sections = append(sections, stageStyle.Render("   "+m.currentSpinner.View()+" Initializing..."))
	}
	return sections
}

// renderCountingPhase renders the counting phase
func (m progressModel) renderCountingPhase() []string {
	var sections []string
	if m.countTotal > 0 {
		barWidth := 30
		progress := float64(m.countProgress) / float64(m.countTotal)
		filled := int(progress * float64(barWidth))
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		percentage := int(progress * 100)

		countInfo := fmt.Sprintf("   %s Counting: %d/%d [%s] %d%%",
			m.currentSpinner.View(),
			m.countProgress,
			m.countTotal,
			bar,
			percentage)
		sections = append(sections, stageStyle.Render(countInfo))

		if m.currentStage != "" {
			sections = append(sections, "     "+m.currentStage)
		}
	}
	return sections
}

// renderProcessingPhase renders the processing phase
func (m progressModel) renderProcessingPhase() []string {
	var sections []string
	if len(m.partitions) > 0 {
		sections = append(sections, tableHeaderStyle.Render("   Processing Partitions"))
		sections = append(sections, "")

		// Show current partition name if we're actively processing
		var overallInfo string
		if m.currentIndex < len(m.partitions) {
			overallInfo = fmt.Sprintf("   Overall: %d/%d partitions (%s)", m.currentIndex, len(m.partitions), m.partitions[m.currentIndex].TableName)
		} else {
			overallInfo = fmt.Sprintf("   Overall: %d/%d partitions", m.currentIndex, len(m.partitions))
		}
		sections = append(sections, progressInfoStyle.Render(overallInfo))

		viewProgress := m.overallProgress.ViewAs(float64(m.currentIndex) / float64(len(m.partitions)))
		sections = append(sections, "   "+viewProgress)

		// Show slice progress if partition is being split
		if m.totalSlices > 0 {
			sections = append(sections, "")
			sliceInfo := fmt.Sprintf("   Partition Slices: %d/%d (%s)", m.currentSliceIndex+1, m.totalSlices, m.currentSliceDate)
			sections = append(sections, progressInfoStyle.Render(sliceInfo))
			viewSliceProgress := m.sliceProgress.ViewAs(float64(m.currentSliceIndex+1) / float64(m.totalSlices))
			sections = append(sections, "   "+viewSliceProgress)
		}

		if m.currentStage != "" {
			stageInfo := fmt.Sprintf("   %s %s", m.currentSpinner.View(), m.currentStage)
			sections = append(sections, "")
			sections = append(sections, stageStyle.Render(stageInfo))
		}

		if m.currentRows > 0 && m.totalRows > 0 {
			rowInfo := fmt.Sprintf("   Rows: %d/%d", m.currentRows, m.totalRows)
			sections = append(sections, progressInfoStyle.Render(rowInfo))
			viewCurrentProgress := m.currentProgress.ViewAs(float64(m.currentRows) / float64(m.totalRows))
			sections = append(sections, "   "+viewCurrentProgress)
		}

		sections = append(sections, "")
		sections = append(sections, m.renderProcessingSummary()...)
	}
	return sections
}

// Constants for recent results display
const (
	maxRecentPartitions = 3
	maxRecentSlices     = 5
)

// formatResultLine formats a ProcessResult into a display line with the given identifier (partition name or date)
func (m progressModel) formatResultLine(identifier string, result ProcessResult) string {
	const indent = "   "
	switch {
	case result.Skipped:
		return fmt.Sprintf("%s⏭  %s - %s", indent, identifier, result.SkipReason)
	case result.Error != nil:
		return fmt.Sprintf("%s❌ %s - Error: %v", indent, identifier, result.Error)
	case result.Uploaded && result.S3Key != "":
		return fmt.Sprintf("%s✅ %s → s3://%s/%s (%d bytes) - Completed in %.1f seconds",
			indent, identifier, m.config.S3.Bucket, result.S3Key, result.BytesWritten, result.Duration.Seconds())
	case result.Uploaded:
		return fmt.Sprintf("%s✅ %s - Uploaded %d bytes - Completed in %.1f seconds",
			indent, identifier, result.BytesWritten, result.Duration.Seconds())
	default:
		return fmt.Sprintf("%s⏸  %s - In progress", indent, identifier)
	}
}

// renderProcessingSummary renders summary of processing results
func (m progressModel) renderProcessingSummary() []string {
	var sections []string

	// Show completed partitions first, then current partition's slices
	hasResults := len(m.results) > 0 || m.sliceResults.len() > 0

	if hasResults {
		sections = append(sections, tableHeaderStyle.Render("   Recent Results"))
		sections = append(sections, "")

		// Show recent partition results first (completed work)
		partStartIndex := 0
		if len(m.results) > maxRecentPartitions {
			partStartIndex = len(m.results) - maxRecentPartitions
		}
		for _, result := range m.results[partStartIndex:] {
			line := m.formatResultLine(result.Partition.TableName, result)
			sections = append(sections, line)
		}

		// Show recent slice results (current partition's in-progress work) only if date-column is configured
		if m.config.DateColumn != "" {
			recentSlices := m.sliceResults.getRecent(maxRecentSlices)
			for _, sliceRes := range recentSlices {
				line := m.formatResultLine(sliceRes.date, sliceRes.result)
				sections = append(sections, line)
			}
		}

		sections = append(sections, "")
	}
	return sections
}

// renderCompletionSummary renders the final completion summary
func (m progressModel) renderCompletionSummary() []string {
	var sections []string

	// Create styled sections for better visual appeal
	successStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#04B575")).
		Bold(true)
	skipStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFAA00"))
	errorStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FF4444")).
		Bold(true)
	statStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#888888"))
	valueStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFFFFF")).
		Bold(true)

	sections = append(sections, "")
	sections = append(sections, tableHeaderStyle.Render("   ═══════════════════════════════════════════════"))
	sections = append(sections, tableHeaderStyle.Render("   Completion Summary"))
	sections = append(sections, tableHeaderStyle.Render("   ═══════════════════════════════════════════════"))
	sections = append(sections, "")

	// Count results by status and collect failures
	var uploaded, skipped, failed int
	var totalBytes int64
	var totalRows int64
	var failedResults []ProcessResult
	var totalDuration time.Duration
	var minDate, maxDate *time.Time

	for _, result := range m.results {
		if result.Error != nil {
			failed++
			failedResults = append(failedResults, result)
		} else if result.Skipped {
			skipped++
		} else if result.Uploaded {
			uploaded++
			totalBytes += result.BytesWritten
			if result.Partition.RowCount > 0 {
				totalRows += result.Partition.RowCount
			}
			totalDuration += result.Duration
		}

		// Track date range
		if !result.Partition.Date.IsZero() {
			if minDate == nil || result.Partition.Date.Before(*minDate) {
				minDate = &result.Partition.Date
			}
			if maxDate == nil || result.Partition.Date.After(*maxDate) {
				maxDate = &result.Partition.Date
			}
		}
	}

	// Calculate total elapsed time
	totalElapsed := time.Since(m.startTime)

	// Calculate success rate
	totalProcessed := uploaded + skipped + failed
	var successRate float64
	if totalProcessed > 0 {
		successRate = float64(uploaded) / float64(totalProcessed) * 100
	}

	// Show statistics with enhanced formatting
	sections = append(sections, statStyle.Render("   Total Partitions:"))
	sections = append(sections, fmt.Sprintf("   %s", valueStyle.Render(fmt.Sprintf("%d", len(m.partitions)))))
	sections = append(sections, "")

	sections = append(sections, successStyle.Render(fmt.Sprintf("   ✅ Uploaded: %d", uploaded)))
	if skipped > 0 {
		sections = append(sections, skipStyle.Render(fmt.Sprintf("   ⏭  Skipped: %d", skipped)))
	}
	if failed > 0 {
		sections = append(sections, errorStyle.Render(fmt.Sprintf("   ❌ Failed: %d", failed)))
	}

	// Show archive rate
	if totalProcessed > 0 {
		rateColor := successStyle
		if successRate < 50 {
			rateColor = errorStyle
		} else if successRate < 90 {
			rateColor = skipStyle
		}
		sections = append(sections, "")
		sections = append(sections, statStyle.Render("   Archive Rate:"))
		sections = append(sections, fmt.Sprintf("   %s", rateColor.Render(fmt.Sprintf("%.1f%%", successRate))))
	}

	sections = append(sections, "")

	// Show total rows transferred
	if totalRows > 0 {
		sections = append(sections, statStyle.Render("   Total Transferred:"))
		sections = append(sections, fmt.Sprintf("   %s", valueStyle.Render(formatNumber(totalRows)+" rows")))
		sections = append(sections, "")
	}

	// Show total bytes if any uploads occurred
	if totalBytes > 0 {
		sections = append(sections, statStyle.Render("   Total Data Uploaded:"))
		sections = append(sections, fmt.Sprintf("   %s", valueStyle.Render(formatBytes(totalBytes))))
		sections = append(sections, "")
	}

	// Show duration and throughput
	if totalElapsed > 0 {
		sections = append(sections, statStyle.Render("   Total Duration:"))
		sections = append(sections, fmt.Sprintf("   %s", valueStyle.Render(formatDuration(totalElapsed))))
		sections = append(sections, "")

		// Calculate throughput
		if totalRows > 0 && totalElapsed.Seconds() > 0 {
			rowsPerSec := float64(totalRows) / totalElapsed.Seconds()
			sections = append(sections, statStyle.Render("   Throughput:"))
			sections = append(sections, fmt.Sprintf("   %s", valueStyle.Render(fmt.Sprintf("%s rows/sec", formatFloat(rowsPerSec)))))
			if totalBytes > 0 {
				mbPerSec := float64(totalBytes) / (1024 * 1024) / totalElapsed.Seconds()
				sections = append(sections, fmt.Sprintf("   %s", valueStyle.Render(fmt.Sprintf("%s MB/sec", formatFloat(mbPerSec)))))
			}
			sections = append(sections, "")
		}

		// Show average time per partition
		if uploaded > 0 && totalDuration > 0 {
			avgDuration := totalDuration / time.Duration(uploaded)
			sections = append(sections, statStyle.Render("   Avg Time per Partition:"))
			sections = append(sections, fmt.Sprintf("   %s", valueStyle.Render(formatDuration(avgDuration))))
			sections = append(sections, "")
		}
	}

	// Show date range
	if minDate != nil && maxDate != nil {
		sections = append(sections, statStyle.Render("   Date Range:"))
		if minDate.Equal(*maxDate) {
			sections = append(sections, fmt.Sprintf("   %s", valueStyle.Render(minDate.Format("2006-01-02"))))
		} else {
			sections = append(sections, fmt.Sprintf("   %s", valueStyle.Render(fmt.Sprintf("%s to %s", minDate.Format("2006-01-02"), maxDate.Format("2006-01-02")))))
		}
		sections = append(sections, "")
	}

	// Show configuration summary
	if m.config != nil {
		sections = append(sections, statStyle.Render("   Configuration:"))
		configParts := []string{}
		if m.config.OutputFormat != "" {
			configParts = append(configParts, fmt.Sprintf("Format: %s", strings.ToUpper(m.config.OutputFormat)))
		}
		if m.config.Compression != "" {
			compStr := strings.ToUpper(m.config.Compression)
			if m.config.CompressionLevel > 0 {
				compStr += fmt.Sprintf(" (level %d)", m.config.CompressionLevel)
			}
			configParts = append(configParts, fmt.Sprintf("Compression: %s", compStr))
		}
		if m.config.S3.Bucket != "" {
			configParts = append(configParts, fmt.Sprintf("S3: s3://%s", m.config.S3.Bucket))
		}
		if len(configParts) > 0 {
			sections = append(sections, fmt.Sprintf("   %s", valueStyle.Render(strings.Join(configParts, " | "))))
		}
		sections = append(sections, "")
	}

	// List failures with details
	if len(failedResults) > 0 {
		sections = append(sections, errorStyle.Render("   Failed Partitions:"))
		sections = append(sections, "")
		for _, result := range failedResults {
			partitionName := result.Partition.TableName
			errorMsg := result.Error.Error()
			// Truncate long error messages for better display
			if len(errorMsg) > 80 {
				errorMsg = errorMsg[:77] + "..."
			}
			sections = append(sections, fmt.Sprintf("   %s %s", errorStyle.Render("❌"), statStyle.Render(partitionName+":")))
			sections = append(sections, fmt.Sprintf("      %s", errorStyle.Render(errorMsg)))
			sections = append(sections, "")
		}
	}

	sections = append(sections, "")
	return sections
}

// formatNumber formats large numbers with comma separators
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	str := fmt.Sprintf("%d", n)
	var result strings.Builder
	length := len(str)

	for i, char := range str {
		if i > 0 && (length-i)%3 == 0 {
			result.WriteString(",")
		}
		result.WriteRune(char)
	}

	return result.String()
}

// formatDuration formats a duration into a human-readable string
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh %dm", hours, minutes)
}

// formatFloat formats a float64 with appropriate precision
func formatFloat(f float64) string {
	if f < 0.01 {
		return fmt.Sprintf("%.4f", f)
	}
	if f < 1 {
		return fmt.Sprintf("%.2f", f)
	}
	if f < 1000 {
		return fmt.Sprintf("%.1f", f)
	}
	if f < 1000000 {
		return fmt.Sprintf("%.0f", f)
	}
	return fmt.Sprintf("%.2f", f)
}

func (m progressModel) View() string {
	var sections []string

	// Render banner
	sections = append(sections, m.renderBanner()...)

	// Render messages
	sections = append(sections, m.renderMessages()...)

	// Render separator
	sections = append(sections, m.renderSeparator()...)

	// Phase-specific content or completion summary
	if m.done && m.phase == PhaseComplete {
		sections = append(sections, m.renderCompletionSummary()...)
	} else {
		switch m.phase { //nolint:exhaustive // PhaseComplete is terminal
		case PhaseConnecting, PhaseCheckingPermissions, PhaseDiscovering:
			sections = append(sections, m.renderInitialPhase()...)
		case PhaseCounting:
			sections = append(sections, m.renderCountingPhase()...)
		case PhaseProcessing:
			sections = append(sections, m.renderProcessingPhase()...)
		}

		// Help text (only show during processing, not on completion)
		sections = append(sections, "")
		sections = append(sections, helpStyle.Render("   Press Ctrl+C or 'q' to quit"))
	}

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}

// formatBytes formats byte count into human-readable string
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func updateProgress(stage string, current, total int64) tea.Cmd {
	return func() tea.Msg {
		return progressMsg{
			stage:   stage,
			current: current,
			total:   total,
		}
	}
}

// Helper functions used by commented-out archiver functions
/*
func completePartition(index int, result ProcessResult) tea.Cmd {
	return func() tea.Msg {
		return partitionCompleteMsg{
			index:  index,
			result: result,
		}
	}
}

func completeAll() tea.Cmd {
	return func() tea.Msg {
		return allCompleteMsg{}
	}
}

func changePhase(phase Phase, message string) tea.Cmd {
	return func() tea.Msg {
		return phaseMsg{
			phase:   phase,
			message: message,
		}
	}
}

func addMessage(message string) tea.Cmd {
	return func() tea.Msg {
		return messageMsg(message)
	}
}
*/

// Unused helper function - kept for potential future use
/*
func setPartitions(partitions []PartitionInfo) tea.Cmd {
	return func() tea.Msg {
		return partitionsFoundMsg{
			partitions: partitions,
		}
	}
}

func updateCount(current, total int, tableName string) tea.Cmd {
	return func() tea.Msg {
		return countProgressMsg{
			current:   current,
			total:     total,
			tableName: tableName,
		}
	}
}
*/
