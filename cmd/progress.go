package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
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
	pendingTables   []struct {
		name string
		date time.Time
	}
	countedPartitions   []PartitionInfo
	currentCountIndex   int
	partitionCache      *PartitionCache
	processingStartTime time.Time
	taskInfo            *TaskInfo
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

		if len(m.partitions) > 0 {
			m.taskInfo.TotalItems = len(m.partitions)
			m.taskInfo.CompletedItems = m.currentIndex
			m.taskInfo.Progress = float64(m.currentIndex) / float64(len(m.partitions))
		}
		_ = WriteTaskInfo(m.taskInfo)
	}
}

func newProgressModelWithArchiver(ctx context.Context, config *Config, archiver *Archiver, errChan chan<- error, resultsChan chan<- []ProcessResult, taskInfo *TaskInfo) progressModel {
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

	// Load cache for row counts
	cache, _ := loadPartitionCache(config.Table)
	if cache == nil {
		cache = &PartitionCache{
			Entries: make(map[string]PartitionCacheEntry),
		}
	}

	return progressModel{
		phase:           PhaseConnecting,
		currentProgress: currentProg,
		overallProgress: overallProg,
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
		partitionCache:  cache,
		taskInfo:        taskInfo,
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

		// Query for partitions
		query := `
			SELECT tablename
			FROM pg_tables
			WHERE schemaname = 'public'
				AND tablename LIKE $1
			ORDER BY tablename;
		`

		pattern := m.config.Table + "_%"
		rows, err := m.archiver.db.Query(query, pattern)
		if err != nil {
			return messageMsg(fmt.Sprintf("❌ Failed to query partitions: %v", err))
		}
		defer rows.Close()

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

		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				continue
			}

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

			discoveredCount++
			matchingTables = append(matchingTables, struct {
				name string
				date time.Time
			}{name: tableName, date: date})
		}

		// Check for errors from iterating over rows
		if err := rows.Err(); err != nil {
			return messageMsg(fmt.Sprintf("❌ Failed to scan partitions: %v", err))
		}

		// Return the discovered tables
		if len(matchingTables) == 0 {
			return messageMsg("⚠️ No matching partitions found")
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
			_ = m.partitionCache.save(m.config.Table)
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
					_ = m.partitionCache.save(m.config.Table)
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
			result := m.archiver.ProcessPartitionWithProgress(partition, index, nil)

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
	}
	return m, nil
}

func (m progressModel) handleKeyMsg(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "ctrl+c" || msg.String() == "q" {
		m.done = true
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

	return m, tea.Batch(cmd, cmd2)
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
		fmt.Printf("Cache viewer server error: %v\n", err)
	}
}

func (m progressModel) handleDiscoveredTablesMsg(msg discoveredTablesMsg) (tea.Model, tea.Cmd) {
	m.messages = append(m.messages, fmt.Sprintf("📊 Found %d partitions to process", len(msg.tables)))
	if len(m.messages) > 10 {
		m.messages = m.messages[len(m.messages)-10:]
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

	m.messages = append(m.messages, fmt.Sprintf("🚀 Starting to process %d partitions", len(msg.partitions)))
	if len(m.messages) > 10 {
		m.messages = m.messages[len(m.messages)-10:]
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

func (m progressModel) handleAllCompleteMsg(msg allCompleteMsg) (tea.Model, tea.Cmd) {
	m.phase = PhaseComplete
	m.done = true
	return m, tea.Sequence(tea.ExitAltScreen, tea.Quit)
}

func (m progressModel) handleStageUpdateMsg(msg stageUpdateMsg) (tea.Model, tea.Cmd) {
	m.currentStage = msg.stage
	m.updateTaskInfo()
	return m, nil
}

func (m progressModel) handleStageTickMsg(msg stageTickMsg) (tea.Model, tea.Cmd) {
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

		overallInfo := fmt.Sprintf("   Overall: %d/%d partitions", m.currentIndex, len(m.partitions))
		sections = append(sections, progressInfoStyle.Render(overallInfo))

		viewProgress := m.overallProgress.ViewAs(float64(m.currentIndex) / float64(len(m.partitions)))
		sections = append(sections, "   "+viewProgress)

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

// renderProcessingSummary renders summary of processing results
func (m progressModel) renderProcessingSummary() []string {
	var sections []string
	if len(m.results) > 0 {
		sections = append(sections, tableHeaderStyle.Render("   Recent Results"))
		sections = append(sections, "")

		startIndex := 0
		if len(m.results) > 5 {
			startIndex = len(m.results) - 5
		}

		for _, result := range m.results[startIndex:] {
			var line string
			if result.Skipped {
				line = fmt.Sprintf("   ⏭  %s - %s", result.Partition.TableName, result.SkipReason)
			} else if result.Error != nil {
				line = fmt.Sprintf("   ❌ %s - Error: %v", result.Partition.TableName, result.Error)
			} else if result.Uploaded {
				line = fmt.Sprintf("   ✅ %s - Uploaded %d bytes", result.Partition.TableName, result.BytesWritten)
			} else {
				line = fmt.Sprintf("   ⏸  %s - In progress", result.Partition.TableName)
			}
			sections = append(sections, line)
		}
		sections = append(sections, "")
	}
	return sections
}

func (m progressModel) View() string {
	if m.done && m.phase == PhaseComplete {
		return ""
	}

	var sections []string

	// Render banner
	sections = append(sections, m.renderBanner()...)

	// Render messages
	sections = append(sections, m.renderMessages()...)

	// Render separator
	sections = append(sections, m.renderSeparator()...)

	// Phase-specific content
	switch m.phase { //nolint:exhaustive // PhaseComplete is terminal
	case PhaseConnecting, PhaseCheckingPermissions, PhaseDiscovering:
		sections = append(sections, m.renderInitialPhase()...)
	case PhaseCounting:
		sections = append(sections, m.renderCountingPhase()...)
	case PhaseProcessing:
		sections = append(sections, m.renderProcessingPhase()...)
	}

	// Help text
	sections = append(sections, "")
	sections = append(sections, helpStyle.Render("   Press Ctrl+C or 'q' to quit"))

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
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
