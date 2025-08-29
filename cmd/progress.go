package cmd

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
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

func newProgressModelWithArchiver(config *Config, archiver *Archiver, errChan chan<- error, resultsChan chan<- []ProcessResult, taskInfo *TaskInfo) progressModel {
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
		if err := m.archiver.connect(); err != nil {
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
		if err := m.archiver.checkTablePermissions(); err != nil {
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
			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table.name)
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
		if msg.String() == "ctrl+c" || msg.String() == "q" {
			m.done = true
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.currentProgress.Width = msg.Width - 10
		m.overallProgress.Width = msg.Width - 10
		return m, nil

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.currentSpinner, cmd = m.currentSpinner.Update(msg)
		return m, cmd

	case progress.FrameMsg:
		progressModel, cmd := m.currentProgress.Update(msg)
		m.currentProgress = progressModel.(progress.Model)

		overallModel, cmd2 := m.overallProgress.Update(msg)
		m.overallProgress = overallModel.(progress.Model)

		return m, tea.Batch(cmd, cmd2)

	case phaseMsg:
		m.phase = msg.phase
		m.currentStage = msg.message

		// Handle phase transitions
		switch msg.phase {
		case PhaseConnecting:
			// Start the connection process
			return m, m.doConnect()
		case PhaseCheckingPermissions:
			// Check table permissions
			return m, m.doCheckPermissions()
		case PhaseDiscovering:
			// Start discovery
			return m, m.doDiscover()
		case PhaseCounting:
			// Start counting
			if len(m.pendingTables) > 0 {
				// Set initial stage to show first table
				if m.currentCountIndex == 0 && len(m.pendingTables) > 0 {
					m.currentStage = fmt.Sprintf("Counting rows: 0/%d - %s", len(m.pendingTables), m.pendingTables[0].name)
				}
				return m, m.countNextTable()
			}
		case PhaseProcessing:
			// Start processing with a ticker for stage updates
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

	case messageMsg:
		m.messages = append(m.messages, string(msg))
		// Keep only last 10 messages
		if len(m.messages) > 10 {
			m.messages = m.messages[len(m.messages)-10:]
		}

		msgStr := string(msg)

		// Check if we need to start the archiving process
		if strings.Contains(msgStr, "Starting archive process") {
			// Start the connection phase
			m.phase = PhaseConnecting
			m.currentStage = "Connecting to database..."
			m.updateTaskInfo()
			return m, m.doConnect()
		}

		// Check if we need to transition phases based on the message
		if strings.Contains(msgStr, "✅ Table permissions verified") && m.phase == PhaseCheckingPermissions {
			// Add S3 connection message
			m.messages = append(m.messages, fmt.Sprintf("✅ Connected to S3 at s3://%s", m.config.S3.Bucket))
			if len(m.messages) > 10 {
				m.messages = m.messages[len(m.messages)-10:]
			}

			// Move to discovery phase after permissions are verified
			m.phase = PhaseDiscovering
			m.currentStage = "Discovering partitions..."
			m.updateTaskInfo()
			return m, m.doDiscover()
		}

		// Check for permission failure
		if strings.Contains(msgStr, "❌ Permission check failed") {
			return m, tea.Sequence(tea.ExitAltScreen, tea.Quit)
		}

		return m, nil

	case connectedMsg:
		// Add connection success message
		m.messages = append(m.messages, fmt.Sprintf("✅ Connected to PostgreSQL at %s", msg.host))

		// Start cache viewer server if enabled
		if m.config.CacheViewer {
			go func() {
				// Start background goroutines for WebSocket
				go broadcastManager()
				go dataMonitor()

				// Create a new mux to avoid conflicts
				mux := http.NewServeMux()
				mux.HandleFunc("/", serveCacheViewer)
				mux.HandleFunc("/api/cache", serveCacheData)
				mux.HandleFunc("/api/status", serveStatusData)
				mux.HandleFunc("/ws", handleWebSocket)

				addr := fmt.Sprintf(":%d", m.config.ViewerPort)
				server := &http.Server{
					Addr:    addr,
					Handler: mux,
				}

				// Start the server
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					// Log error but don't crash the archiver
					fmt.Printf("Cache viewer server error: %v\n", err)
				}
			}()
			m.messages = append(m.messages, fmt.Sprintf("🌐 Cache viewer started at http://localhost:%d", m.config.ViewerPort))
		}

		if len(m.messages) > 10 {
			m.messages = m.messages[len(m.messages)-10:]
		}
		// Move to permission checking phase
		m.phase = PhaseCheckingPermissions
		m.currentStage = "Checking table permissions..."
		m.updateTaskInfo()
		return m, m.doCheckPermissions()

	case discoveredTablesMsg:
		// We've discovered tables, now we need to count them or skip counting
		m.messages = append(m.messages, fmt.Sprintf("📊 Found %d partitions to process", len(msg.tables)))
		if len(m.messages) > 10 {
			m.messages = m.messages[len(m.messages)-10:]
		}

		if m.config.SkipCount {
			// Skip counting, create partitions with unknown counts
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
		} else {
			// Start counting phase
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

	case partitionsFoundMsg:
		// Mark counting as complete first
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
		m.currentStage = "" // Clear the stage from counting

		// Add a message about starting processing
		m.messages = append(m.messages, fmt.Sprintf("🚀 Starting to process %d partitions", len(msg.partitions)))
		if len(m.messages) > 10 {
			m.messages = m.messages[len(m.messages)-10:]
		}

		// Start processing the first partition with a small delay to show completion
		if len(msg.partitions) > 0 {
			return m, tea.Sequence(
				tea.Tick(200*time.Millisecond, func(time.Time) tea.Msg {
					return nil
				}),
				m.processNext(),
			)
		}
		return m, nil

	case tableCountedMsg:
		// Store the result
		if msg.count >= 0 {
			m.countedPartitions = append(m.countedPartitions, PartitionInfo{
				TableName: msg.table.name,
				Date:      msg.table.date,
				RowCount:  msg.count,
			})
		}

		// Update progress
		m.currentCountIndex++
		m.countProgress = m.currentCountIndex
		m.currentStage = fmt.Sprintf("Counting rows: %d/%d - %s", m.currentCountIndex, m.countTotal, msg.table.name)

		// Continue counting the next table or finish
		return m, m.countNextTable()

	case countProgressMsg:
		m.countProgress = msg.current
		m.countTotal = msg.total
		m.currentStage = fmt.Sprintf("Counting rows: %d/%d - %s", msg.current, msg.total, msg.tableName)
		return m, nil

	case progressMsg:
		m.currentStage = msg.stage
		m.currentRows = msg.current
		m.totalRows = msg.total
		m.updateTaskInfo() // Update task info when progress changes

		if msg.total > 0 {
			percent := float64(msg.current) / float64(msg.total)
			cmd := m.currentProgress.SetPercent(percent)
			return m, cmd
		}
		return m, nil

	case partitionCompleteMsg:
		m.results = append(m.results, msg.result)
		m.currentIndex = msg.index + 1
		m.updateTaskInfo() // Update task info when partition completes

		// Clear the current stage and reset processing start time for next partition
		m.currentStage = ""
		m.processingStartTime = time.Time{} // Reset to zero value

		// Update overall progress
		if len(m.partitions) > 0 {
			overallPercent := float64(m.currentIndex) / float64(len(m.partitions))
			// Continue processing next partition with a new ticker
			return m, tea.Batch(
				m.overallProgress.SetPercent(overallPercent),
				m.processNext(), // Process the next partition
				tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
					return stageTickMsg(t)
				}),
			)
		}
		return m, nil

	case allCompleteMsg:
		m.phase = PhaseComplete
		m.done = true
		return m, tea.Sequence(tea.ExitAltScreen, tea.Quit)

	case stageUpdateMsg:
		m.currentStage = msg.stage
		m.updateTaskInfo()
		return m, nil

	case stageTickMsg:
		// Update stage based on elapsed time if we're processing
		if m.phase == PhaseProcessing && m.currentIndex < len(m.partitions) && !m.processingStartTime.IsZero() {
			elapsed := time.Since(m.processingStartTime)

			// Simulate stages based on elapsed time
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

			// Continue ticking if still processing
			return m, tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
				return stageTickMsg(t)
			})
		}
		return m, nil
	}

	return m, nil
}

func (m progressModel) View() string {
	if m.done && m.phase == PhaseComplete {
		return ""
	}

	var sections []string

	// ASCII Banner with gradient colors
	titleStyle1 := lipgloss.NewStyle().Foreground(lipgloss.Color("#FF7CCB")).Bold(true)
	titleStyle2 := lipgloss.NewStyle().Foreground(lipgloss.Color("#FDFF8C")).Bold(true)
	authorStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#999999"))

	// Banner with proper box drawing
	const boxWidth = 66  // Total width including the borders (reduced by 1 more)
	const indent = "   " // 3 spaces indentation

	// Helper function to create a properly padded box line using lipgloss to measure width
	makeLine := func(content string) string {
		// Measure the actual visible width using lipgloss
		visibleWidth := lipgloss.Width(content)
		// Calculate padding needed (boxWidth - 4 for "║  " prefix and "  ║" suffix)
		targetWidth := boxWidth - 4
		padding := targetWidth - visibleWidth
		if padding < 0 {
			padding = 0
		}
		return fmt.Sprintf("%s║  %s%s║", indent, content, strings.Repeat(" ", padding))
	}

	// Create the top and bottom borders (these are boxWidth characters wide)
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

	// Always show messages section (even if empty)
	sections = append(sections, helpStyle.Render("   Log:"))
	if len(m.messages) == 0 {
		sections = append(sections, "     (waiting for operations...)")
	} else {
		for _, msg := range m.messages {
			sections = append(sections, "     "+msg)
		}
	}

	// Add horizontal separator
	separatorWidth := 80
	if m.width > 0 && m.width < 200 {
		separatorWidth = m.width - 6 // Leave some margin with extra padding
	}
	separator := "   " + strings.Repeat("─", separatorWidth)
	sections = append(sections, "")
	sections = append(sections, lipgloss.NewStyle().Foreground(lipgloss.Color("#444")).Render(separator))
	sections = append(sections, "")

	// Phase-specific content
	switch m.phase {
	case PhaseConnecting, PhaseCheckingPermissions, PhaseDiscovering:
		// Show current operation with spinner
		if m.currentStage != "" {
			stageInfo := fmt.Sprintf("   %s %s", m.currentSpinner.View(), m.currentStage)
			sections = append(sections, stageStyle.Render(stageInfo))
		} else {
			sections = append(sections, stageStyle.Render("   "+m.currentSpinner.View()+" Initializing..."))
		}

	case PhaseCounting:
		// Show counting progress with spinner, bar, and current table all on one line
		if m.countTotal > 0 {
			// Create a progress bar for counting
			barWidth := 30
			progress := float64(m.countProgress) / float64(m.countTotal)
			filled := int(progress * float64(barWidth))

			bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

			// Check if counting is complete
			isComplete := m.countProgress >= m.countTotal

			// Extract just the table name from the current stage
			tableName := ""
			if strings.Contains(m.currentStage, " - ") {
				parts := strings.Split(m.currentStage, " - ")
				if len(parts) > 1 {
					tableName = parts[1]
				}
			}

			// Show checkmark if complete, spinner if still counting
			icon := m.currentSpinner.View()
			currentTable := tableName
			if isComplete {
				icon = "✅"
				currentTable = "Complete!"
			}

			// Combine icon, label, progress bar, percentage, and current table
			stageInfo := fmt.Sprintf("   %s Counting rows:    %s %d%% (%d/%d) - %s",
				icon,
				bar,
				int(progress*100),
				m.countProgress,
				m.countTotal,
				currentTable)

			sections = append(sections, stageStyle.Render(stageInfo))
		}

	case PhaseProcessing:
		// 1. COUNTING ROWS (completed) - Always show this first
		if m.countTotal > 0 && m.countProgress >= m.countTotal {
			// Show completed counting progress - same format as in progress but with checkmark and 100%
			barWidth := 30
			bar := strings.Repeat("█", barWidth)

			stageInfo := fmt.Sprintf("   ✅ Counting rows:    %s 100%% (%d/%d) - Complete!",
				bar,
				m.countTotal,
				m.countTotal)

			sections = append(sections, stageStyle.Render(stageInfo))
			sections = append(sections, "") // Add newline after counting
		}

		// 2. CURRENT STEP STATUS
		if m.currentIndex < len(m.partitions) && len(m.partitions) > 0 {
			partition := m.partitions[m.currentIndex]

			// Current operation stage with spinner - show detailed operation
			objectPath := fmt.Sprintf("export/%s/%s/%s.jsonl.zst",
				m.config.Table,
				partition.Date.Format("2006/01"),
				partition.Date.Format("2006-01-02"))

			// Full S3 path with bucket
			s3Path := fmt.Sprintf("s3://%s/%s", m.config.S3.Bucket, objectPath)

			var operationInfo string
			// Check if we're still showing counting-related stages
			if strings.Contains(m.currentStage, "Counting rows") {
				// If still in counting phase, show initial processing message with spinner
				// Add space before spinner to align better
				operationInfo = fmt.Sprintf("   %s Current step: Preparing to process %s...", m.currentSpinner.View(), partition.TableName)
				sections = append(sections, helpStyle.Render(operationInfo))
			} else {
				// Show actual processing status with spinner
				if m.currentStage == "" || strings.Contains(m.currentStage, "Checking") {
					operationInfo = fmt.Sprintf("   %s Checking if %s exists...", m.currentSpinner.View(), s3Path)
				} else if strings.Contains(m.currentStage, "Extracting") {
					operationInfo = fmt.Sprintf("   %s Extracting data from %s...", m.currentSpinner.View(), partition.TableName)
				} else if strings.Contains(m.currentStage, "Compressing") {
					operationInfo = fmt.Sprintf("   %s Compressing %s.jsonl...", m.currentSpinner.View(), partition.TableName)
				} else if strings.Contains(m.currentStage, "Uploading") {
					operationInfo = fmt.Sprintf("   %s Uploading to %s...", m.currentSpinner.View(), s3Path)
				} else {
					operationInfo = fmt.Sprintf("   %s Processing %s...", m.currentSpinner.View(), partition.TableName)
				}
				sections = append(sections, stageStyle.Render(operationInfo))

				// Row progress if applicable (only show for extraction which has row counts)
				if m.totalRows > 0 && strings.Contains(m.currentStage, "Extracting") {
					rowInfo := fmt.Sprintf("   Rows: %d / %d", m.currentRows, m.totalRows)
					sections = append(sections, helpStyle.Render(rowInfo))
					// Show progress bar only for extraction with row counts
					sections = append(sections, m.currentProgress.View())
				}
			}
			sections = append(sections, "") // Add newline after current step
		}

		// 3. OVERALL PROGRESS
		if len(m.partitions) > 0 {
			// Create inline progress bar with gradient effect
			barWidth := 30
			progress := float64(m.currentIndex) / float64(len(m.partitions))
			filled := int(progress * float64(barWidth))

			// Create a simple gradient effect using different block characters
			var bar string
			for i := 0; i < barWidth; i++ {
				if i < filled {
					bar += "█"
				} else {
					bar += "░"
				}
			}

			overallInfo := fmt.Sprintf("   📊 Overall Progress: %s %d%% (%d/%d partitions)",
				bar,
				int(progress*100),
				m.currentIndex,
				len(m.partitions))
			sections = append(sections, helpStyle.Render(overallInfo))
		}
	}

	// Stats
	sections = append(sections, "") // Empty line
	elapsed := time.Since(m.startTime)
	stats := fmt.Sprintf("   ⏱️  Elapsed: %s", elapsed.Round(time.Second))

	if m.currentIndex > 0 {
		avgTime := elapsed / time.Duration(m.currentIndex)
		remaining := avgTime * time.Duration(len(m.partitions)-m.currentIndex)
		stats += fmt.Sprintf(" | Remaining: ~%s", remaining.Round(time.Second))
	}
	sections = append(sections, helpStyle.Render(stats))

	// Recent results
	if len(m.results) > 0 {
		sections = append(sections, "") // Empty line
		sections = append(sections, helpStyle.Render("   Recent completions:"))

		start := len(m.results) - 3
		if start < 0 {
			start = 0
		}

		for i := start; i < len(m.results); i++ {
			result := m.results[i]
			status := "✅"
			if result.Error != nil {
				status = "❌"
			} else if result.Skipped {
				status = "⏭️"
			}

			line := fmt.Sprintf("      %s %s", status, result.Partition.TableName)
			if result.Skipped && strings.Contains(result.SkipReason, "exists") {
				line += " (already exists)"
			} else if result.BytesWritten > 0 {
				line += fmt.Sprintf(" (%.2f MB)", float64(result.BytesWritten)/(1024*1024))
			}
			sections = append(sections, helpStyle.Render(line))
		}
	}

	// Help
	sections = append(sections, "") // Empty line
	sections = append(sections, helpStyle.Render("Press 'q' or 'ctrl+c' to quit"))

	return strings.Join(sections, "\n")
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
