let allData = [];
let currentData = {};  // Track current data by partition key
let currentSort = { column: 'partition', direction: 'asc' };  // Default sort by partition name
let ws = null;
let wsReconnectInterval = null;
let wsReconnectAttempts = 0;  // Track reconnection attempts globally
let wsReconnectDelay = 1000;  // Current reconnection delay
let lastTaskState = null;  // Store last known task state to avoid flashing
let archiverRunning = false;  // Track archiver running state
let updateDebounceTimer = null;  // Debounce timer for WebSocket updates
let pendingCacheData = null;  // Store pending cache data to batch updates
let lastStatsValues = null;  // Cache last stats values to avoid unnecessary updates
let lastSummaryValues = null;  // Cache last summary values to avoid unnecessary updates
let currentTaskInfo = null;  // Store current task info for accurate "Not Synced" calculation
let archiverStartTime = null;  // Track when archiver started to identify current run partitions
let currentPartition = null;  // Track current partition being processed for row highlighting
let statsMode = 'current';  // 'current' or 'all-time' - controls which stats to show
let logsWebSocket = null;  // WebSocket connection for log streaming
let logsPaused = false;  // Whether log streaming is paused
let logsBuffer = [];  // Buffer for logs when paused
let maxLogEntries = 10000;  // Maximum number of log entries to keep in memory
let followActive = false;  // Whether to follow the current processing entry
let followInterval = null;  // Interval for checking and scrolling to current partition

// Extract output format from S3 key
function getOutputFormat(s3Key) {
    if (!s3Key) {
        return '—';
    }

    // Extract filename from S3 key (last part after /)
    const filename = s3Key.split('/').pop();
    if (!filename) {
        return '—';
    }

    // Split by dots to get extensions
    const parts = filename.split('.');
    if (parts.length < 2) {
        return '—';
    }

    // Format extension is typically the second-to-last extension (before compression)
    // Known formats: jsonl, csv, parquet
    // Known compression: zst, lz4, gz, bz2, xz
    const compressionExts = ['zst', 'lz4', 'gz', 'bz2', 'xz', 'zstd'];
    const formatExts = ['jsonl', 'csv', 'parquet', 'json'];

    // If last extension is compression, format is second-to-last
    const lastExt = parts[parts.length - 1].toLowerCase();
    if (compressionExts.includes(lastExt) && parts.length >= 3) {
        const formatExt = parts[parts.length - 2].toLowerCase();
        if (formatExts.includes(formatExt)) {
            return formatExt.toUpperCase();
        }
    }

    // Otherwise, check if last extension is a format
    if (formatExts.includes(lastExt)) {
        return lastExt.toUpperCase();
    }

    // Fallback: return the extension (capitalized)
    return parts[parts.length - 1].toUpperCase();
}

// Extract compression type from S3 key
function getCompressionType(s3Key) {
    if (!s3Key) {
        return null;
    }

    // Extract filename from S3 key (last part after /)
    const filename = s3Key.split('/').pop();
    if (!filename) {
        return null;
    }

    // Split by dots to get extensions
    const parts = filename.split('.');
    if (parts.length < 2) {
        return null;
    }

    // Check last extension for compression
    const lastExt = parts[parts.length - 1].toLowerCase();
    const compressionMap = {
        'zst': 'ZSTD',
        'zstd': 'ZSTD',
        'lz4': 'LZ4',
        'gz': 'GZIP',
        'gzip': 'GZIP',
        'bz2': 'BZIP2',
        'xz': 'XZ'
    };

    if (compressionMap[lastExt]) {
        return compressionMap[lastExt];
    }

    return null;
}

// Extract S3 bucket from S3 key (if key contains s3:// prefix)
function getS3Bucket(s3Key) {
    if (!s3Key) {
        return null;
    }

    // If key starts with s3://, extract bucket
    if (s3Key.startsWith('s3://')) {
        const parts = s3Key.substring(5).split('/');
        return parts[0] || null;
    }

    return null;
}

// HTML escape function to prevent XSS attacks
function escapeHTML(str) {
    if (str === null || str === undefined) {
        return '';
    }
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// Escape HTML attributes to prevent XSS in attribute context
function escapeHTMLAttr(str) {
    if (str === null || str === undefined) {
        return '';
    }
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/'/g, '&#39;')
        .replace(/"/g, '&quot;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

// Screen reader announcement helper (WCAG 2.1 AA compliant)
function announceToScreenReader(message) {
    let announcementRegion = document.getElementById('sr-announcements');
    if (!announcementRegion) {
        announcementRegion = document.createElement('div');
        announcementRegion.id = 'sr-announcements';
        announcementRegion.setAttribute('aria-live', 'polite');
        announcementRegion.setAttribute('aria-atomic', 'true');
        announcementRegion.className = 'sr-only';
        document.body.appendChild(announcementRegion);
    }
    announcementRegion.textContent = message;
    // Clear after announcement to allow re-announcement if needed
    setTimeout(() => {
        announcementRegion.textContent = '';
    }, 1000);
}

// Alert notification system
function showAlert(type, title, description = '', duration = 5000) {
    let alertContainer = document.getElementById('alert-container');
    if (!alertContainer) {
        alertContainer = document.createElement('div');
        alertContainer.id = 'alert-container';
        document.body.appendChild(alertContainer);
    }

    const alert = document.createElement('div');
    alert.className = `alert alert-${type}`;
    alert.setAttribute('role', 'alert');
    alert.setAttribute('aria-live', 'assertive');

    const iconSvg = {
        success: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>',
        error: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line></svg>',
        warning: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>',
        info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line></svg>'
    };

    alert.innerHTML = `
        <div class="alert-icon">${iconSvg[type]}</div>
        <div class="alert-content">
            <div class="alert-title">${escapeHTML(title)}</div>
            ${description ? `<div class="alert-description">${escapeHTML(description)}</div>` : ''}
        </div>
        <button class="alert-close" aria-label="Close alert">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <line x1="18" y1="6" x2="6" y2="18"></line>
                <line x1="6" y1="6" x2="18" y2="18"></line>
            </svg>
        </button>
    `;

    alertContainer.appendChild(alert);

    // Close button functionality
    alert.querySelector('.alert-close').addEventListener('click', () => {
        alert.style.opacity = '0';
        alert.style.transform = 'translateY(-10px)';
        setTimeout(() => alert.remove(), 300);
    });

    // Auto-remove after duration
    if (duration > 0) {
        setTimeout(() => {
            if (alert.parentElement) {
                alert.style.opacity = '0';
                alert.style.transform = 'translateY(-10px)';
                setTimeout(() => {
                    if (alert.parentElement) {
                        alert.remove();
                    }
                }, 300);
            }
        }, duration);
    }

    // Announce to screen readers
    announceToScreenReader(`${type}: ${title}${description ? '. ' + description : ''}`);
}

// Show loading skeleton
function showLoadingSkeleton() {
    const tbody = document.getElementById('table-body');
    const skeletonRows = Array(5).fill(0).map(() => `
        <tr>
            <td><div class="skeleton skeleton-text" style="width: 80%;"></div></td>
            <td><div class="skeleton skeleton-text" style="width: 60%;"></div></td>
            <td><div class="skeleton skeleton-text" style="width: 70%;"></div></td>
            <td><div class="skeleton skeleton-text" style="width: 70%;"></div></td>
            <td><div class="skeleton skeleton-text" style="width: 50%;"></div></td>
            <td><div class="skeleton skeleton-text" style="width: 90%;"></div></td>
            <td><div class="skeleton skeleton-text" style="width: 60%;"></div></td>
            <td><div class="skeleton skeleton-text" style="width: 55%;"></div></td>
            <td><div class="skeleton skeleton-text" style="width: 40%;"></div></td>
        </tr>
    `).join('');
    tbody.innerHTML = skeletonRows;
}

// Format bytes
function formatBytes(bytes) {
    if (!bytes || bytes === 0) return '—';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Format number with comma separators
function formatNumber(n) {
    if (n < 1000) {
        return n.toString();
    }
    return n.toLocaleString();
}

// Calculate compression ratio
function calculateRatio(uncompressed, compressed) {
    if (!uncompressed || !compressed) return '—';
    const ratio = uncompressed / compressed;
    return ratio.toFixed(1) + 'x';
}

// Format duration in seconds to human-readable string
function formatDuration(seconds) {
    if (!seconds || seconds < 0) return '0s';

    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);

    if (days > 0) {
        return days + 'd ' + hours + 'h';
    } else if (hours > 0) {
        return hours + 'h ' + minutes + 'm';
    } else if (minutes > 0) {
        return minutes + 'm ' + secs + 's';
    } else {
        return secs + 's';
    }
}

// Calculate age from completion time (fileTime or s3UploadTime)
function calculateAge(dateStr) {
    if (!dateStr) return { text: '—', class: 'old' };
    const date = new Date(dateStr);
    if (isNaN(date.getTime()) || date.getTime() === 0) return { text: '—', class: 'old' };

    // Check if date is unreasonably old (likely uninitialized)
    const now = new Date();
    const yearsDiff = (now - date) / (1000 * 60 * 60 * 24 * 365);
    if (yearsDiff > 10) return { text: '—', class: 'old' };

    const hours = Math.floor((now - date) / (1000 * 60 * 60));

    if (hours < 1) {
        const minutes = Math.floor((now - date) / (1000 * 60));
        return { text: minutes + 'm', class: 'fresh' };
    } else if (hours < 6) {
        return { text: hours + 'h', class: 'fresh' };
    } else if (hours < 24) {
        return { text: hours + 'h', class: 'recent' };
    } else {
        const days = Math.floor(hours / 24);
        return { text: days + 'd', class: 'old' };
    }
}

// Calculate age from completion time (prefers s3UploadTime, falls back to fileTime)
function calculateCompletionAge(entry) {
    // Prefer s3UploadTime (most recent completion), fall back to fileTime
    const completionTime = entry.s3UploadTime && entry.s3UploadTime !== '0001-01-01T00:00:00Z'
        ? entry.s3UploadTime
        : (entry.fileTime && entry.fileTime !== '0001-01-01T00:00:00Z' ? entry.fileTime : null);

    if (!completionTime) {
        // No completion time, use countTime as fallback
        return calculateAge(entry.countTime);
    }

    return calculateAge(completionTime);
}

// Calculate processing time duration
// Processing time should be from when processing started to when it completed
// Use processStartTime if available (accurate for current job), otherwise fall back to countTime
function calculateProcessingTime(entry) {
    // Determine start time: prefer processStartTime if available, otherwise countTime
    let startTime = null;
    if (entry.processStartTime && entry.processStartTime !== '0001-01-01T00:00:00Z') {
        startTime = new Date(entry.processStartTime);
    } else if (entry.countTime && entry.countTime !== '0001-01-01T00:00:00Z') {
        startTime = new Date(entry.countTime);
    }

    if (!startTime || isNaN(startTime.getTime()) || startTime.getTime() === 0) {
        return '—'; // No start time available
    }

    // Determine end time: prefer s3UploadTime if uploaded, otherwise fileTime, otherwise null
    let endTime = null;
    if (entry.s3Uploaded && entry.s3UploadTime && entry.s3UploadTime !== '0001-01-01T00:00:00Z') {
        endTime = new Date(entry.s3UploadTime);
    } else if (entry.fileTime && entry.fileTime !== '0001-01-01T00:00:00Z') {
        endTime = new Date(entry.fileTime);
    }

    if (!endTime || isNaN(endTime.getTime()) || endTime.getTime() === 0) {
        return '—'; // Not processed yet
    }

    // Calculate duration in milliseconds
    const durationMs = endTime - startTime;
    if (durationMs < 0) return '—'; // Invalid (end before start)

    // Format duration
    const seconds = Math.floor(durationMs / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    if (days > 0) {
        return days + 'd ' + (hours % 24) + 'h';
    } else if (hours > 0) {
        return hours + 'h ' + (minutes % 60) + 'm';
    } else if (minutes > 0) {
        return minutes + 'm ' + (seconds % 60) + 's';
    } else {
        return seconds + 's';
    }
}

// Get processing time in seconds for sorting
function getProcessingTimeSeconds(entry) {
    // Determine start time: prefer processStartTime if available, otherwise countTime
    let startTime = null;
    if (entry.processStartTime && entry.processStartTime !== '0001-01-01T00:00:00Z') {
        startTime = new Date(entry.processStartTime);
    } else if (entry.countTime && entry.countTime !== '0001-01-01T00:00:00Z') {
        startTime = new Date(entry.countTime);
    }

    if (!startTime || isNaN(startTime.getTime()) || startTime.getTime() === 0) {
        return 0;
    }

    let endTime = null;
    if (entry.s3Uploaded && entry.s3UploadTime && entry.s3UploadTime !== '0001-01-01T00:00:00Z') {
        endTime = new Date(entry.s3UploadTime);
    } else if (entry.fileTime && entry.fileTime !== '0001-01-01T00:00:00Z') {
        endTime = new Date(entry.fileTime);
    }

    if (!endTime || isNaN(endTime.getTime()) || endTime.getTime() === 0) {
        return 0;
    }

    const durationMs = endTime - startTime;
    if (durationMs < 0) return 0;

    return Math.floor(durationMs / 1000);
}

// Create row key
function getRowKey(entry) {
    return entry.table + '|' + entry.partition;
}

// Schedule WebSocket reconnection with exponential backoff
function scheduleReconnection() {
    // Clear any existing reconnection timer
    if (wsReconnectInterval) {
        clearTimeout(wsReconnectInterval);
        wsReconnectInterval = null;
    }

    wsReconnectAttempts++;
    const maxAttempts = 30;

    if (wsReconnectAttempts > maxAttempts) {
        document.getElementById('status-text').textContent = 'Connection failed - please refresh the page';
        return;
    }

    console.log('Reconnect attempt ' + wsReconnectAttempts + '/' + maxAttempts + ' (delay: ' + wsReconnectDelay + 'ms)');
    document.getElementById('status-text').textContent = 'Reconnecting (' + wsReconnectAttempts + '/' + maxAttempts + ')...';

    wsReconnectInterval = setTimeout(function() {
        wsReconnectInterval = null;
        connectWebSocket();
        // Double the delay for next attempt, cap at 30 seconds
        wsReconnectDelay = Math.min(wsReconnectDelay * 2, 30000);
    }, wsReconnectDelay);
}

// WebSocket connection management
function connectWebSocket() {
    // Clear any pending reconnection attempts
    if (wsReconnectInterval) {
        clearTimeout(wsReconnectInterval);
        wsReconnectInterval = null;
    }

    // Close existing WebSocket if it exists
    if (ws) {
        // Remove all handlers to prevent triggering reconnection
        ws.onopen = null;
        ws.onclose = null;
        ws.onerror = null;
        ws.onmessage = null;
        ws.close();
        ws = null;
    }
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = protocol + '//' + window.location.host + '/ws';

    ws = new WebSocket(wsUrl);

    ws.onopen = function() {
        console.log('WebSocket connected');
        document.getElementById('status').classList.add('connected');
        document.getElementById('status').classList.remove('disconnected');
        document.getElementById('status-text').textContent = 'Connected to live updates';

        // Reset reconnection state on successful connection
        wsReconnectAttempts = 0;
        wsReconnectDelay = 1000;
        if (wsReconnectInterval) {
            clearTimeout(wsReconnectInterval);
            wsReconnectInterval = null;
        }
    };

    ws.onmessage = function(event) {
        try {
            const message = JSON.parse(event.data);

            if (message.type === 'cache') {
                handleCacheData(message.data);
            } else if (message.type === 'status') {
                updateTaskPanel(message.data);
            }
        } catch (error) {
            console.error('Error handling WebSocket message:', error);
        }
    };

    ws.onerror = function(error) {
        console.error('WebSocket error:', error);
    };

    ws.onclose = function(event) {
        console.log('WebSocket disconnected', event);
        document.getElementById('status').classList.remove('connected');
        document.getElementById('status').classList.add('disconnected');

        // Provide better error messaging
        if (event.wasClean) {
            document.getElementById('status-text').textContent = 'Connection closed - reconnecting...';
        } else {
            document.getElementById('status-text').textContent = 'Connection lost - attempting to reconnect...';
        }

        // Schedule reconnection using global state machine
        scheduleReconnection();
    };
}

// Update task panel
function updateTaskPanel(status) {
    const panel = document.getElementById('task-panel');

    // Track archiver running state
    archiverRunning = status.archiverRunning || false;

    // Update version information
    if (status.version) {
        const versionInfo = document.getElementById('version-info');
        versionInfo.textContent = 'v' + status.version;

        // Show update banner if available
        if (status.updateAvailable && status.latestVersion) {
            const updateBanner = document.getElementById('update-banner');
            const updateMessage = document.getElementById('update-message');
            updateMessage.innerHTML = 'Update available: v' + status.version + ' → v' + status.latestVersion +
                ' <a href="' + (status.releaseUrl || 'https://github.com/airframesio/postgresql-archiver/releases') +
                '" target="_blank">Download</a>';
            updateBanner.style.display = 'flex';
        }
    }

    if (status.archiverRunning) {
        panel.classList.remove('idle');

        document.getElementById('task-title').textContent = 'Archiver Running';
        document.getElementById('task-pid').textContent = 'PID: ' + status.pid;

        // Handle task info if available
        if (status.currentTask) {
            // Store the last known task state
            lastTaskState = status.currentTask;
            currentTaskInfo = status.currentTask;  // Store for stats calculation

            // Track current partition for row highlighting
            const newCurrentPartition = status.currentTask.current_partition || null;
            const partitionChanged = newCurrentPartition !== currentPartition;
            currentPartition = newCurrentPartition;

            // If following is active and partition changed, scroll to it
            if (followActive && partitionChanged && newCurrentPartition) {
                scrollToCurrentPartition();
            }

            // Track archiver start time to identify current run partitions
            if (!archiverStartTime && status.currentTask.start_time) {
                archiverStartTime = new Date(status.currentTask.start_time);
            }

            const task = status.currentTask;

            // Update current task and partition info
            const currentTaskText = task.current_step || task.current_task || 'Processing...';
            const statusElement = document.getElementById('task-status-text');

            if (task.current_partition) {
                // Show as "Table: partition - operation" using safe DOM manipulation
                statusElement.textContent = ''; // Clear existing content
                statusElement.appendChild(document.createTextNode('Table: '));

                const link = document.createElement('a');
                link.href = '#';
                link.className = 'partition-link';
                link.setAttribute('data-partition', task.current_partition);
                link.textContent = task.current_partition;
                statusElement.appendChild(link);

                statusElement.appendChild(document.createTextNode(' - ' + currentTaskText));
            } else {
                statusElement.textContent = currentTaskText;
            }

            // Refresh table if partition changed to update highlighting and status badges
            if (partitionChanged) {
                updateTable();
            }

            // Always refresh table on status updates to ensure status badges are current
            // (partition might finish processing and status needs to update)
            updateTable();

            // Force stats update when task info changes (to ensure sync rate uses latest partition counts)
            forceStatsUpdate();
        } else if (lastTaskState) {
            // Use last known state if task info temporarily unavailable
            const task = lastTaskState;
            const currentTaskText = task.current_step || task.current_task || 'Processing...';
            const statusElement = document.getElementById('task-status-text');

            if (task.current_partition) {
                // Show as "Table: partition - operation" using safe DOM manipulation
                statusElement.textContent = ''; // Clear existing content
                statusElement.appendChild(document.createTextNode('Table: '));

                const link = document.createElement('a');
                link.href = '#';
                link.className = 'partition-link';
                link.setAttribute('data-partition', task.current_partition);
                link.textContent = task.current_partition;
                statusElement.appendChild(link);

                statusElement.appendChild(document.createTextNode(' - ' + currentTaskText));
            } else {
                statusElement.textContent = currentTaskText;
            }
        } else {
            // Only show initializing if we've never had task info
            document.getElementById('task-status-text').textContent = 'Starting...';
        }

        // Use current task or last known task for progress bar and stats
        const taskForProgress = status.currentTask || lastTaskState;
        if (taskForProgress && taskForProgress.total_items > 0) {
            const progressBar = document.getElementById('task-progress-bar');
            const progressFill = document.getElementById('task-progress-fill');
            progressBar.style.display = 'block';

            const percent = (taskForProgress.completed_items / taskForProgress.total_items) * 100;
            progressFill.style.width = percent + '%';
            progressBar.setAttribute('aria-valuenow', Math.round(percent));

            // Build stats text with partition and slice info
            let statsText = taskForProgress.completed_items + '/' + taskForProgress.total_items + ' partitions (' + Math.round(percent) + '%)';

            // Add partition counting info if available
            if (taskForProgress.total_partitions > 0) {
                statsText = taskForProgress.total_partitions + ' total | ' + statsText;
            }
            if (taskForProgress.partitions_counted > 0 && taskForProgress.partitions_counted !== taskForProgress.total_partitions) {
                statsText += ' | ' + taskForProgress.partitions_counted + ' counted';
            }

            // Add slice info if available
            if (status.isSlicing && status.totalSlices > 0) {
                statsText += ' | ' + (status.currentSliceIndex + 1) + '/' + status.totalSlices + ' slices';
            }
            if (taskForProgress.slices_processed > 0) {
                statsText += ' | ' + taskForProgress.slices_processed + ' slices processed';
            }

            document.getElementById('task-stats').textContent = statsText;
        } else {
            document.getElementById('task-progress-bar').style.display = 'none';
            document.getElementById('task-stats').textContent = '';
        }

        // Show elapsed time
        if (taskForProgress && taskForProgress.start_time) {
            const start = new Date(taskForProgress.start_time);
            const elapsed = Math.floor((new Date() - start) / 1000);
            const minutes = Math.floor(elapsed / 60);
            const seconds = elapsed % 60;
            document.getElementById('task-time').textContent = minutes + 'm ' + seconds + 's';
        }

        // Show slice progress if slicing is active
        if (status.isSlicing && status.totalSlices > 0) {
            const sliceProgressBar = document.getElementById('task-slice-progress-bar');
            const sliceProgressFill = document.getElementById('task-slice-progress-fill');
            sliceProgressBar.style.display = 'block';

            const slicePercent = ((status.currentSliceIndex + 1) / status.totalSlices) * 100;
            sliceProgressFill.style.width = slicePercent + '%';
            sliceProgressBar.setAttribute('aria-valuenow', Math.round(slicePercent));
        } else {
            document.getElementById('task-slice-progress-bar').style.display = 'none';
        }
    } else {
        panel.classList.add('idle');
        document.getElementById('task-title').textContent = 'Archiver Idle';
        document.getElementById('task-pid').textContent = '';
        document.getElementById('task-status-text').textContent = 'No active archiving process';
        document.getElementById('task-progress-bar').style.display = 'none';
        document.getElementById('task-slice-progress-bar').style.display = 'none';
        document.getElementById('task-stats').textContent = '';
        document.getElementById('task-time').textContent = '';
        // Clear task info when archiver stops
        currentTaskInfo = null;
        archiverStartTime = null;  // Clear start time when archiver stops
        currentPartition = null;  // Clear current partition when archiver stops
        // Clear last task state when idle
        lastTaskState = null;

        // Stop following when archiver stops
        if (followActive) {
            toggleFollow(); // This will turn off follow mode
        }

        // Update stats when archiver stops (switches from current run to all-time view)
        updateStats();
    }

    // Update completion summary when archiver is idle (debounced)
    if (!status.archiverRunning) {
        if (updateDebounceTimer) {
            clearTimeout(updateDebounceTimer);
        }
        updateDebounceTimer = setTimeout(() => {
            updateCompletionSummary();
        }, 250);
    }
}

// Handle cache data from WebSocket or initial fetch
function handleCacheData(data) {
    // Store pending data for debounced processing
    pendingCacheData = data;

    // Clear existing debounce timer
    if (updateDebounceTimer) {
        clearTimeout(updateDebounceTimer);
    }

    // Debounce updates to prevent flickering (250ms delay for smoother updates)
    updateDebounceTimer = setTimeout(() => {
        if (!pendingCacheData) return;

    // Flatten all entries
    const newData = [];
    const tables = new Set();

        pendingCacheData.tables.forEach(table => {
        tables.add(table.tableName);
        table.entries.forEach(entry => {
            newData.push(entry);
        });
    });

    // Update table filter
    updateTableFilter(Array.from(tables));

    // Update data
    allData = newData;

    // Apply sort (always, since we have a default)
    sortData(currentSort.column, false);

    // Update display
    updateStats();
        updateTable();  // Refresh table to update row highlighting

    document.getElementById('last-update').textContent = 'Last updated: ' + new Date().toLocaleTimeString();

        // Update completion summary if archiver is not running (will be hidden if running)
        updateCompletionSummary();

        // Clear pending data
        pendingCacheData = null;
    }, 250);
}

// Force stats update when status changes (to ensure sync rate uses latest partition counts)
function forceStatsUpdate() {
    // Clear cached values to force recalculation
    lastStatsValues = null;
    updateStats();
}

// Fetch initial data (fallback for when WebSocket is not yet connected)
async function fetchInitialData() {
    try {
        // Fetch cache data
        const cacheResponse = await fetch('/api/cache');
        if (cacheResponse.ok) {
            const cacheData = await cacheResponse.json();
            console.log('Fetched cache data:', cacheData);
            // Don't debounce initial data load - process immediately
            const newData = [];
            const tables = new Set();

            if (cacheData.tables && Array.isArray(cacheData.tables)) {
                cacheData.tables.forEach(table => {
                    tables.add(table.tableName);
                    if (table.entries && Array.isArray(table.entries)) {
                        table.entries.forEach(entry => {
                            newData.push(entry);
                        });
                    }
                });
            }

            console.log('Processed data:', newData.length, 'entries');
            updateTableFilter(Array.from(tables));
            allData = newData;
            sortData(currentSort.column, false);
            updateStats();
            updateTable();
            document.getElementById('last-update').textContent = 'Last updated: ' + new Date().toLocaleTimeString();
            updateCompletionSummary();
        } else {
            console.error('Failed to fetch cache data:', cacheResponse.status, cacheResponse.statusText);
        }

        // Fetch status data
        const statusResponse = await fetch('/api/status');
        if (statusResponse.ok) {
            const statusData = await statusResponse.json();
            updateTaskPanel(statusData);
            // Refresh table to update row highlighting when status changes
            updateTable();
        }
    } catch (error) {
        console.error('Error fetching initial data:', error);
    }
}

// Update table filter
function updateTableFilter(tables) {
    const select = document.getElementById('table-filter');
    const currentValue = select.value;

    select.innerHTML = '<option value="">All Tables</option>';
    tables.sort().forEach(table => {
        const option = document.createElement('option');
        option.value = table;
        option.textContent = table;
        select.appendChild(option);
    });

    if (currentValue && tables.includes(currentValue)) {
        select.value = currentValue;
    }
}

// Update statistics with animations - update individual cards to prevent flickering
function updateStats() {
    const totalPartitions = allData.length;
    const cachedFiles = allData.filter(d => d.fileSize > 0).length;
    const withErrors = allData.filter(d => d.lastError).length;

    // Calculate statistics - respect statsMode setting
    let successful, skipped, failed;
    let currentRunData = null; // Declare outside if block for use in pending calculation
    const useCurrentRun = statsMode === 'current' && archiverRunning && currentTaskInfo && currentTaskInfo.partitions_processed > 0;

    if (useCurrentRun) {
        // When showing current run stats, use partitions_processed to identify current run partitions
        const processedCount = currentTaskInfo.partitions_processed || 0;
        const currentTable = currentTaskInfo.table || '';

        // Filter to partitions matching the current table
        const tablePartitions = allData.filter(d => !currentTable || d.table === currentTable);

        // Sort partitions by partition name (to match archiver's processing order)
        const sortedPartitions = [...tablePartitions].sort((a, b) => {
            return (a.partition || '').localeCompare(b.partition || '');
        });

        // Take the first N partitions (where N = partitions_processed)
        // These represent the partitions processed so far in the current run
        currentRunData = sortedPartitions.slice(0, processedCount);

        // Count successful (newly uploaded) vs skipped (already existed in S3)
        // An item is "successful" (newly uploaded) if:
        // - It's in S3 AND has fileSize AND fileTime is recent (after archiver start)
        // An item is "skipped" if it was actually skipped during this run:
        // - It's in S3 but fileTime is old (was already there before this run) OR
        // - It's in S3 but has no fileSize (was skipped because it already existed)
        successful = currentRunData.filter(d => {
            if (!d.s3Uploaded || d.lastError || !d.fileSize || d.fileSize === 0) return false;
            // Check if fileTime is recent (within current run timeframe)
            // If fileTime is after archiver start time, it was newly uploaded
            if (d.fileTime && archiverStartTime) {
                const fileTime = new Date(d.fileTime);
                return fileTime >= archiverStartTime;
            }
            // If no fileTime or archiverStartTime, we can't determine - be conservative and don't count as successful
            return false;
        }).length;

        // Skipped: Only count entries that were actually skipped during THIS run
        // - In S3 but fileTime is old (was already there before this run) - this means it was skipped
        // - In S3 but no fileSize (was skipped because it already existed)
        skipped = currentRunData.filter(d => {
            if (d.lastError) return false; // Errors are not skips
            // In S3 but fileTime is old (was already there before this run) - this was skipped
            if (d.s3Uploaded && d.fileTime && archiverStartTime) {
                const fileTime = new Date(d.fileTime);
                if (fileTime < archiverStartTime) {
                    return true; // Was already in S3 before this run, so it was skipped
                }
            }
            // In S3 but no fileSize (was skipped because it already existed)
            if (d.s3Uploaded && (!d.fileSize || d.fileSize === 0)) {
                return true;
            }
            return false;
        }).length;

        failed = currentRunData.filter(d => d.lastError).length;
    } else {
        // Use all cache data (shows historical/all-time totals)
        successful = allData.filter(d => d.s3Uploaded && !d.lastError).length;
        skipped = allData.filter(d => !d.fileSize && d.rowCount > 0).length;
        failed = allData.filter(d => d.lastError).length;
    }

    // Calculate "Not Synced" - use TaskInfo if available and showing current run, otherwise fall back to cache data
    let notSynced;
    if (useCurrentRun && currentTaskInfo && currentTaskInfo.total_partitions > 0 && typeof currentTaskInfo.partitions_processed === 'number') {
        // Use archiver's tracking: Total Partitions - Partitions Processed
        notSynced = currentTaskInfo.total_partitions - currentTaskInfo.partitions_processed;
    } else if (useCurrentRun && currentTaskInfo && currentTaskInfo.total_items > 0 && typeof currentTaskInfo.completed_items === 'number') {
        // Fallback to total_items and completed_items
        notSynced = currentTaskInfo.total_items - currentTaskInfo.completed_items;
    } else {
        // Fallback: A partition is "processed" if it has been counted (rowCount > 0) or has a file or has an error
        const processed = allData.filter(d => d.rowCount > 0 || d.fileSize > 0 || d.lastError).length;
        notSynced = totalPartitions - processed;
    }

    // Ensure notSynced is always a valid number
    if (isNaN(notSynced) || notSynced < 0) {
        notSynced = 0;
    }

    const totalCompressed = allData.reduce((sum, d) => sum + (d.fileSize || 0), 0);
    const totalUncompressed = allData.reduce((sum, d) => sum + (d.uncompressedSize || 0), 0);
    const totalRows = allData.reduce((sum, d) => sum + (d.rowCount || 0), 0);

    const avgRatio = calculateRatio(totalUncompressed, totalCompressed);

    // Calculate sync rate: (success + skipped) vs (failed + pending)
    // Pending: entries that haven't been processed yet (total partitions - processed)
    let pending = 0;
    if (useCurrentRun && currentTaskInfo && currentTaskInfo.total_partitions > 0 && typeof currentTaskInfo.partitions_processed === 'number') {
        // Pending = Total partitions - Partitions processed
        pending = currentTaskInfo.total_partitions - currentTaskInfo.partitions_processed;
        // Ensure pending is non-negative
        if (pending < 0) pending = 0;
    } else {
        // For all-time view, pending would be partitions counted but not processed
        pending = allData.filter(d => {
            return d.rowCount > 0 && !d.fileSize && !d.lastError && !d.s3Uploaded;
        }).length;
    }

    const totalProcessed = successful + skipped + pending + failed;
    let syncRate = 0;
    if (totalProcessed > 0) {
        syncRate = ((successful + skipped) / totalProcessed) * 100;
    }

    // Calculate date range and throughput - use current run data if showing current run stats
    let dataForDateRange = allData;
    if (useCurrentRun) {
        // Use current run data for date range and throughput calculations
        const processedCount = currentTaskInfo.partitions_processed || 0;
        const currentTable = currentTaskInfo.table || '';
        const tablePartitions = allData.filter(d => !currentTable || d.table === currentTable);
        const sortedPartitions = [...tablePartitions].sort((a, b) => {
            return (a.partition || '').localeCompare(b.partition || '');
        });
        dataForDateRange = sortedPartitions.slice(0, processedCount);
    }

    const dates = dataForDateRange
        .map(d => {
            // Try to extract date from partition name or use fileTime
            if (d.fileTime && d.fileTime !== '0001-01-01T00:00:00Z') {
                return new Date(d.fileTime);
            }
            return null;
        })
        .filter(d => d !== null)
        .sort((a, b) => a - b);

    let dateRangeText = 'N/A';
    if (dates.length > 0) {
        const minDate = dates[0];
        const maxDate = dates[dates.length - 1];
        if (minDate.getTime() === maxDate.getTime()) {
            dateRangeText = minDate.toLocaleDateString();
        } else {
            dateRangeText = minDate.toLocaleDateString() + ' to ' + maxDate.toLocaleDateString();
        }
    }

    // Calculate throughput (rows/sec) - approximate from file times
    // Only count rows from successfully uploaded entries (not skipped ones)
    let throughputText = 'N/A';
    if (dates.length > 1) {
        // Filter to only successfully uploaded entries for throughput calculation
        const uploadedEntries = dataForDateRange.filter(d => {
            if (useCurrentRun && archiverStartTime) {
                // For current run, only count entries uploaded during this run
                return d.s3Uploaded && d.fileSize > 0 && d.fileTime &&
                       new Date(d.fileTime) >= archiverStartTime;
            } else {
                // For all-time, count all uploaded entries
                return d.s3Uploaded && d.fileSize > 0;
            }
        });

        const currentRunRows = uploadedEntries.reduce((sum, d) => sum + (d.rowCount || 0), 0);
        if (currentRunRows > 0) {
            // Use file times from uploaded entries only
            const uploadedDates = uploadedEntries
                .map(d => {
                    if (d.fileTime && d.fileTime !== '0001-01-01T00:00:00Z') {
                        return new Date(d.fileTime);
                    }
                    return null;
                })
                .filter(d => d !== null)
                .sort((a, b) => a - b);

            if (uploadedDates.length > 1) {
                const timeSpan = (uploadedDates[uploadedDates.length - 1] - uploadedDates[0]) / 1000; // seconds
                if (timeSpan > 0) {
                    const rowsPerSec = currentRunRows / timeSpan;
                    throughputText = formatNumber(Math.round(rowsPerSec)) + ' rows/sec';
                }
            }
        }
    }

    // Calculate new values
    const newValues = {
        partitions: totalPartitions.toLocaleString(),
        size: formatBytes(totalCompressed),
        ratio: avgRatio,
        rows: totalRows.toLocaleString(),
        successRate: (totalProcessed > 0 ? syncRate.toFixed(1) + '%' : 'N/A'),
        throughput: throughputText,
        notSynced: notSynced.toLocaleString(),
        partitionsDetail: cachedFiles + ' cached, ' + withErrors + ' errors',
        sizeDetail: 'Uncompressed: ' + formatBytes(totalUncompressed),
        successRateDetail: successful + ' uploaded, ' + skipped + ' skipped, ' + pending + ' pending, ' + failed + ' failed',
        throughputDetail: dateRangeText,
        notSyncedDetail: (notSynced > 0 ? 'Entries pending processing' : 'All processed')
    };

    // Check if values actually changed to avoid unnecessary updates
    // But always update sync rate detail text since it includes counts that change frequently
    if (lastStatsValues &&
        lastStatsValues.partitions === newValues.partitions &&
        lastStatsValues.size === newValues.size &&
        lastStatsValues.ratio === newValues.ratio &&
        lastStatsValues.rows === newValues.rows &&
        lastStatsValues.successRate === newValues.successRate &&
        lastStatsValues.throughput === newValues.throughput &&
        lastStatsValues.notSynced === newValues.notSynced &&
        lastStatsValues.successRateDetail === newValues.successRateDetail) {
        return; // No changes, skip update
    }

    // Cache new values
    lastStatsValues = newValues;

    const statsGrid = document.getElementById('stats-grid');

    // Ensure stats grid exists and has cards - initialize synchronously
    if (!statsGrid || statsGrid.children.length === 0) {
        // Initialize stats grid if it doesn't exist
        statsGrid.innerHTML = '<div class="stat-card"><div class="label">Total Partitions</div><div class="value"></div><div class="detail"></div></div>' +
            '<div class="stat-card"><div class="label">Total Size</div><div class="value"></div><div class="detail"></div></div>' +
            '<div class="stat-card"><div class="label">Compression</div><div class="value"></div><div class="detail">Average ratio</div></div>' +
            '<div class="stat-card"><div class="label">Total Rows</div><div class="value"></div><div class="detail">Across all partitions</div></div>' +
            '<div class="stat-card"><div class="label">Sync Rate</div><div class="value"></div><div class="detail"></div></div>' +
            '<div class="stat-card"><div class="label">Throughput</div><div class="value"></div><div class="detail"></div></div>' +
            '<div class="stat-card"><div class="label">Not Synced</div><div class="value"></div><div class="detail"></div></div>';
    } else {
        // Ensure all cards have the proper structure
        const cards = statsGrid.children;
        const labels = ['Total Partitions', 'Total Size', 'Compression', 'Total Rows', 'Sync Rate', 'Throughput', 'Not Synced'];
        const details = ['', '', 'Average ratio', 'Across all partitions', '', '', ''];
        for (let i = 0; i < Math.min(cards.length, 7); i++) {
            if (!cards[i].querySelector('.label')) {
                cards[i].innerHTML = '<div class="label">' + labels[i] + '</div><div class="value"></div><div class="detail">' + details[i] + '</div>';
            }
        }
    }

    // Determine sync rate color
    let syncRateClass = 'success-rate-high';
    if (syncRate < 50) {
        syncRateClass = 'success-rate-low';
    } else if (syncRate < 90) {
        syncRateClass = 'success-rate-medium';
    }

    // Update individual stat cards without replacing entire grid
    requestAnimationFrame(() => {
        // Card 1: Total Partitions
        const card1 = statsGrid.children[0];
        if (card1) {
            const valueEl = card1.querySelector('.value');
            const detailEl = card1.querySelector('.detail');
            if (valueEl && valueEl.textContent !== newValues.partitions) {
                valueEl.textContent = newValues.partitions;
                animateCell(valueEl);
            }
            if (detailEl && detailEl.textContent !== newValues.partitionsDetail) {
                detailEl.textContent = newValues.partitionsDetail;
            }
        }

        // Card 2: Total Size
        const card2 = statsGrid.children[1];
        if (card2) {
            const valueEl = card2.querySelector('.value');
            const detailEl = card2.querySelector('.detail');
            if (valueEl && valueEl.textContent !== newValues.size) {
                valueEl.textContent = newValues.size;
                animateCell(valueEl);
            }
            if (detailEl && detailEl.textContent !== newValues.sizeDetail) {
                detailEl.textContent = newValues.sizeDetail;
            }
        }

        // Card 3: Compression
        const card3 = statsGrid.children[2];
        if (card3) {
            const valueEl = card3.querySelector('.value');
            if (valueEl && valueEl.textContent !== newValues.ratio) {
                valueEl.textContent = newValues.ratio;
                animateCell(valueEl);
            }
        }

        // Card 4: Total Rows
        const card4 = statsGrid.children[3];
        if (card4) {
            const valueEl = card4.querySelector('.value');
            if (valueEl && valueEl.textContent !== newValues.rows) {
                valueEl.textContent = newValues.rows;
                animateCell(valueEl);
            }
        }

        // Card 5: Sync Rate
        const card5 = statsGrid.children[4];
        if (card5) {
            const valueEl = card5.querySelector('.value');
            const detailEl = card5.querySelector('.detail');
            if (valueEl) {
                const newText = newValues.successRate;
                if (valueEl.textContent !== newText) {
                    valueEl.textContent = newText;
                    valueEl.className = 'value ' + syncRateClass;
                    animateCell(valueEl);
                } else if (!valueEl.classList.contains(syncRateClass)) {
                    valueEl.className = 'value ' + syncRateClass;
                }
            }
            if (detailEl && detailEl.textContent !== newValues.successRateDetail) {
                detailEl.textContent = newValues.successRateDetail;
            }
        }

        // Card 6: Throughput
        const card6 = statsGrid.children[5];
        if (card6) {
            const valueEl = card6.querySelector('.value');
            const detailEl = card6.querySelector('.detail');
            if (valueEl && valueEl.textContent !== newValues.throughput) {
                valueEl.textContent = newValues.throughput;
                animateCell(valueEl);
            }
            if (detailEl && detailEl.textContent !== newValues.throughputDetail) {
                detailEl.textContent = newValues.throughputDetail;
            }
        }

        // Card 7: Not Synced
        const card7 = statsGrid.children[6];
        if (card7) {
            const valueEl = card7.querySelector('.value');
            const detailEl = card7.querySelector('.detail');
            if (valueEl && valueEl.textContent !== newValues.notSynced) {
                valueEl.textContent = newValues.notSynced;
                animateCell(valueEl);
            }
            if (detailEl && detailEl.textContent !== newValues.notSyncedDetail) {
                detailEl.textContent = newValues.notSyncedDetail;
            }
        }
    });
}

// Update completion summary panel
function updateCompletionSummary() {
    const summaryPanel = document.getElementById('completion-summary');
    const summaryGrid = document.getElementById('summary-grid');

    // Show summary when:
    // 1. Archiver is idle (completed normally)
    // 2. Archiver was running but stopped (cancelled/interrupted) - show partial progress
    // Don't show when archiver is currently running
    if (allData.length === 0) {
        summaryPanel.style.display = 'none';
        return;
    }

    // If archiver is currently running, hide the summary
    if (archiverRunning) {
        summaryPanel.style.display = 'none';
        return;
    }

    // Archiver is idle - check if we have any processed data to show
    const hasProcessedData = allData.some(d => d.s3Uploaded || d.fileSize > 0 || d.lastError);
    if (!hasProcessedData) {
        summaryPanel.style.display = 'none';
        return;
    }

    // Calculate statistics
    const successful = allData.filter(d => d.s3Uploaded && !d.lastError).length;
    const skipped = allData.filter(d => !d.fileSize && d.rowCount > 0).length;
    const failed = allData.filter(d => d.lastError).length;
    const totalProcessed = successful + skipped + failed;

    const totalRows = allData.reduce((sum, d) => sum + (d.rowCount || 0), 0);
    const totalBytes = allData.reduce((sum, d) => sum + (d.fileSize || 0), 0);
    const totalUncompressed = allData.reduce((sum, d) => sum + (d.uncompressedSize || 0), 0);

    // Calculate success rate
    let successRate = 0;
    if (totalProcessed > 0) {
        successRate = (successful / totalProcessed) * 100;
    }

    // Determine success rate color
    let successRateClass = 'success-rate-high';
    let successRateColor = '#04B575'; // green
    if (successRate < 50) {
        successRateClass = 'success-rate-low';
        successRateColor = '#FF4444'; // red
    } else if (successRate < 90) {
        successRateClass = 'success-rate-medium';
        successRateColor = '#FFAA00'; // orange
    }

    // Calculate date range
    const dates = allData
        .map(d => {
            if (d.fileTime && d.fileTime !== '0001-01-01T00:00:00Z') {
                return new Date(d.fileTime);
            }
            return null;
        })
        .filter(d => d !== null)
        .sort((a, b) => a - b);

    let dateRangeText = 'N/A';
    if (dates.length > 0) {
        const minDate = dates[0];
        const maxDate = dates[dates.length - 1];
        if (minDate.getTime() === maxDate.getTime()) {
            dateRangeText = minDate.toLocaleDateString();
        } else {
            dateRangeText = minDate.toLocaleDateString() + ' to ' + maxDate.toLocaleDateString();
        }
    }

    // Calculate throughput based on elapsed time from first to last upload
    let throughputRows = 'N/A';
    let throughputMB = 'N/A';
    let totalElapsedSeconds = 0;
    if (dates.length > 1 && totalRows > 0) {
        const timeSpan = (dates[dates.length - 1] - dates[0]) / 1000; // seconds
        totalElapsedSeconds = timeSpan;
        if (timeSpan > 0) {
            const rowsPerSec = totalRows / timeSpan;
            throughputRows = formatNumber(Math.round(rowsPerSec)) + ' rows/sec';
            if (totalBytes > 0) {
                const mbPerSec = totalBytes / (1024 * 1024) / timeSpan;
                throughputMB = mbPerSec.toFixed(2) + ' MB/sec';
            }
        }
    }

    // Calculate average time per partition for uploaded partitions
    // Use processing time from processStartTime to completion (s3UploadTime or fileTime)
    let avgTimePerPartition = 'N/A';
    const uploadedEntries = allData.filter(d => d.s3Uploaded && !d.lastError && d.fileSize > 0);
    if (uploadedEntries.length > 0) {
        let totalProcessingTime = 0;
        let validProcessingTimes = 0;

        uploadedEntries.forEach(entry => {
            const processingTime = getProcessingTimeSeconds(entry);
            if (processingTime > 0) {
                totalProcessingTime += processingTime;
                validProcessingTimes++;
            }
        });

        if (validProcessingTimes > 0) {
            const avgSeconds = totalProcessingTime / validProcessingTimes;
            avgTimePerPartition = formatDuration(avgSeconds);
        }
    }

    // Calculate summary values
    const summaryValues = {
        totalPartitions: allData.length,
        successful: successful,
        skipped: skipped,
        failed: failed,
        successRate: successRate.toFixed(1),
        totalRows: totalRows,
        totalBytes: totalBytes,
        totalUncompressed: totalUncompressed,
        throughputRows: throughputRows,
        throughputMB: throughputMB,
        dateRange: dateRangeText
    };

    // Check if values actually changed to avoid unnecessary updates
    if (lastSummaryValues &&
        lastSummaryValues.totalPartitions === summaryValues.totalPartitions &&
        lastSummaryValues.successful === summaryValues.successful &&
        lastSummaryValues.skipped === summaryValues.skipped &&
        lastSummaryValues.failed === summaryValues.failed &&
        lastSummaryValues.successRate === summaryValues.successRate &&
        lastSummaryValues.totalRows === summaryValues.totalRows &&
        lastSummaryValues.totalBytes === summaryValues.totalBytes) {
        return; // No changes, skip update
    }

    // Cache new values
    lastSummaryValues = summaryValues;

    // Build summary HTML (batched in requestAnimationFrame to prevent flickering)
    requestAnimationFrame(() => {
        let summaryHTML = '';

    // Total partitions
    summaryHTML += '<div class="summary-stat">' +
        '<div class="summary-label">Total Partitions</div>' +
        '<div class="summary-value">' + allData.length.toLocaleString() + '</div>' +
        '</div>';

    // Success/Skip/Failed counts with color coding
    summaryHTML += '<div class="summary-stat summary-success">' +
        '<div class="summary-label">✅ Uploaded</div>' +
        '<div class="summary-value">' + successful.toLocaleString() + '</div>' +
        '</div>';

    if (skipped > 0) {
        summaryHTML += '<div class="summary-stat summary-skip">' +
            '<div class="summary-label">⏭ Skipped</div>' +
            '<div class="summary-value">' + skipped.toLocaleString() + '</div>' +
            '</div>';
    }

    if (failed > 0) {
        summaryHTML += '<div class="summary-stat summary-error">' +
            '<div class="summary-label">❌ Failed</div>' +
            '<div class="summary-value">' + failed.toLocaleString() + '</div>' +
            '</div>';
    }

    // Success rate
    if (totalProcessed > 0) {
        summaryHTML += '<div class="summary-stat">' +
            '<div class="summary-label">Success Rate</div>' +
            '<div class="summary-value ' + successRateClass + '" style="color: ' + successRateColor + '">' +
            successRate.toFixed(1) + '%' +
            '</div>' +
            '</div>';
    }

    // Total rows transferred
    if (totalRows > 0) {
        summaryHTML += '<div class="summary-stat">' +
            '<div class="summary-label">Total Rows Transferred</div>' +
            '<div class="summary-value">' + totalRows.toLocaleString() + '</div>' +
            '</div>';
    }

    // Total data uploaded
    if (totalBytes > 0) {
        summaryHTML += '<div class="summary-stat">' +
            '<div class="summary-label">Total Data Uploaded</div>' +
            '<div class="summary-value">' + formatBytes(totalBytes) + '</div>' +
            '<div class="summary-detail">Uncompressed: ' + formatBytes(totalUncompressed) + '</div>' +
            '</div>';
    }

    // Throughput
    if (throughputRows !== 'N/A') {
        summaryHTML += '<div class="summary-stat">' +
            '<div class="summary-label">Throughput</div>' +
            '<div class="summary-value">' + throughputRows + '</div>';
        if (throughputMB !== 'N/A') {
            summaryHTML += '<div class="summary-detail">' + throughputMB + '</div>';
        }
        summaryHTML += '</div>';
    }

    // Date range
    if (dateRangeText !== 'N/A') {
        summaryHTML += '<div class="summary-stat">' +
            '<div class="summary-label">Date Range</div>' +
            '<div class="summary-value">' + dateRangeText + '</div>' +
            '</div>';
    }

    // Average time per partition
    if (avgTimePerPartition !== 'N/A') {
        summaryHTML += '<div class="summary-stat">' +
            '<div class="summary-label">Avg Time per Partition</div>' +
            '<div class="summary-value">' + avgTimePerPartition + '</div>' +
            '</div>';
    }

    // Total duration (if we have date range)
    if (totalElapsedSeconds > 0) {
        summaryHTML += '<div class="summary-stat">' +
            '<div class="summary-label">Total Duration</div>' +
            '<div class="summary-value">' + formatDuration(totalElapsedSeconds) + '</div>' +
            '</div>';
    }

    // Configuration summary - extract from uploaded entries
    const configParts = [];

    // Get format from uploaded entries
    const formats = new Set();
    const compressions = new Set();
    const buckets = new Set();

    uploadedEntries.forEach(entry => {
        if (entry.s3Key) {
            const format = getOutputFormat(entry.s3Key);
            if (format && format !== '—') {
                formats.add(format);
            }

            const compression = getCompressionType(entry.s3Key);
            if (compression) {
                compressions.add(compression);
            }

            const bucket = getS3Bucket(entry.s3Key);
            if (bucket) {
                buckets.add(bucket);
            }
        }
    });

    // Build configuration string
    if (formats.size > 0) {
        const formatList = Array.from(formats).sort().join(', ');
        configParts.push('Format: ' + formatList);
    }

    if (compressions.size > 0) {
        const compList = Array.from(compressions).sort().join(', ');
        configParts.push('Compression: ' + compList);
    }

    if (buckets.size > 0) {
        const bucketList = Array.from(buckets).sort().join(', ');
        configParts.push('S3: s3://' + bucketList);
    }

    // Display configuration if we have any info
    if (configParts.length > 0) {
        summaryHTML += '<div class="summary-stat">' +
            '<div class="summary-label">Configuration</div>' +
            '<div class="summary-value">' + configParts.join(' | ') + '</div>' +
            '</div>';
    }

        summaryGrid.innerHTML = summaryHTML;
        summaryPanel.style.display = 'block';
    });
}

// Sort data
function sortData(column, toggle = true) {
    if (toggle && currentSort.column === column) {
        currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
    } else if (toggle) {
        currentSort.column = column;
        currentSort.direction = 'asc';
    }

    allData.sort((a, b) => {
        let aVal = a[column];
        let bVal = b[column];

        // Handle processing time column specially
        if (column === 'processingTime') {
            aVal = getProcessingTimeSeconds(a);
            bVal = getProcessingTimeSeconds(b);
        } else if (column === 'fileTime' || column === 'countTime' || column === 'errorTime') {
        // Handle date columns specially
            aVal = aVal ? new Date(aVal).getTime() : 0;
            bVal = bVal ? new Date(bVal).getTime() : 0;
        } else if (typeof aVal === 'string') {
            aVal = (aVal || '').toLowerCase();
            bVal = (bVal || '').toLowerCase();
        } else {
            aVal = aVal || 0;
            bVal = bVal || 0;
        }

        if (aVal < bVal) return currentSort.direction === 'asc' ? -1 : 1;
        if (aVal > bVal) return currentSort.direction === 'asc' ? 1 : -1;
        return 0;
    });

    updateTable();
    updateSortIndicators();
}

// Update sort indicators and ARIA attributes
function updateSortIndicators() {
    document.querySelectorAll('thead th.sortable').forEach(th => {
        th.className = 'sortable';
        if (th.dataset.column === currentSort.column) {
            th.className = 'sortable sorted-' + currentSort.direction;
            // Set aria-sort for screen readers
            th.setAttribute('aria-sort', currentSort.direction === 'asc' ? 'ascending' : 'descending');
            // Announce sort change to screen reader users
            announceToScreenReader('Column ' + th.textContent.trim() + ' sorted ' + (currentSort.direction === 'asc' ? 'ascending' : 'descending'));
        } else {
            // Remove aria-sort from unsorted columns
            th.setAttribute('aria-sort', 'none');
        }
    });
}

// Helper function to determine status of an entry
function getEntryStatus(entry) {
    // Check if this is the currently processing partition
    if (archiverRunning && currentPartition &&
        entry.partition === currentPartition && entry.table === (currentTaskInfo?.table || '')) {
        return 'processing';
    }
    // Error takes precedence
    if (entry.lastError) {
        return 'error';
    }
    // Uploaded to S3
    if (entry.s3Uploaded && !entry.lastError) {
        return 'uploaded';
    }
    // File exists but not uploaded yet
    if (entry.fileSize > 0 && !entry.s3Uploaded && !entry.lastError) {
        return 'not-synced';
    }
    // Has row count but no file (skipped during processing - likely empty partition)
    if (entry.rowCount > 0 && !entry.fileSize && !entry.lastError) {
        return 'skipped';
    }
    // No row count yet (pending counting/processing)
    if (!entry.rowCount || entry.rowCount === 0) {
        return 'pending';
    }
    return 'unknown';
}

// Update table with smart refresh
function updateTable() {
    const searchTerm = document.getElementById('search-box').value;
    const tableFilter = document.getElementById('table-filter').value;
    const statusFilter = document.getElementById('status-filter').value;

    let filteredData = allData;

    if (tableFilter) {
        filteredData = filteredData.filter(d => d.table === tableFilter);
    }

    if (statusFilter) {
        filteredData = filteredData.filter(d => getEntryStatus(d) === statusFilter);
    }

    if (searchTerm) {
        const search = searchTerm.toLowerCase();
        filteredData = filteredData.filter(d =>
            d.partition.toLowerCase().includes(search) ||
            d.table.toLowerCase().includes(search)
        );
    }

    const tbody = document.getElementById('table-body');

    // Handle empty state
    if (filteredData.length === 0 && (searchTerm || tableFilter || statusFilter)) {
        tbody.innerHTML = '<tr><td colspan="11" style="text-align: center; padding: 60px 20px;">' +
            '<div class="empty-state">' +
            '<h3>No Results Found</h3>' +
            '<p>No cache entries match your filters. Try adjusting your search or filter.</p>' +
            '<button class="clear-search-btn" id="clear-filters-btn">Clear Filters</button>' +
            '</div>' +
            '</td></tr>';

        // Add event listener for clear button
        document.getElementById('clear-filters-btn')?.addEventListener('click', () => {
            document.getElementById('search-box').value = '';
            document.getElementById('table-filter').value = '';
            document.getElementById('status-filter').value = '';
            updateTable();
        });
        return;
    }

    // Handle completely empty state (no data at all)
    if (filteredData.length === 0 && allData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="11" style="text-align: center; padding: 60px 20px;">' +
            '<div class="empty-state">' +
            '<h3>No Cache Data</h3>' +
            '<p>No cache entries found. The archiver will populate this table as it processes partitions.</p>' +
            '</div>' +
            '</td></tr>';
        return;
    }

    const existingRowsMap = {};
    const rowsToKeep = new Set();

    // Build map of existing rows
    tbody.querySelectorAll('tr').forEach(row => {
        existingRowsMap[row.dataset.key] = row;
    });

    // Use requestAnimationFrame to batch DOM updates and reduce flickering
    requestAnimationFrame(() => {
        // Update existing rows and track which ones to keep
    filteredData.forEach(entry => {
        const key = getRowKey(entry);
            const row = existingRowsMap[key];

        if (row) {
                // Update existing row in place
            updateRow(row, entry);
                rowsToKeep.add(key);
            }
        });

        // Remove rows that are no longer in the filtered data
        tbody.querySelectorAll('tr').forEach(row => {
            const key = row.dataset.key;
            if (!rowsToKeep.has(key) && !row.querySelector('.empty-state')) {
                row.remove();
            }
        });

        // Add new rows that don't exist yet - append to end for simplicity
        // (sorting will be handled by reordering if needed)
        filteredData.forEach(entry => {
            const key = getRowKey(entry);
            if (!existingRowsMap[key]) {
                const row = createRow(entry);
        tbody.appendChild(row);
            }
        });

        // Reorder rows if needed - check if order matches expected sort order
        const currentOrder = Array.from(tbody.querySelectorAll('tr')).map(r => r.dataset.key);
        const expectedOrder = filteredData.map(e => getRowKey(e));
        const orderMatches = currentOrder.length === expectedOrder.length &&
            currentOrder.every((key, i) => key === expectedOrder[i]);

        if (!orderMatches && currentOrder.length > 0) {
            // Reorder by collecting rows and appending in correct order
            const rowsByKey = {};
            tbody.querySelectorAll('tr').forEach(row => {
                rowsByKey[row.dataset.key] = row;
            });

            // Clear and rebuild in correct order (batched in requestAnimationFrame)
            tbody.innerHTML = '';
            expectedOrder.forEach(key => {
                if (rowsByKey[key]) {
                    tbody.appendChild(rowsByKey[key]);
                }
            });
        }
    });
}

// Create a new row with ARIA attributes for accessibility
function createRow(entry) {
    const row = document.createElement('tr');
    row.dataset.key = getRowKey(entry);
    row.setAttribute('role', 'row');
    // Add aria-label describing the row for screen readers
    row.setAttribute('aria-label', 'Partition ' + entry.partition + ' in table ' + entry.table);
    updateRow(row, entry);
    return row;
}

// Update row cells with change detection
function updateRow(row, entry) {
    const age = calculateCompletionAge(entry);
    const processingTime = calculateProcessingTime(entry);
    const hasFile = entry.fileSize > 0;
    const hasError = !!entry.lastError;
    const isUploaded = entry.s3Uploaded;
    const ratio = calculateRatio(entry.uncompressedSize, entry.fileSize);

    // Check if this is the currently processing partition
    const isCurrentlyProcessing = archiverRunning && currentPartition &&
        entry.partition === currentPartition && entry.table === (currentTaskInfo?.table || '');

    let statusBadge = '';
    if (isCurrentlyProcessing) {
        statusBadge = '<span class="status-badge processing">Processing</span>';
    } else if (hasError) {
        statusBadge = '<span class="status-badge error">Error</span>';
    } else if (isUploaded) {
        statusBadge = '<span class="status-badge uploaded">In S3</span>';
    } else if (hasFile) {
        statusBadge = '<span class="status-badge cached">Processed</span>';
    } else {
        statusBadge = '<span class="status-badge no-file">Count Only</span>';
    }

    // Add/remove highlighting class for currently processing row
    if (isCurrentlyProcessing) {
        row.classList.add('processing-row');
    } else {
        row.classList.remove('processing-row');
    }

    // Check if this is a new row (no cells yet) or an existing row
    const cells = row.querySelectorAll('td');
    const isNewRow = cells.length === 0;

    // Format hash display with multipart ETag if available
    let hashDisplay = '—';
    if (entry.fileMD5) {
        const hashTitle = entry.fileMD5 + (entry.multipartETag ? ' (multipart: ' + entry.multipartETag + ')' : '');
        hashDisplay = '<span title="' + escapeHTMLAttr(hashTitle) + '">' + escapeHTML(entry.fileMD5.substring(0, 12)) + '...' +
            (entry.multipartETag ? ' <span class="multipart-tag" title="Multipart ETag: ' + escapeHTMLAttr(entry.multipartETag) + '">[MP]</span>' : '') +
            '</span>';
    } else if (entry.multipartETag) {
        hashDisplay = '<span class="multipart-tag" title="Multipart ETag: ' + escapeHTMLAttr(entry.multipartETag) + '">' + escapeHTML(entry.multipartETag) + '</span>';
    }

    if (isNewRow) {
        // For new rows, use innerHTML for fast initial population
        const formatType = getOutputFormat(entry.s3Key);
        row.innerHTML = '<td>' +
            '<div class="partition-name">' + escapeHTML(entry.partition) + '</div>' +
            '<div class="table-name">' + escapeHTML(entry.table) + '</div>' +
            '</td>' +
            '<td class="size">' + (entry.rowCount != null ? entry.rowCount.toLocaleString() : '—') + '</td>' +
            '<td class="format">' + formatType + '</td>' +
            '<td class="size">' + formatBytes(entry.uncompressedSize) + '</td>' +
            '<td class="size">' + formatBytes(entry.fileSize) + '</td>' +
            '<td class="ratio">' + ratio + '</td>' +
            '<td class="hash">' + hashDisplay + '</td>' +
            '<td><span class="age ' + age.class + '">' + age.text + '</span></td>' +
            '<td class="processing-time">' + processingTime + '</td>' +
            '<td>' + statusBadge + '</td>' +
            '<td>' + (hasError ? '<div class="error-text" title="' + escapeHTMLAttr(entry.lastError) + '">' + escapeHTML(entry.lastError) + '</div>' : '—') + '</td>';
    } else {
        // For existing rows, selectively update only changed cells
        // Calculate new values
        const newRowCount = entry.rowCount != null ? entry.rowCount.toLocaleString() : '—';
        const newFormat = getOutputFormat(entry.s3Key);
        const newUncompressed = formatBytes(entry.uncompressedSize);
        const newCompressed = formatBytes(entry.fileSize);

        // Cell 2: Row count
        const cell2 = cells[1];
        if (cell2) {
            const oldValue = cell2.textContent;
            if (oldValue !== newRowCount) {
                cell2.textContent = newRowCount;
                if (oldValue && oldValue !== '—') {
                    animateCell(cell2);
                }
            }
        }

        // Cell 3: Format
        const cell3 = cells[2];
        if (cell3) {
            const oldValue = cell3.textContent;
            if (oldValue !== newFormat) {
                cell3.textContent = newFormat;
            }
        }

        // Cell 4: Uncompressed size
        const cell4 = cells[3];
        if (cell4) {
            const oldValue = cell4.textContent;
            if (oldValue !== newUncompressed) {
                cell4.textContent = newUncompressed;
                if (oldValue && oldValue !== '—') {
                    animateCell(cell4);
                }
            }
        }

        // Cell 5: Compressed size
        const cell5 = cells[4];
        if (cell5) {
            const oldValue = cell5.textContent;
            if (oldValue !== newCompressed) {
                cell5.textContent = newCompressed;
                if (oldValue && oldValue !== '—') {
                    animateCell(cell5);
                }
            }
        }

        // Cell 6: Ratio (always update, no animation)
        const cell6 = cells[5];
        if (cell6 && cell6.textContent !== ratio) {
            cell6.textContent = ratio;
        }

        // Cell 7: Hash (update if different, no animation)
        const cell7 = cells[6];
        if (cell7) {
            if (cell7.innerHTML !== hashDisplay) {
                cell7.innerHTML = hashDisplay;
            }
        }

        // Cell 8: Age (update if different, no animation)
        const cell8 = cells[7];
        if (cell8) {
            const newAgeHTML = '<span class="age ' + age.class + '">' + age.text + '</span>';
            if (cell8.innerHTML !== newAgeHTML) {
                cell8.innerHTML = newAgeHTML;
            }
        }

        // Cell 9: Processing Time (update if different, no animation)
        const cell9 = cells[8];
        if (cell9) {
            if (cell9.textContent !== processingTime) {
                cell9.textContent = processingTime;
            }
        }

        // Cell 10: Status (update if different, no animation)
        const cell10 = cells[9];
        if (cell10) {
            if (cell10.innerHTML !== statusBadge) {
                cell10.innerHTML = statusBadge;
            }
        }

        // Cell 11: Error (update if different, no animation)
        const cell11 = cells[10];
        if (cell11) {
            const newErrorHTML = hasError ? '<div class="error-text" title="' + escapeHTMLAttr(entry.lastError) + '">' + escapeHTML(entry.lastError) + '</div>' : '—';
            if (cell11.innerHTML !== newErrorHTML) {
                cell11.innerHTML = newErrorHTML;
            }
        }
    }
}

// Add animation to element
function animateCell(element) {
    if (element) {
        element.classList.remove('updated');
        void element.offsetWidth; // Force reflow
        element.classList.add('updated');
        setTimeout(() => element.classList.remove('updated'), 800);
    }
}

// Event listeners
const searchBox = document.getElementById('search-box');
const clearBtn = document.getElementById('clear-search');

searchBox.addEventListener('input', (e) => {
    const value = e.target.value;
    clearBtn.style.display = value ? 'flex' : 'none';
    updateTable();
});

clearBtn.addEventListener('click', () => {
    searchBox.value = '';
    clearBtn.style.display = 'none';
    updateTable();
    searchBox.focus();
    announceToScreenReader('Search cleared');
});

document.getElementById('table-filter').addEventListener('change', updateTable);
document.getElementById('status-filter').addEventListener('change', updateTable);

// Stats mode toggle - switch between current run and all-time stats
document.getElementById('stats-mode-toggle').addEventListener('change', function(e) {
    statsMode = e.target.value;
    updateStats();
});

// Follow button - toggle following the current processing entry
document.getElementById('follow-button').addEventListener('click', toggleFollow);

// Sort handlers with keyboard support
document.addEventListener('click', (e) => {
    const th = e.target.closest('th.sortable');
    if (th) {
        sortData(th.dataset.column);
    }
});

document.addEventListener('keydown', (e) => {
    const th = e.target.closest('th.sortable');
    if (th && (e.key === 'Enter' || e.key === ' ')) {
        e.preventDefault();
        sortData(th.dataset.column);
    }
});

// Handle partition link clicks with event delegation
document.addEventListener('click', (e) => {
    const link = e.target.closest('.partition-link');
    if (link) {
        e.preventDefault();
        const partitionName = link.dataset.partition;
        if (partitionName) {
            scrollToPartition(partitionName);
        }
    }
});

// Initial load
fetchInitialData();
updateSortIndicators();  // Show initial sort indicator

// Connect WebSocket for real-time updates
connectWebSocket();

// Update age column every minute to keep it dynamic
setInterval(() => {
    // Only update if we have data and the table is visible
    if (allData.length > 0) {
        const tbody = document.getElementById('table-body');
        if (tbody) {
            const rows = tbody.querySelectorAll('tr');
            rows.forEach(row => {
                const key = row.dataset.key;
                if (key) {
                    // Find the entry for this row
                    const [table, partition] = key.split('|');
                    const entry = allData.find(d => d.table === table && d.partition === partition);
                    if (entry) {
                        // Update age cell (cell 8, index 7)
                        const cells = row.querySelectorAll('td');
                        const ageCell = cells[7];
                        if (ageCell) {
                            const age = calculateCompletionAge(entry);
                            const newAgeHTML = '<span class="age ' + age.class + '">' + age.text + '</span>';
                            ageCell.innerHTML = newAgeHTML;
                        }
                    }
                }
            });
        }
    }
}, 60000); // Update every minute

// Logs Modal Functions
function openLogsModal() {
    const modal = document.getElementById('logs-modal');
    modal.style.display = 'flex';
    modal.setAttribute('aria-hidden', 'false');
    document.body.style.overflow = 'hidden'; // Prevent background scrolling

    // Connect to logs WebSocket if not already connected
    if (!logsWebSocket || logsWebSocket.readyState !== WebSocket.OPEN) {
        connectLogsWebSocket();
    }

    // Scroll to bottom to show latest logs
    const logsContainer = document.getElementById('logs-container');
    setTimeout(() => {
        logsContainer.scrollTop = logsContainer.scrollHeight;
    }, 100);
}

function closeLogsModal() {
    const modal = document.getElementById('logs-modal');
    modal.style.display = 'none';
    modal.setAttribute('aria-hidden', 'true');
    document.body.style.overflow = ''; // Restore scrolling

    // Close logs WebSocket when modal is closed
    if (logsWebSocket) {
        logsWebSocket.close();
        logsWebSocket = null;
    }

    // Clear pause state
    logsPaused = false;
    logsBuffer = [];
    const pauseBtn = document.getElementById('logs-pause');
    if (pauseBtn) {
        pauseBtn.textContent = 'Pause';
        pauseBtn.classList.remove('paused');
    }
}

function connectLogsWebSocket() {
    // Close existing connection if any
    if (logsWebSocket) {
        logsWebSocket.close();
    }

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = protocol + '//' + window.location.host + '/ws/logs';

    logsWebSocket = new WebSocket(wsUrl);

    logsWebSocket.onopen = function() {
        console.log('Logs WebSocket connected');
    };

    logsWebSocket.onmessage = function(event) {
        console.log('Logs WebSocket message received:', event.data);
        if (logsPaused) {
            // Buffer logs when paused
            logsBuffer.push(JSON.parse(event.data));
            // Limit buffer size
            if (logsBuffer.length > maxLogEntries) {
                logsBuffer.shift();
            }
        } else {
            handleLogMessage(JSON.parse(event.data));
        }
    };

    logsWebSocket.onerror = function(error) {
        console.error('Logs WebSocket error:', error);
    };

    logsWebSocket.onclose = function() {
        console.log('Logs WebSocket disconnected');
        // Attempt to reconnect after 3 seconds if modal is still open
        setTimeout(() => {
            const modal = document.getElementById('logs-modal');
            if (modal && modal.style.display !== 'none') {
                connectLogsWebSocket();
            }
        }, 3000);
    };
}

function handleLogMessage(logMsg) {
    console.log('handleLogMessage called with:', logMsg);
    const logsContainer = document.getElementById('logs-container');
    if (!logsContainer) {
        console.error('logs-container not found!');
        return;
    }

    const logEntry = document.createElement('div');
    logEntry.className = 'log-entry ' + (logMsg.level || 'info').toLowerCase();

    const timestamp = logMsg.timestamp || '';
    const level = logMsg.level || 'INFO';
    const message = logMsg.message || '';

    logEntry.innerHTML = '<span class="log-timestamp">' + escapeHTML(timestamp) + '</span>' +
        '<span class="log-level">' + escapeHTML(level) + '</span>' +
        '<span class="log-message">' + escapeHTML(message) + '</span>';

    logsContainer.appendChild(logEntry);

    // Limit number of log entries to prevent memory issues
    const entries = logsContainer.querySelectorAll('.log-entry');
    if (entries.length > maxLogEntries) {
        entries[0].remove();
    }

    // Auto-scroll to bottom if user is near the bottom
    const isNearBottom = logsContainer.scrollHeight - logsContainer.scrollTop - logsContainer.clientHeight < 100;
    if (isNearBottom) {
        logsContainer.scrollTop = logsContainer.scrollHeight;
    }
}

function clearLogs() {
    const logsContainer = document.getElementById('logs-container');
    if (logsContainer) {
        logsContainer.innerHTML = '';
        logsBuffer = [];
    }
}

function toggleLogsPause() {
    logsPaused = !logsPaused;
    const pauseBtn = document.getElementById('logs-pause');
    if (pauseBtn) {
        pauseBtn.textContent = logsPaused ? 'Resume' : 'Pause';
        pauseBtn.classList.toggle('paused', logsPaused);
    }

    // If resuming, process buffered logs
    if (!logsPaused && logsBuffer.length > 0) {
        logsBuffer.forEach(logMsg => {
            handleLogMessage(logMsg);
        });
        logsBuffer = [];
    }
}

// Event listeners for logs modal
document.getElementById('logs-button').addEventListener('click', openLogsModal);
document.getElementById('logs-modal-close').addEventListener('click', closeLogsModal);
document.getElementById('logs-modal-overlay').addEventListener('click', closeLogsModal);
document.getElementById('logs-clear').addEventListener('click', clearLogs);
document.getElementById('logs-pause').addEventListener('click', toggleLogsPause);

// Close modal on Escape key
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
        const modal = document.getElementById('logs-modal');
        if (modal && modal.style.display !== 'none') {
            closeLogsModal();
        }
    }
});

// Function to scroll to a specific partition in the table (global for inline onclick)
window.scrollToPartition = function(partitionName) {
    // Small delay to ensure table is rendered
    setTimeout(() => {
        const rows = document.querySelectorAll('#table-body tr');
        for (const row of rows) {
            const partitionDiv = row.querySelector('td:first-child .partition-name');
            if (partitionDiv && partitionDiv.textContent.trim() === partitionName) {
                // Highlight the row temporarily
                const originalBg = row.style.background;
                row.style.background = 'var(--color-warning-50)';
                row.scrollIntoView({ behavior: 'smooth', block: 'center' });

                // Also ensure the table wrapper scrolls if needed
                const tableWrapper = document.querySelector('.table-wrapper');
                if (tableWrapper) {
                    const rowTop = row.offsetTop;
                    const rowHeight = row.offsetHeight;
                    const wrapperScrollTop = tableWrapper.scrollTop;
                    const wrapperHeight = tableWrapper.offsetHeight;

                    if (rowTop < wrapperScrollTop || rowTop + rowHeight > wrapperScrollTop + wrapperHeight) {
                        tableWrapper.scrollTop = rowTop - (wrapperHeight / 2) + (rowHeight / 2);
                    }
                }

                setTimeout(() => {
                    row.style.background = originalBg || '';
                }, 2000);
                break;
            }
        }
    }, 100);
};

// Function to scroll to and follow the current processing partition
function scrollToCurrentPartition() {
    if (!followActive || !archiverRunning || !currentPartition) {
        return;
    }

    const rows = document.querySelectorAll('#table-body tr');
    for (const row of rows) {
        const partitionDiv = row.querySelector('td:first-child .partition-name');
        const tableDiv = row.querySelector('td:first-child .table-name');
        if (partitionDiv && partitionDiv.textContent.trim() === currentPartition) {
            // Check if table matches (if available)
            const expectedTable = currentTaskInfo?.table || '';
            if (expectedTable && tableDiv && tableDiv.textContent.trim() !== expectedTable) {
                continue;
            }

            // Scroll to the row
            row.scrollIntoView({ behavior: 'smooth', block: 'center' });

            // Also ensure the table wrapper scrolls if needed
            const tableWrapper = document.querySelector('.table-wrapper');
            if (tableWrapper) {
                const rowTop = row.offsetTop;
                const rowHeight = row.offsetHeight;
                const wrapperScrollTop = tableWrapper.scrollTop;
                const wrapperHeight = tableWrapper.offsetHeight;

                if (rowTop < wrapperScrollTop || rowTop + rowHeight > wrapperScrollTop + wrapperHeight) {
                    tableWrapper.scrollTop = rowTop - (wrapperHeight / 2) + (rowHeight / 2);
                }
            }
            break;
        }
    }
}

// Toggle follow mode
function toggleFollow() {
    followActive = !followActive;
    const followBtn = document.getElementById('follow-button');
    const followIcon = document.getElementById('follow-icon');
    const followText = document.getElementById('follow-text');

    if (followBtn) {
        followBtn.classList.toggle('active', followActive);
        followText.textContent = followActive ? 'Following' : 'Follow';
    }

    if (followActive) {
        // Start following - scroll immediately and set up interval
        scrollToCurrentPartition();
        // Check every 500ms to keep following
        if (followInterval) {
            clearInterval(followInterval);
        }
        followInterval = setInterval(() => {
            scrollToCurrentPartition();
        }, 500);
    } else {
        // Stop following
        if (followInterval) {
            clearInterval(followInterval);
            followInterval = null;
        }
    }
}

// Clean up on page unload
window.addEventListener('beforeunload', () => {
    if (ws) {
        ws.close();
    }
    if (wsReconnectInterval) {
        clearTimeout(wsReconnectInterval);
    }
    if (followInterval) {
        clearInterval(followInterval);
    }
    if (logsWebSocket) {
        logsWebSocket.close();
    }
});
