let allData = [];
let currentData = {};  // Track current data by partition key
let currentSort = { column: 'partition', direction: 'asc' };  // Default sort by partition name
let ws = null;
let wsReconnectInterval = null;
let lastTaskState = null;  // Store last known task state to avoid flashing

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

// Calculate compression ratio
function calculateRatio(uncompressed, compressed) {
    if (!uncompressed || !compressed) return '—';
    const ratio = uncompressed / compressed;
    return ratio.toFixed(1) + 'x';
}

// Calculate age
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

// Create row key
function getRowKey(entry) {
    return entry.table + '|' + entry.partition;
}

// WebSocket connection management
function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = protocol + '//' + window.location.host + '/ws';

    ws = new WebSocket(wsUrl);

    ws.onopen = function() {
        console.log('WebSocket connected');
        document.getElementById('status').classList.add('connected');
        document.getElementById('status').classList.remove('disconnected');
        document.getElementById('status-text').textContent = 'Connected to live updates';

        // Clear reconnect interval if exists
        if (wsReconnectInterval) {
            clearInterval(wsReconnectInterval);
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

        // Set up reconnection with exponential backoff
        if (!wsReconnectInterval) {
            let reconnectAttempts = 0;
            wsReconnectInterval = setInterval(() => {
                reconnectAttempts++;
                const maxAttempts = 30;

                if (reconnectAttempts > maxAttempts) {
                    clearInterval(wsReconnectInterval);
                    wsReconnectInterval = null;
                    document.getElementById('status-text').textContent = 'Connection failed - please refresh the page';
                    return;
                }

                console.log('Reconnect attempt ' + reconnectAttempts + '/' + maxAttempts);
                document.getElementById('status-text').textContent = 'Reconnecting (' + reconnectAttempts + '/' + maxAttempts + ')...';
                connectWebSocket();
            }, 2000);
        }
    };
}

// Update task panel
function updateTaskPanel(status) {
    const panel = document.getElementById('task-panel');

    if (status.archiverRunning) {
        panel.classList.remove('idle');

        document.getElementById('task-title').textContent = 'Archiver Running';
        document.getElementById('task-pid').textContent = 'PID: ' + status.pid;

        // Handle task info if available
        if (status.currentTask) {
            // Store the last known task state
            lastTaskState = status.currentTask;
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

            document.getElementById('task-stats').textContent =
                taskForProgress.completed_items + '/' + taskForProgress.total_items + ' partitions (' + Math.round(percent) + '%)';
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
    } else {
        panel.classList.add('idle');
        document.getElementById('task-title').textContent = 'Archiver Idle';
        document.getElementById('task-pid').textContent = '';
        document.getElementById('task-status-text').textContent = 'No active archiving process';
        document.getElementById('task-progress-bar').style.display = 'none';
        document.getElementById('task-stats').textContent = '';
        document.getElementById('task-time').textContent = '';
        // Clear last task state when idle
        lastTaskState = null;
    }
}

// Handle cache data from WebSocket or initial fetch
function handleCacheData(data) {
    // Flatten all entries
    const newData = [];
    const tables = new Set();

    data.tables.forEach(table => {
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
    updateTable();

    document.getElementById('last-update').textContent = 'Last updated: ' + new Date().toLocaleTimeString();
}

// Fetch initial data (fallback for when WebSocket is not yet connected)
async function fetchInitialData() {
    try {
        // Fetch cache data
        const cacheResponse = await fetch('/api/cache');
        if (cacheResponse.ok) {
            const cacheData = await cacheResponse.json();
            handleCacheData(cacheData);
        }

        // Fetch status data
        const statusResponse = await fetch('/api/status');
        if (statusResponse.ok) {
            const statusData = await statusResponse.json();
            updateTaskPanel(statusData);
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

// Update statistics with animations
function updateStats() {
    const totalPartitions = allData.length;
    const cachedFiles = allData.filter(d => d.fileSize > 0).length;
    const withErrors = allData.filter(d => d.lastError).length;
    const totalCompressed = allData.reduce((sum, d) => sum + (d.fileSize || 0), 0);
    const totalUncompressed = allData.reduce((sum, d) => sum + (d.uncompressedSize || 0), 0);
    const totalRows = allData.reduce((sum, d) => sum + (d.rowCount || 0), 0);

    const avgRatio = calculateRatio(totalUncompressed, totalCompressed);

    // Store old values
    const statsGrid = document.getElementById('stats-grid');
    const oldValues = {
        partitions: statsGrid.querySelector('.stat-card:nth-child(1) .value')?.textContent,
        size: statsGrid.querySelector('.stat-card:nth-child(2) .value')?.textContent,
        ratio: statsGrid.querySelector('.stat-card:nth-child(3) .value')?.textContent,
        rows: statsGrid.querySelector('.stat-card:nth-child(4) .value')?.textContent
    };

    statsGrid.innerHTML = '<div class="stat-card">' +
        '<div class="label">Total Partitions</div>' +
        '<div class="value">' + totalPartitions.toLocaleString() + '</div>' +
        '<div class="detail">' + cachedFiles + ' cached, ' + withErrors + ' errors</div>' +
        '</div>' +
        '<div class="stat-card">' +
        '<div class="label">Total Size</div>' +
        '<div class="value">' + formatBytes(totalCompressed) + '</div>' +
        '<div class="detail">Uncompressed: ' + formatBytes(totalUncompressed) + '</div>' +
        '</div>' +
        '<div class="stat-card">' +
        '<div class="label">Compression</div>' +
        '<div class="value">' + avgRatio + '</div>' +
        '<div class="detail">Average ratio</div>' +
        '</div>' +
        '<div class="stat-card">' +
        '<div class="label">Total Rows</div>' +
        '<div class="value">' + totalRows.toLocaleString() + '</div>' +
        '<div class="detail">Across all partitions</div>' +
        '</div>';

    // Animate changed stats
    const newValues = {
        partitions: totalPartitions.toLocaleString(),
        size: formatBytes(totalCompressed),
        ratio: avgRatio,
        rows: totalRows.toLocaleString()
    };

    setTimeout(() => {
        if (oldValues.partitions && oldValues.partitions !== newValues.partitions) {
            animateCell(statsGrid.querySelector('.stat-card:nth-child(1) .value'));
        }
        if (oldValues.size && oldValues.size !== newValues.size) {
            animateCell(statsGrid.querySelector('.stat-card:nth-child(2) .value'));
        }
        if (oldValues.ratio && oldValues.ratio !== newValues.ratio) {
            animateCell(statsGrid.querySelector('.stat-card:nth-child(3) .value'));
        }
        if (oldValues.rows && oldValues.rows !== newValues.rows) {
            animateCell(statsGrid.querySelector('.stat-card:nth-child(4) .value'));
        }
    }, 10);
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

        // Handle date columns specially
        if (column === 'fileTime' || column === 'countTime' || column === 'errorTime') {
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

// Update table with smart refresh
function updateTable() {
    const searchTerm = document.getElementById('search-box').value;
    const tableFilter = document.getElementById('table-filter').value;

    let filteredData = allData;

    if (tableFilter) {
        filteredData = filteredData.filter(d => d.table === tableFilter);
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
    if (filteredData.length === 0 && (searchTerm || tableFilter)) {
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 60px 20px;">' +
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
            updateTable();
        });
        return;
    }

    const existingRowsMap = {};

    // Build map of existing rows
    tbody.querySelectorAll('tr').forEach(row => {
        existingRowsMap[row.dataset.key] = row;
    });

    // Clear tbody
    tbody.innerHTML = '';

    // Add rows in sorted order
    filteredData.forEach(entry => {
        const key = getRowKey(entry);
        let row = existingRowsMap[key];

        if (row) {
            // Update existing row
            updateRow(row, entry);
        } else {
            // Create new row
            row = createRow(entry);
        }

        tbody.appendChild(row);
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
    const age = calculateAge(entry.fileTime || entry.countTime);
    const hasFile = entry.fileSize > 0;
    const hasError = !!entry.lastError;
    const isUploaded = entry.s3Uploaded;
    const ratio = calculateRatio(entry.uncompressedSize, entry.fileSize);

    let statusBadge = '';
    if (hasError) {
        statusBadge = '<span class="status-badge error">Error</span>';
    } else if (isUploaded) {
        statusBadge = '<span class="status-badge uploaded">In S3</span>';
    } else if (hasFile) {
        statusBadge = '<span class="status-badge cached">Processed</span>';
    } else {
        statusBadge = '<span class="status-badge no-file">Count Only</span>';
    }

    // Store old values to detect changes
    const oldValues = {
        rowCount: row.querySelector('td:nth-child(2)')?.textContent,
        uncompressed: row.querySelector('td:nth-child(3)')?.textContent,
        compressed: row.querySelector('td:nth-child(4)')?.textContent,
        status: row.querySelector('td:nth-child(8) .status-badge')?.textContent
    };

    row.innerHTML = '<td>' +
        '<div class="partition-name">' + escapeHTML(entry.partition) + '</div>' +
        '<div class="table-name">' + escapeHTML(entry.table) + '</div>' +
        '</td>' +
        '<td class="size">' + (entry.rowCount ? entry.rowCount.toLocaleString() : '—') + '</td>' +
        '<td class="size">' + formatBytes(entry.uncompressedSize) + '</td>' +
        '<td class="size">' + formatBytes(entry.fileSize) + '</td>' +
        '<td class="ratio">' + ratio + '</td>' +
        '<td class="hash">' + (entry.fileMD5 ? '<span title="' + escapeHTMLAttr(entry.fileMD5) + '">' + escapeHTML(entry.fileMD5.substring(0, 12)) + '...</span>' : '—') + '</td>' +
        '<td><span class="age ' + age.class + '">' + age.text + '</span></td>' +
        '<td>' + statusBadge + '</td>' +
        '<td>' + (hasError ? '<div class="error-text" title="' + escapeHTMLAttr(entry.lastError) + '">' + escapeHTML(entry.lastError) + '</div>' : '—') + '</td>';

    // Check for changes and animate updated cells
    const newValues = {
        rowCount: entry.rowCount ? entry.rowCount.toLocaleString() : '—',
        uncompressed: formatBytes(entry.uncompressedSize),
        compressed: formatBytes(entry.fileSize),
        status: row.querySelector('td:nth-child(8) .status-badge')?.textContent
    };

    // Animate changed cells
    setTimeout(() => {
        if (oldValues.rowCount !== newValues.rowCount && oldValues.rowCount) {
            animateCell(row.querySelector('td:nth-child(2)'));
        }
        if (oldValues.uncompressed !== newValues.uncompressed && oldValues.uncompressed) {
            animateCell(row.querySelector('td:nth-child(3)'));
        }
        if (oldValues.compressed !== newValues.compressed && oldValues.compressed) {
            animateCell(row.querySelector('td:nth-child(4)'));
        }
        if (oldValues.status !== newValues.status && oldValues.status) {
            animateCell(row.querySelector('td:nth-child(8)'));
        }
    }, 10);
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
                row.style.background = '#fffacd';
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

// Clean up on page unload
window.addEventListener('beforeunload', () => {
    if (ws) {
        ws.close();
    }
    if (wsReconnectInterval) {
        clearInterval(wsReconnectInterval);
    }
});
