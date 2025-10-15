package cmd

const cacheViewerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PostgreSQL Archiver - Cache Viewer</title>
    <style>
        :root {
            /* Brand Colors */
            --color-primary-500: #667eea;
            --color-primary-600: #5568d3;
            --color-accent-500: #764ba2;

            /* Semantic Colors */
            --color-success-50: #d4edda;
            --color-success-500: #28a745;
            --color-success-700: #155724;
            --color-warning-50: #fff3cd;
            --color-warning-500: #ffc107;
            --color-warning-700: #856404;
            --color-error-50: #f8d7da;
            --color-error-500: #dc3545;
            --color-error-700: #721c24;
            --color-info-50: #cce5ff;
            --color-info-500: #17a2b8;

            /* Neutral Colors */
            --color-neutral-50: #f8f9fa;
            --color-neutral-100: #f0f0f0;
            --color-neutral-200: #e0e0e0;
            --color-neutral-500: #888;
            --color-neutral-600: #666;
            --color-neutral-700: #555;
            --color-neutral-800: #333;
            --color-neutral-900: #24292e;

            /* Spacing Scale (8px base) */
            --spacing-1: 0.25rem;
            --spacing-2: 0.5rem;
            --spacing-3: 0.75rem;
            --spacing-4: 1rem;
            --spacing-5: 1.25rem;
            --spacing-6: 1.5rem;
            --spacing-8: 2rem;
            --spacing-10: 2.5rem;

            /* Border Radius */
            --radius-sm: 0.5rem;
            --radius-md: 0.75rem;
            --radius-lg: 1rem;
            --radius-xl: 1.25rem;
            --radius-full: 9999px;

            /* Shadows */
            --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
            --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
            --shadow-xl: 0 20px 25px -5px rgba(0, 0, 0, 0.1);
            --shadow-2xl: 0 20px 60px rgba(0, 0, 0, 0.15);

            /* Typography */
            --font-size-xs: 0.75rem;
            --font-size-sm: 0.875rem;
            --font-size-base: 1rem;
            --font-size-lg: 1.125rem;
            --font-size-xl: 1.25rem;
            --font-size-2xl: 1.5rem;
            --font-size-3xl: 2rem;
            --font-size-4xl: 2.5rem;
            --line-height-tight: 1.2;
            --line-height-normal: 1.5;
            --line-height-relaxed: 1.75;
            --font-weight-normal: 400;
            --font-weight-medium: 500;
            --font-weight-semibold: 600;
            --font-weight-bold: 700;

            /* Gradients */
            --gradient-primary: linear-gradient(135deg, var(--color-primary-500) 0%, var(--color-accent-500) 100%);

            /* Transitions */
            --transition-fast: 0.15s ease;
            --transition-base: 0.3s ease;
            --transition-slow: 0.5s ease;
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, var(--color-primary-500) 0%, var(--color-accent-500) 100%);
            min-height: 100vh;
            padding: var(--spacing-5);
            color: var(--color-neutral-800);
        }
        
        .container {
            max-width: 1600px;
            margin: 0 auto;
        }
        
        .header {
            background: rgba(255, 255, 255, 0.95);
            border-radius: var(--radius-xl);
            padding: var(--spacing-10);
            margin-bottom: var(--spacing-10);
            box-shadow: var(--shadow-2xl);
            backdrop-filter: blur(10px);
            position: relative;
        }

        .header h1 {
            font-size: var(--font-size-4xl);
            background: var(--gradient-primary);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: var(--spacing-3);
        }

        .header .subtitle {
            color: var(--color-neutral-600);
            font-size: var(--font-size-lg);
            display: flex;
            align-items: center;
            gap: var(--spacing-6);
            flex-wrap: wrap;
        }
        
        .github-link {
            position: absolute;
            top: var(--spacing-10);
            right: var(--spacing-10);
            display: flex;
            align-items: center;
            gap: var(--spacing-2);
            padding: var(--spacing-3) var(--spacing-6);
            background: var(--color-neutral-900);
            color: white;
            text-decoration: none;
            border-radius: var(--radius-sm);
            font-weight: var(--font-weight-medium);
            transition: all var(--transition-base);
        }
        
        .github-link:hover {
            background: #1a1e22;
            transform: translateY(-2px);
            box-shadow: var(--shadow-lg);
        }
        
        .github-link svg {
            width: 20px;
            height: 20px;
            fill: white;
        }
        
        .status {
            display: inline-flex;
            align-items: center;
            gap: var(--spacing-2);
            padding: var(--spacing-2) var(--spacing-4);
            background: var(--color-neutral-100);
            border-radius: var(--radius-full);
            font-size: var(--font-size-sm);
        }

        .status.connected {
            background: var(--color-success-50);
            color: var(--color-success-700);
        }

        .status.disconnected {
            background: var(--color-error-50);
            color: var(--color-error-700);
        }
        
        .pulse {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: currentColor;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }
        
        .refresh-control {
            display: inline-flex;
            align-items: center;
            gap: 10px;
        }
        
        .refresh-select {
            padding: 6px 12px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            background: white;
            cursor: pointer;
            font-size: 0.9em;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: var(--spacing-6);
            margin-bottom: var(--spacing-10);
        }

        .stat-card {
            background: rgba(255, 255, 255, 0.95);
            border-radius: var(--radius-md);
            padding: var(--spacing-6);
            box-shadow: var(--shadow-lg);
            backdrop-filter: blur(10px);
            transition: transform var(--transition-base);
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
        }
        
        .stat-card .label {
            color: var(--color-neutral-700);
            font-size: var(--font-size-sm);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: var(--spacing-2);
        }

        .stat-card .value {
            font-size: var(--font-size-3xl);
            font-weight: var(--font-weight-bold);
            color: var(--color-neutral-800);
        }

        .stat-card .detail {
            font-size: var(--font-size-sm);
            color: var(--color-neutral-600);
            margin-top: var(--spacing-1);
        }
        
        .table-container {
            background: rgba(255, 255, 255, 0.98);
            border-radius: 16px;
            padding: 30px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.15);
            backdrop-filter: blur(10px);
            overflow: hidden;
        }
        
        .table-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 15px;
        }
        
        .table-title {
            font-size: 1.5em;
            font-weight: bold;
            color: #333;
        }
        
        .controls {
            display: flex;
            gap: 15px;
            align-items: center;
        }
        
        .search-box {
            padding: 10px 20px;
            border: 2px solid #e0e0e0;
            border-radius: 25px;
            font-size: 1em;
            min-width: 300px;
            transition: all 0.3s ease;
        }
        
        .search-box:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        
        .filter-select {
            padding: 10px 20px;
            border: 2px solid #e0e0e0;
            border-radius: 25px;
            font-size: 1em;
            background: white;
            cursor: pointer;
            transition: all 0.3s ease;
            appearance: none;
            background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 12 12"><path fill="%23666" d="M6 9L1 4h10z"/></svg>');
            background-repeat: no-repeat;
            background-position: right 15px center;
            padding-right: 40px;
        }
        
        .filter-select:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        
        .filter-select:hover {
            border-color: #667eea;
        }

        .refresh-select:focus {
            outline: 2px solid #667eea;
            outline-offset: 2px;
        }

        .github-link:focus {
            outline: 2px solid #667eea;
            outline-offset: 2px;
        }
        
        .table-wrapper {
            overflow-x: auto;
            max-height: 600px;
            overflow-y: auto;
        }
        
        table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
        }
        
        thead th {
            background: #f8f9fa;
            color: #666;
            font-weight: 600;
            text-align: left;
            padding: 15px 10px;
            font-size: 0.85em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border-bottom: 2px solid #e0e0e0;
            cursor: pointer;
            user-select: none;
            position: sticky;
            top: 0;
            z-index: 10;
            white-space: nowrap;
        }
        
        thead th:hover {
            background: #f0f1f3;
        }

        thead th:focus {
            outline: 2px solid #667eea;
            outline-offset: -2px;
            background: #f0f1f3;
        }
        
        thead th.sortable:after {
            content: ' ↕';
            opacity: 0.3;
        }
        
        thead th.sorted-asc:after {
            content: ' ↑';
            opacity: 1;
        }
        
        thead th.sorted-desc:after {
            content: ' ↓';
            opacity: 1;
        }
        
        tbody tr {
            transition: background 0.2s ease;
            border-bottom: 1px solid #f0f0f0;
        }
        
        tbody tr:hover {
            background: #f8f9fa;
        }
        
        tbody td {
            padding: 12px 10px;
            color: #333;
            font-size: 0.9em;
        }
        
        .partition-name {
            font-weight: 600;
            color: #667eea;
        }
        
        .table-name {
            color: #666;
            font-size: 0.9em;
        }
        
        .size {
            font-family: 'SF Mono', Monaco, 'Courier New', monospace;
            color: #333;
            white-space: nowrap;
        }
        
        .ratio {
            color: #666;
            font-size: 0.85em;
        }
        
        .hash {
            font-family: 'SF Mono', Monaco, 'Courier New', monospace;
            font-size: 0.85em;
            color: #555;
            word-break: break-all;
        }
        
        .age {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 500;
        }
        
        .age.fresh {
            background: #d4edda;
            color: #155724;
        }
        
        .age.recent {
            background: #cce5ff;
            color: #004085;
        }
        
        .age.old {
            background: #f8d7da;
            color: #721c24;
        }
        
        .status-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }
        
        .status-badge.cached {
            background: #6c757d;
            color: white;
        }
        
        .status-badge.uploaded {
            background: #28a745;
            color: white;
        }
        
        .status-badge.error {
            background: #dc3545;
            color: white;
        }
        
        .status-badge.no-file {
            background: #ffc107;
            color: #333;
        }
        
        .error-text {
            color: #dc3545;
            font-size: 0.85em;
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        
        .error-text:hover {
            white-space: normal;
            word-break: break-word;
        }
        
        @keyframes text-update {
            0% {
                color: #667eea;
                transform: scale(1.1);
            }
            50% {
                color: #667eea;
                transform: scale(1.05);
            }
            100% {
                color: inherit;
                transform: scale(1);
            }
        }
        
        .updated {
            display: inline-block;
            animation: text-update 0.6s ease-out;
        }
        
        td.updated {
            animation: text-update 0.6s ease-out;
        }
        
        .stat-card .value.updated {
            animation: text-update 0.6s ease-out;
        }
        
        .task-panel {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 16px;
            padding: 20px;
            margin-bottom: 20px;
            color: white;
            box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
        }
        
        .task-panel.idle {
            background: linear-gradient(135deg, #6c757d 0%, #495057 100%);
        }
        
        .task-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        
        .task-title {
            font-size: 1.2em;
            font-weight: bold;
        }
        
        .task-status {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .task-progress-bar {
            background: rgba(255, 255, 255, 0.2);
            border-radius: 10px;
            height: 10px;
            margin-bottom: 10px;
            overflow: hidden;
        }
        
        .task-progress-fill {
            background: rgba(255, 255, 255, 0.9);
            height: 100%;
            transition: width 0.3s ease;
            border-radius: 10px;
        }
        
        .task-details {
            display: flex;
            justify-content: space-between;
            font-size: 0.9em;
            opacity: 0.9;
        }
        
        .task-details a {
            color: white;
            text-decoration: underline;
            opacity: 1;
            transition: opacity 0.2s ease;
        }
        
        .task-details a:hover {
            opacity: 0.8;
            text-shadow: 0 0 10px rgba(255, 255, 255, 0.5);
        }
        
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #999;
        }

        .empty-state h3 {
            font-size: 1.5em;
            margin-bottom: 10px;
            color: #666;
        }

        .clear-search-btn {
            margin-top: 15px;
            padding: 10px 20px;
            background: var(--color-primary-500);
            color: white;
            border: none;
            border-radius: var(--radius-sm);
            cursor: pointer;
            font-size: 1em;
            transition: all 0.3s ease;
        }

        .clear-search-btn:hover {
            background: var(--color-primary-600);
            transform: translateY(-2px);
        }

        .clear-search-btn:focus {
            outline: 2px solid var(--color-primary-500);
            outline-offset: 2px;
        }
        
        /* Tablet styles */
        @media (max-width: 1024px) and (min-width: 769px) {
            .stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }

        /* Mobile styles */
        @media (max-width: 768px) {
            .header h1 {
                font-size: 1.8em;
            }

            .github-link {
                position: static;
                margin-top: 15px;
                min-height: 44px;
            }

            .stats-grid {
                grid-template-columns: 1fr;
            }

            .search-box {
                min-width: 100%;
                min-height: 44px;
                padding: 12px 20px;
            }

            .filter-select {
                min-height: 44px;
                padding: 12px 40px 12px 20px;
            }

            .table-container {
                padding: 15px;
            }

            thead th {
                min-height: 44px;
                padding: 15px 12px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>
                <svg style="display: inline-block; width: 1em; height: 1em; vertical-align: -0.125em;" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <rect x="3" y="3" width="7" height="7"></rect>
                    <rect x="14" y="3" width="7" height="7"></rect>
                    <rect x="14" y="14" width="7" height="7"></rect>
                    <rect x="3" y="14" width="7" height="7"></rect>
                </svg>
                PostgreSQL Archiver Cache Viewer
            </h1>
            <div class="subtitle">
                <span class="status connected" id="status" role="status" aria-live="polite">
                    <span class="pulse" aria-hidden="true"></span>
                    <span id="status-text">Connected to local cache</span>
                </span>
                <span id="last-update">Last updated: Never</span>
            </div>
            <a href="https://github.com/airframesio/postgresql-archiver" target="_blank" class="github-link">
                <svg viewBox="0 0 16 16">
                    <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>
                </svg>
                View on GitHub
            </a>
        </div>
        
        <div class="stats-grid" id="stats-grid" aria-live="polite" aria-atomic="false">
            <!-- Stats will be inserted here -->
        </div>
        
        <div class="task-panel idle" id="task-panel">
            <div class="task-header">
                <div class="task-title" id="task-title">Archiver Status</div>
                <div class="task-status">
                    <span id="task-pid"></span>
                    <span id="task-time"></span>
                </div>
            </div>
            <div class="task-progress-bar" id="task-progress-bar" style="display: none;" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0" aria-label="Archiving progress">
                <div class="task-progress-fill" id="task-progress-fill"></div>
            </div>
            <div class="task-details">
                <span id="task-status-text" aria-live="polite">No active task</span>
                <span id="task-stats" aria-live="polite"></span>
            </div>
        </div>
        
        <div class="table-container">
            <div class="table-header">
                <h2 class="table-title">Cache Entries</h2>
                <div class="controls">
                    <select class="filter-select" id="table-filter" aria-label="Filter by table">
                        <option value="">All Tables</option>
                    </select>
                    <input type="text" class="search-box" placeholder="Search partitions..." id="search-box" aria-label="Search partitions">
                </div>
            </div>
            <div class="table-wrapper">
                <table id="data-table" aria-label="Cache entries table">
                    <thead>
                        <tr>
                            <th class="sortable" data-column="partition" tabindex="0" aria-label="Partition, sortable">Partition</th>
                            <th class="sortable" data-column="rowCount" tabindex="0" aria-label="Rows, sortable">Rows</th>
                            <th class="sortable" data-column="uncompressedSize" tabindex="0" aria-label="Uncompressed size, sortable">Uncompressed</th>
                            <th class="sortable" data-column="fileSize" tabindex="0" aria-label="Compressed size, sortable">Compressed</th>
                            <th aria-label="Compression ratio">Ratio</th>
                            <th aria-label="MD5 hash">MD5</th>
                            <th class="sortable" data-column="fileTime" tabindex="0" aria-label="Age, sortable">Age</th>
                            <th aria-label="Status">Status</th>
                            <th aria-label="Error message">Error</th>
                        </tr>
                    </thead>
                    <tbody id="table-body">
                        <!-- Rows will be inserted here -->
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <script>
        let allData = [];
        let currentData = {};  // Track current data by partition key
        let currentSort = { column: 'partition', direction: 'asc' };  // Default sort by partition name
        let ws = null;
        let wsReconnectInterval = null;
        let lastTaskState = null;  // Store last known task state to avoid flashing
        
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

                        console.log(` + "`" + `Reconnect attempt ${reconnectAttempts}/${maxAttempts}` + "`" + `);
                        document.getElementById('status-text').textContent = ` + "`" + `Reconnecting (${reconnectAttempts}/${maxAttempts})...` + "`" + `;
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
                        // Show as "Table: partition - operation"
                        statusElement.innerHTML = 'Table: <a href="#" class="partition-link" data-partition="' + task.current_partition + '">' + task.current_partition + '</a> - ' + currentTaskText;
                    } else {
                        statusElement.textContent = currentTaskText;
                    }
                } else if (lastTaskState) {
                    // Use last known state if task info temporarily unavailable
                    const task = lastTaskState;
                    const currentTaskText = task.current_step || task.current_task || 'Processing...';
                    const statusElement = document.getElementById('task-status-text');
                    
                    if (task.current_partition) {
                        // Show as "Table: partition - operation"
                        statusElement.innerHTML = 'Table: <a href="#" class="partition-link" data-partition="' + task.current_partition + '">' + task.current_partition + '</a> - ' + currentTaskText;
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
            
            const avgRatio = totalUncompressed > 0 ? (totalUncompressed / totalCompressed).toFixed(1) : '—';
            
            // Store old values
            const statsGrid = document.getElementById('stats-grid');
            const oldValues = {
                partitions: statsGrid.querySelector('.stat-card:nth-child(1) .value')?.textContent,
                size: statsGrid.querySelector('.stat-card:nth-child(2) .value')?.textContent,
                ratio: statsGrid.querySelector('.stat-card:nth-child(3) .value')?.textContent,
                rows: statsGrid.querySelector('.stat-card:nth-child(4) .value')?.textContent
            };
            
            statsGrid.innerHTML = ` + "`" + `
                <div class="stat-card">
                    <div class="label">Total Partitions</div>
                    <div class="value">${totalPartitions.toLocaleString()}</div>
                    <div class="detail">${cachedFiles} cached, ${withErrors} errors</div>
                </div>
                <div class="stat-card">
                    <div class="label">Total Size</div>
                    <div class="value">${formatBytes(totalCompressed)}</div>
                    <div class="detail">Uncompressed: ${formatBytes(totalUncompressed)}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Compression</div>
                    <div class="value">${avgRatio}x</div>
                    <div class="detail">Average ratio</div>
                </div>
                <div class="stat-card">
                    <div class="label">Total Rows</div>
                    <div class="value">${totalRows.toLocaleString()}</div>
                    <div class="detail">Across all partitions</div>
                </div>
            ` + "`" + `;
            
            // Animate changed stats
            const newValues = {
                partitions: totalPartitions.toLocaleString(),
                size: formatBytes(totalCompressed),
                ratio: avgRatio + 'x',
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
        
        // Update sort indicators
        function updateSortIndicators() {
            document.querySelectorAll('thead th.sortable').forEach(th => {
                th.className = 'sortable';
                if (th.dataset.column === currentSort.column) {
                    th.className = 'sortable sorted-' + currentSort.direction;
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
                tbody.innerHTML = ` + "`" + `
                    <tr>
                        <td colspan="9" style="text-align: center; padding: 60px 20px;">
                            <div class="empty-state">
                                <h3>No Results Found</h3>
                                <p>No cache entries match your filters. Try adjusting your search or filter.</p>
                                <button class="clear-search-btn" id="clear-filters-btn">Clear Filters</button>
                            </div>
                        </td>
                    </tr>
                ` + "`" + `;

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
        
        // Create a new row
        function createRow(entry) {
            const row = document.createElement('tr');
            row.dataset.key = getRowKey(entry);
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
            
            row.innerHTML = ` + "`" + `
                <td>
                    <div class="partition-name">${entry.partition}</div>
                    <div class="table-name">${entry.table}</div>
                </td>
                <td class="size">${entry.rowCount ? entry.rowCount.toLocaleString() : '—'}</td>
                <td class="size">${formatBytes(entry.uncompressedSize)}</td>
                <td class="size">${formatBytes(entry.fileSize)}</td>
                <td class="ratio">${ratio}</td>
                <td class="hash">${entry.fileMD5 ? '<span title="' + entry.fileMD5 + '">' + entry.fileMD5.substring(0, 12) + '...</span>' : '—'}</td>
                <td><span class="age ${age.class}">${age.text}</span></td>
                <td>${statusBadge}</td>
                <td>${hasError ? '<div class="error-text" title="' + entry.lastError + '">' + entry.lastError + '</div>' : '—'}</td>
            ` + "`" + `;
            
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
        document.getElementById('search-box').addEventListener('input', updateTable);
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
        }
        
        // Clean up on page unload
        window.addEventListener('beforeunload', () => {
            if (ws) {
                ws.close();
            }
            if (wsReconnectInterval) {
                clearInterval(wsReconnectInterval);
            }
        });
    </script>
</body>
</html>`
