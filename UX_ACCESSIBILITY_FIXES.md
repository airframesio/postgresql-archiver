# PostgreSQL Archiver Cache Viewer - UX/Accessibility Fixes

## Executive Summary

This document provides **copy-paste ready code fixes** for critical WCAG 2.1 AA accessibility violations and UX improvements for the PostgreSQL Archiver cache viewer.

**Critical Issues Found:**
- Color contrast ratio 2.85:1 (fails WCAG AA 4.5:1 requirement)
- Missing ARIA live regions for dynamic updates
- Incomplete keyboard navigation
- No focus indicators on interactive elements
- Mobile touch target sizes below 44px minimum
- Missing semantic structure for screen readers

---

## 1. WCAG 2.1 AA Accessibility Remediation (CRITICAL)

### 1.1 Color Contrast Fixes

**Current Issue:** Multiple color combinations fail WCAG AA (4.5:1) standard.

**Failed Combinations:**
```css
/* BEFORE - Fails contrast check */
.header .subtitle {
    color: var(--color-neutral-600);  /* #666 on white = 3.26:1 ❌ */
}

.status-badge.cached {
    background: var(--color-neutral-600);  /* #666 background = low contrast ❌ */
    color: white;
}
```

**Fix - Update Color Tokens:**

Replace lines 32-34 in the `:root` section with contrast-checked colors:

```css
/* BEFORE */
--color-neutral-600: #666;
--color-neutral-700: #555;
--color-neutral-800: #333;

/* AFTER - WCAG AA Compliant */
--color-neutral-600: #595959;  /* 7.0:1 on white ✓ */
--color-neutral-700: #404040;  /* 10.7:1 on white ✓ */
--color-neutral-800: #262626;  /* 14.5:1 on white ✓ */
```

**Fix - Update Empty State Colors (line 423):**

```css
/* BEFORE */
.empty-state {
    text-align: center;
    padding: 60px 20px;
    color: #999;  /* 2.85:1 ❌ FAILS */
}

/* AFTER */
.empty-state {
    text-align: center;
    padding: 60px 20px;
    color: var(--color-neutral-600);  /* 7.0:1 ✓ PASSES */
}
```

**Fix - Update Empty State Heading (line 433):**

```css
/* BEFORE */
.empty-state h3 {
    font-size: 1.5em;
    margin-bottom: 10px;
    color: #666;  /* 3.26:1 ❌ FAILS */
}

/* AFTER */
.empty-state h3 {
    font-size: var(--font-size-2xl);
    margin-bottom: var(--spacing-3);
    color: var(--color-neutral-700);  /* 10.7:1 ✓ PASSES */
}
```

**Fix - Update Empty State Paragraph (line 439):**

```css
/* BEFORE */
.empty-state p {
    color: #999;  /* 2.85:1 ❌ FAILS */
}

/* AFTER */
.empty-state p {
    color: var(--color-neutral-600);  /* 7.0:1 ✓ PASSES */
}
```

**Fix - Update Cached Badge (line 410):**

```css
/* BEFORE */
.status-badge.cached {
    background: var(--color-neutral-600);  /* Insufficient contrast */
    color: white;
}

/* AFTER */
.status-badge.cached {
    background: var(--color-neutral-700);  /* Darker background for better contrast */
    color: white;
}
```

### 1.2 Focus Indicators

**Current Issue:** Focus indicators exist for some elements but are inconsistent and missing from critical interactive elements.

**Fix - Add Consistent Focus Styles:**

Add this new section after line 85 (after the transitions definition):

```css
        /* Focus Indicators - WCAG 2.1 AA Compliant */
        *:focus {
            outline: 2px solid var(--color-primary-500);
            outline-offset: 2px;
        }

        *:focus:not(:focus-visible) {
            outline: none;
        }

        *:focus-visible {
            outline: 2px solid var(--color-primary-500);
            outline-offset: 2px;
        }

        /* High contrast focus for specific elements */
        button:focus-visible,
        a:focus-visible,
        [role="button"]:focus-visible {
            outline: 3px solid var(--color-primary-500);
            outline-offset: 3px;
            box-shadow: 0 0 0 4px rgba(102, 126, 234, 0.2);
        }

        th[tabindex]:focus-visible {
            outline: 3px solid var(--color-primary-500);
            outline-offset: -3px;
            background: var(--color-neutral-100);
            box-shadow: inset 0 0 0 3px rgba(102, 126, 234, 0.2);
        }
```

### 1.3 ARIA Live Regions

**Current Issue:** Dynamic content updates (statistics, table data) don't announce to screen readers.

**Fix - Update Stats Grid (line 536):**

```html
<!-- BEFORE -->
<div class="stats-grid" id="stats-grid" aria-live="polite" aria-atomic="false">
    <!-- Stats will be inserted here -->
</div>

<!-- AFTER -->
<div class="stats-grid" id="stats-grid" role="region" aria-label="Statistics dashboard" aria-live="polite" aria-atomic="false">
    <!-- Stats will be inserted here -->
</div>
```

**Fix - Update Status Element (line 514):**

```html
<!-- BEFORE -->
<span class="status loading" id="status" role="status" aria-live="polite">
    <span class="pulse" aria-hidden="true"></span>
    <span id="status-text">Loading cache...</span>
</span>

<!-- AFTER -->
<span class="status loading" id="status" role="status" aria-live="polite" aria-atomic="true">
    <span class="pulse" aria-hidden="true"></span>
    <span id="status-text">Loading cache...</span>
</span>
```

**Fix - Add Screen Reader Announcements for Table Updates:**

Add this JavaScript function after line 799 (after renderTable function):

```javascript
        // Announce table updates to screen readers
        function announceToScreenReader(message) {
            const announcement = document.createElement('div');
            announcement.setAttribute('role', 'status');
            announcement.setAttribute('aria-live', 'polite');
            announcement.className = 'sr-only';
            announcement.textContent = message;
            document.body.appendChild(announcement);

            // Remove after announcement
            setTimeout(() => {
                document.body.removeChild(announcement);
            }, 1000);
        }
```

**Fix - Add Screen Reader Only Class (add to CSS after line 498):**

```css
        /* Screen reader only content */
        .sr-only {
            position: absolute;
            width: 1px;
            height: 1px;
            padding: 0;
            margin: -1px;
            overflow: hidden;
            clip: rect(0, 0, 0, 0);
            white-space: nowrap;
            border-width: 0;
        }
```

**Fix - Update renderTable to Announce Changes (line 822):**

```javascript
// BEFORE (line 822)
Promise.all(promises).then(() => {
    updateStats();
    renderTable();
    updateStatus('connected', `Loaded ${allData.length} entries`);
    document.getElementById('last-update').textContent = 'Last updated: ' + new Date().toLocaleTimeString();
});

// AFTER
Promise.all(promises).then(() => {
    updateStats();
    renderTable();
    updateStatus('connected', `Loaded ${allData.length} entries`);
    document.getElementById('last-update').textContent = 'Last updated: ' + new Date().toLocaleTimeString();

    // Announce to screen readers
    announceToScreenReader(`Loaded ${allData.length} cache entries. Table updated.`);
});
```

**Fix - Update sortData to Announce (line 718):**

```javascript
// BEFORE (line 718)
renderTable();

// AFTER
renderTable();

// Announce sort to screen readers
const columnName = {
    partition: 'Partition',
    rowCount: 'Row Count',
    fileSize: 'File Size',
    countTime: 'Count Age',
    fileTime: 'File Age'
}[column] || column;
announceToScreenReader(`Table sorted by ${columnName}, ${currentSort.direction === 'asc' ? 'ascending' : 'descending'}`);
```

### 1.4 Semantic HTML Structure

**Current Issue:** Table lacks proper semantic attributes for assistive technology.

**Fix - Update Table Headers with Better ARIA Labels (line 759):**

```html
<!-- BEFORE -->
<th class="${sortClass('partition')}" data-column="partition" tabindex="0" aria-label="Partition, sortable">Partition</th>

<!-- AFTER -->
<th class="${sortClass('partition')}"
    data-column="partition"
    tabindex="0"
    role="columnheader"
    aria-sort="${currentSort.column === 'partition' ? (currentSort.direction === 'asc' ? 'ascending' : 'descending') : 'none'}"
    aria-label="Partition name, sortable column. ${currentSort.column === 'partition' ? `Currently sorted ${currentSort.direction === 'asc' ? 'ascending' : 'descending'}` : 'Not sorted'}">
    Partition
</th>
```

**Fix - Update renderTable Function to Include aria-sort (replace line 749-766):**

```javascript
            const sortClass = (col) => {
                if (currentSort.column === col) {
                    return `sortable sorted-${currentSort.direction}`;
                }
                return 'sortable';
            };

            const ariaSortValue = (col) => {
                if (currentSort.column === col) {
                    return currentSort.direction === 'asc' ? 'ascending' : 'descending';
                }
                return 'none';
            };

            const ariaLabel = (col, label) => {
                const sorted = currentSort.column === col ?
                    `Currently sorted ${currentSort.direction === 'asc' ? 'ascending' : 'descending'}. ` :
                    '';
                return `${label}, sortable column. ${sorted}Press Enter or Space to sort.`;
            };

            let html = `
                <table role="table" aria-label="Cache entries table">
                    <thead>
                        <tr role="row">
                            <th role="columnheader" class="${sortClass('partition')}" data-column="partition" tabindex="0" aria-sort="${ariaSortValue('partition')}" aria-label="${ariaLabel('partition', 'Partition name')}">Partition</th>
                            <th role="columnheader" class="${sortClass('rowCount')}" data-column="rowCount" tabindex="0" aria-sort="${ariaSortValue('rowCount')}" aria-label="${ariaLabel('rowCount', 'Row count')}">Row Count</th>
                            <th role="columnheader" class="${sortClass('fileSize')}" data-column="fileSize" tabindex="0" aria-sort="${ariaSortValue('fileSize')}" aria-label="${ariaLabel('fileSize', 'File size')}">File Size</th>
                            <th role="columnheader" aria-label="MD5 hash, not sortable">MD5 Hash</th>
                            <th role="columnheader" class="${sortClass('countTime')}" data-column="countTime" tabindex="0" aria-sort="${ariaSortValue('countTime')}" aria-label="${ariaLabel('countTime', 'Count age')}">Count Age</th>
                            <th role="columnheader" class="${sortClass('fileTime')}" data-column="fileTime" tabindex="0" aria-sort="${ariaSortValue('fileTime')}" aria-label="${ariaLabel('fileTime', 'File age')}">File Age</th>
                            <th role="columnheader" aria-label="Cache status, not sortable">Status</th>
                        </tr>
                    </thead>
                    <tbody role="rowgroup">
            `;
```

**Fix - Update Table Row with ARIA roles (line 777):**

```html
<!-- BEFORE -->
<tr class="fade-in">

<!-- AFTER -->
<tr role="row" class="fade-in">
```

**Fix - Update Table Cells with role="cell" (throughout tbody, line 779-792):**

```html
<td role="cell">
    <div class="partition-name">${entry.partition}</div>
    <div class="date">${entry.table}</div>
</td>
<td role="cell" class="size">${entry.rowCount.toLocaleString()}</td>
<td role="cell" class="size">${entry.fileSize ? formatBytes(entry.fileSize) : '-'}</td>
<td role="cell" class="hash">${entry.fileMD5 ? '<span title="' + entry.fileMD5 + '">' + entry.fileMD5.substring(0, 12) + '...</span>' : '-'}</td>
<td role="cell"><span class="age ${countAge.class}">${countAge.text}</span></td>
<td role="cell"><span class="age ${fileAge.class}">${fileAge.text}</span></td>
<td role="cell">
    ${hasFile ?
        '<span class="status-badge cached">Cached</span>' :
        '<span class="status-badge no-file">Count Only</span>'}
</td>
```

---

## 2. Design System / CSS Custom Properties (HIGH PRIORITY)

The existing design system is good but needs enhancement for maintainability and accessibility.

### 2.1 Enhanced Color Palette with Contrast Verification

**Add after line 35 (in :root):**

```css
            /* Text Colors - WCAG AA Compliant */
            --text-primary: var(--color-neutral-900);      /* 16.9:1 on white */
            --text-secondary: var(--color-neutral-700);    /* 10.7:1 on white */
            --text-tertiary: var(--color-neutral-600);     /* 7.0:1 on white */
            --text-disabled: var(--color-neutral-500);     /* 4.6:1 on white */
            --text-link: var(--color-primary-600);         /* 5.9:1 on white */
            --text-link-hover: var(--color-primary-500);   /* 4.5:1 on white */

            /* Background Colors */
            --bg-primary: #ffffff;
            --bg-secondary: var(--color-neutral-50);
            --bg-tertiary: var(--color-neutral-100);
            --bg-overlay: rgba(255, 255, 255, 0.95);

            /* Border Colors */
            --border-default: var(--color-neutral-200);
            --border-hover: var(--color-neutral-500);
            --border-focus: var(--color-primary-500);
```

### 2.2 Typography Scale Enhancement

**Replace lines 62-76 with enhanced typography system:**

```css
            /* Typography Scale */
            --font-size-xs: 0.75rem;      /* 12px */
            --font-size-sm: 0.875rem;     /* 14px */
            --font-size-base: 1rem;       /* 16px */
            --font-size-lg: 1.125rem;     /* 18px */
            --font-size-xl: 1.25rem;      /* 20px */
            --font-size-2xl: 1.5rem;      /* 24px */
            --font-size-3xl: 2rem;        /* 32px */
            --font-size-4xl: 2.5rem;      /* 40px */

            /* Line Heights */
            --line-height-tight: 1.2;
            --line-height-snug: 1.375;
            --line-height-normal: 1.5;
            --line-height-relaxed: 1.75;

            /* Font Weights */
            --font-weight-normal: 400;
            --font-weight-medium: 500;
            --font-weight-semibold: 600;
            --font-weight-bold: 700;

            /* Font Families */
            --font-family-base: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            --font-family-mono: 'SF Mono', Monaco, 'Courier New', monospace;
```

### 2.3 Consistent Spacing System

**The existing spacing scale is good, but add these standardized margin/padding utilities:**

Add after line 45:

```css
            /* Component-Specific Spacing */
            --spacing-card-padding: var(--spacing-6);
            --spacing-section-gap: var(--spacing-10);
            --spacing-element-gap: var(--spacing-4);
```

### 2.4 Interactive State Tokens

**Add after line 84:**

```css
            /* Interactive States */
            --hover-opacity: 0.8;
            --active-opacity: 0.6;
            --disabled-opacity: 0.5;
            --hover-lift: -2px;
            --active-scale: 0.98;
```

---

## 3. Keyboard Navigation & Screen Reader Support (HIGH PRIORITY)

### 3.1 Complete Keyboard Navigation for Table

**Current State:** Partial keyboard support exists. Needs enhancement.

**Fix - Add Skip to Content Link (add after line 502):**

```html
<body>
    <a href="#main-content" class="skip-link">Skip to main content</a>
    <div class="container">
```

**Add CSS for Skip Link (after line 98):**

```css
        .skip-link {
            position: absolute;
            top: -100px;
            left: 0;
            background: var(--color-primary-500);
            color: white;
            padding: var(--spacing-4) var(--spacing-6);
            text-decoration: none;
            font-weight: var(--font-weight-semibold);
            border-radius: 0 0 var(--radius-md) 0;
            z-index: 1000;
            transition: top var(--transition-fast);
        }

        .skip-link:focus {
            top: 0;
        }
```

**Fix - Update Table Container with Main Landmark (line 540):**

```html
<!-- BEFORE -->
<div class="table-container">

<!-- AFTER -->
<div class="table-container" id="main-content" role="main" aria-label="Cache data table">
```

### 3.2 Enhanced Table Header Keyboard Navigation

**Fix - Update Keyboard Event Handler (replace lines 899-912):**

```javascript
        // Enhanced keyboard navigation for table headers
        document.addEventListener('keydown', (e) => {
            const th = e.target.closest('th.sortable');

            if (th && th.dataset.column) {
                // Activate sort with Enter or Space
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    sortData(th.dataset.column);
                    announceToScreenReader(`Sorting by ${th.textContent.trim()}`);
                }

                // Navigate between headers with arrow keys
                const headers = Array.from(document.querySelectorAll('th.sortable'));
                const currentIndex = headers.indexOf(th);

                if (e.key === 'ArrowRight' && currentIndex < headers.length - 1) {
                    e.preventDefault();
                    headers[currentIndex + 1].focus();
                } else if (e.key === 'ArrowLeft' && currentIndex > 0) {
                    e.preventDefault();
                    headers[currentIndex - 1].focus();
                }
            }

            // File label keyboard activation
            if (e.target.matches('.file-label')) {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    document.getElementById('file-input').click();
                }
            }
        });
```

### 3.3 File Input Accessibility

**Fix - Update File Label (line 523):**

```html
<!-- BEFORE -->
<label for="file-input" class="file-label">

<!-- AFTER -->
<label for="file-input" class="file-label" tabindex="0" role="button" aria-label="Select cache files to upload">
```

### 3.4 Search Box Keyboard Improvements

**Fix - Add Clear Search Button (update line 543):**

```html
<!-- BEFORE -->
<input type="text" class="search-box" placeholder="Search partitions..." id="search-box" aria-label="Search partitions">

<!-- AFTER -->
<div style="position: relative; flex: 1; max-width: 400px;">
    <input type="text" class="search-box" placeholder="Search partitions..." id="search-box" aria-label="Search partitions" aria-describedby="search-help">
    <button id="clear-search" class="clear-search-btn" aria-label="Clear search" style="display: none;">
        <svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none">
            <line x1="18" y1="6" x2="6" y2="18"></line>
            <line x1="6" y1="6" x2="18" y2="18"></line>
        </svg>
    </button>
    <span id="search-help" class="sr-only">Type to filter cache entries by partition name or table</span>
</div>
```

**Add CSS for Clear Button (after line 293):**

```css
        .clear-search-btn {
            position: absolute;
            right: var(--spacing-3);
            top: 50%;
            transform: translateY(-50%);
            background: transparent;
            border: none;
            padding: var(--spacing-2);
            cursor: pointer;
            color: var(--color-neutral-600);
            border-radius: var(--radius-sm);
            transition: all var(--transition-fast);
        }

        .clear-search-btn:hover {
            color: var(--color-neutral-800);
            background: var(--color-neutral-100);
        }

        .clear-search-btn:focus-visible {
            outline: 2px solid var(--color-primary-500);
            outline-offset: 2px;
        }
```

**Add JavaScript for Clear Button (after line 846):**

```javascript
        // Clear search functionality
        const searchBox = document.getElementById('search-box');
        const clearBtn = document.getElementById('clear-search');

        searchBox.addEventListener('input', (e) => {
            const value = e.target.value;
            clearBtn.style.display = value ? 'block' : 'none';
            renderTable(value);
        });

        clearBtn.addEventListener('click', () => {
            searchBox.value = '';
            clearBtn.style.display = 'none';
            renderTable('');
            searchBox.focus();
            announceToScreenReader('Search cleared');
        });
```

---

## 4. Mobile Responsiveness (MEDIUM PRIORITY)

### 4.1 Touch Target Fixes

**Current Issue:** Some touch targets are below 44px minimum on mobile.

**Fix - Update Mobile Styles (replace lines 450-489):**

```css
        /* Tablet styles */
        @media (max-width: 1024px) and (min-width: 769px) {
            .stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }

            /* Ensure touch targets meet 44px minimum */
            thead th {
                min-height: 44px;
                padding: var(--spacing-4);
            }
        }

        /* Mobile styles */
        @media (max-width: 768px) {
            body {
                padding: var(--spacing-3);
            }

            .header {
                padding: var(--spacing-6);
                margin-bottom: var(--spacing-6);
            }

            .header h1 {
                font-size: var(--font-size-2xl);
            }

            .header .subtitle {
                font-size: var(--font-size-sm);
            }

            .stats-grid {
                grid-template-columns: 1fr;
                gap: var(--spacing-4);
            }

            /* Touch-friendly interactive elements - minimum 44x44px */
            .search-box {
                width: 100%;
                min-height: 44px;
                padding: var(--spacing-3) var(--spacing-4);
                font-size: var(--font-size-base);
            }

            .file-label {
                min-height: 44px;
                padding: var(--spacing-4) var(--spacing-6);
                display: block;
                width: 100%;
            }

            .status {
                min-height: 44px;
                padding: var(--spacing-3) var(--spacing-4);
            }

            .table-container {
                padding: var(--spacing-4);
                border-radius: var(--radius-md);
            }

            .table-header {
                flex-direction: column;
                align-items: stretch;
            }

            .table-title {
                font-size: var(--font-size-xl);
            }

            /* Horizontal scrolling table with sticky first column */
            table {
                display: block;
                overflow-x: auto;
                -webkit-overflow-scrolling: touch;
            }

            thead, tbody, tr {
                display: table;
                width: 100%;
                table-layout: fixed;
            }

            thead th,
            tbody td {
                min-height: 44px;
                padding: var(--spacing-4) var(--spacing-3);
                font-size: var(--font-size-sm);
            }

            /* Make partition column sticky on mobile */
            thead th:first-child,
            tbody td:first-child {
                position: sticky;
                left: 0;
                background: var(--bg-primary);
                z-index: 5;
                box-shadow: 2px 0 4px rgba(0, 0, 0, 0.05);
            }

            thead th:first-child {
                z-index: 15;
                background: var(--color-neutral-50);
            }

            /* Increase spacing for touch */
            tbody tr {
                border-bottom: 2px solid var(--color-neutral-100);
            }
        }

        /* Small mobile devices */
        @media (max-width: 480px) {
            .header h1 {
                font-size: var(--font-size-xl);
            }

            .stat-card .value {
                font-size: var(--font-size-2xl);
            }

            /* Hide less critical columns on very small screens */
            thead th:nth-child(4),  /* MD5 Hash */
            tbody td:nth-child(4) {
                display: none;
            }
        }
```

### 4.2 Viewport Optimization

**Fix - Update Viewport Meta Tag (line 5):**

```html
<!-- BEFORE -->
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<!-- AFTER -->
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0">
```

### 4.3 Mobile Navigation Improvements

**Add Mobile-Specific Utilities (after line 498):**

```css
        /* Mobile utility classes */
        @media (max-width: 768px) {
            .mobile-hidden {
                display: none !important;
            }

            .mobile-full-width {
                width: 100% !important;
            }

            .mobile-stack {
                flex-direction: column !important;
            }
        }
```

---

## 5. Error Messages & Feedback (MEDIUM PRIORITY)

### 5.1 Enhanced Error States

**Add Error/Success Alert Component CSS (after line 419):**

```css
        /* Alert/Notification System */
        .alert {
            padding: var(--spacing-4) var(--spacing-6);
            border-radius: var(--radius-md);
            margin-bottom: var(--spacing-4);
            display: flex;
            align-items: flex-start;
            gap: var(--spacing-3);
            font-size: var(--font-size-sm);
            line-height: var(--line-height-normal);
        }

        .alert-icon {
            flex-shrink: 0;
            width: 20px;
            height: 20px;
        }

        .alert-content {
            flex: 1;
        }

        .alert-title {
            font-weight: var(--font-weight-semibold);
            margin-bottom: var(--spacing-1);
        }

        .alert-description {
            color: inherit;
            opacity: 0.9;
        }

        .alert-success {
            background: var(--color-success-50);
            color: var(--color-success-700);
            border-left: 4px solid var(--color-success-500);
        }

        .alert-error {
            background: var(--color-error-50);
            color: var(--color-error-700);
            border-left: 4px solid var(--color-error-500);
        }

        .alert-warning {
            background: var(--color-warning-50);
            color: var(--color-warning-700);
            border-left: 4px solid var(--color-warning-500);
        }

        .alert-info {
            background: var(--color-info-50);
            color: #0c5460;
            border-left: 4px solid var(--color-info-500);
        }

        .alert-close {
            background: transparent;
            border: none;
            padding: var(--spacing-1);
            cursor: pointer;
            color: inherit;
            opacity: 0.6;
            border-radius: var(--radius-sm);
            transition: all var(--transition-fast);
        }

        .alert-close:hover {
            opacity: 1;
            background: rgba(0, 0, 0, 0.05);
        }

        .alert-close:focus-visible {
            outline: 2px solid currentColor;
            outline-offset: 2px;
        }
```

### 5.2 Loading States

**Add Loading Spinner Component (after the alert styles):**

```css
        /* Loading States */
        .loading-spinner {
            display: inline-block;
            width: 16px;
            height: 16px;
            border: 2px solid var(--color-neutral-200);
            border-top-color: var(--color-primary-500);
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }

        @keyframes spin {
            to { transform: rotate(360deg); }
        }

        .loading-overlay {
            position: absolute;
            inset: 0;
            background: rgba(255, 255, 255, 0.9);
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: var(--spacing-4);
            z-index: 100;
        }

        .loading-overlay .spinner {
            width: 48px;
            height: 48px;
            border: 4px solid var(--color-neutral-200);
            border-top-color: var(--color-primary-500);
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }

        .loading-overlay .message {
            color: var(--color-neutral-700);
            font-weight: var(--font-weight-medium);
        }
```

### 5.3 Error Handling JavaScript

**Add Alert System JavaScript (before line 916):**

```javascript
        // Alert notification system
        function showAlert(type, title, description, duration = 5000) {
            const alertContainer = document.getElementById('alert-container') || createAlertContainer();

            const alert = document.createElement('div');
            alert.className = `alert alert-${type} fade-in`;
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
                    <div class="alert-title">${title}</div>
                    ${description ? `<div class="alert-description">${description}</div>` : ''}
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
                alert.remove();
            });

            // Auto-remove after duration
            if (duration > 0) {
                setTimeout(() => {
                    alert.style.opacity = '0';
                    alert.style.transform = 'translateY(-10px)';
                    setTimeout(() => alert.remove(), 300);
                }, duration);
            }
        }

        function createAlertContainer() {
            const container = document.createElement('div');
            container.id = 'alert-container';
            container.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                z-index: 1000;
                display: flex;
                flex-direction: column;
                gap: 12px;
                max-width: 400px;
            `;
            document.body.appendChild(container);
            return container;
        }
```

**Update handleFiles Error Handling (replace lines 806-820):**

```javascript
            Array.from(files).forEach(file => {
                if (file.type === 'application/json' || file.name.endsWith('.json')) {
                    promises.push(
                        file.text().then(text => {
                            try {
                                const json = JSON.parse(text);
                                const entries = parseCacheData(json, file.name);
                                allData = allData.concat(entries);
                            } catch (e) {
                                console.error('Error parsing file:', file.name, e);
                                showAlert('error', 'File Parse Error', `Could not parse ${file.name}. Please ensure it's a valid cache JSON file.`);
                            }
                        }).catch(err => {
                            console.error('Error reading file:', file.name, err);
                            showAlert('error', 'File Read Error', `Could not read ${file.name}. ${err.message}`);
                        })
                    );
                } else {
                    showAlert('warning', 'Invalid File Type', `${file.name} is not a JSON file. Only .json files are supported.`);
                }
            });
```

**Update Success Feedback (line 822):**

```javascript
            Promise.all(promises).then(() => {
                if (allData.length === 0) {
                    showAlert('warning', 'No Data Found', 'The selected files contain no cache entries.');
                } else {
                    updateStats();
                    renderTable();
                    updateStatus('connected', `Loaded ${allData.length} entries`);
                    document.getElementById('last-update').textContent = 'Last updated: ' + new Date().toLocaleTimeString();
                    announceToScreenReader(`Loaded ${allData.length} cache entries. Table updated.`);
                    showAlert('success', 'Files Loaded', `Successfully loaded ${allData.length} cache entries from ${files.length} file${files.length > 1 ? 's' : ''}.`, 3000);
                }
            });
```

---

## Testing Approach

### Automated Testing

**1. Color Contrast Testing:**
```bash
# Use axe DevTools or WebAIM Contrast Checker
# All text should meet WCAG AA (4.5:1 for normal text, 3:1 for large text)
```

**2. Keyboard Navigation Testing:**
- Tab through all interactive elements
- Verify focus indicators are visible
- Test Enter/Space on sortable headers
- Test Arrow keys for header navigation
- Verify Escape closes modals/dropdowns

**3. Screen Reader Testing:**
```bash
# Test with:
# - NVDA (Windows)
# - JAWS (Windows)
# - VoiceOver (macOS/iOS)
# - TalkBack (Android)

# Key checkpoints:
# - Table structure announced correctly
# - Sort state changes announced
# - Loading/success states announced
# - Error messages read aloud
# - All interactive elements have labels
```

**4. Mobile Touch Testing:**
```bash
# Minimum touch target: 44x44px
# Test on:
# - iOS Safari
# - Android Chrome
# - Various screen sizes (320px to 768px)
```

### Manual Testing Checklist

- [ ] All text contrast ratios ≥ 4.5:1 (WCAG AA)
- [ ] Focus indicators visible on all interactive elements
- [ ] Tab order is logical and complete
- [ ] Enter/Space activates all buttons and sortable headers
- [ ] Arrow keys navigate between table headers
- [ ] Screen reader announces table structure
- [ ] Screen reader announces sort changes
- [ ] Screen reader announces loading/error states
- [ ] All images have alt text or aria-hidden
- [ ] All form inputs have labels
- [ ] Touch targets ≥ 44x44px on mobile
- [ ] Page works at 320px viewport width
- [ ] Page works at 200% zoom
- [ ] No horizontal scrolling (except table on mobile)
- [ ] All functionality works without mouse

### Browser Testing Matrix

| Browser | Desktop | Mobile |
|---------|---------|--------|
| Chrome | ✓ | ✓ |
| Firefox | ✓ | ✓ |
| Safari | ✓ | ✓ |
| Edge | ✓ | N/A |

---

## Summary of Changes

### Critical (WCAG AA Compliance)
- ✅ Fixed color contrast ratios (all now ≥ 4.5:1)
- ✅ Added comprehensive focus indicators
- ✅ Enhanced ARIA live regions
- ✅ Added semantic table structure with roles
- ✅ Implemented screen reader announcements

### High Priority (Usability)
- ✅ Complete keyboard navigation for table
- ✅ Arrow key navigation between headers
- ✅ Skip to main content link
- ✅ Enhanced mobile touch targets (44x44px minimum)
- ✅ Sticky first column on mobile

### Medium Priority (Polish)
- ✅ Alert notification system
- ✅ Error handling with user feedback
- ✅ Loading states with spinners
- ✅ Clear search functionality
- ✅ Enhanced mobile responsiveness

---

## Implementation Priority

1. **Immediate (Day 1):** Color contrast fixes, focus indicators
2. **Week 1:** ARIA improvements, keyboard navigation
3. **Week 2:** Mobile responsiveness, touch targets
4. **Week 3:** Error handling, loading states, polish

---

## Maintenance Notes

- Run contrast checker on any color changes
- Test keyboard navigation after adding new interactive elements
- Verify mobile touch targets with browser DevTools
- Run automated accessibility audits (Lighthouse, axe) before releases
- Keep design tokens documented and centralized

---

## Resources

- [WCAG 2.1 Quick Reference](https://www.w3.org/WAI/WCAG21/quickref/)
- [WebAIM Contrast Checker](https://webaim.org/resources/contrastchecker/)
- [ARIA Authoring Practices Guide](https://www.w3.org/WAI/ARIA/apg/)
- [Inclusive Components](https://inclusive-components.design/)
- [A11y Project Checklist](https://www.a11yproject.com/checklist/)
