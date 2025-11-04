#!/bin/bash

# Automated Testing Script for UI Improvements
# Run this before manual UAT to catch basic issues

set -e

echo "========================================"
echo "PostgreSQL Archiver - Automated Testing"
echo "Branch: feature/ui-improvements"
echo "========================================"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass_count=0
fail_count=0

function test_pass() {
    echo -e "${GREEN}✓ PASS${NC}: $1"
    ((pass_count++))
}

function test_fail() {
    echo -e "${RED}✗ FAIL${NC}: $1"
    ((fail_count++))
}

function test_warn() {
    echo -e "${YELLOW}⚠ WARN${NC}: $1"
}

echo "1. Code Quality Checks"
echo "----------------------"

# Test: Go Build
echo -n "Building application... "
if go build -o postgresql-archiver 2>/dev/null; then
    test_pass "Go build successful"
else
    test_fail "Go build failed"
    exit 1
fi

# Test: Go Vet
echo -n "Running go vet... "
if go vet ./... 2>/dev/null; then
    test_pass "Go vet passed"
else
    test_fail "Go vet found issues"
fi

# Test: Go Fmt
echo -n "Checking code formatting... "
unformatted=$(gofmt -l . | grep -v vendor || true)
if [ -z "$unformatted" ]; then
    test_pass "All Go files properly formatted"
else
    test_fail "Some files need formatting: $unformatted"
fi

echo ""
echo "2. File Structure Checks"
echo "------------------------"

# Test: Web assets exist
files_to_check=(
    "cmd/web/viewer.html"
    "cmd/web/styles.css"
    "cmd/web/script.js"
    "cmd/cache_viewer_html.go"
    "docs/design-system/index.html"
)

for file in "${files_to_check[@]}"; do
    if [ -f "$file" ]; then
        test_pass "File exists: $file"
    else
        test_fail "Missing file: $file"
    fi
done

echo ""
echo "3. HTML Validation Checks"
echo "-------------------------"

# Test: HTML has DOCTYPE
if grep -q "<!DOCTYPE html>" cmd/web/viewer.html; then
    test_pass "viewer.html has DOCTYPE"
else
    test_fail "viewer.html missing DOCTYPE"
fi

# Test: HTML has lang attribute
if grep -q '<html lang="en">' cmd/web/viewer.html; then
    test_pass "viewer.html has lang attribute"
else
    test_fail "viewer.html missing lang attribute"
fi

# Test: HTML has viewport meta
if grep -q 'name="viewport"' cmd/web/viewer.html; then
    test_pass "viewer.html has viewport meta tag"
else
    test_fail "viewer.html missing viewport meta tag"
fi

# Test: Skip link present
if grep -q 'skip-link' cmd/web/viewer.html; then
    test_pass "Skip to content link present"
else
    test_fail "Skip to content link missing"
fi

# Test: Main landmark
if grep -q 'role="main"' cmd/web/viewer.html || grep -q '<main' cmd/web/viewer.html; then
    test_pass "Main landmark present"
else
    test_fail "Main landmark missing"
fi

echo ""
echo "4. CSS Validation Checks"
echo "-------------------------"

# Test: Design tokens present
if grep -q ':root {' cmd/web/styles.css; then
    test_pass "CSS custom properties (design tokens) present"
else
    test_fail "CSS custom properties missing"
fi

# Test: Focus styles present
if grep -q 'focus-visible' cmd/web/styles.css; then
    test_pass "Focus-visible styles present"
else
    test_warn "Focus-visible styles not found (may use :focus)"
fi

# Test: Responsive breakpoints
if grep -q '@media.*max-width.*768px' cmd/web/styles.css; then
    test_pass "Mobile breakpoint present"
else
    test_fail "Mobile breakpoint missing"
fi

if grep -q '@media.*1024px' cmd/web/styles.css; then
    test_pass "Tablet breakpoint present"
else
    test_warn "Tablet breakpoint not found"
fi

# Test: Alert system styles
if grep -q '\.alert' cmd/web/styles.css; then
    test_pass "Alert system styles present"
else
    test_fail "Alert system styles missing"
fi

# Test: Loading/skeleton styles
if grep -q 'skeleton' cmd/web/styles.css; then
    test_pass "Skeleton loading styles present"
else
    test_fail "Skeleton loading styles missing"
fi

echo ""
echo "5. JavaScript Validation Checks"
echo "--------------------------------"

# Test: Screen reader function
if grep -q 'announceToScreenReader' cmd/web/script.js; then
    test_pass "Screen reader announcement function present"
else
    test_fail "Screen reader announcement function missing"
fi

# Test: Alert function
if grep -q 'showAlert' cmd/web/script.js; then
    test_pass "Alert system function present"
else
    test_fail "Alert system function missing"
fi

# Test: Skeleton function
if grep -q 'showLoadingSkeleton' cmd/web/script.js; then
    test_pass "Loading skeleton function present"
else
    test_fail "Loading skeleton function missing"
fi

echo ""
echo "6. Accessibility Checks"
echo "-----------------------"

# Test: ARIA labels
aria_count=$(grep -c 'aria-' cmd/web/viewer.html || echo "0")
if [ "$aria_count" -gt 10 ]; then
    test_pass "ARIA attributes present ($aria_count found)"
else
    test_fail "Insufficient ARIA attributes ($aria_count found, need > 10)"
fi

# Test: Alt text for images/SVGs
if grep -q 'aria-label' cmd/web/viewer.html || grep -q 'aria-hidden="true"' cmd/web/viewer.html; then
    test_pass "SVG accessibility attributes present"
else
    test_warn "Check SVG accessibility"
fi

# Test: Form labels
if grep -q 'aria-label.*search' cmd/web/viewer.html; then
    test_pass "Search input has aria-label"
else
    test_fail "Search input missing aria-label"
fi

echo ""
echo "7. Design System Documentation Checks"
echo "--------------------------------------"

# Test: Design system has theme toggle
if grep -q 'toggleTheme' docs/design-system/index.html; then
    test_pass "Theme toggle functionality present"
else
    test_fail "Theme toggle missing"
fi

# Test: Design system has navigation
if grep -q 'sidebar' docs/design-system/index.html; then
    test_pass "Sidebar navigation present"
else
    test_fail "Sidebar navigation missing"
fi

# Test: Design system has dark mode CSS
if grep -q 'data-theme.*dark' docs/design-system/index.html; then
    test_pass "Dark mode support present"
else
    test_fail "Dark mode support missing"
fi

echo ""
echo "8. Code Statistics"
echo "------------------"

viewer_lines=$(wc -l < cmd/web/viewer.html | tr -d ' ')
css_lines=$(wc -l < cmd/web/styles.css | tr -d ' ')
js_lines=$(wc -l < cmd/web/script.js | tr -d ' ')
design_lines=$(wc -l < docs/design-system/index.html | tr -d ' ')

echo "viewer.html:        $viewer_lines lines"
echo "styles.css:         $css_lines lines"
echo "script.js:          $js_lines lines"
echo "design-system:      $design_lines lines"
echo "Total web assets:   $((viewer_lines + css_lines + js_lines)) lines"

echo ""
echo "========================================"
echo "Test Results Summary"
echo "========================================"
echo -e "${GREEN}Passed: $pass_count${NC}"
echo -e "${RED}Failed: $fail_count${NC}"
echo ""

if [ $fail_count -eq 0 ]; then
    echo -e "${GREEN}✓ All automated tests passed!${NC}"
    echo "Ready for manual UAT testing."
    echo ""
    echo "Next steps:"
    echo "1. Open docs/design-system/index.html in browser"
    echo "2. Start cache viewer: ./postgresql-archiver view-cache"
    echo "3. Follow UAT_TEST_PLAN.md for manual testing"
    echo "4. Record results in UAT_TEST_RESULTS.md"
    exit 0
else
    echo -e "${RED}✗ Some tests failed. Please fix before proceeding.${NC}"
    exit 1
fi
