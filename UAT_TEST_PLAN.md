# User Acceptance Testing (UAT) & Cross-Browser Verification Plan

**Branch:** feature/ui-improvements
**Date:** 2025-11-04
**Testing Environment:** Local development

---

## Quick Start Testing Instructions

### 1. Start the Application
```bash
# Build the application
go build -o postgresql-archiver

# Start the cache viewer server (if available)
./postgresql-archiver view-cache

# OR test the design system documentation directly
open docs/design-system/index.html
```

### 2. Access Points
- **Cache Viewer:** `http://localhost:8080` (or configured port)
- **Design System:** `file:///path/to/docs/design-system/index.html`

---

## UAT Test Plan

### Test Environment Setup
- [ ] Application builds successfully
- [ ] Server starts without errors
- [ ] Can access cache viewer in browser
- [ ] Design system documentation opens correctly

---

## Section 1: Accessibility Testing (WCAG 2.1 AA)

### 1.1 Keyboard Navigation ⌨️
**Priority:** CRITICAL

#### Test Steps:
1. [ ] **Tab Key Navigation**
   - Press Tab repeatedly through the page
   - All interactive elements receive focus (buttons, links, inputs, table headers)
   - Focus order is logical (top to bottom, left to right)
   - No keyboard traps (can Tab out of all elements)

2. [ ] **Skip to Content Link**
   - Press Tab once on page load
   - Skip link appears at top-left
   - Press Enter
   - Focus jumps to main content area (table)

3. [ ] **Search Functionality**
   - Tab to search box
   - Type search query
   - Clear button appears
   - Tab to clear button
   - Press Enter or Space
   - Search clears and focus returns to input

4. [ ] **Table Sorting**
   - Tab to a sortable column header
   - Press Enter or Space
   - Column sorts ascending
   - Press Enter/Space again
   - Column sorts descending
   - Visual sort indicator updates

5. [ ] **Form Controls**
   - Tab to table filter dropdown
   - Press Space or Enter to open
   - Use arrow keys to navigate options
   - Press Enter to select

**Pass Criteria:**
- ✅ All interactive elements reachable via keyboard
- ✅ Visual focus indicators clearly visible
- ✅ No keyboard traps
- ✅ Logical tab order
- ✅ All actions possible without mouse

---

### 1.2 Focus Indicators 👁️
**Priority:** CRITICAL

#### Test Steps:
1. [ ] **General Focus Styles**
   - Tab through all elements
   - Every focused element has visible outline
   - Focus outline is at least 2px thick
   - Outline color contrasts with background

2. [ ] **Button Focus**
   - Tab to each button variant
   - Focus ring is 3px with box-shadow
   - Sufficient offset from button edge

3. [ ] **Table Header Focus**
   - Tab to sortable headers
   - Focus style visible (inset style)
   - Background changes on focus

4. [ ] **Link Focus**
   - Tab to GitHub link
   - Focus indicator clearly visible
   - Sufficient contrast

**Pass Criteria:**
- ✅ All focused elements have visible indicators
- ✅ Focus indicators meet 3:1 contrast ratio
- ✅ Indicators don't obscure content
- ✅ Consistent focus styling throughout

---

### 1.3 Screen Reader Support 🔊
**Priority:** HIGH

#### Test with: NVDA (Windows), JAWS (Windows), or VoiceOver (Mac)

**VoiceOver (Mac) Instructions:**
```
Cmd + F5 to enable VoiceOver
VO = Control + Option
VO + Right Arrow = Next element
VO + Left Arrow = Previous element
VO + Space = Activate element
```

#### Test Steps:
1. [ ] **Page Structure**
   - Headings read correctly (H1, H2)
   - Landmarks identified (main, navigation)
   - Skip link announced and functional

2. [ ] **Interactive Elements**
   - Buttons announce their purpose
   - Links announce destination
   - Form inputs have labels
   - Table headers associated with cells

3. [ ] **Dynamic Content**
   - Status updates announced (Connected/Disconnected)
   - Table sort changes announced
   - Search results count announced
   - Alert notifications read aloud

4. [ ] **ARIA Attributes**
   - aria-label values descriptive
   - aria-live regions update properly
   - aria-sort indicates direction
   - Progress bars announce percentage

**Pass Criteria:**
- ✅ All content accessible via screen reader
- ✅ Meaningful labels and descriptions
- ✅ Dynamic updates announced
- ✅ No missing alternative text

---

### 1.4 Color Contrast 🎨
**Priority:** CRITICAL

#### Test with: Chrome DevTools Lighthouse or WebAIM Contrast Checker

#### Test Steps:
1. [ ] **Light Mode Text Contrast**
   - Body text on background: ≥ 4.5:1
   - Secondary text: ≥ 4.5:1
   - Link text: ≥ 4.5:1
   - Button text: ≥ 4.5:1

2. [ ] **Dark Mode Text Contrast**
   - Toggle to dark mode
   - Body text on background: ≥ 4.5:1
   - All text meets contrast requirements

3. [ ] **Status Badges**
   - Success badge text: ≥ 4.5:1
   - Warning badge text: ≥ 4.5:1
   - Error badge text: ≥ 4.5:1
   - Info badge text: ≥ 4.5:1

4. [ ] **Focus Indicators**
   - Focus outline vs background: ≥ 3:1
   - In both light and dark modes

**Pass Criteria:**
- ✅ All text contrast ≥ 4.5:1 (AA)
- ✅ Focus indicators ≥ 3:1
- ✅ No color-only communication
- ✅ Works in both light/dark modes

---

### 1.5 Touch Targets (Mobile) 📱
**Priority:** HIGH

#### Test with: Chrome DevTools Device Emulation

#### Test Steps:
1. [ ] **Enable Mobile Emulation**
   - Open Chrome DevTools (F12)
   - Click device icon (Ctrl+Shift+M)
   - Select iPhone SE (375px width)

2. [ ] **Measure Touch Targets**
   - Search input: ≥ 44x44px
   - Filter dropdown: ≥ 44x44px
   - All buttons: ≥ 44x44px
   - Table headers: ≥ 44px height
   - Status indicators: ≥ 44px height

3. [ ] **Test Spacing**
   - Adequate spacing between touch targets
   - No accidental taps on adjacent elements

**Pass Criteria:**
- ✅ All interactive elements ≥ 44x44px on mobile
- ✅ Adequate spacing between targets
- ✅ Easy to tap without mistakes

---

## Section 2: Functional Testing

### 2.1 Search Functionality 🔍
**Priority:** HIGH

#### Test Steps:
1. [ ] **Basic Search**
   - Type text in search box
   - Table filters in real-time
   - Only matching rows shown
   - Row count updates

2. [ ] **Clear Search**
   - Enter search text
   - Clear button appears
   - Click clear button
   - Search clears
   - All rows return
   - Focus returns to input

3. [ ] **Empty Results**
   - Search for non-existent text
   - Empty state displayed
   - Clear messaging

4. [ ] **Case Sensitivity**
   - Search is case-insensitive
   - "TEST" matches "test"

**Pass Criteria:**
- ✅ Search filters correctly
- ✅ Clear button works
- ✅ Good empty state
- ✅ Case-insensitive

---

### 2.2 Table Sorting 📊
**Priority:** HIGH

#### Test Steps:
1. [ ] **Sort Ascending**
   - Click partition column header
   - Sorts alphabetically A-Z
   - Arrow indicator points up
   - aria-sort="ascending"

2. [ ] **Sort Descending**
   - Click same header again
   - Sorts Z-A
   - Arrow indicator points down
   - aria-sort="descending"

3. [ ] **Numeric Sorting**
   - Click "Rows" column
   - Sorts numerically (not alphabetically)
   - 2 before 10 (not 10 before 2)

4. [ ] **Date Sorting**
   - Click "Age" column
   - Sorts by timestamp
   - Recent to oldest or vice versa

**Pass Criteria:**
- ✅ Sorts correctly by data type
- ✅ Visual indicators clear
- ✅ ARIA attributes update
- ✅ Works with keyboard

---

### 2.3 Filter Dropdown 🔽
**Priority:** MEDIUM

#### Test Steps:
1. [ ] **Filter by Table**
   - Select a table from dropdown
   - Only rows for that table shown
   - Works with search simultaneously

2. [ ] **Reset Filter**
   - Select "All Tables"
   - All rows return

3. [ ] **Keyboard Operation**
   - Tab to dropdown
   - Space to open
   - Arrow keys to select
   - Enter to confirm

**Pass Criteria:**
- ✅ Filters work correctly
- ✅ Combines with search
- ✅ Keyboard accessible
- ✅ Visual feedback clear

---

### 2.4 Alert System 🔔
**Priority:** MEDIUM

#### Test Steps (if alerts trigger in app):
1. [ ] **Alert Appearance**
   - Alert slides in from top-right
   - Smooth animation
   - Appropriate icon shows

2. [ ] **Alert Types**
   - Success alert is green
   - Error alert is red
   - Warning alert is yellow/orange
   - Info alert is blue

3. [ ] **Alert Dismissal**
   - Click X button to close
   - Alert slides out
   - Auto-dismisses after 5 seconds

4. [ ] **Screen Reader**
   - Alert announced immediately
   - Content read aloud

**Pass Criteria:**
- ✅ Alerts appear correctly
- ✅ Proper semantic colors
- ✅ Dismissible
- ✅ Screen reader accessible

---

### 2.5 Loading States ⏳
**Priority:** MEDIUM

#### Test Steps:
1. [ ] **Skeleton Screen**
   - On initial load, skeleton shows
   - Animated shimmer effect
   - Transitions smoothly to real content

2. [ ] **Loading Spinner**
   - Spinner visible during operations
   - Smooth rotation animation

3. [ ] **Screen Reader**
   - Loading states announced
   - "Loading" or "Please wait" message

**Pass Criteria:**
- ✅ Loading feedback present
- ✅ Smooth animations
- ✅ No jarring transitions
- ✅ Accessible

---

## Section 3: Responsive Design Testing

### 3.1 Mobile (320px - 767px) 📱
**Priority:** HIGH

#### Devices to Test:
- iPhone SE (375x667)
- iPhone 12 Pro (390x844)
- Galaxy S21 (360x800)

#### Test Steps:
1. [ ] **Layout**
   - Stats grid: 1 column
   - No horizontal scrolling (except table)
   - All text readable
   - Touch targets adequate

2. [ ] **Header**
   - Logo/title scaled appropriately
   - GitHub link full-width
   - Status indicator visible

3. [ ] **Controls**
   - Search box full-width
   - Filter dropdown full-width
   - Stacked vertically
   - Proper spacing

4. [ ] **Table**
   - Horizontal scroll if needed
   - Headers sticky
   - Cells readable
   - Row height adequate for touch

**Pass Criteria:**
- ✅ No layout breaks
- ✅ All content accessible
- ✅ Touch-friendly
- ✅ Readable text

---

### 3.2 Tablet (768px - 1024px) 📱
**Priority:** MEDIUM

#### Devices to Test:
- iPad (768x1024)
- iPad Pro (1024x1366)

#### Test Steps:
1. [ ] **Layout**
   - Stats grid: 2 columns
   - Proper use of space
   - No awkward gaps

2. [ ] **Controls**
   - Search and filter on same row
   - Adequate spacing
   - Touch targets proper size

3. [ ] **Table**
   - Full width utilization
   - Readable without scrolling if possible
   - Comfortable padding

**Pass Criteria:**
- ✅ Optimal use of space
- ✅ 2-column grid works well
- ✅ Touch-friendly
- ✅ Good readability

---

### 3.3 Desktop (1025px+) 🖥️
**Priority:** HIGH

#### Resolutions to Test:
- 1920x1080 (Full HD)
- 2560x1440 (QHD)
- 1366x768 (common laptop)

#### Test Steps:
1. [ ] **Layout**
   - Stats grid: 4 columns at full width
   - Container max-width 1600px
   - Centered on very large screens

2. [ ] **Spacing**
   - Generous padding
   - Good whitespace
   - Not cramped

3. [ ] **Hover States**
   - Cards lift on hover
   - Table rows highlight
   - Buttons change on hover
   - Smooth transitions

**Pass Criteria:**
- ✅ Optimal desktop layout
- ✅ Not too stretched
- ✅ Hover effects work
- ✅ Professional appearance

---

## Section 4: Cross-Browser Testing

### 4.1 Chrome/Chromium-based 🌐
**Priority:** HIGH

#### Browsers to Test:
- Google Chrome (latest)
- Microsoft Edge (latest)
- Brave Browser

#### Test Steps:
1. [ ] **Visual Rendering**
   - Gradients render correctly
   - Box shadows display
   - Border radius smooth
   - Fonts load properly

2. [ ] **Functionality**
   - All features work
   - WebSocket connects (if applicable)
   - Animations smooth
   - No console errors

3. [ ] **DevTools Lighthouse**
   - Run Lighthouse audit
   - Accessibility score ≥ 95
   - Performance score ≥ 90
   - Best practices ≥ 90

**Pass Criteria:**
- ✅ Perfect visual rendering
- ✅ All features functional
- ✅ High Lighthouse scores
- ✅ No console errors

---

### 4.2 Firefox 🦊
**Priority:** HIGH

#### Test Steps:
1. [ ] **CSS Features**
   - CSS custom properties work
   - Flexbox layouts correct
   - Grid layouts correct
   - Focus-visible styles work

2. [ ] **Animations**
   - Transitions smooth
   - Keyframe animations work
   - No stuttering

3. [ ] **Accessibility**
   - Focus indicators visible
   - Screen reader compatible
   - ARIA attributes respected

4. [ ] **Console Check**
   - Open console (F12)
   - No errors or warnings

**Pass Criteria:**
- ✅ Visual parity with Chrome
- ✅ All animations smooth
- ✅ No errors
- ✅ Fully functional

---

### 4.3 Safari (macOS/iOS) 🧭
**Priority:** HIGH

#### Test Steps:
1. [ ] **WebKit Compatibility**
   - Backdrop-filter works (glassmorphism)
   - -webkit-text-fill-color works
   - Smooth scrolling works
   - Border radius correct

2. [ ] **iOS Safari**
   - Test on actual iPhone if possible
   - Touch targets work
   - Scroll behavior smooth
   - No layout shifts

3. [ ] **Viewport Behavior**
   - Address bar hide/show doesn't break layout
   - Safe area insets respected
   - No horizontal scroll

**Pass Criteria:**
- ✅ Visual consistency
- ✅ All features work
- ✅ Smooth mobile experience
- ✅ No webkit-specific issues

---

### 4.4 Safari (Older Versions) 🧭
**Priority:** MEDIUM

#### Test Safari 14-15 if possible

#### Test Steps:
1. [ ] **Fallback Styles**
   - Gradient text has color fallback
   - Focus-visible has focus fallback
   - Modern CSS features degrade gracefully

2. [ ] **Core Functionality**
   - All features work without modern CSS
   - Readable without enhancements

**Pass Criteria:**
- ✅ Graceful degradation
- ✅ Core features work
- ✅ Still usable

---

## Section 5: Design System Documentation Testing

### 5.1 Navigation 🧭
**Priority:** HIGH

#### Test Steps:
1. [ ] **Sidebar Navigation**
   - All links work
   - Smooth scrolling to sections
   - Active link highlights
   - Active link updates on scroll

2. [ ] **Mobile Navigation**
   - Sidebar behavior on mobile
   - Toggle if implemented
   - All sections accessible

**Pass Criteria:**
- ✅ All links functional
- ✅ Smooth scrolling
- ✅ Active states work
- ✅ Mobile-friendly

---

### 5.2 Theme Toggle 🌓
**Priority:** HIGH

#### Test Steps:
1. [ ] **Toggle Functionality**
   - Click theme toggle button
   - Page switches to dark mode
   - All colors update
   - Smooth transition

2. [ ] **Persistence**
   - Toggle to dark mode
   - Refresh page
   - Dark mode persists
   - LocalStorage saves preference

3. [ ] **Dark Mode Quality**
   - All text readable
   - Proper contrast maintained
   - Colors appropriate
   - No visual artifacts

4. [ ] **Component Adaptation**
   - All examples update
   - Buttons adapt to theme
   - Badges adapt to theme
   - Cards adapt to theme

**Pass Criteria:**
- ✅ Toggle works smoothly
- ✅ Preference persists
- ✅ Both themes look good
- ✅ All components adapt

---

### 5.3 Component Examples 🧩
**Priority:** MEDIUM

#### Test Steps:
1. [ ] **Interactive Examples**
   - Hover over buttons
   - Hover effects work
   - Visual feedback clear

2. [ ] **Color Swatches**
   - All colors display correctly
   - Hex values accurate
   - Names descriptive

3. [ ] **Typography Scale**
   - All sizes display
   - Proportions correct
   - Font weights render

4. [ ] **Code Snippets**
   - Code readable
   - Syntax makes sense
   - Copy-paste ready

**Pass Criteria:**
- ✅ All examples work
- ✅ Visual accuracy
- ✅ Code snippets useful
- ✅ Clear documentation

---

## Section 6: Performance Testing

### 6.1 Page Load Performance ⚡
**Priority:** MEDIUM

#### Test Steps:
1. [ ] **Initial Load**
   - Page loads in < 2 seconds
   - No FOUC (Flash of Unstyled Content)
   - Smooth rendering

2. [ ] **Asset Loading**
   - Embedded CSS loads instantly
   - Embedded JS loads instantly
   - No external dependencies

3. [ ] **Lighthouse Performance**
   - Run Chrome Lighthouse
   - Performance score ≥ 90
   - First Contentful Paint < 1.5s
   - Time to Interactive < 3.5s

**Pass Criteria:**
- ✅ Fast initial load
- ✅ No render blocking
- ✅ Good Lighthouse score
- ✅ Smooth experience

---

### 6.2 Runtime Performance 🏃
**Priority:** MEDIUM

#### Test Steps:
1. [ ] **Animations**
   - Skeleton animations smooth
   - Spinner rotates smoothly
   - Transitions don't stutter
   - Maintains 60fps

2. [ ] **Large Datasets**
   - Load table with 1000+ rows
   - Scroll performance good
   - Search still responsive
   - Sort doesn't freeze

3. [ ] **Memory**
   - Check DevTools Memory
   - No memory leaks
   - Reasonable memory usage

**Pass Criteria:**
- ✅ Smooth animations
- ✅ Handles large data
- ✅ No leaks
- ✅ Responsive interactions

---

## Test Results Summary Template

### Overall Results
- **Date Tested:** _______________
- **Tester:** _______________
- **Environment:** _______________

### Scores
- **Accessibility:** _____ / 100
- **Functionality:** _____ / 100
- **Responsiveness:** _____ / 100
- **Cross-Browser:** _____ / 100
- **Performance:** _____ / 100

### Critical Issues Found
1. ________________________________
2. ________________________________
3. ________________________________

### Minor Issues Found
1. ________________________________
2. ________________________________
3. ________________________________

### Blockers for Release
- [ ] None - Ready to release
- [ ] Critical issues must be fixed
- [ ] Minor issues should be addressed

### Sign-off
- [ ] Accessibility approved
- [ ] UX approved
- [ ] QA approved
- [ ] Product approved

---

## Automated Testing Commands

```bash
# Build and verify
go build -o postgresql-archiver
go vet ./...
go test ./...

# Format check
gofmt -l .

# Start server for manual testing
./postgresql-archiver view-cache

# Open design system
open docs/design-system/index.html
```

---

## Browser Testing URLs

### Automated Testing Tools
- **Lighthouse:** Chrome DevTools > Lighthouse tab
- **axe DevTools:** https://www.deque.com/axe/devtools/
- **WAVE:** https://wave.webaim.org/extension/
- **WebAIM Contrast Checker:** https://webaim.org/resources/contrastchecker/

### Screen Readers
- **NVDA (Windows):** https://www.nvaccess.org/download/
- **JAWS (Windows):** https://www.freedomscientific.com/products/software/jaws/
- **VoiceOver (Mac):** Built-in (Cmd + F5)

---

## Next Steps After UAT

1. Document all findings
2. Fix critical issues
3. Re-test fixes
4. Update CHANGELOG.md
5. Create pull request
6. Request code review
7. Merge to main

---

**Happy Testing! 🎉**
