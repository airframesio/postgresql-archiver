# Quick UAT & Cross-Browser Testing Guide

**Status:** 🧪 Ready for Testing
**Time Estimate:** 30-45 minutes for comprehensive testing

---

## What's Already Done ✅

1. **Design System Documentation** - Now open in your browser!
2. **Automated Tests** - Script created and ready
3. **Test Plan** - Comprehensive UAT plan available
4. **Test Results Template** - Ready to fill in

---

## Quick Start (5 minutes)

### 1. Test the Design System First (Currently Open!)

The design system documentation should now be open in your browser. Try these:

#### Immediate Tests:
- [ ] **Theme Toggle** - Click the toggle button in the sidebar
  - Page should smoothly transition to dark mode
  - All colors should adapt
  - Refresh page - dark mode should persist

- [ ] **Navigation** - Click any nav link in sidebar
  - Page should smooth scroll to that section
  - Active link should highlight
  - Try: Colors → Typography → Components

- [ ] **Responsive** - Resize your browser window
  - At < 768px: Sidebar should hide (mobile)
  - At 768-1024px: Content should reflow nicely
  - At > 1024px: Full desktop layout

- [ ] **Keyboard Navigation** - Press Tab repeatedly
  - All links should receive visible focus
  - Focus outline clearly visible
  - Can navigate entire page with keyboard

**Expected Result:** Everything should work smoothly!

---

## Cross-Browser Testing (15 minutes)

### Priority 1: Chrome/Chromium ⭐
**Status:** Primary development browser

1. Open in Chrome: `file:///path/to/docs/design-system/index.html`
2. **Visual Check:**
   - Gradients render smoothly
   - Colors look correct
   - Shadows visible
3. **Run Lighthouse:**
   - F12 → Lighthouse tab
   - Check "Accessibility" + "Performance"
   - Click "Generate report"
   - **Target Scores:** Accessibility ≥ 95, Performance ≥ 90
4. **Take Screenshot** (for documentation)

---

### Priority 2: Firefox 🦊
**Test Compatibility:**

1. Open same file in Firefox
2. **Compare with Chrome:**
   - Layout should be identical
   - Focus outlines should work (may look slightly different)
   - Theme toggle should work
   - All animations smooth
3. **Console Check:**
   - F12 → Console
   - Should see NO errors (warnings OK)
4. **Take Screenshot**

---

### Priority 3: Safari 🧭
**WebKit Compatibility:**

1. Open in Safari
2. **Check WebKit Features:**
   - Glassmorphism effects (backdrop-filter)
   - Gradient text in headers
   - Border radius smoothness
3. **Test on iPhone/iPad** (if available):
   - AirDrop the file or use local server
   - Test touch targets
   - Check scroll behavior
4. **Take Screenshot**

---

### Priority 4: Edge (Optional)
Same as Chrome/Chromium - should be nearly identical.

---

## Responsive Design Testing (10 minutes)

### Using Chrome DevTools:
1. Open design system in Chrome
2. Press **F12** (or Cmd+Option+I on Mac)
3. Click **device icon** (or Ctrl+Shift+M / Cmd+Shift+M)

### Test These Breakpoints:

#### Mobile (375px - iPhone SE)
- [ ] Sidebar hidden or full-width
- [ ] All content readable
- [ ] Touch targets adequate (tap with mouse)
- [ ] No horizontal scrolling

#### Tablet (768px - iPad)
- [ ] Layout adapts appropriately
- [ ] Stats in 2 columns
- [ ] Good use of space

#### Desktop (1920px)
- [ ] Stats in 4 columns (if applicable)
- [ ] Content centered, not too wide
- [ ] Hover effects work

**Quick Test:** Slowly drag browser width from narrow to wide - layout should adapt smoothly at breakpoints.

---

## Accessibility Testing (10 minutes)

### Keyboard Navigation Test:
1. **Reload page** (to start fresh)
2. Press **Tab** key repeatedly:
   - First tab: "Toggle Dark Mode" button?
   - Continue tabbing through all nav links
   - Every element should show blue outline
3. Press **Enter** on a nav link:
   - Page scrolls to section
4. Press **Shift+Tab**:
   - Goes backwards through elements

**Pass Criteria:** All interactive elements reachable, visible focus on all.

---

### Screen Reader Test (Mac Only):
1. **Enable VoiceOver:** Cmd + F5
2. **Basic Navigation:**
   - Control+Option+Right Arrow (move next)
   - Listen to announcements
   - Headings should be announced
   - Links should announce destination
3. **Disable VoiceOver:** Cmd + F5

**Pass Criteria:** All content accessible, meaningful descriptions.

---

### Color Contrast Check:
1. **Visual Inspection:**
   - In light mode: All text readable?
   - In dark mode: All text readable?
   - No pure black on pure white (should be softer)

2. **Use Browser Extension** (Optional):
   - Install: WAVE or axe DevTools
   - Run scan
   - Check for contrast errors

---

## Cache Viewer Testing (If Available)

### If you can run the application:

```bash
# In terminal:
./postgresql-archiver view-cache

# Then open in browser:
http://localhost:8080
```

### Quick Tests:
- [ ] Search box works
- [ ] Clear search button appears when typing
- [ ] Table sorts when clicking headers
- [ ] Keyboard navigation works (Tab through elements)
- [ ] Mobile view (use DevTools device mode)
- [ ] Dark mode? (If implemented)

---

## Common Issues to Look For 🔍

### Visual Issues:
- ❌ Text too small on mobile
- ❌ Hover effects not visible
- ❌ Colors look wrong in dark mode
- ❌ Shadows missing or too harsh
- ❌ Animations janky or stuttering

### Functional Issues:
- ❌ Buttons don't click
- ❌ Links don't navigate
- ❌ Theme toggle doesn't persist
- ❌ Search doesn't filter
- ❌ Keyboard navigation broken

### Accessibility Issues:
- ❌ No focus indicators
- ❌ Focus indicators too subtle
- ❌ Can't reach element with keyboard
- ❌ Screen reader doesn't announce
- ❌ Poor color contrast

---

## Recording Your Results

### Quick Method (During Testing):
Open `UAT_TEST_RESULTS.md` and fill in as you go:
- Mark checkboxes: `[x]` for pass
- Add notes in the code blocks
- Record any issues found

### Screenshot Checklist:
1. Chrome - Light Mode (desktop)
2. Chrome - Dark Mode (desktop)
3. Chrome - Lighthouse scores
4. Firefox - Main view
5. Safari - Main view
6. Mobile view (375px)
7. Any bugs found

---

## Quick Test Results

### ✅ PASS Criteria:
- All features work in Chrome/Firefox/Safari
- Keyboard navigation works completely
- Mobile responsive (no horizontal scroll)
- Lighthouse Accessibility ≥ 95
- Theme toggle persists
- No console errors

### ⚠️ Issues to Note:
- Visual differences between browsers (document but may be OK)
- Minor styling inconsistencies
- Performance issues (slow loading, janky animations)

### 🚫 FAIL Criteria (Blockers):
- Feature completely broken
- Can't navigate with keyboard
- Accessibility score < 90
- Console errors on load
- Layout breaks on mobile

---

## After Testing

### 1. Fill Out Results
Edit `UAT_TEST_RESULTS.md`:
- Check all boxes you tested
- Fill in scores
- List issues found
- Add your sign-off

### 2. Save Screenshots
Create folder: `test-screenshots/`
Add all screenshots with clear names

### 3. Report Summary
In Slack/Email/etc:
```
UAT Complete for feature/ui-improvements

Tested: Chrome, Firefox, Safari
Result: [PASS / NEEDS WORK / BLOCKED]

Key Findings:
- ✅ Accessibility excellent
- ✅ Cross-browser compatible
- ⚠️  Minor issue: [describe]

Recommendation: [Ready to merge / Needs fixes]
```

---

## Need Help?

### Common Questions:

**Q: Page looks broken in Safari**
A: Check Console (Cmd+Option+C) for errors. May need webkit prefixes.

**Q: Lighthouse score is low**
A: Check specific issues in report. Most common: contrast, missing labels.

**Q: Can't test on iPhone**
A: Use Chrome DevTools device emulation as fallback.

**Q: Theme toggle doesn't work**
A: Check browser Console for JavaScript errors. Clear localStorage and try again.

**Q: What's a passing Lighthouse score?**
A: Accessibility ≥ 95 is excellent, ≥ 90 is acceptable, < 90 needs work.

---

## Time Estimates

- **Quick Test** (design system only): 10 minutes
- **Thorough Test** (all browsers): 30 minutes
- **Complete UAT** (including cache viewer): 45-60 minutes

---

## Next Steps After UAT

✅ **If PASS:**
1. Commit all changes
2. Update CHANGELOG.md
3. Create pull request
4. Request code review

⚠️ **If NEEDS WORK:**
1. Document issues clearly
2. Prioritize (critical vs minor)
3. Fix issues
4. Re-test
5. Then proceed with PR

---

**Happy Testing! 🚀**

The design system should be open in your browser now.
Start with theme toggle and navigation - if those work, you're in good shape!
