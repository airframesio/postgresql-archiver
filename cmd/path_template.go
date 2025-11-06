package cmd

import (
	"fmt"
	"strings"
	"time"
)

// Duration constants for output splitting
const (
	DurationHourly  = "hourly"
	DurationDaily   = "daily"
	DurationWeekly  = "weekly"
	DurationMonthly = "monthly"
	DurationYearly  = "yearly"
)

// PathTemplate provides functionality to generate S3 paths from templates
type PathTemplate struct {
	template string
}

// NewPathTemplate creates a new PathTemplate instance
func NewPathTemplate(template string) *PathTemplate {
	return &PathTemplate{template: template}
}

// Generate replaces placeholders in the template with actual values
// Supports: {table}, {YYYY}, {MM}, {DD}, {HH}
func (pt *PathTemplate) Generate(tableName string, timestamp time.Time) string {
	result := pt.template

	// Replace table placeholder
	result = strings.ReplaceAll(result, "{table}", tableName)

	// Replace date/time placeholders
	result = strings.ReplaceAll(result, "{YYYY}", timestamp.Format("2006"))
	result = strings.ReplaceAll(result, "{MM}", timestamp.Format("01"))
	result = strings.ReplaceAll(result, "{DD}", timestamp.Format("02"))
	result = strings.ReplaceAll(result, "{HH}", timestamp.Format("15"))

	return result
}

// GenerateFilename creates a filename based on duration and timestamp
func GenerateFilename(tableName string, timestamp time.Time, duration string, formatExt string, compressionExt string) string {
	var basename string

	switch duration {
	case DurationHourly:
		basename = fmt.Sprintf("%s-%s", tableName, timestamp.Format("2006-01-02-15"))
	case DurationDaily:
		basename = fmt.Sprintf("%s-%s", tableName, timestamp.Format("2006-01-02"))
	case DurationWeekly:
		// Use ISO week format: YYYY-Www (e.g., 2024-W03)
		year, week := timestamp.ISOWeek()
		basename = fmt.Sprintf("%s-%04d-W%02d", tableName, year, week)
	case DurationMonthly:
		basename = fmt.Sprintf("%s-%s", tableName, timestamp.Format("2006-01"))
	case DurationYearly:
		basename = fmt.Sprintf("%s-%s", tableName, timestamp.Format("2006"))
	default:
		// Default to daily
		basename = fmt.Sprintf("%s-%s", tableName, timestamp.Format("2006-01-02"))
	}

	// Add format extension
	filename := basename + formatExt

	// Add compression extension if not "none"
	if compressionExt != "" {
		filename += compressionExt
	}

	return filename
}

// GetTimeRangeForDuration returns the start and end time for a given duration
func GetTimeRangeForDuration(baseTime time.Time, duration string) (time.Time, time.Time) {
	var start, end time.Time

	switch duration {
	case DurationHourly:
		// Start at beginning of hour, end at beginning of next hour
		start = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), baseTime.Hour(), 0, 0, 0, baseTime.Location())
		end = start.Add(time.Hour)

	case DurationDaily:
		// Start at beginning of day, end at beginning of next day
		start = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 0, 0, 0, 0, baseTime.Location())
		end = start.AddDate(0, 0, 1)

	case DurationWeekly:
		// Start at beginning of week (Monday), end at beginning of next week
		// Find the Monday of this week
		weekday := int(baseTime.Weekday())
		if weekday == 0 { // Sunday
			weekday = 7
		}
		daysToMonday := weekday - 1
		start = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day()-daysToMonday, 0, 0, 0, 0, baseTime.Location())
		end = start.AddDate(0, 0, 7)

	case DurationMonthly:
		// Start at beginning of month, end at beginning of next month
		start = time.Date(baseTime.Year(), baseTime.Month(), 1, 0, 0, 0, 0, baseTime.Location())
		end = start.AddDate(0, 1, 0)

	case DurationYearly:
		// Start at beginning of year, end at beginning of next year
		start = time.Date(baseTime.Year(), time.January, 1, 0, 0, 0, 0, baseTime.Location())
		end = start.AddDate(1, 0, 0)

	default:
		// Default to daily
		start = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 0, 0, 0, 0, baseTime.Location())
		end = start.AddDate(0, 0, 1)
	}

	return start, end
}

// SplitPartitionByDuration splits a partition's date range into multiple time ranges based on duration
func SplitPartitionByDuration(partitionStart, partitionEnd time.Time, duration string) []struct {
	Start time.Time
	End   time.Time
} {
	var ranges []struct {
		Start time.Time
		End   time.Time
	}

	current := partitionStart
	for current.Before(partitionEnd) {
		start, end := GetTimeRangeForDuration(current, duration)

		// Clamp the end to partition end
		if end.After(partitionEnd) {
			end = partitionEnd
		}

		// Only add if this range is within partition bounds
		if start.Before(partitionEnd) {
			ranges = append(ranges, struct {
				Start time.Time
				End   time.Time
			}{Start: start, End: end})
		}

		// Move to next period
		switch duration {
		case DurationHourly:
			current = current.Add(time.Hour)
		case DurationDaily:
			current = current.AddDate(0, 0, 1)
		case DurationWeekly:
			current = current.AddDate(0, 0, 7)
		case DurationMonthly:
			current = current.AddDate(0, 1, 0)
		case DurationYearly:
			current = current.AddDate(1, 0, 0)
		default:
			current = current.AddDate(0, 0, 1)
		}
	}

	return ranges
}
