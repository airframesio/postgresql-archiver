package cmd

import (
	"context"
	"testing"
)

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		name     string
		v1       string
		v2       string
		expected int
	}{
		{
			name:     "v1 greater than v2",
			v1:       "1.2.0",
			v2:       "1.1.0",
			expected: 1,
		},
		{
			name:     "v1 less than v2",
			v1:       "1.1.0",
			v2:       "1.2.0",
			expected: -1,
		},
		{
			name:     "equal versions",
			v1:       "1.1.0",
			v2:       "1.1.0",
			expected: 0,
		},
		{
			name:     "major version difference",
			v1:       "2.0.0",
			v2:       "1.9.9",
			expected: 1,
		},
		{
			name:     "minor version difference",
			v1:       "1.10.0",
			v2:       "1.9.0",
			expected: 1,
		},
		{
			name:     "patch version difference",
			v1:       "1.1.5",
			v2:       "1.1.4",
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareVersions(tt.v1, tt.v2)
			if result != tt.expected {
				t.Errorf("compareVersions(%s, %s) = %d, want %d",
					tt.v1, tt.v2, result, tt.expected)
			}
		})
	}
}

func TestParseVersion(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		expected [3]int
	}{
		{
			name:     "standard version",
			version:  "1.2.3",
			expected: [3]int{1, 2, 3},
		},
		{
			name:     "double digit versions",
			version:  "10.20.30",
			expected: [3]int{10, 20, 30},
		},
		{
			name:     "single component",
			version:  "5",
			expected: [3]int{5, 0, 0},
		},
		{
			name:     "two components",
			version:  "1.2",
			expected: [3]int{1, 2, 0},
		},
		{
			name:     "zero version",
			version:  "0.0.0",
			expected: [3]int{0, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseVersion(tt.version)
			if result != tt.expected {
				t.Errorf("parseVersion(%s) = %v, want %v",
					tt.version, result, tt.expected)
			}
		})
	}
}

func TestFormatUpdateMessage(t *testing.T) {
	result := VersionCheckResult{
		UpdateAvailable: true,
		CurrentVersion:  "1.0.0",
		LatestVersion:   "1.1.0",
		ReleaseURL:      "https://github.com/airframesio/postgresql-archiver/releases/tag/v1.1.0",
	}

	message := formatUpdateMessage(result)
	expected := "Update available: v1.0.0 → v1.1.0 (visit https://github.com/airframesio/postgresql-archiver/releases/tag/v1.1.0)"

	if message != expected {
		t.Errorf("formatUpdateMessage() = %q, want %q", message, expected)
	}
}

func TestVersionCheckSkipsDev(t *testing.T) {
	// Test that version check is skipped for development builds
	result := checkForUpdates(context.Background(), "dev")

	if result.UpdateAvailable {
		t.Error("checkForUpdates() should skip dev builds, but reported update available")
	}

	if result.CurrentVersion != "dev" {
		t.Errorf("checkForUpdates() CurrentVersion = %s, want dev", result.CurrentVersion)
	}
}

func TestVersionCheckSkipsEmpty(t *testing.T) {
	// Test that version check is skipped for empty version
	result := checkForUpdates(context.Background(), "")

	if result.UpdateAvailable {
		t.Error("checkForUpdates() should skip empty version, but reported update available")
	}

	if result.CurrentVersion != "" {
		t.Errorf("checkForUpdates() CurrentVersion = %s, want empty", result.CurrentVersion)
	}
}
