package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Static errors for version checking
var (
	ErrVersionCheckFailed = errors.New("version check failed")
)

// GitHubRelease represents the structure of GitHub's latest release API response
type GitHubRelease struct {
	TagName     string    `json:"tag_name"`
	Name        string    `json:"name"`
	PublishedAt time.Time `json:"published_at"`
	HTMLURL     string    `json:"html_url"`
}

// VersionCheckResult contains the result of checking for updates
type VersionCheckResult struct {
	UpdateAvailable bool
	CurrentVersion  string
	LatestVersion   string
	ReleaseURL      string
	Error           error
}

const (
	githubAPIURL        = "https://api.github.com/repos/airframesio/postgresql-archiver/releases/latest"
	versionCheckTimeout = 5 * time.Second
	cacheExpiry         = 24 * time.Hour // Cache version check for 24 hours
)

// checkForUpdates queries GitHub API for the latest release and compares with current version
// This function is non-blocking and handles errors gracefully
func checkForUpdates(ctx context.Context, currentVersion string) VersionCheckResult {
	result := VersionCheckResult{
		CurrentVersion: currentVersion,
	}

	// Skip version check for development builds
	if currentVersion == "dev" || currentVersion == "" {
		return result
	}

	// Check cache first
	if cached := getVersionCheckCache(); cached != nil {
		if time.Since(cached.Timestamp) < cacheExpiry {
			return VersionCheckResult{
				UpdateAvailable: cached.UpdateAvailable,
				CurrentVersion:  currentVersion,
				LatestVersion:   cached.LatestVersion,
				ReleaseURL:      cached.ReleaseURL,
			}
		}
	}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: versionCheckTimeout,
	}

	// Create request with context for cancellation
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubAPIURL, nil)
	if err != nil {
		result.Error = fmt.Errorf("failed to create request: %w", err)
		return result
	}

	// Set User-Agent header (GitHub API requires this)
	req.Header.Set("User-Agent", fmt.Sprintf("postgresql-archiver/%s", currentVersion))

	// Make the request
	resp, err := client.Do(req)
	if err != nil {
		result.Error = fmt.Errorf("failed to fetch latest release: %w", err)
		return result
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Errorf("%w: status %d", ErrVersionCheckFailed, resp.StatusCode)
		return result
	}

	// Parse response
	var release GitHubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		result.Error = fmt.Errorf("failed to decode response: %w", err)
		return result
	}

	// Extract version from tag (e.g., "v1.1.0" -> "1.1.0")
	latestVersion := strings.TrimPrefix(release.TagName, "v")
	result.LatestVersion = latestVersion
	result.ReleaseURL = release.HTMLURL

	// Compare versions
	currentNormalized := strings.TrimPrefix(currentVersion, "v")
	if compareVersions(latestVersion, currentNormalized) > 0 {
		result.UpdateAvailable = true
	}

	// Cache the result
	saveVersionCheckCache(VersionCheckCache{
		UpdateAvailable: result.UpdateAvailable,
		LatestVersion:   latestVersion,
		ReleaseURL:      result.ReleaseURL,
		Timestamp:       time.Now(),
	})

	return result
}

// compareVersions compares two semantic version strings
// Returns: 1 if v1 > v2, -1 if v1 < v2, 0 if equal
func compareVersions(v1, v2 string) int {
	parts1 := parseVersion(v1)
	parts2 := parseVersion(v2)

	for i := 0; i < 3; i++ {
		if parts1[i] > parts2[i] {
			return 1
		}
		if parts1[i] < parts2[i] {
			return -1
		}
	}
	return 0
}

// parseVersion parses a semantic version string into [major, minor, patch]
func parseVersion(version string) [3]int {
	var parts [3]int
	components := strings.Split(version, ".")

	for i := 0; i < 3 && i < len(components); i++ {
		var num int
		_, _ = fmt.Sscanf(components[i], "%d", &num)
		parts[i] = num
	}

	return parts
}

// VersionCheckCache represents cached version check data
type VersionCheckCache struct {
	UpdateAvailable bool      `json:"update_available"`
	LatestVersion   string    `json:"latest_version"`
	ReleaseURL      string    `json:"release_url"`
	Timestamp       time.Time `json:"timestamp"`
}

// getVersionCheckCachePath returns the path to the version check cache file
func getVersionCheckCachePath() string {
	homeDir, _ := os.UserHomeDir()
	return filepath.Join(homeDir, ".data-archiver", "version_check.json")
}

// getVersionCheckCache reads cached version check data
func getVersionCheckCache() *VersionCheckCache {
	path := getVersionCheckCachePath()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	var cache VersionCheckCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil
	}

	return &cache
}

// saveVersionCheckCache writes version check data to cache
func saveVersionCheckCache(cache VersionCheckCache) {
	path := getVersionCheckCachePath()

	// Ensure directory exists
	dir := filepath.Dir(path)
	_ = os.MkdirAll(dir, 0o755)

	data, err := json.Marshal(cache)
	if err != nil {
		return
	}

	_ = os.WriteFile(path, data, 0o600)
}

// formatUpdateMessage creates a user-friendly update notification message
func formatUpdateMessage(result VersionCheckResult) string {
	return fmt.Sprintf("Update available: v%s → v%s (visit %s)",
		result.CurrentVersion,
		result.LatestVersion,
		result.ReleaseURL,
	)
}
