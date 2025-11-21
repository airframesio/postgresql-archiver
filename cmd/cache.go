package cmd

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CacheScope represents a unique namespace for cached metadata.
// It combines the executing subcommand with the fully-qualified output path
// so that different commands or destinations do not clobber each other's cache.
type CacheScope struct {
	Command    string
	Table      string
	OutputPath string
}

// NewCacheScope builds a cache scope for the provided command/config pair.
func NewCacheScope(command string, cfg *Config) CacheScope {
	if cfg == nil {
		return CacheScope{}
	}

	table := cfg.Table
	if table == "" {
		table = cfg.Database.Name
	}

	return CacheScope{
		Command:    command,
		Table:      table,
		OutputPath: buildAbsoluteOutputPath(cfg),
	}
}

// buildAbsoluteOutputPath converts the configured bucket/path template into a
// canonical absolute S3-style path for cache namespacing purposes.
func buildAbsoluteOutputPath(cfg *Config) string {
	if cfg == nil {
		return ""
	}

	bucket := strings.TrimSpace(cfg.S3.Bucket)
	pathTemplate := strings.TrimSpace(cfg.S3.PathTemplate)

	for strings.HasPrefix(pathTemplate, "/") {
		pathTemplate = strings.TrimPrefix(pathTemplate, "/")
	}

	if bucket == "" {
		return pathTemplate
	}

	if strings.Contains(bucket, "://") {
		bucket = strings.TrimSuffix(bucket, "/")
		if pathTemplate == "" {
			return bucket
		}
		return fmt.Sprintf("%s/%s", bucket, pathTemplate)
	}

	if pathTemplate == "" {
		return fmt.Sprintf("s3://%s", bucket)
	}

	return fmt.Sprintf("s3://%s/%s", bucket, pathTemplate)
}

func (s CacheScope) normalize() CacheScope {
	normalized := s
	if normalized.Command == "" {
		normalized.Command = "default"
	}
	if normalized.Table == "" {
		normalized.Table = "global"
	}
	if normalized.OutputPath == "" {
		normalized.OutputPath = "default"
	}
	return normalized
}

func sanitizeCacheComponent(value, fallback string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		value = fallback
	}

	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}

	return b.String()
}

func (s CacheScope) fileIdentifier() string {
	normalized := s.normalize()

	commandComponent := sanitizeCacheComponent(normalized.Command, "cmd")
	tableComponent := sanitizeCacheComponent(normalized.Table, "table")

	hashSource := normalized.OutputPath
	if hashSource == "" {
		hashSource = fmt.Sprintf("%s:%s", normalized.Command, normalized.Table)
	}
	hash := sha1.Sum([]byte(hashSource))
	hashComponent := hex.EncodeToString(hash[:])[:16]

	return fmt.Sprintf("%s_%s_%s", commandComponent, tableComponent, hashComponent)
}

// PartitionCache stores both row counts and file metadata
type PartitionCache struct {
	Entries map[string]PartitionCacheEntry `json:"entries"`
}

type PartitionCacheEntry struct {
	// Row count information
	RowCount  int64     `json:"row_count"`
	CountTime time.Time `json:"count_time"`

	// File metadata (stored after processing)
	FileSize         int64     `json:"file_size,omitempty"`         // Compressed size
	UncompressedSize int64     `json:"uncompressed_size,omitempty"` // Original size
	FileMD5          string    `json:"file_md5,omitempty"`
	MultipartETag    string    `json:"multipart_etag,omitempty"` // S3 multipart ETag for files >100MB
	FileTime         time.Time `json:"file_time,omitempty"`

	// S3 information
	S3Key        string    `json:"s3_key,omitempty"`
	S3Uploaded   bool      `json:"s3_uploaded,omitempty"`
	S3UploadTime time.Time `json:"s3_upload_time,omitempty"`

	// Error tracking
	LastError string    `json:"last_error,omitempty"`
	ErrorTime time.Time `json:"error_time,omitempty"`

	// Processing time tracking
	ProcessStartTime time.Time `json:"process_start_time,omitempty"` // When processing started for this job
}

// Legacy support - keep old structure for backward compatibility
type RowCountCache struct {
	Counts map[string]RowCountEntry `json:"counts"`
}

type RowCountEntry struct {
	Count     int64     `json:"count"`
	Timestamp time.Time `json:"timestamp"`
}

func getCachePath(scope CacheScope) string {
	scope = scope.normalize()

	homeDir, _ := os.UserHomeDir()
	cacheDir := filepath.Join(homeDir, ".data-archiver", "cache")
	_ = os.MkdirAll(cacheDir, 0o755)
	return filepath.Join(cacheDir, fmt.Sprintf("%s_metadata.json", scope.fileIdentifier()))
}

func getLegacyMetadataPath(tableName string) string {
	homeDir, _ := os.UserHomeDir()
	cacheDir := filepath.Join(homeDir, ".data-archiver", "cache")
	_ = os.MkdirAll(cacheDir, 0o755)
	return filepath.Join(cacheDir, fmt.Sprintf("%s_metadata.json", tableName))
}

// Legacy cache path for migration
func getLegacyCachePath(tableName string) string {
	homeDir, _ := os.UserHomeDir()
	cacheDir := filepath.Join(homeDir, ".data-archiver", "cache")
	return filepath.Join(cacheDir, fmt.Sprintf("%s_counts.json", tableName))
}

func loadPartitionCache(scope CacheScope) (*PartitionCache, error) {
	cachePath := getCachePath(scope)

	// Try to load new cache format
	data, err := os.ReadFile(cachePath)
	if err != nil {
		if os.IsNotExist(err) {
			// Attempt to migrate from old metadata filename first
			if migrated, migrateErr := migrateLegacyMetadata(scope); migrateErr == nil && migrated != nil {
				return migrated, nil
			}

			// Try to migrate from legacy cache
			legacyCache, err := loadLegacyCache(scope.Table)
			if err == nil && legacyCache != nil {
				// Migrate legacy cache
				newCache := &PartitionCache{
					Entries: make(map[string]PartitionCacheEntry),
				}
				for partition, entry := range legacyCache.Counts {
					newCache.Entries[partition] = PartitionCacheEntry{
						RowCount:  entry.Count,
						CountTime: entry.Timestamp,
					}
				}
				// Save in new format
				_ = newCache.save(scope)
				// Remove old cache file
				_ = os.Remove(getLegacyCachePath(scope.Table))
				return newCache, nil
			}
			// Return empty cache if no legacy cache exists
			return &PartitionCache{
				Entries: make(map[string]PartitionCacheEntry),
			}, nil
		}
		return nil, err
	}

	var cache PartitionCache
	if err := json.Unmarshal(data, &cache); err != nil {
		// If cache is corrupted, return empty cache
		return &PartitionCache{
			Entries: make(map[string]PartitionCacheEntry),
		}, nil
	}

	if cache.Entries == nil {
		cache.Entries = make(map[string]PartitionCacheEntry)
	}

	return &cache, nil
}

// Load legacy cache for migration
func loadLegacyCache(tableName string) (*RowCountCache, error) {
	cachePath := getLegacyCachePath(tableName)

	data, err := os.ReadFile(cachePath)
	if err != nil {
		return nil, err
	}

	var cache RowCountCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, err
	}

	return &cache, nil
}

func migrateLegacyMetadata(scope CacheScope) (*PartitionCache, error) {
	if scope.Table == "" {
		return nil, os.ErrNotExist
	}

	legacyPath := getLegacyMetadataPath(scope.Table)
	data, err := os.ReadFile(legacyPath)
	if err != nil {
		return nil, err
	}

	var cache PartitionCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, err
	}

	if cache.Entries == nil {
		cache.Entries = make(map[string]PartitionCacheEntry)
	}

	if err := cache.save(scope); err != nil {
		return nil, err
	}

	_ = os.Remove(legacyPath)
	return &cache, nil
}

// Backward compatibility wrapper - kept for potential future use
/*
func loadCache(tableName string) (*RowCountCache, error) {
	partitionCache, err := loadPartitionCache(tableName)
	if err != nil {
		return nil, err
	}

	// Convert to old format for backward compatibility
	rowCountCache := &RowCountCache{
		Counts: make(map[string]RowCountEntry),
	}
	for partition, entry := range partitionCache.Entries {
		rowCountCache.Counts[partition] = RowCountEntry{
			Count:     entry.RowCount,
			Timestamp: entry.CountTime,
		}
	}

	return rowCountCache, nil
}
*/

func (c *PartitionCache) save(scope CacheScope) error {
	cachePath := getCachePath(scope)

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(cachePath, data, 0o600)
}

// Backward compatibility wrapper - kept for potential future use
/*
func (c *RowCountCache) save(tableName string) error {
	// Convert to new format
	partitionCache := &PartitionCache{
		Entries: make(map[string]PartitionCacheEntry),
	}
	for partition, entry := range c.Counts {
		partitionCache.Entries[partition] = PartitionCacheEntry{
			RowCount:  entry.Count,
			CountTime: entry.Timestamp,
		}
	}

	return partitionCache.save(tableName)
}
*/

// Get file metadata from cache
func (c *PartitionCache) getFileMetadata(tablePartition string, s3Key string, partitionDate time.Time) (size int64, md5 string, found bool) {
	entry, exists := c.Entries[tablePartition]
	if !exists {
		return 0, "", false
	}

	// Check if we have file metadata
	if entry.FileSize == 0 || entry.FileMD5 == "" {
		return 0, "", false
	}

	// File metadata is cached permanently (not expired)
	// Only today's partition needs recalculation

	// Always recalculate today's partition
	today := time.Now().Truncate(24 * time.Hour)
	if partitionDate.Equal(today) || partitionDate.After(today) {
		// Clear file metadata for today's partition
		entry.FileSize = 0
		entry.FileMD5 = ""
		entry.FileTime = time.Time{}
		c.Entries[tablePartition] = entry
		return 0, "", false
	}

	// Verify S3 key matches (in case path structure changed)
	if entry.S3Key != "" && entry.S3Key != s3Key {
		// Path changed, invalidate file metadata
		entry.FileSize = 0
		entry.FileMD5 = ""
		entry.FileTime = time.Time{}
		entry.S3Key = s3Key
		c.Entries[tablePartition] = entry
		return 0, "", false
	}

	return entry.FileSize, entry.FileMD5, true
}

// getFileMetadataWithETag gets file metadata including multipart ETag from cache
func (c *PartitionCache) getFileMetadataWithETag(tablePartition string, s3Key string, partitionDate time.Time) (size int64, md5 string, multipartETag string, found bool) {
	entry, exists := c.Entries[tablePartition]
	if !exists {
		return 0, "", "", false
	}

	// Check if we have file metadata
	if entry.FileSize == 0 || entry.FileMD5 == "" {
		return 0, "", "", false
	}

	// File metadata is cached permanently (not expired)
	// Only today's partition needs recalculation

	// Always recalculate today's partition
	today := time.Now().Truncate(24 * time.Hour)
	if partitionDate.Equal(today) || partitionDate.After(today) {
		// Clear file metadata for today's partition
		entry.FileSize = 0
		entry.FileMD5 = ""
		entry.MultipartETag = ""
		entry.FileTime = time.Time{}
		c.Entries[tablePartition] = entry
		return 0, "", "", false
	}

	// Verify S3 key matches (in case path structure changed)
	if entry.S3Key != "" && entry.S3Key != s3Key {
		// Path changed, invalidate file metadata
		entry.FileSize = 0
		entry.FileMD5 = ""
		entry.MultipartETag = ""
		entry.FileTime = time.Time{}
		entry.S3Key = s3Key
		c.Entries[tablePartition] = entry
		return 0, "", "", false
	}

	return entry.FileSize, entry.FileMD5, entry.MultipartETag, true
}

// Set file metadata in cache (with S3 upload status)
func (c *PartitionCache) setFileMetadata(tablePartition string, s3Key string, compressedSize int64, uncompressedSize int64, md5 string, s3Uploaded bool) {
	c.setFileMetadataWithETagAndStartTime(tablePartition, s3Key, compressedSize, uncompressedSize, md5, "", s3Uploaded, time.Time{})
}

func (c *PartitionCache) setFileMetadataWithETagAndStartTime(tablePartition string, s3Key string, compressedSize int64, uncompressedSize int64, md5 string, multipartETag string, s3Uploaded bool, processStartTime time.Time) {
	entry := c.Entries[tablePartition]
	entry.FileSize = compressedSize
	entry.UncompressedSize = uncompressedSize
	entry.FileMD5 = md5
	entry.MultipartETag = multipartETag
	entry.FileTime = time.Now()
	entry.S3Key = s3Key
	entry.S3Uploaded = s3Uploaded
	if s3Uploaded {
		entry.S3UploadTime = time.Now()
	}
	// Set process start time if provided
	if !processStartTime.IsZero() {
		entry.ProcessStartTime = processStartTime
	}
	// Clear any previous error when successful
	entry.LastError = ""
	entry.ErrorTime = time.Time{}
	c.Entries[tablePartition] = entry
}

// Set error in cache
func (c *PartitionCache) setError(tablePartition string, errMsg string) {
	entry := c.Entries[tablePartition]
	entry.LastError = errMsg
	entry.ErrorTime = time.Now()
	c.Entries[tablePartition] = entry
}

// Get row count from cache
func (c *PartitionCache) getRowCount(tablePartition string, partitionDate time.Time) (int64, bool) {
	entry, exists := c.Entries[tablePartition]
	if !exists {
		return 0, false
	}

	// Check if cache is expired (24 hours)
	if time.Since(entry.CountTime) > 24*time.Hour {
		delete(c.Entries, tablePartition)
		return 0, false
	}

	// Always recount today's partition
	today := time.Now().Truncate(24 * time.Hour)
	if partitionDate.Equal(today) || partitionDate.After(today) {
		delete(c.Entries, tablePartition)
		return 0, false
	}

	return entry.RowCount, true
}

// Set row count in cache (preserving existing metadata)
func (c *PartitionCache) setRowCount(tablePartition string, count int64) {
	entry, exists := c.Entries[tablePartition]
	if !exists {
		entry = PartitionCacheEntry{}
	}
	entry.RowCount = count
	entry.CountTime = time.Now()
	c.Entries[tablePartition] = entry
}

// Backward compatibility wrappers - kept for potential future use
/*
func (c *RowCountCache) getCount(tablePartition string, partitionDate time.Time) (int64, bool) {
	entry, exists := c.Counts[tablePartition]
	if !exists {
		return 0, false
	}

	// Check if cache is expired (24 hours)
	if time.Since(entry.Timestamp) > 24*time.Hour {
		delete(c.Counts, tablePartition)
		return 0, false
	}

	// Always recount today's partition
	today := time.Now().Truncate(24 * time.Hour)
	if partitionDate.Equal(today) || partitionDate.After(today) {
		delete(c.Counts, tablePartition)
		return 0, false
	}

	return entry.Count, true
}

func (c *RowCountCache) setCount(tablePartition string, count int64) {
	c.Counts[tablePartition] = RowCountEntry{
		Count:     count,
		Timestamp: time.Now(),
	}
}
*/

func (c *PartitionCache) cleanExpired() {
	for partition, entry := range c.Entries {
		modified := false

		// Only clean expired row counts (daily)
		if !entry.CountTime.IsZero() && time.Since(entry.CountTime) > 24*time.Hour {
			entry.RowCount = 0
			entry.CountTime = time.Time{}
			modified = true
		}

		// Don't expire file metadata - keep it indefinitely
		// The metadata will be updated if the file changes

		// Clean old errors after 7 days
		if !entry.ErrorTime.IsZero() && time.Since(entry.ErrorTime) > 7*24*time.Hour {
			entry.LastError = ""
			entry.ErrorTime = time.Time{}
			modified = true
		}

		// Update entry if modified
		if modified {
			// Only delete entry if it has no useful data
			if entry.RowCount == 0 && entry.FileSize == 0 && entry.LastError == "" {
				delete(c.Entries, partition)
			} else {
				c.Entries[partition] = entry
			}
		}
	}
}

// Backward compatibility wrapper - kept for potential future use
/*
func (c *RowCountCache) cleanExpired() {
	for partition, entry := range c.Counts {
		if time.Since(entry.Timestamp) > 24*time.Hour {
			delete(c.Counts, partition)
		}
	}
}
*/
