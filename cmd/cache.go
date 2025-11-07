package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

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
	FileTime         time.Time `json:"file_time,omitempty"`

	// S3 information
	S3Key        string    `json:"s3_key,omitempty"`
	S3Uploaded   bool      `json:"s3_uploaded,omitempty"`
	S3UploadTime time.Time `json:"s3_upload_time,omitempty"`

	// Error tracking
	LastError string    `json:"last_error,omitempty"`
	ErrorTime time.Time `json:"error_time,omitempty"`
}

// Legacy support - keep old structure for backward compatibility
type RowCountCache struct {
	Counts map[string]RowCountEntry `json:"counts"`
}

type RowCountEntry struct {
	Count     int64     `json:"count"`
	Timestamp time.Time `json:"timestamp"`
}

func getCachePath(tableName string) string {
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

func loadPartitionCache(tableName string) (*PartitionCache, error) {
	cachePath := getCachePath(tableName)

	// Try to load new cache format
	data, err := os.ReadFile(cachePath)
	if err != nil {
		if os.IsNotExist(err) {
			// Try to migrate from legacy cache
			legacyCache, err := loadLegacyCache(tableName)
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
				_ = newCache.save(tableName)
				// Remove old cache file
				_ = os.Remove(getLegacyCachePath(tableName))
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

func (c *PartitionCache) save(tableName string) error {
	cachePath := getCachePath(tableName)

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

// Set file metadata in cache (with S3 upload status)
func (c *PartitionCache) setFileMetadata(tablePartition string, s3Key string, compressedSize int64, uncompressedSize int64, md5 string, s3Uploaded bool) {
	entry := c.Entries[tablePartition]
	entry.FileSize = compressedSize
	entry.UncompressedSize = uncompressedSize
	entry.FileMD5 = md5
	entry.FileTime = time.Now()
	entry.S3Key = s3Key
	entry.S3Uploaded = s3Uploaded
	if s3Uploaded {
		entry.S3UploadTime = time.Now()
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
