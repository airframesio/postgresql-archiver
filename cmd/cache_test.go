package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const testTablePartition = "test_table_20240101"

func TestPartitionCache(t *testing.T) {
	// Create a temporary directory for test cache
	tempDir, err := os.MkdirTemp("", "cache_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Override home directory for testing
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tempDir)
	defer os.Setenv("HOME", originalHome)

	tableName := "test_table"

	t.Run("NewCache", func(t *testing.T) {
		cache, err := loadPartitionCache(tableName)
		if err != nil {
			t.Fatal(err)
		}
		if cache == nil {
			t.Fatal("cache should not be nil")
		}
		if len(cache.Entries) != 0 {
			t.Fatal("new cache should have no entries")
		}
	})

	t.Run("SetAndGetRowCount", func(t *testing.T) {
		cache := &PartitionCache{
			Entries: make(map[string]PartitionCacheEntry),
		}

		partition := testTablePartition
		count := int64(1000)
		cache.setRowCount(partition, count)

		// Get count for old date (should be cached)
		oldDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		gotCount, found := cache.getRowCount(partition, oldDate)
		if !found {
			t.Fatal("count should be found")
		}
		if gotCount != count {
			t.Fatalf("expected count %d, got %d", count, gotCount)
		}

		// Get count for today (should not be cached)
		today := time.Now().Truncate(24 * time.Hour)
		_, found = cache.getRowCount(partition, today)
		if found {
			t.Fatal("today's partition should not be cached")
		}
	})

	t.Run("SetAndGetFileMetadata", func(t *testing.T) {
		cache := &PartitionCache{
			Entries: make(map[string]PartitionCacheEntry),
		}

		partition := testTablePartition
		s3Key := "export/test_table/2024/01/2024-01-01.jsonl.zst"
		compressedSize := int64(1024)
		uncompressedSize := int64(5120)
		md5 := "abc123"

		cache.setFileMetadata(partition, s3Key, compressedSize, uncompressedSize, md5, true)

		oldDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		size, hash, found := cache.getFileMetadata(partition, s3Key, oldDate)
		if !found {
			t.Fatal("metadata should be found")
		}
		if size != compressedSize {
			t.Fatalf("expected size %d, got %d", compressedSize, size)
		}
		if hash != md5 {
			t.Fatalf("expected hash %s, got %s", md5, hash)
		}

		// Check S3 upload status
		entry := cache.Entries[partition]
		if !entry.S3Uploaded {
			t.Fatal("S3 upload status should be true")
		}
		if entry.UncompressedSize != uncompressedSize {
			t.Fatalf("expected uncompressed size %d, got %d", uncompressedSize, entry.UncompressedSize)
		}
	})

	t.Run("PreserveMetadataWhenUpdatingRowCount", func(t *testing.T) {
		cache := &PartitionCache{
			Entries: make(map[string]PartitionCacheEntry),
		}

		partition := testTablePartition
		s3Key := "export/test_table/2024/01/2024-01-01.jsonl.zst"

		// Set file metadata first
		cache.setFileMetadata(partition, s3Key, 1024, 5120, "abc123", true)

		// Update row count
		cache.setRowCount(partition, 2000)

		// Check that file metadata is preserved
		entry := cache.Entries[partition]
		if entry.FileSize != 1024 {
			t.Fatal("file size should be preserved")
		}
		if entry.FileMD5 != "abc123" {
			t.Fatal("MD5 should be preserved")
		}
		if !entry.S3Uploaded {
			t.Fatal("S3 upload status should be preserved")
		}
		if entry.RowCount != 2000 {
			t.Fatal("row count should be updated")
		}
	})

	t.Run("SetAndGetError", func(t *testing.T) {
		cache := &PartitionCache{
			Entries: make(map[string]PartitionCacheEntry),
		}

		partition := testTablePartition
		errMsg := "connection failed"

		cache.setError(partition, errMsg)

		entry := cache.Entries[partition]
		if entry.LastError != errMsg {
			t.Fatalf("expected error %s, got %s", errMsg, entry.LastError)
		}
		if entry.ErrorTime.IsZero() {
			t.Fatal("error time should be set")
		}
	})

	t.Run("CleanExpired", func(t *testing.T) {
		cache := &PartitionCache{
			Entries: make(map[string]PartitionCacheEntry),
		}

		// Add entry with old row count
		oldEntry := PartitionCacheEntry{
			RowCount:  1000,
			CountTime: time.Now().Add(-25 * time.Hour),
			FileSize:  1024,
			FileMD5:   "abc123",
			FileTime:  time.Now(),
		}
		cache.Entries["old_partition"] = oldEntry

		// Add entry with recent row count
		newEntry := PartitionCacheEntry{
			RowCount:  2000,
			CountTime: time.Now(),
		}
		cache.Entries["new_partition"] = newEntry

		cache.cleanExpired()

		// Old row count should be cleared but file metadata preserved
		oldPartition := cache.Entries["old_partition"]
		if oldPartition.RowCount != 0 {
			t.Fatal("old row count should be cleared")
		}
		if oldPartition.FileSize != 1024 {
			t.Fatal("file metadata should be preserved")
		}

		// New row count should be preserved
		newPartition := cache.Entries["new_partition"]
		if newPartition.RowCount != 2000 {
			t.Fatal("recent row count should be preserved")
		}
	})

	t.Run("SaveAndLoad", func(t *testing.T) {
		cache := &PartitionCache{
			Entries: make(map[string]PartitionCacheEntry),
		}

		// Add test data
		cache.Entries["partition1"] = PartitionCacheEntry{
			RowCount:         1000,
			CountTime:        time.Now(),
			FileSize:         2048,
			UncompressedSize: 10240,
			FileMD5:          "xyz789",
			S3Uploaded:       true,
		}

		// Save cache
		err := cache.save(tableName)
		if err != nil {
			t.Fatal(err)
		}

		// Load cache
		loaded, err := loadPartitionCache(tableName)
		if err != nil {
			t.Fatal(err)
		}

		// Verify data
		entry, exists := loaded.Entries["partition1"]
		if !exists {
			t.Fatal("partition1 should exist")
		}
		if entry.RowCount != 1000 {
			t.Fatal("row count mismatch")
		}
		if entry.FileSize != 2048 {
			t.Fatal("file size mismatch")
		}
		if !entry.S3Uploaded {
			t.Fatal("S3 upload status mismatch")
		}
	})

	t.Run("LegacyCacheMigration", func(t *testing.T) {
		// Create legacy cache file
		legacyCache := &RowCountCache{
			Counts: map[string]RowCountEntry{
				"legacy_partition": {
					Count:     5000,
					Timestamp: time.Now(),
				},
			},
		}

		// Save legacy cache
		cacheDir := filepath.Join(tempDir, ".postgresql-archiver", "cache")
		_ = os.MkdirAll(cacheDir, 0o755)
		legacyPath := filepath.Join(cacheDir, "legacy_table_counts.json")

		data, _ := json.MarshalIndent(legacyCache, "", "  ")
		_ = os.WriteFile(legacyPath, data, 0o644)

		// Load should migrate
		cache, err := loadPartitionCache("legacy_table")
		if err != nil {
			t.Fatal(err)
		}

		entry, exists := cache.Entries["legacy_partition"]
		if !exists {
			t.Fatal("migrated partition should exist")
		}
		if entry.RowCount != 5000 {
			t.Fatal("migrated row count mismatch")
		}

		// Legacy file should be removed
		if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
			t.Fatal("legacy cache file should be removed after migration")
		}
	})
}

func TestCachePathGeneration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "cache_path_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tempDir)
	defer os.Setenv("HOME", originalHome)

	tableName := "my_table"
	expectedPath := filepath.Join(tempDir, ".postgresql-archiver", "cache", "my_table_metadata.json")

	actualPath := getCachePath(tableName)
	if actualPath != expectedPath {
		t.Fatalf("expected path %s, got %s", expectedPath, actualPath)
	}

	// Check directory was created
	dir := filepath.Dir(actualPath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Fatal("cache directory should be created")
	}
}
