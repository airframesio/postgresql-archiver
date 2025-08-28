package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type RowCountCache struct {
	Counts map[string]RowCountEntry `json:"counts"`
}

type RowCountEntry struct {
	Count     int64     `json:"count"`
	Timestamp time.Time `json:"timestamp"`
}

func getCachePath(tableName string) string {
	homeDir, _ := os.UserHomeDir()
	cacheDir := filepath.Join(homeDir, ".postgresql-archiver", "cache")
	os.MkdirAll(cacheDir, 0755)
	return filepath.Join(cacheDir, fmt.Sprintf("%s_counts.json", tableName))
}

func loadCache(tableName string) (*RowCountCache, error) {
	cachePath := getCachePath(tableName)
	
	data, err := os.ReadFile(cachePath)
	if err != nil {
		if os.IsNotExist(err) {
			// Return empty cache if file doesn't exist
			return &RowCountCache{
				Counts: make(map[string]RowCountEntry),
			}, nil
		}
		return nil, err
	}
	
	var cache RowCountCache
	if err := json.Unmarshal(data, &cache); err != nil {
		// If cache is corrupted, return empty cache
		return &RowCountCache{
			Counts: make(map[string]RowCountEntry),
		}, nil
	}
	
	if cache.Counts == nil {
		cache.Counts = make(map[string]RowCountEntry)
	}
	
	return &cache, nil
}

func (c *RowCountCache) save(tableName string) error {
	cachePath := getCachePath(tableName)
	
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	
	return os.WriteFile(cachePath, data, 0644)
}

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

func (c *RowCountCache) cleanExpired() {
	for partition, entry := range c.Counts {
		if time.Since(entry.Timestamp) > 24*time.Hour {
			delete(c.Counts, partition)
		}
	}
}