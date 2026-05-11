package monerium

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/CommonsHub/chb/sources"
)

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []sources.File {
	return []sources.File{
		{Name: "<account>.json", Description: "Monthly Monerium orders.", Private: true},
	}
}

func RelPath(elems ...string) string {
	parts := append([]string{"sources", Source}, elems...)
	return filepath.Join(parts...)
}

func Path(dataDir, year, month string, elems ...string) string {
	parts := []string{dataDir, year}
	if month != "" {
		parts = append(parts, month)
	}
	parts = append(parts, RelPath(elems...))
	return filepath.Join(parts...)
}

func FileName(slug string) string {
	return normalize(slug) + ".json"
}

func WriteJSON(dataDir, year, month string, v interface{}, elems ...string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	path := Path(dataDir, year, month, elems...)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	_ = os.Chmod(filepath.Dir(path), 0700)
	_ = os.Chmod(path, 0600)
	return nil
}

func LoadCache(path string) (CacheFile, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return CacheFile{}, false
	}
	var cache CacheFile
	if json.Unmarshal(data, &cache) != nil {
		return CacheFile{}, false
	}
	return cache, true
}

func LatestCachedOrderID(filePath string) string {
	cache, ok := LoadCache(filePath)
	if !ok || len(cache.Orders) == 0 {
		return ""
	}
	return cache.Orders[0].ID
}

func normalize(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.ReplaceAll(s, "_", "-")
	s = strings.Join(strings.Fields(s), "-")
	return s
}
