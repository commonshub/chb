// Package bitcoin archives on-chain Bitcoin transactions for a tracked
// address, fetched from an Esplora-compatible API (mempool.space by default,
// override with MEMPOOL_API_URL). Raw transaction objects are stored
// unchanged, grouped by the Brussels month of their confirmation time.
package bitcoin

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/CommonsHub/chb/providers"
)

const Source = "bitcoin"

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []providers.File {
	return []providers.File{
		{Name: "<account>.json", Description: "Monthly confirmed Bitcoin transactions touching the account address (raw Esplora JSON)."},
	}
}

// CacheFile is one month of raw transactions for one address.
type CacheFile struct {
	Address      string            `json:"address"`
	CachedAt     string            `json:"cachedAt"`
	Transactions []json.RawMessage `json:"transactions"`
}

func RelPath(elems ...string) string {
	parts := append([]string{"providers", Source}, elems...)
	return filepath.Join(parts...)
}

func Path(dataDir, year, month string, elems ...string) string {
	return filepath.Join(append([]string{dataDir, year, month}, RelPath(elems...))...)
}

func FileName(slug string) string {
	return strings.ToLower(strings.TrimSpace(slug)) + ".json"
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

// SameTransactions reports whether two raw transaction lists are byte-identical
// (after compaction), so unchanged months are not rewritten.
func SameTransactions(a, b []json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		var ca, cb bytes.Buffer
		if json.Compact(&ca, a[i]) != nil || json.Compact(&cb, b[i]) != nil || !bytes.Equal(ca.Bytes(), cb.Bytes()) {
			return false
		}
	}
	return true
}

func WriteCache(path string, cache CacheFile) error {
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
