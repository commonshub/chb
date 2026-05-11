package etherscan

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
		{Name: "<chain>/<account>.<token>.json", Description: "Monthly ERC20 token transfers from Etherscan V2.", Private: false},
	}
}

func RelPath(chain string, elems ...string) string {
	parts := []string{"sources", Source}
	if chain != "" {
		parts = append(parts, normalize(chain))
	}
	parts = append(parts, elems...)
	return filepath.Join(parts...)
}

func Path(dataDir, year, month, chain string, elems ...string) string {
	parts := []string{dataDir, year}
	if month != "" {
		parts = append(parts, month)
	}
	parts = append(parts, RelPath(chain, elems...))
	return filepath.Join(parts...)
}

func FileName(slug, tokenSymbol string) string {
	return normalize(slug) + "." + tokenSymbol + ".json"
}

func WriteJSON(dataDir, year, month, chain string, v interface{}, elems ...string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	path := Path(dataDir, year, month, chain, elems...)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
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

func LatestCachedTxHash(filePath string) string {
	cache, ok := LoadCache(filePath)
	if !ok || len(cache.Transactions) == 0 {
		return ""
	}
	return cache.Transactions[0].Hash
}

func LatestCachedTxHashGlobal(dataDir, chain, filename string) string {
	yearDirs, err := os.ReadDir(dataDir)
	if err != nil {
		return ""
	}
	var latestYM string
	var latestPath string
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			ym := yd.Name() + "-" + md.Name()
			fp := Path(dataDir, yd.Name(), md.Name(), chain, filename)
			if _, err := os.Stat(fp); err == nil && ym > latestYM {
				latestYM = ym
				latestPath = fp
			}
		}
	}
	if latestPath == "" {
		return ""
	}
	return LatestCachedTxHash(latestPath)
}

func normalize(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.ReplaceAll(s, "_", "-")
	s = strings.Join(strings.Fields(s), "-")
	return s
}
