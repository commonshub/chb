package etherscan

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/CommonsHub/chb/providers"
)

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []providers.File {
	return []providers.File{
		{Name: "<chain>/<account>.<0xaddr>.<token>.json", Description: "Monthly ERC20 token transfers from Etherscan V2.", Private: false},
	}
}

func RelPath(chain string, elems ...string) string {
	parts := []string{"providers", Source}
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

// ShortAddr renders a wallet/contract address as 0x<first4>-<last4>, lowercased
// (e.g. 0x6fDF…912CBf -> 0x6fdf-2cbf). Used to disambiguate provider filenames
// that share a slug+token. Returns the cleaned input unchanged when it is too
// short to abbreviate (including the empty string).
func ShortAddr(addr string) string {
	addr = strings.ToLower(strings.TrimSpace(addr))
	hex := strings.TrimPrefix(addr, "0x")
	if len(hex) < 8 {
		return addr
	}
	return "0x" + hex[:4] + "-" + hex[len(hex)-4:]
}

// FileName builds the per-account monthly archive name:
//
//	{slug}.{0xshort-addr}.{tokenSymbol}.json   e.g. savings.0x6fdf-2cbf.EURe.json
//
// When addr is empty it falls back to the legacy address-less name so callers
// that lack a wallet address still produce a name FindFile can match.
func FileName(slug, addr, tokenSymbol string) string {
	short := ShortAddr(addr)
	if short == "" {
		return legacyFileName(slug, tokenSymbol)
	}
	return normalize(slug) + "." + short + "." + tokenSymbol + ".json"
}

// legacyFileName is the older address-less name {slug}.{token}.json, kept so
// FindFile and `chb clean` can still locate files written before the address
// was added to the filename.
func legacyFileName(slug, tokenSymbol string) string {
	return normalize(slug) + "." + tokenSymbol + ".json"
}

// FindFileForAddr locates the monthly archive for one specific account,
// identified by (slug, addr, tokenSymbol). When addr is non-empty it matches
// only that wallet's address-qualified file (plus the legacy address-less name
// for the same slug, as a pre-clean transition) — so two accounts that share a
// slug across a wallet migration never read each other's files. With an empty
// addr it falls back to the slug+token glob via FindFile.
func FindFileForAddr(dataDir, year, month, chain, slug, addr, tokenSymbol string) (string, bool) {
	short := ShortAddr(addr)
	if short == "" {
		return FindFile(dataDir, year, month, chain, slug, tokenSymbol)
	}
	dir := Path(dataDir, year, month, chain)
	exact := filepath.Join(dir, normalize(slug)+"."+short+"."+tokenSymbol+".json")
	if _, err := os.Stat(exact); err == nil {
		return exact, true
	}
	legacy := filepath.Join(dir, legacyFileName(slug, tokenSymbol))
	if _, err := os.Stat(legacy); err == nil {
		return legacy, true
	}
	return "", false
}

// FindFile locates the monthly archive for (slug, tokenSymbol) in a chain dir,
// matching both the current {slug}.{addr}.{token}.json layout and the legacy
// address-less {slug}.{token}.json name. Returns ("", false) when none exists.
// It matches regardless of the embedded address; account-level callers that
// know their wallet should prefer FindFileForAddr to avoid conflating two
// wallets that share a slug.
func FindFile(dataDir, year, month, chain, slug, tokenSymbol string) (string, bool) {
	dir := Path(dataDir, year, month, chain)
	// Prefer the address-qualified name.
	if matches, _ := filepath.Glob(filepath.Join(dir, normalize(slug)+".*."+tokenSymbol+".json")); len(matches) > 0 {
		return matches[0], true
	}
	legacy := filepath.Join(dir, legacyFileName(slug, tokenSymbol))
	if _, err := os.Stat(legacy); err == nil {
		return legacy, true
	}
	return "", false
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

func LatestCachedTxHashGlobal(dataDir, chain, slug, addr, tokenSymbol string) string {
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
			if fp, ok := FindFileForAddr(dataDir, yd.Name(), md.Name(), chain, slug, addr, tokenSymbol); ok && ym > latestYM {
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
