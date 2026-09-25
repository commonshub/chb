package cmd

// Integrity manifests.
//
// Every completed month gets a hashes.json: one entry per provider
// archive (what the raw data contains, how big it is, and a content hash),
// plus one hash for the whole month. Two chb instances that hold the same
// raw data produce the same hashes, so "do we have the right data?" is one
// comparison — and because a hash reveals nothing about its input, the
// manifest is public. It lives once at the month root (YYYY/MM/hashes.json,
// world-readable like the tier-less month directory itself) rather than in
// each audience tier: the tiers separate what people may *read*, and a
// hash is the same for everyone.
//
// Instance-independent by construction: JSON archives are canonicalised
// before hashing (keys sorted, no whitespace, and the keys that only record
// *when this instance fetched* — cachedAt, fetchedAt, generatedAt, … — are
// dropped), non-JSON archives (ics, csv, images) are hashed as bytes, and
// the per-instance Odoo outbox (providers/odoo/…/pending/) is excluded.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const integrityFile = "hashes.json"

// legacyIntegrityFile is where v3.12.0 wrote the manifest, once per tier;
// generateIntegrity removes those copies.
const legacyIntegrityFile = "integrity.json"

// integrityPath is YYYY/MM/hashes.json, or latest/hashes.json when year is "latest".
func integrityPath(dataDir, year, month string) string {
	if year == "latest" {
		return filepath.Join(dataDir, "latest", integrityFile)
	}
	return filepath.Join(dataDir, year, month, integrityFile)
}

// ProviderIntegrity describes one provider archive for one month.
type ProviderIntegrity struct {
	Provider string         `json:"provider"` // "stripe", "odoo/commonshub", …
	Summary  string         `json:"summary"`  // "1,234 transactions"
	Stats    map[string]int `json:"stats,omitempty"`
	Files    int            `json:"files"`
	Bytes    int64          `json:"bytes"`
	Hash     string         `json:"hash"` // sha256, hex
}

// MonthIntegrityFile is YYYY/MM/hashes.json.
type MonthIntegrityFile struct {
	Month       string              `json:"month"`
	GeneratedAt string              `json:"generatedAt"`
	Algorithm   string              `json:"algorithm"` // "sha256/canonical-json-v1"
	Providers   int                 `json:"providers"`
	Files       int                 `json:"files"`
	Bytes       int64               `json:"bytes"`
	Hash        string              `json:"hash"`
	Entries     []ProviderIntegrity `json:"entries"`
}

// IntegrityIndexFile is latest/hashes.json: every month's hash.
type IntegrityIndexFile struct {
	GeneratedAt string                `json:"generatedAt"`
	Algorithm   string                `json:"algorithm"`
	Months      []IntegrityIndexEntry `json:"months"`
}

type IntegrityIndexEntry struct {
	Month     string `json:"month"`
	Providers int    `json:"providers"`
	Files     int    `json:"files"`
	Bytes     int64  `json:"bytes"`
	Hash      string `json:"hash"`
}

const integrityAlgorithm = "sha256/canonical-json-v1"

// volatileJSONKeys only record when *this instance* fetched or wrote the
// file; they differ between instances holding identical data.
var volatileJSONKeys = map[string]bool{
	"cachedAt": true, "fetchedAt": true, "generatedAt": true, "updatedAt": true,
	"syncedAt": true, "lastSync": true, "lastSyncAt": true, "pulledAt": true,
}

// canonicalJSON re-serialises a JSON document with sorted keys, no
// whitespace, exact numbers, and volatile keys removed. Non-JSON input is
// returned unchanged (ok=false).
func canonicalJSON(data []byte) ([]byte, bool) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return data, false
	}
	stripVolatile(v)
	out, err := json.Marshal(v) // Go sorts map keys
	if err != nil {
		return data, false
	}
	return out, true
}

func stripVolatile(v interface{}) {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, sub := range t {
			if volatileJSONKeys[k] {
				delete(t, k)
				continue
			}
			stripVolatile(sub)
		}
	case []interface{}:
		for _, sub := range t {
			stripVolatile(sub)
		}
	}
}

// integrityExcluded reports archive paths that are per-instance state, not
// provider data.
func integrityExcluded(rel string) bool {
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		if part == "pending" || strings.HasPrefix(part, ".") {
			return true
		}
	}
	return false
}

// providerArchiveUnits splits providers/ into hashing units: one per
// provider, except Odoo, where each database namespace (providers/odoo/<db>)
// is its own unit — an instance mirroring an extra test database must not
// change the hash of the production one.
func providerArchiveUnits(providersDir string) ([]string, error) {
	entries, err := os.ReadDir(providersDir)
	if err != nil {
		return nil, err
	}
	var units []string
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if e.Name() == "odoo" {
			dbs, _ := os.ReadDir(filepath.Join(providersDir, "odoo"))
			for _, db := range dbs {
				if db.IsDir() && !strings.HasPrefix(db.Name(), ".") {
					units = append(units, "odoo/"+db.Name())
				}
			}
			continue
		}
		units = append(units, e.Name())
	}
	sort.Strings(units)
	return units, nil
}

// hashProviderUnit walks one unit (sorted by relative path) and returns its
// entry: relative path + canonical content hash per file, folded into one.
func hashProviderUnit(providersDir, unit string) (ProviderIntegrity, error) {
	root := filepath.Join(providersDir, filepath.FromSlash(unit))
	h := sha256.New()
	entry := ProviderIntegrity{Provider: unit, Stats: map[string]int{}}
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if integrityExcluded(rel) {
			return nil
		}
		files = append(files, rel)
		return nil
	})
	if err != nil {
		return entry, err
	}
	sort.Strings(files)
	for _, rel := range files {
		data, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			return entry, err
		}
		entry.Files++
		entry.Bytes += int64(len(data))
		content, isJSON := canonicalJSON(data)
		sum := sha256.Sum256(content)
		fmt.Fprintf(h, "%s\n%s\n", filepath.ToSlash(rel), hex.EncodeToString(sum[:]))
		countProviderStats(unit, rel, data, isJSON, entry.Stats)
	}
	entry.Hash = hex.EncodeToString(h.Sum(nil))
	entry.Summary = providerSummary(unit, entry.Stats, entry.Files)
	if len(entry.Stats) == 0 {
		entry.Stats = nil
	}
	return entry, nil
}

// countProviderStats accumulates what a file contributes to the unit's
// counts: array lengths under the keys each provider uses, plus files of
// a kind (channels, calendars, attachments, statements).
func countProviderStats(unit, rel string, data []byte, isJSON bool, stats map[string]int) {
	base := filepath.Base(rel)
	ext := strings.ToLower(filepath.Ext(base))
	switch {
	case ext == ".xml" && unit == "intervat":
		stats["declarations"]++
		return
	case ext == ".ics":
		stats["calendars"]++
		stats["events"] += bytes.Count(data, []byte("BEGIN:VEVENT"))
		return
	case ext == ".csv":
		stats["statements"]++
		if n := bytes.Count(data, []byte("\n")); n > 1 {
			stats["rows"] += n - 1
		}
		return
	case ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".gif" || ext == ".webp" || ext == ".pdf":
		stats["attachments"]++
		return
	}
	if !isJSON {
		return
	}
	var doc map[string]json.RawMessage
	if json.Unmarshal(data, &doc) != nil {
		return
	}
	count := func(key string) int {
		raw, ok := doc[key]
		if !ok {
			return 0
		}
		var arr []json.RawMessage
		if json.Unmarshal(raw, &arr) == nil {
			return len(arr)
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(raw, &obj) == nil {
			return len(obj)
		}
		return 0
	}
	switch strings.SplitN(unit, "/", 2)[0] {
	case "discord":
		if base == "messages.json" {
			stats["channels"]++
			stats["messages"] += count("messages")
		}
	case "stripe":
		for _, k := range []string{"transactions", "charges", "customers", "products", "subscriptions", "payouts"} {
			stats[k] += count(k)
		}
	case "odoo":
		if strings.HasPrefix(filepath.ToSlash(rel), "journals/") {
			stats["journals"]++
			stats["lines"] += count("lines")
		}
		if strings.HasPrefix(filepath.ToSlash(rel), "private/") {
			return // same documents as the public projection, counted once
		}
		for _, k := range []string{"invoices", "bills", "partners", "mappings"} {
			stats[k] += count(k)
		}
	case "etherscan":
		stats["accounts"]++
		stats["transfers"] += count("transactions")
	case "monerium":
		stats["accounts"]++
		stats["orders"] += count("orders")
	case "nostr":
		stats["annotations"] += count("transactions") + count("addresses")
	default:
		for _, k := range []string{"transactions", "entries", "items", "records"} {
			stats[k] += count(k)
		}
	}
}

// providerSummary renders the counts the way a person would say them.
func providerSummary(unit string, stats map[string]int, files int) string {
	order := map[string][]string{
		"discord":   {"channels", "messages", "attachments"},
		"stripe":    {"transactions", "charges", "customers", "products", "subscriptions", "payouts"},
		"odoo":      {"journals", "lines", "invoices", "bills", "partners"},
		"etherscan": {"accounts", "transfers"},
		"monerium":  {"accounts", "orders"},
		"ics":       {"calendars", "events"},
		"nostr":     {"annotations"},
		"intervat":  {"declarations"},
	}
	keys, ok := order[strings.SplitN(unit, "/", 2)[0]]
	if !ok {
		keys = make([]string, 0, len(stats))
		for k := range stats {
			keys = append(keys, k)
		}
		sort.Strings(keys)
	}
	var parts []string
	for _, k := range keys {
		if n, ok := stats[k]; ok && n > 0 {
			parts = append(parts, fmt.Sprintf("%s %s", formatCount(n), k))
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("%s %s", formatCount(files), pluralWord(files, "file", "files"))
	}
	return strings.Join(parts, ", ")
}

func formatCount(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

func pluralWord(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// computeMonthIntegrity hashes every provider unit of a month and folds the
// unit hashes (sorted by provider) into the month hash.
func computeMonthIntegrity(dataDir, year, month string) (MonthIntegrityFile, error) {
	providersDir := filepath.Join(dataDir, year, month, "providers")
	units, err := providerArchiveUnits(providersDir)
	if err != nil {
		return MonthIntegrityFile{}, err
	}
	out := MonthIntegrityFile{
		Month:       year + "-" + month,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Algorithm:   integrityAlgorithm,
	}
	h := sha256.New()
	for _, unit := range units {
		entry, err := hashProviderUnit(providersDir, unit)
		if err != nil {
			return out, err
		}
		if entry.Files == 0 {
			continue
		}
		out.Entries = append(out.Entries, entry)
		out.Files += entry.Files
		out.Bytes += entry.Bytes
		fmt.Fprintf(h, "%s\n%s\n", entry.Provider, entry.Hash)
	}
	out.Providers = len(out.Entries)
	out.Hash = hex.EncodeToString(h.Sum(nil))
	return out, nil
}

// integrityIsStale reports whether a month's manifest is missing or older
// than any provider file (a backfill or a re-sync changes the data).
func integrityIsStale(dataDir, year, month string) bool {
	manifest := integrityPath(dataDir, year, month)
	info, err := os.Stat(manifest)
	if err != nil {
		return true
	}
	stale := false
	providersDir := filepath.Join(dataDir, year, month, "providers")
	_ = filepath.WalkDir(providersDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || stale {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(providersDir, path)
		if integrityExcluded(rel) {
			return nil
		}
		if fi, err := d.Info(); err == nil && fi.ModTime().After(info.ModTime()) {
			stale = true
		}
		return nil
	})
	return stale
}

// completedMonths lists YYYY/MM directories strictly before the current
// Brussels month, oldest first.
func completedMonths(dataDir string) []string {
	current := time.Now().In(BrusselsTZ()).Format("2006-01")
	var months []string
	years, _ := os.ReadDir(dataDir)
	for _, y := range years {
		if !y.IsDir() || !isYearSegment(y.Name()) {
			continue
		}
		ms, _ := os.ReadDir(filepath.Join(dataDir, y.Name()))
		for _, m := range ms {
			if !m.IsDir() || !isMonthSegment(m.Name()) {
				continue
			}
			ym := y.Name() + "-" + m.Name()
			if ym >= current {
				continue
			}
			if _, err := os.Stat(filepath.Join(dataDir, y.Name(), m.Name(), "providers")); err != nil {
				continue
			}
			months = append(months, ym)
		}
	}
	sort.Strings(months)
	return months
}

// generateIntegrity writes hashes.json for every completed month whose
// manifest is missing or stale (all of them with force), then rebuilds the
// latest/hashes.json index. Returns the number of months hashed.
func generateIntegrity(dataDir string, only string, force bool) (int, error) {
	months := completedMonths(dataDir)
	if only != "" {
		months = []string{only}
	}
	hashed := 0
	for _, ym := range months {
		year, month := ym[:4], ym[5:]
		if !force && !integrityIsStale(dataDir, year, month) {
			continue
		}
		mf, err := computeMonthIntegrity(dataDir, year, month)
		if err != nil {
			return hashed, fmt.Errorf("%s: %w", ym, err)
		}
		data, err := json.MarshalIndent(mf, "", "  ")
		if err != nil {
			return hashed, err
		}
		if err := writeDataFile(integrityPath(dataDir, year, month), data); err != nil {
			return hashed, fmt.Errorf("%s: %w", ym, err)
		}
		hashed++
	}
	removeLegacyIntegrityFiles(dataDir, months)
	if err := rebuildIntegrityIndex(dataDir); err != nil {
		return hashed, err
	}
	return hashed, nil
}

func rebuildIntegrityIndex(dataDir string) error {
	index := IntegrityIndexFile{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Algorithm:   integrityAlgorithm,
	}
	for _, ym := range completedMonths(dataDir) {
		data, err := os.ReadFile(integrityPath(dataDir, ym[:4], ym[5:]))
		if err != nil {
			continue
		}
		var mf MonthIntegrityFile
		if json.Unmarshal(data, &mf) != nil {
			continue
		}
		index.Months = append(index.Months, IntegrityIndexEntry{
			Month: mf.Month, Providers: mf.Providers, Files: mf.Files, Bytes: mf.Bytes, Hash: mf.Hash,
		})
	}
	if len(index.Months) == 0 {
		return nil
	}
	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	return writeDataFile(integrityPath(dataDir, "latest", ""), data)
}

// removeLegacyIntegrityFiles deletes the integrity.json copies that v3.12.0
// wrote in each tier and in the legacy generated/ mirror (month and
// latest/), now that the manifest lives once at the month root.
func removeLegacyIntegrityFiles(dataDir string, months []string) {
	for _, ym := range months {
		for _, a := range Audiences {
			os.Remove(audiencePath(dataDir, ym[:4], ym[5:], a, legacyIntegrityFile))
		}
		os.Remove(filepath.Join(dataDir, ym[:4], ym[5:], legacyGeneratedDirName, legacyIntegrityFile))
	}
	for _, a := range Audiences {
		os.Remove(audiencePath(dataDir, "latest", "", a, legacyIntegrityFile))
	}
	os.Remove(filepath.Join(dataDir, "latest", legacyGeneratedDirName, legacyIntegrityFile))
}

// Integrity is `chb integrity [YYYY/MM] [--force] [--json]`.
func Integrity(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printIntegrityHelp()
		return nil
	}
	force := HasFlag(args, "--force")
	asJSON := HasFlag(args, "--json")
	dataDir := DataDir()
	only := ""
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			continue
		}
		parts := strings.Split(strings.ReplaceAll(a, "-", "/"), "/")
		if len(parts) == 2 && isYearSegment(parts[0]) && isMonthSegment(parts[1]) {
			only = parts[0] + "-" + parts[1]
		} else {
			return fmt.Errorf("expected YYYY/MM, got %q", a)
		}
	}
	hashed, err := generateIntegrity(dataDir, only, force)
	if err != nil {
		return err
	}
	months := completedMonths(dataDir)
	if only != "" {
		months = []string{only}
	}
	if asJSON {
		var all []MonthIntegrityFile
		for _, ym := range months {
			if mf, ok := readMonthIntegrity(dataDir, ym); ok {
				all = append(all, mf)
			}
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(all)
	}
	for _, ym := range months {
		mf, ok := readMonthIntegrity(dataDir, ym)
		if !ok {
			continue
		}
		fmt.Printf("%s%s%s  %s, %s  %s%s%s\n", Fmt.Bold, strings.ReplaceAll(mf.Month, "-", ""), Fmt.Reset,
			Pluralize(mf.Providers, "provider", ""), formatKB(mf.Bytes), Fmt.Dim, mf.Hash, Fmt.Reset)
		for _, e := range mf.Entries {
			fmt.Printf("   %-18s %-48s %s%s%s\n", e.Provider, e.Summary, Fmt.Dim, e.Hash[:16]+"…", Fmt.Reset)
		}
	}
	if hashed > 0 {
		fmt.Printf("\n  %s✓ hashed %s%s\n", Fmt.Green, Pluralize(hashed, "month", ""), Fmt.Reset)
	}
	return nil
}

func readMonthIntegrity(dataDir, ym string) (MonthIntegrityFile, bool) {
	data, err := os.ReadFile(integrityPath(dataDir, ym[:4], ym[5:]))
	if err != nil {
		return MonthIntegrityFile{}, false
	}
	var mf MonthIntegrityFile
	if json.Unmarshal(data, &mf) != nil {
		return MonthIntegrityFile{}, false
	}
	return mf, true
}

func formatKB(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	return fmt.Sprintf("%s KB", formatCount(int(b/1024)))
}

func printIntegrityHelp() {
	fmt.Printf(`
chb integrity — content hashes of the raw provider archives, per month

USAGE
  chb integrity [YYYY/MM] [--force] [--json]

Writes YYYY/MM/hashes.json for every completed month whose manifest is
missing or older than its provider files (--force: all of them), and
latest/hashes.json with every month's hash. Also runs at the end of
'chb generate'. Hashes are public, so the file sits once at the month
root instead of in each audience tier.

Two instances holding the same raw data produce the same hashes: JSON is
canonicalised (sorted keys, fetch timestamps dropped) before hashing, and
the Odoo outbox (pending/) is excluded. Hashes are public.
`)
}

func nowBrusselsYearMonth() string { return time.Now().In(BrusselsTZ()).Format("2006-01") }

func timeNowPlus(seconds int) time.Time { return time.Now().Add(time.Duration(seconds) * time.Second) }
