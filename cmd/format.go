package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
	_ "time/tzdata"
)

// Pluralize returns "<n> <noun>" with the noun's number adjusted to match
// n. When plural is empty, the plural form is `singular + "s"`. Negative
// counts are pluralized just like positive ones above 1 (e.g. -3 items).
//
//	Pluralize(1, "tx", "")          → "1 tx"
//	Pluralize(3, "tx", "")          → "3 txs"
//	Pluralize(2, "summary", "summaries") → "2 summaries"
//	Pluralize(1, "fetch", "fetches")     → "1 fetch"
func Pluralize(n int, singular, plural string) string {
	if n == 1 || n == -1 {
		return fmt.Sprintf("%d %s", n, singular)
	}
	if plural == "" {
		plural = defaultPlural(singular)
	}
	return fmt.Sprintf("%d %s", n, plural)
}

// pluralIrregulars holds short abbreviations and other words whose English
// plural doesn't follow the spelling rules. "tx" looks like it should
// become "txes" by the x→es rule, but the convention is "txs" (it's an
// abbreviation for "transaction", not a word in its own right).
var pluralIrregulars = map[string]string{
	"tx": "txs",
}

// defaultPlural applies the common English plural rules:
//   - consonant + y → ies (category → categories, party → parties)
//   - vowel + y → s (day → days, key → keys, relay → relays)
//   - ends in s/x/z/sh/ch → es (box → boxes, fetch → fetches)
//   - otherwise → s
//
// Common abbreviations that break the rules (e.g. "tx" → "txs") are in
// pluralIrregulars. Caller can always pass an explicit plural to override
// (e.g. for "person" → "people").
func defaultPlural(singular string) string {
	if singular == "" {
		return ""
	}
	if p, ok := pluralIrregulars[singular]; ok {
		return p
	}
	last := singular[len(singular)-1]
	if last == 'y' && len(singular) >= 2 {
		prev := singular[len(singular)-2]
		switch prev {
		case 'a', 'e', 'i', 'o', 'u':
			return singular + "s"
		default:
			return singular[:len(singular)-1] + "ies"
		}
	}
	if last == 's' || last == 'x' || last == 'z' {
		return singular + "es"
	}
	if len(singular) >= 2 {
		tail := singular[len(singular)-2:]
		if tail == "sh" || tail == "ch" {
			return singular + "es"
		}
	}
	return singular + "s"
}

const TIMEZONE = "Europe/Brussels"

var brusselsTZ *time.Location

func init() {
	var err error
	brusselsTZ, err = time.LoadLocation(TIMEZONE)
	if err != nil {
		brusselsTZ = time.UTC
	}
}

func BrusselsTZ() *time.Location {
	return brusselsTZ
}

func FmtDate(t time.Time) string {
	t = t.In(brusselsTZ)
	return t.Format("Mon 02 Jan")
}

func FmtTime(t time.Time) string {
	t = t.In(brusselsTZ)
	return t.Format("15:04")
}

func Pad(s string, length int) string {
	if len(s) >= length {
		return s[:length]
	}
	return s + spaces(length-len(s))
}

func Truncate(s string, length int) string {
	if len(s) <= length {
		return s
	}
	if length <= 1 {
		return s[:length]
	}
	return s[:length-1] + "…"
}

func spaces(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = ' '
	}
	return string(b)
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func FormatDateLong(t time.Time) string {
	t = t.In(brusselsTZ)
	return t.Format("Monday, January 2, 2006")
}

func FormatTimeBrussels(t time.Time) string {
	t = t.In(brusselsTZ)
	return t.Format("15:04")
}

func TruncateDescription(desc string, maxLen int) string {
	if desc == "" {
		return ""
	}
	if len(desc) <= maxLen {
		return desc
	}
	return desc[:maxLen] + "..."
}

// DataDir returns the generated data directory.
// DATA_DIR is kept as an explicit/backward-compatible override; otherwise it
// defaults to APP_DATA_DIR/data.
func DataDir() string {
	dir := resolveDataDir()
	return ensureManagedDataDir(dir)
}

func resolveDataDir() string {
	if d := os.Getenv("DATA_DIR"); d != "" {
		return d
	}
	return filepath.Join(AppDataDir(), "data")
}

// writeMonthFile writes data to dataDir/year/month/<relPath> AND mirrors
// it to dataDir/latest/<relPath> so the latest/ directory always has the most
// recent version of every file across all sources.
func writeMonthFile(dataDir, year, month, relPath string, data []byte) error {
	// Primary: YYYY/MM/<relPath> (or just dataDir/latest/<relPath> when year="latest")
	monthDst := filepath.Join(dataDir, year, month, relPath)
	if err := writeDataFile(monthDst, data); err != nil {
		return err
	}

	// Mirror month files to latest/ (not year files, not latest/ itself)
	if year != "latest" && month != "" {
		latestDst := filepath.Join(dataDir, "latest", relPath)
		if err := writeDataFile(latestDst, data); err != nil {
			return err
		}
	}

	return nil
}

func displayMonthRelPath(year, month, relPath string) string {
	if year == "latest" || month == "" {
		return filepath.ToSlash(filepath.Join(year, relPath))
	}
	return filepath.ToSlash(filepath.Join(year, month, relPath))
}
