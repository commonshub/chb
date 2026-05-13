package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
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
		plural = singular + "s"
	}
	return fmt.Sprintf("%d %s", n, plural)
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

	// Mirror to latest/ (skip if already writing to latest/)
	if year != "latest" {
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
