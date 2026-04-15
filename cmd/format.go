package cmd

import (
	"os"
	"path/filepath"
	"time"
)

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

// DataDir returns the data directory from env or default (~/.chb/data), creating it if needed
func DataDir() string {
	var dir string
	if d := os.Getenv("DATA_DIR"); d != "" {
		dir = d
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			dir = "./data"
		} else {
			dir = filepath.Join(home, ".chb", "data")
		}
	}
	return ensureManagedDataDir(dir)
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
