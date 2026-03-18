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

// DataDir returns the data directory from env or default
func DataDir() string {
	if d := os.Getenv("DATA_DIR"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "./data"
	}
	return filepath.Join(home, ".chb", "data")
}
