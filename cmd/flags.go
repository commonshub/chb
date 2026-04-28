package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func HasFlag(args []string, flags ...string) bool {
	for _, a := range args {
		for _, f := range flags {
			if a == f {
				return true
			}
		}
	}
	return false
}

// filterFlag returns args with every occurrence of the given flag removed.
// Only strips boolean flags — does not consume a following value.
func filterFlag(args []string, flag string) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		if a == flag {
			continue
		}
		out = append(out, a)
	}
	return out
}

func GetOption(args []string, flags ...string) string {
	for _, flag := range flags {
		for i, a := range args {
			if a == flag && i+1 < len(args) {
				return args[i+1]
			}
			if strings.HasPrefix(a, flag+"=") {
				return strings.SplitN(a, "=", 2)[1]
			}
		}
	}
	return ""
}

func GetNumber(args []string, flags []string, defaultVal int) int {
	val := GetOption(args, flags...)
	if val == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return n
}

// ParseYearMonthArg extracts a positional year or year/month argument from args.
// Accepts formats: "2025", "2025/11", "2025/1".
// Returns (year, month, found). If only year, month is "".
// month is always zero-padded (e.g. "01").
func ParseYearMonthArg(args []string) (year string, month string, found bool) {
	// Skip flags and their values
	skipNext := false
	for _, a := range args {
		if skipNext {
			skipNext = false
			continue
		}
		if strings.HasPrefix(a, "--") || strings.HasPrefix(a, "-") {
			// Flags that take a value
			switch a {
			case "--since", "--until", "--month", "--channel", "--room", "--account",
				"--currency", "--limit", "--skip", "-n":
				skipNext = true
			}
			continue
		}
		// Try to parse as year or year/month
		// Supported formats: YYYY, YYYY/MM, YYYY-MM, YYYYMM
		parts := strings.SplitN(strings.ReplaceAll(a, "-", "/"), "/", 2)

		// Handle YYYYMM (no separator)
		if len(parts) == 1 && len(parts[0]) == 6 {
			parts = []string{parts[0][:4], parts[0][4:6]}
		}

		if len(parts[0]) != 4 {
			continue
		}
		y, err := strconv.Atoi(parts[0])
		if err != nil || y < 2000 || y > 2100 {
			continue
		}
		year = parts[0]
		found = true
		if len(parts) == 2 {
			m, err := strconv.Atoi(parts[1])
			if err != nil || m < 1 || m > 12 {
				continue
			}
			month = fmt.Sprintf("%02d", m)
		}
		return
	}
	return "", "", false
}

func ParseSinceDate(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	clean := strings.ReplaceAll(s, "-", "")
	if len(clean) != 8 {
		return time.Time{}, false
	}
	t, err := time.Parse("20060102", clean)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// ParseSinceMonth parses a month string in formats: YYYY/MM, YYYYMM, YYYY-MM
// Returns year, month as strings (zero-padded month), and whether parsing succeeded.
func ParseSinceMonth(s string) (year string, month string, ok bool) {
	if s == "" {
		return "", "", false
	}
	// Normalize: remove slashes and dashes
	clean := strings.ReplaceAll(strings.ReplaceAll(s, "/", ""), "-", "")
	if len(clean) != 6 {
		return "", "", false
	}
	y, err := strconv.Atoi(clean[:4])
	if err != nil || y < 2000 || y > 2100 {
		return "", "", false
	}
	m, err := strconv.Atoi(clean[4:6])
	if err != nil || m < 1 || m > 12 {
		return "", "", false
	}
	return clean[:4], fmt.Sprintf("%02d", m), true
}

// ResolveSinceMonth determines the start month for syncing.
// Priority: --since flag > --history (scan cache) > caller-defined default window.
// sourceSubdir is the subdirectory to look for within each month (e.g. "calendars", "finance", "messages")
func ResolveSinceMonth(args []string, sourceSubdir string) (startMonth string, isHistory bool) {
	// Check --since flag
	sinceStr := GetOption(args, "--since")
	if sinceStr != "" {
		if y, m, ok := ParseSinceMonth(sinceStr); ok {
			return fmt.Sprintf("%s-%s", y, m), true
		}
		// Also try as YYYYMMDD date
		if d, ok := ParseSinceDate(sinceStr); ok {
			return fmt.Sprintf("%d-%02d", d.Year(), d.Month()), true
		}
	}

	// Check --history flag
	// Full history starts from the first supported month.
	// Use --since if you want a narrower historical window.
	if HasFlag(args, "--history") {
		return "2024-01", true
	}

	return "", false
}

// DefaultRecentStartMonth returns the first month in the default "recent"
// window: current month plus the previous month.
func DefaultRecentStartMonth(now time.Time) string {
	tz := BrusselsTZ()
	current := now.In(tz)
	start := time.Date(current.Year(), current.Month(), 1, 0, 0, 0, 0, tz).AddDate(0, -1, 0)
	return start.Format("2006-01")
}

// findOldestCachedMonth finds the oldest month in DATA_DIR that has
// data for the given source subdirectory, ignoring future months.
func findOldestCachedMonth(sourceSubdir string) string {
	dataDir := DataDir()
	now := time.Now()
	currentYM := fmt.Sprintf("%d-%02d", now.Year(), now.Month())
	oldest := ""

	years, err := os.ReadDir(dataDir)
	if err != nil {
		return ""
	}

	for _, yd := range years {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		year := yd.Name()
		if _, err := strconv.Atoi(year); err != nil {
			continue
		}

		months, _ := os.ReadDir(filepath.Join(dataDir, year))
		for _, md := range months {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			month := md.Name()
			ym := year + "-" + month

			// Ignore future months
			if ym > currentYM {
				continue
			}

			srcPath := filepath.Join(dataDir, year, month, sourceSubdir)
			if _, err := os.Stat(srcPath); err == nil {
				if oldest == "" || ym < oldest {
					oldest = ym
				}
			}
		}
	}

	return oldest
}
