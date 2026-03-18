package cmd

import (
	"fmt"
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
			if a == "--since" || a == "--month" || a == "--channel" || a == "--room" || a == "-n" {
				skipNext = true
			}
			continue
		}
		// Try to parse as year or year/month
		parts := strings.SplitN(a, "/", 2)
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
// Priority: --since flag > --history (scan cache) > default (current month)
// sourceSubdir is the subdirectory to look for within each month (e.g. "events", "finance", "calendars", "channels")
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

	// Check --history flag: always start from 2024/01
	// Each sync fetches all data in one API call anyway — the month range
	// only controls which months get saved. Existing months are skipped
	// unless --force is used, so no wasted work on subsequent runs.
	if HasFlag(args, "--history") {
		return "2024-01", true
	}

	return "", false
}
