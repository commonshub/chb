package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type DateSpec struct {
	Start      time.Time
	End        time.Time // exclusive
	StartMonth string
	EndMonth   string
	Precision  string // day, month, quarter, semester, year, range
}

const DateFormatHelp = "YYYYMMDD, YYYY-MM-DD, YYYY/MM/DD, YYYYMM, YYYY-MM, YYYY/MM, YYYY/M, YYYY-M, or YYYY"
const DateRangeFormatHelp = "a date, YYYY/Q[1-4], YYYY/S[1-2], or YYYYMMDD-YYYYMMDD"

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

func GetOptions(args []string, flags ...string) []string {
	var values []string
	for i, a := range args {
		for _, flag := range flags {
			if a == flag && i+1 < len(args) {
				values = append(values, args[i+1])
			}
			if strings.HasPrefix(a, flag+"=") {
				values = append(values, strings.SplitN(a, "=", 2)[1])
			}
		}
	}
	return values
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

func ParseDateSpec(s string) (DateSpec, bool) {
	raw := strings.ToUpper(strings.TrimSpace(s))
	if raw == "" {
		return DateSpec{}, false
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == '/' || r == '-' })
	if len(parts) == 1 {
		clean := parts[0]
		switch {
		case len(clean) == 8 && allDigits(clean):
			return buildDaySpec(clean[:4], clean[4:6], clean[6:8])
		case len(clean) == 6 && allDigits(clean):
			return buildMonthSpec(clean[:4], clean[4:6])
		case len(clean) == 6 && (clean[4] == 'Q' || clean[4] == 'S') && clean[5] >= '1' && clean[5] <= '9':
			return buildPeriodSpec(clean[:4], clean[4], string(clean[5]))
		case len(clean) == 4 && allDigits(clean):
			return buildYearSpec(clean)
		}
		return DateSpec{}, false
	}
	if len(parts) == 2 {
		if len(parts[1]) >= 2 && (parts[1][0] == 'Q' || parts[1][0] == 'S') {
			return buildPeriodSpec(parts[0], parts[1][0], parts[1][1:])
		}
		return buildMonthSpec(parts[0], parts[1])
	}
	if len(parts) == 3 {
		return buildDaySpec(parts[0], parts[1], parts[2])
	}
	return DateSpec{}, false
}

func ParseDateValue(s string) (DateSpec, bool) {
	spec, ok := ParseDateSpec(s)
	if !ok || spec.Precision == "quarter" || spec.Precision == "semester" || spec.Precision == "range" {
		return DateSpec{}, false
	}
	return spec, true
}

func ParseDateRangeSpec(s string) (DateSpec, bool) {
	if start, end, ok := parseExplicitDateRange(s); ok {
		if end.Start.Before(start.Start) {
			return DateSpec{}, false
		}
		return makeDateSpec(start.Start, end.End, "range"), true
	}
	return ParseDateSpec(s)
}

func parseExplicitDateRange(s string) (DateSpec, DateSpec, bool) {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return DateSpec{}, DateSpec{}, false
	}
	if parts := strings.Split(raw, " - "); len(parts) == 2 {
		start, startOK := ParseDateValue(parts[0])
		end, endOK := ParseDateValue(parts[1])
		if startOK && endOK {
			return start, end, true
		}
	}
	for i, r := range raw {
		if r != '-' {
			continue
		}
		start, startOK := ParseDateValue(raw[:i])
		end, endOK := ParseDateValue(raw[i+1:])
		if startOK && endOK {
			return start, end, true
		}
	}
	return DateSpec{}, DateSpec{}, false
}

// allDigits is defined in spread.go.

func parseDateSpecYear(s string) (int, bool) {
	if len(s) != 4 || !allDigits(s) {
		return 0, false
	}
	y, err := strconv.Atoi(s)
	if err != nil || y < 2000 || y > 2100 {
		return 0, false
	}
	return y, true
}

func buildDaySpec(yStr, mStr, dStr string) (DateSpec, bool) {
	y, ok := parseDateSpecYear(yStr)
	if !ok {
		return DateSpec{}, false
	}
	m, err := strconv.Atoi(mStr)
	if err != nil || m < 1 || m > 12 {
		return DateSpec{}, false
	}
	d, err := strconv.Atoi(dStr)
	if err != nil || d < 1 || d > 31 {
		return DateSpec{}, false
	}
	loc := BrusselsTZ()
	start := time.Date(y, time.Month(m), d, 0, 0, 0, 0, loc)
	if start.Year() != y || int(start.Month()) != m || start.Day() != d {
		return DateSpec{}, false
	}
	return makeDateSpec(start, start.AddDate(0, 0, 1), "day"), true
}

func buildMonthSpec(yStr, mStr string) (DateSpec, bool) {
	y, ok := parseDateSpecYear(yStr)
	if !ok {
		return DateSpec{}, false
	}
	m, err := strconv.Atoi(mStr)
	if err != nil || m < 1 || m > 12 {
		return DateSpec{}, false
	}
	start := time.Date(y, time.Month(m), 1, 0, 0, 0, 0, BrusselsTZ())
	return makeDateSpec(start, start.AddDate(0, 1, 0), "month"), true
}

func buildYearSpec(yStr string) (DateSpec, bool) {
	y, ok := parseDateSpecYear(yStr)
	if !ok {
		return DateSpec{}, false
	}
	start := time.Date(y, 1, 1, 0, 0, 0, 0, BrusselsTZ())
	return makeDateSpec(start, start.AddDate(1, 0, 0), "year"), true
}

func buildPeriodSpec(yStr string, kind byte, nStr string) (DateSpec, bool) {
	y, ok := parseDateSpecYear(yStr)
	if !ok {
		return DateSpec{}, false
	}
	n, err := strconv.Atoi(nStr)
	if err != nil {
		return DateSpec{}, false
	}
	switch kind {
	case 'Q':
		if n < 1 || n > 4 {
			return DateSpec{}, false
		}
		start := time.Date(y, time.Month((n-1)*3+1), 1, 0, 0, 0, 0, BrusselsTZ())
		return makeDateSpec(start, start.AddDate(0, 3, 0), "quarter"), true
	case 'S':
		if n < 1 || n > 2 {
			return DateSpec{}, false
		}
		start := time.Date(y, time.Month((n-1)*6+1), 1, 0, 0, 0, 0, BrusselsTZ())
		return makeDateSpec(start, start.AddDate(0, 6, 0), "semester"), true
	}
	return DateSpec{}, false
}

func makeDateSpec(start, end time.Time, precision string) DateSpec {
	return DateSpec{
		Start:      start,
		End:        end,
		StartMonth: start.Format("2006-01"),
		EndMonth:   end.AddDate(0, 0, -1).Format("2006-01"),
		Precision:  precision,
	}
}

func ParseMonthRangeValue(s string) (startMonth, endMonth string, ok bool) {
	spec, ok := ParseDateRangeSpec(s)
	if !ok {
		return "", "", false
	}
	return spec.StartMonth, spec.EndMonth, true
}

func ParseMonthRangeArg(args []string) (startMonth, endMonth string, found bool) {
	value, ok := firstPositionalDateArg(args)
	if !ok {
		return "", "", false
	}
	return ParseMonthRangeValue(value)
}

func ExpandMonthRange(startMonth, endMonth string) []string {
	if startMonth == "" || endMonth == "" || endMonth < startMonth {
		return nil
	}
	start, ok := parseMonthStart(startMonth)
	if !ok {
		return nil
	}
	var out []string
	for t := start; ; t = t.AddDate(0, 1, 0) {
		ym := t.Format("2006-01")
		if ym > endMonth {
			break
		}
		out = append(out, ym)
	}
	return out
}

func parseMonthStart(month string) (time.Time, bool) {
	spec, ok := ParseDateSpec(month)
	if !ok || spec.Precision != "month" {
		return time.Time{}, false
	}
	return spec.Start, true
}

func firstPositionalDateArg(args []string) (string, bool) {
	skipNext := false
	for _, a := range args {
		if skipNext {
			skipNext = false
			continue
		}
		if strings.HasPrefix(a, "--") || strings.HasPrefix(a, "-") {
			switch a {
			case "--since", "--until", "--month", "--channel", "--room", "--account",
				"--currency", "--limit", "--skip", "--tag", "--tags", "--event",
				"--application", "--payment-link", "--collective", "--category", "-n":
				skipNext = true
			}
			continue
		}
		if _, ok := ParseDateRangeSpec(a); ok {
			return a, true
		}
	}
	return "", false
}

// ParseYearMonthArg extracts a positional date/month/year argument from args.
// Prefer ParseMonthRangeArg when the caller can process multiple months.
// Accepts formats from DateFormatHelp.
// Returns (year, month, found). If only year, month is "".
// month is always zero-padded (e.g. "01").
func ParseYearMonthArg(args []string) (year string, month string, found bool) {
	value, ok := firstPositionalDateArg(args)
	if !ok {
		return "", "", false
	}
	spec, ok := ParseDateValue(value)
	if !ok {
		return "", "", false
	}
	year = spec.Start.Format("2006")
	if spec.Precision != "year" {
		month = spec.Start.Format("01")
	}
	return year, month, true
}

func ParseSinceDate(s string) (time.Time, bool) {
	spec, ok := ParseDateValue(s)
	if !ok {
		return time.Time{}, false
	}
	return spec.Start, true
}

func ParseDateEndExclusive(s string) (time.Time, bool) {
	spec, ok := ParseDateValue(s)
	if !ok {
		return time.Time{}, false
	}
	return spec.End, true
}

// ParseSinceMonth parses a month string in formats: YYYY/MM, YYYYMM, YYYY-MM
// Returns year, month as strings (zero-padded month), and whether parsing succeeded.
func ParseSinceMonth(s string) (year string, month string, ok bool) {
	spec, ok := ParseDateValue(s)
	if !ok {
		return "", "", false
	}
	return spec.Start.Format("2006"), spec.Start.Format("01"), true
}

// ResolveSinceMonth determines the start month for syncing.
// Priority: --since flag > --history (scan cache) > caller-defined default window.
// sourceSubdir is the subdirectory to look for within each month (e.g. "providers/ics", "providers/etherscan", "providers/discord")
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
