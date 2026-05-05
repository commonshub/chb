package cmd

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ParseSpreadInput accepts the date-spread shorthand used by the TUI editor and
// returns the expanded list of months (each "YYYY-MM"), sorted and deduplicated.
//
// Supported forms (commas combine any of them):
//   - YYYY              full year, all 12 months
//   - YYYY-YYYY         range of full years
//   - YYYY-MM           single month
//   - YYYYMM            single month (compact)
//   - YYYYMM-YYYYMM     month range, inclusive
//   - YYYY-MM-YYYY-MM   month range, inclusive (long form)
//
// An empty input returns (nil, nil) — meaning "no spread".
func ParseSpreadInput(input string) ([]string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, nil
	}

	seen := map[string]bool{}
	var months []string
	for _, piece := range strings.Split(input, ",") {
		piece = strings.TrimSpace(piece)
		if piece == "" {
			continue
		}
		expanded, err := parseSpreadPiece(piece)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", piece, err)
		}
		for _, m := range expanded {
			if !seen[m] {
				seen[m] = true
				months = append(months, m)
			}
		}
	}
	sort.Strings(months)
	return months, nil
}

func parseSpreadPiece(piece string) ([]string, error) {
	parts := strings.Split(piece, "-")
	switch len(parts) {
	case 1:
		s := parts[0]
		switch {
		case len(s) == 4 && allDigits(s):
			return monthsInYear(s)
		case len(s) == 6 && allDigits(s):
			return []string{s[:4] + "-" + s[4:]}, validateYM(s[:4] + "-" + s[4:])
		}
	case 2:
		a, b := parts[0], parts[1]
		switch {
		case len(a) == 4 && len(b) == 4 && allDigits(a) && allDigits(b):
			return monthsInYearRange(a, b)
		case len(a) == 4 && len(b) == 2 && allDigits(a) && allDigits(b):
			ym := a + "-" + b
			return []string{ym}, validateYM(ym)
		case len(a) == 6 && len(b) == 6 && allDigits(a) && allDigits(b):
			return monthsInMonthRange(a[:4]+"-"+a[4:], b[:4]+"-"+b[4:])
		}
	case 4:
		// YYYY-MM-YYYY-MM
		if len(parts[0]) == 4 && len(parts[1]) == 2 && len(parts[2]) == 4 && len(parts[3]) == 2 &&
			allDigits(parts[0]) && allDigits(parts[1]) && allDigits(parts[2]) && allDigits(parts[3]) {
			return monthsInMonthRange(parts[0]+"-"+parts[1], parts[2]+"-"+parts[3])
		}
	}
	return nil, fmt.Errorf("unrecognized format")
}

func monthsInYear(year string) ([]string, error) {
	if err := validateYear(year); err != nil {
		return nil, err
	}
	out := make([]string, 12)
	for i := 1; i <= 12; i++ {
		out[i-1] = fmt.Sprintf("%s-%02d", year, i)
	}
	return out, nil
}

func monthsInYearRange(startYear, endYear string) ([]string, error) {
	if err := validateYear(startYear); err != nil {
		return nil, err
	}
	if err := validateYear(endYear); err != nil {
		return nil, err
	}
	sy, _ := strconv.Atoi(startYear)
	ey, _ := strconv.Atoi(endYear)
	if sy > ey {
		return nil, fmt.Errorf("start year %d after end year %d", sy, ey)
	}
	out := make([]string, 0, (ey-sy+1)*12)
	for y := sy; y <= ey; y++ {
		for i := 1; i <= 12; i++ {
			out = append(out, fmt.Sprintf("%04d-%02d", y, i))
		}
	}
	return out, nil
}

func monthsInMonthRange(start, end string) ([]string, error) {
	if err := validateYM(start); err != nil {
		return nil, err
	}
	if err := validateYM(end); err != nil {
		return nil, err
	}
	st, _ := time.Parse("2006-01", start)
	et, _ := time.Parse("2006-01", end)
	if st.After(et) {
		return nil, fmt.Errorf("start %s after end %s", start, end)
	}
	var out []string
	for cur := st; !cur.After(et); cur = cur.AddDate(0, 1, 0) {
		out = append(out, cur.Format("2006-01"))
		if len(out) > 1200 {
			return nil, fmt.Errorf("range exceeds 100 years")
		}
	}
	return out, nil
}

func validateYear(s string) error {
	y, err := strconv.Atoi(s)
	if err != nil || y < 1900 || y > 2200 {
		return fmt.Errorf("year %q out of range", s)
	}
	return nil
}

func validateYM(s string) error {
	if _, err := time.Parse("2006-01", s); err != nil {
		return fmt.Errorf("invalid month %q", s)
	}
	return nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// spreadAllocationForMonth returns the (signed) amount allocated to ym from the
// given spread, and whether ym was found at all. A returned (0, false) means
// "this spread does not touch ym"; (0, true) is theoretically possible if a
// publisher emitted a zero-amount spread row but practically rare.
func spreadAllocationForMonth(spread []SpreadEntry, ym string) (float64, bool) {
	for _, e := range spread {
		if e.Month != ym {
			continue
		}
		v, err := strconv.ParseFloat(e.Amount, 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

// BuildSpreadEntries distributes total evenly across months at cent precision.
// The last month absorbs the rounding remainder so the entries sum to exactly
// `total` (within float epsilon). Negative totals are supported.
func BuildSpreadEntries(months []string, total float64) []SpreadEntry {
	n := len(months)
	if n == 0 {
		return nil
	}
	per := math.Round((total/float64(n))*100) / 100
	out := make([]SpreadEntry, n)
	accum := 0.0
	for i, m := range months {
		amt := per
		if i == n-1 {
			amt = math.Round((total-accum)*100) / 100
		} else {
			accum += amt
		}
		out[i] = SpreadEntry{
			Month:  m,
			Amount: strconv.FormatFloat(amt, 'f', 2, 64),
		}
	}
	return out
}
