package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func TransactionsStats(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintTransactionsStatsHelp()
		return
	}

	jsonOut := GetOption(args, "--format") == "json"
	dataDir := DataDir()

	// Parse optional year/month filter
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	type sourceStats struct {
		Count    int     `json:"count"`
		In       float64 `json:"in"`
		Out      float64 `json:"out"`
		Net      float64 `json:"net"`
		Currency string  `json:"currency,omitempty"`
	}

	type monthStats struct {
		Month   string                 `json:"month"`
		Count   int                    `json:"count"`
		In      float64                `json:"in"`
		Out     float64                `json:"out"`
		Net     float64                `json:"net"`
		Sources map[string]*sourceStats `json:"sources"`
	}

	monthData := map[string]*monthStats{}
	totalCount := 0
	totalIn := 0.0
	totalOut := 0.0

	// Scan year/month directories for generated transactions.json
	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		year := yd.Name()

		// Year filter
		if posFound && posMonth == "" && year != posYear {
			continue
		}

		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, year))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			month := md.Name()
			ym := year + "-" + month

			// Month filter
			if posFound && posMonth != "" && (year != posYear || month != posMonth) {
				continue
			}

			txPath := filepath.Join(dataDir, year, month, "generated", "transactions.json")
			data, err := os.ReadFile(txPath)
			if err != nil {
				continue
			}

			var txFile TransactionsFile
			if json.Unmarshal(data, &txFile) != nil {
				continue
			}

			ms := &monthStats{
				Month:   ym,
				Sources: map[string]*sourceStats{},
			}

			for _, tx := range txFile.Transactions {
				amount := tx.NormalizedAmount
				if amount == 0 {
					amount = tx.Amount
				}

				// Determine source label (provider-level: stripe, gnosis, monerium, etc.)
				source := tx.Provider
				if source == "etherscan" && tx.Chain != nil {
					source = *tx.Chain
				}
				if source == "" {
					source = "unknown"
				}

				// Track currency per source (e.g. CHT vs EUR)
				currency := tx.Currency
				if currency == "" {
					currency = "EUR"
				}

				// Key sources by "source:currency" when non-EUR to keep them separate
				sourceKey := source
				if !isEURCurrency(currency) {
					sourceKey = source + ":" + currency
				}

				ss, ok := ms.Sources[sourceKey]
				if !ok {
					ss = &sourceStats{Currency: currency}
					ms.Sources[sourceKey] = ss
				}

				ss.Count++
				ms.Count++
				totalCount++

				// Skip internal transfers and token transfers from In/Out totals
				if tx.Type == "INTERNAL" || tx.Type == "TRANSFER" {
					continue
				}

				absAmount := math.Abs(amount)
				if isEURCurrency(currency) {
					if tx.Type == "CREDIT" || amount > 0 {
						ss.In += absAmount
						ms.In += absAmount
						totalIn += absAmount
					} else {
						ss.Out += absAmount
						ms.Out += absAmount
						totalOut += absAmount
					}
				} else {
					// Non-EUR token: track in/out without adding to EUR totals
					if tx.Type == "CREDIT" || amount > 0 {
						ss.In += absAmount
					} else {
						ss.Out += absAmount
					}
				}
			}

			ss := ms.Sources
			for k := range ss {
				ss[k].Net = ss[k].In - ss[k].Out
			}
			ms.Net = ms.In - ms.Out

			monthData[ym] = ms
		}
	}

	// Sort months descending
	var months []*monthStats
	for _, ms := range monthData {
		months = append(months, ms)
	}
	sort.Slice(months, func(i, j int) bool {
		return months[i].Month > months[j].Month
	})

	if jsonOut {
		out := struct {
			Total  int            `json:"total"`
			In     float64        `json:"in"`
			Out    float64        `json:"out"`
			Net    float64        `json:"net"`
			Months []*monthStats  `json:"months"`
		}{
			Total:  totalCount,
			In:     totalIn,
			Out:    totalOut,
			Net:    totalIn - totalOut,
			Months: months,
		}
		data, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(data))
		return
	}

	// Pretty print
	f := Fmt
	net := totalIn - totalOut

	fmt.Printf("\n%s💰 Transactions: %d total%s\n", f.Bold, totalCount, f.Reset)
	fmt.Printf("   %s↓ In:%s  %s\n", f.Green, f.Reset, fmtEUR(totalIn))
	fmt.Printf("   %s↑ Out:%s %s\n", f.Red, f.Reset, fmtEUR(totalOut))
	fmt.Printf("   %sNet:%s  %s\n\n", f.Bold, f.Reset, fmtEURSigned(net))

	for _, ms := range months {
		mNet := ms.In - ms.Out
		fmt.Printf("  %s%-10s%s  %4d tx  %s↓%s%-12s  %s↑%s%-12s  %snet %s%s\n",
			f.Bold, ms.Month, f.Reset,
			ms.Count,
			f.Green, f.Reset, fmtEUR(ms.In),
			f.Red, f.Reset, fmtEUR(ms.Out),
			f.Dim, fmtEURSigned(mNet), f.Reset,
		)

		// Separate EUR and non-EUR sources
		type namedSource struct {
			name string
			ss   *sourceStats
		}
		var eurSources []namedSource
		var tokenSources []namedSource

		for name, ss := range ms.Sources {
			if isEURCurrency(ss.Currency) || ss.Currency == "" {
				eurSources = append(eurSources, namedSource{name, ss})
			} else {
				tokenSources = append(tokenSources, namedSource{name, ss})
			}
		}
		sort.Slice(eurSources, func(i, j int) bool {
			return (eurSources[i].ss.In + eurSources[i].ss.Out) > (eurSources[j].ss.In + eurSources[j].ss.Out)
		})
		sort.Slice(tokenSources, func(i, j int) bool {
			return (tokenSources[i].ss.In + tokenSources[i].ss.Out) > (tokenSources[j].ss.In + tokenSources[j].ss.Out)
		})

		// Print EUR sources
		for _, s := range eurSources {
			parts := []string{}
			if s.ss.In > 0 {
				parts = append(parts, fmt.Sprintf("%s↓%s%-12s", f.Green, f.Reset, fmtEUR(s.ss.In)))
			}
			if s.ss.Out > 0 {
				parts = append(parts, fmt.Sprintf("%s↑%s%-12s", f.Red, f.Reset, fmtEUR(s.ss.Out)))
			}
			fmt.Printf("    %-10s  %4d tx  %s\n",
				s.name,
				s.ss.Count,
				strings.Join(parts, "  "),
			)
		}

		// Print non-EUR token sources (e.g. CHT)
		for _, s := range tokenSources {
			sym := s.ss.Currency
			parts := []string{}
			if s.ss.In > 0 {
				parts = append(parts, fmt.Sprintf("%s↓%s%-16s", f.Green, f.Reset, fmtToken(s.ss.In, sym)))
			}
			if s.ss.Out > 0 {
				parts = append(parts, fmt.Sprintf("%s↑%s%-16s", f.Red, f.Reset, fmtToken(s.ss.Out, sym)))
			}
			// Strip ":SYMBOL" suffix from key for display
			displayName := s.name
			if idx := strings.Index(displayName, ":"); idx >= 0 {
				displayName = displayName[:idx]
			}
			fmt.Printf("    %-10s  %4d tx  %s\n",
				displayName+"("+sym+")",
				s.ss.Count,
				strings.Join(parts, "  "),
			)
		}
	}
	fmt.Println()
}

// isEURCurrency returns true for EUR-family currencies (EUR, EURe, EURb, etc.)
func isEURCurrency(currency string) bool {
	return currency == "" || strings.HasPrefix(strings.ToUpper(currency), "EUR")
}

// fmtToken formats a token amount with its symbol, e.g. "1,234.56 CHT"
func fmtToken(v float64, symbol string) string {
	return fmtNumber(math.Abs(v)) + " " + symbol
}

// fmtEUR formats a number as €12,345.67
func fmtEUR(v float64) string {
	return "€" + fmtNumber(math.Abs(v))
}

// fmtEURSigned formats with +/- prefix
func fmtEURSigned(v float64) string {
	if v >= 0 {
		return "+" + fmtEUR(v)
	}
	return "-" + fmtEUR(-v)
}

// fmtNumber formats a float with thousands separators: 12,345.67
func fmtNumber(v float64) string {
	// Split integer and decimal parts
	intPart := int64(v)
	decPart := v - float64(intPart)
	dec := fmt.Sprintf("%.2f", decPart)[1:] // ".67"

	// Format integer with commas
	s := fmt.Sprintf("%d", intPart)
	if len(s) <= 3 {
		return s + dec
	}
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result) + dec
}

func joinStrings(ss []string) string {
	r := ""
	for i, s := range ss {
		if i > 0 {
			r += ", "
		}
		r += s
	}
	return r
}

func PrintTransactionsStatsHelp() {
	f := Fmt
	fmt.Printf(`
%sUSAGE%s
  %schb transactions%s [year[/month]] [options]

%sOPTIONS%s
  %s<year>%s              Show stats for a specific year (e.g. 2025)
  %s<year/month>%s        Show stats for a specific month (e.g. 2025/03)
  %s--format json%s       Output as JSON
  %s--help, -h%s          Show this help

%sEXAMPLES%s
  %schb transactions%s              All-time breakdown
  %schb transactions 2025%s         2025 only
  %schb transactions 2025/03%s      March 2025 only
  %schb transactions --format json%s  JSON output

%sNOTE%s
  Reads from generated transactions.json files. Run %schb sync%s first.
`,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
	)
}
