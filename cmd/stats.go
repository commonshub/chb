package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type dirSize struct {
	path  string
	bytes int64
	files int
}

// Stats shows data directory statistics, optionally filtered by year or year/month.
func Stats(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintStatsHelp()
		return
	}

	dataDir := DataDir()
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	// Header
	if posFound {
		label := posYear
		if posMonth != "" {
			label += "/" + posMonth
		}
		fmt.Printf("\n%s📊 Stats for %s%s\n", Fmt.Bold, label, Fmt.Reset)
	} else {
		fmt.Printf("\n%s📊 Stats (all time)%s\n", Fmt.Bold, Fmt.Reset)
	}
	fmt.Printf("  %sData:%s %s\n", Fmt.Dim, Fmt.Reset, dataDir)

	// Collect matching year/month directories
	type monthEntry struct {
		year  string
		month string
		label string
		path  string
		size  int64
		files int
	}
	var months []monthEntry

	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() {
			continue
		}
		year := yd.Name()
		if _, err := strconv.Atoi(year); err != nil || len(year) != 4 {
			continue
		}
		if posFound && posMonth == "" && year != posYear {
			continue
		}

		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, year))
		for _, md := range monthDirs {
			if !md.IsDir() {
				continue
			}
			month := md.Name()
			if _, err := strconv.Atoi(month); err != nil || len(month) != 2 {
				continue
			}
			if posFound && posMonth != "" && (year != posYear || month != posMonth) {
				continue
			}

			mPath := filepath.Join(dataDir, year, month)
			size, files := dirStats(mPath)
			months = append(months, monthEntry{
				year:  year,
				month: month,
				label: year + "/" + month,
				path:  mPath,
				size:  size,
				files: files,
			})
		}
	}

	if len(months) == 0 {
		fmt.Printf("\n  %sNo data found.%s\n\n", Fmt.Dim, Fmt.Reset)
		return
	}

	// Sort chronologically
	sort.Slice(months, func(i, j int) bool {
		return months[i].label < months[j].label
	})

	// ── Storage breakdown ──
	var totalSize int64
	var totalFiles int
	for _, m := range months {
		totalSize += m.size
		totalFiles += m.files
	}
	fmt.Printf("  %sStorage:%s %s (%d files)\n", Fmt.Dim, Fmt.Reset, formatBytes(totalSize), totalFiles)

	// ── Transaction stats ──
	type txSummary struct {
		Count int
		In    float64
		Out   float64
	}
	grandTx := txSummary{}
	monthTx := map[string]*txSummary{}
	sourceTotals := map[string]*txSummary{}

	for _, m := range months {
		txPath := filepath.Join(m.path, "generated", "transactions.json")
		data, err := os.ReadFile(txPath)
		if err != nil {
			continue
		}
		var txFile TransactionsFile
		if json.Unmarshal(data, &txFile) != nil {
			continue
		}

		ms := &txSummary{}
		for _, tx := range txFile.Transactions {
			if tx.Type == "INTERNAL" {
				continue
			}

			currency := tx.Currency
			if currency == "" {
				currency = "EUR"
			}
			if !isEURCurrency(currency) {
				continue // only EUR in the main summary
			}

			amount := tx.NormalizedAmount
			if amount == 0 {
				amount = tx.Amount
			}

			source := tx.Provider
			if source == "etherscan" && tx.Chain != nil {
				source = *tx.Chain
			}
			if source == "" {
				source = "unknown"
			}

			ss, ok := sourceTotals[source]
			if !ok {
				ss = &txSummary{}
				sourceTotals[source] = ss
			}

			absAmount := math.Abs(amount)
			ms.Count++
			ss.Count++
			if tx.Type == "CREDIT" || amount > 0 {
				ms.In += absAmount
				ss.In += absAmount
			} else {
				ms.Out += absAmount
				ss.Out += absAmount
			}
		}
		monthTx[m.label] = ms
		grandTx.Count += ms.Count
		grandTx.In += ms.In
		grandTx.Out += ms.Out
	}

	if grandTx.Count > 0 {
		net := grandTx.In - grandTx.Out
		fmt.Printf("\n%s💰 Transactions%s (%d)\n", Fmt.Bold, Fmt.Reset, grandTx.Count)
		fmt.Printf("  %s↑ In:%s  %s\n", Fmt.Green, Fmt.Reset, fmtEUR(grandTx.In))
		fmt.Printf("  %s↓ Out:%s %s\n", Fmt.Red, Fmt.Reset, fmtEUR(grandTx.Out))
		fmt.Printf("  %sNet:%s  %s\n", Fmt.Bold, Fmt.Reset, fmtEURSigned(net))

		// Per-source breakdown
		type namedSource struct {
			name string
			s    *txSummary
		}
		var sources []namedSource
		for name, s := range sourceTotals {
			sources = append(sources, namedSource{name, s})
		}
		sort.Slice(sources, func(i, j int) bool {
			return (sources[i].s.In + sources[i].s.Out) > (sources[j].s.In + sources[j].s.Out)
		})
		for _, s := range sources {
			parts := []string{}
			if s.s.In > 0 {
				parts = append(parts, fmt.Sprintf("%s↑%s%s", Fmt.Green, Fmt.Reset, fmtEUR(s.s.In)))
			}
			if s.s.Out > 0 {
				parts = append(parts, fmt.Sprintf("%s↓%s%s", Fmt.Red, Fmt.Reset, fmtEUR(s.s.Out)))
			}
			fmt.Printf("    %-10s %4d tx  %s\n", s.name, s.s.Count, strings.Join(parts, "  "))
		}

		// Per-month breakdown (only if multiple months)
		if len(months) > 1 {
			fmt.Printf("\n%s  By Month%s\n", Fmt.Bold, Fmt.Reset)
			for _, m := range months {
				ms, ok := monthTx[m.label]
				if !ok || ms.Count == 0 {
					continue
				}
				mNet := ms.In - ms.Out
				fmt.Printf("    %-8s %4d tx  %s↑%s%-12s %s↓%s%-12s %snet %s%s\n",
					m.label, ms.Count,
					Fmt.Green, Fmt.Reset, fmtEUR(ms.In),
					Fmt.Red, Fmt.Reset, fmtEUR(ms.Out),
					Fmt.Dim, fmtEURSigned(mNet), Fmt.Reset,
				)
			}
		}
	}

	// ── Data types breakdown ──
	typeMap := make(map[string]dirSize)
	for _, m := range months {
		typeDirs, _ := os.ReadDir(m.path)
		for _, td := range typeDirs {
			if !td.IsDir() {
				continue
			}
			tName := td.Name()
			tPath := filepath.Join(m.path, tName)
			size, files := dirStats(tPath)
			ds := typeMap[tName]
			ds.bytes += size
			ds.files += files
			typeMap[tName] = ds
		}
	}

	if len(typeMap) > 0 {
		fmt.Printf("\n%s📁 By Type%s\n", Fmt.Bold, Fmt.Reset)
		type typeEntry struct {
			name  string
			size  int64
			files int
		}
		var types []typeEntry
		for name, ds := range typeMap {
			types = append(types, typeEntry{name, ds.bytes, ds.files})
		}
		sort.Slice(types, func(i, j int) bool {
			return types[i].size > types[j].size
		})
		for _, t := range types {
			icon := typeIcon(t.name)
			fmt.Printf("  %s %-14s %7s (%d files)\n", icon, t.name, formatBytes(t.size), t.files)
		}
	}

	fmt.Println()
}

func dirStats(path string) (int64, int) {
	var totalSize int64
	var totalFiles int
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			totalSize += info.Size()
			totalFiles++
		}
		return nil
	})
	return totalSize, totalFiles
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func makeBar(value, max int64, width int) string {
	if max == 0 {
		return strings.Repeat("░", width)
	}
	filled := int(float64(value) / float64(max) * float64(width))
	if filled < 1 && value > 0 {
		filled = 1
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func typeIcon(name string) string {
	switch name {
	case "events":
		return "📅"
	case "calendars":
		return "📆"
	case "finance":
		return "💰"
	case "messages":
		return "💬"
	case "latest":
		return "📌"
	case "generated":
		return "⚙️"
	default:
		return "📁"
	}
}

func PrintStatsHelp() {
	f := Fmt
	fmt.Printf(`
%sUSAGE%s
  %schb stats%s [year[/month]] [options]

%sDESCRIPTION%s
  Show storage, transaction, and data type statistics.
  Without arguments, shows all-time stats.
  With a year or year/month, filters to that period.

%sOPTIONS%s
  %s<year>%s              Show stats for a specific year (e.g. 2025)
  %s<year/month>%s        Show stats for a specific month (e.g. 2026/03)
  %s--help, -h%s          Show this help

%sEXAMPLES%s
  %schb stats%s                All-time overview
  %schb stats 2025%s           2025 only
  %schb stats 2026/03%s        March 2026 only
`,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
