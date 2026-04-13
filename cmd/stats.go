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
	"time"
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
	displayPath := dataDir
	if posFound {
		label := posYear
		displayPath = filepath.Join(dataDir, posYear)
		if posMonth != "" {
			label += "/" + posMonth
			displayPath = filepath.Join(dataDir, posYear, posMonth)
		}
		fmt.Printf("\n%s📊 Stats for %s%s\n", Fmt.Bold, label, Fmt.Reset)
	} else {
		fmt.Printf("\n%s📊 Stats (all time)%s\n", Fmt.Bold, Fmt.Reset)
	}
	fmt.Printf("  %sData:%s %s\n", Fmt.Dim, Fmt.Reset, displayPath)

	// Last sync info
	state := LoadSyncState()
	if state.Calendars != nil && state.Calendars.LastSync != "" {
		if t, err := time.Parse(time.RFC3339, state.Calendars.LastSync); err == nil {
			fmt.Printf("  %sLast sync:%s %s\n", Fmt.Dim, Fmt.Reset, formatTimeAgo(t))
		}
	}

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
	// For filtered views, compute from the display path directly
	totalSize, totalFiles := dirStats(displayPath)
	fmt.Printf("  %sStorage:%s %s (%d files)\n", Fmt.Dim, Fmt.Reset, formatBytes(totalSize), totalFiles)

	// ── Transaction stats ──
	type txSummary struct {
		Count     int
		In        float64
		Out       float64
		Transfers int
	}
	grandTx := txSummary{}
	monthEUR := map[string]*txSummary{}
	monthToken := map[string]map[string]*txSummary{} // month -> symbol -> summary
	sourceTotals := map[string]*txSummary{}
	categoryTotals := map[string]*txSummary{} // category slug -> in/out

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

		msEUR := &txSummary{} // EUR-family only for month breakdown
		for _, tx := range txFile.Transactions {
			if tx.Type == "INTERNAL" {
				continue
			}

			currency := tx.Currency
			if currency == "" {
				currency = "EUR"
			}

			amount := tx.NormalizedAmount
			if amount == 0 {
				amount = tx.Amount
			}

			// Source label: use provider name, but for etherscan use the currency symbol
			source := tx.Provider
			if source == "etherscan" {
				source = currency
			}
			if source == "" {
				source = "unknown"
			}

			isEUR := isEURCurrency(currency)

			// Key by source (EUR-family sources grouped together, tokens by symbol)
			sourceKey := source
			if !isEUR {
				sourceKey = currency
			}
			ss, ok := sourceTotals[sourceKey]
			if !ok {
				ss = &txSummary{}
				sourceTotals[sourceKey] = ss
			}

			absAmount := math.Abs(amount)
			ss.Count++
			grandTx.Count++

			// Track by category (EUR only)
			if isEUR && tx.Category != "" {
				cat := tx.Category
				cs, ok := categoryTotals[cat]
				if !ok {
					cs = &txSummary{}
					categoryTotals[cat] = cs
				}
				cs.Count++
				if tx.Type == "CREDIT" {
					cs.In += absAmount
				} else if tx.Type == "DEBIT" {
					cs.Out += absAmount
				}
			}

			if tx.Type == "TRANSFER" {
				ss.Transfers++
				// Track per-month token transfers
				if !isEUR {
					if monthToken[m.label] == nil {
						monthToken[m.label] = map[string]*txSummary{}
					}
					mt, ok := monthToken[m.label][currency]
					if !ok {
						mt = &txSummary{}
						monthToken[m.label][currency] = mt
					}
					mt.Count++
					mt.Transfers++
				}
			} else if tx.Type == "CREDIT" {
				ss.In += absAmount
				if isEUR {
					msEUR.In += absAmount
					grandTx.In += absAmount
				} else {
					if monthToken[m.label] == nil {
						monthToken[m.label] = map[string]*txSummary{}
					}
					mt, ok := monthToken[m.label][currency]
					if !ok {
						mt = &txSummary{}
						monthToken[m.label][currency] = mt
					}
					mt.Count++
					mt.In += absAmount
				}
			} else {
				ss.Out += absAmount
				if isEUR {
					msEUR.Out += absAmount
					grandTx.Out += absAmount
				} else {
					if monthToken[m.label] == nil {
						monthToken[m.label] = map[string]*txSummary{}
					}
					mt, ok := monthToken[m.label][currency]
					if !ok {
						mt = &txSummary{}
						monthToken[m.label][currency] = mt
					}
					mt.Count++
					mt.Out += absAmount
				}
			}
			msEUR.Count++
		}
		monthEUR[m.label] = msEUR
	}

	if grandTx.Count > 0 {
		fmt.Printf("\n%s💰 Transactions%s (%d)\n", Fmt.Bold, Fmt.Reset, grandTx.Count)

		// EUR sources (stripe, EURe, EURb, etc.)
		fmt.Printf("  %sEUR:%s\n", Fmt.Bold, Fmt.Reset)
		eurSourceCount := 0
		eurTotalTx := 0
		for s, totals := range sourceTotals {
			if isEURCurrency(s) || s == "stripe" || s == "monerium" {
				eurSourceCount++
				eurTotalTx += totals.Count
				fmt.Printf("    %-8s %4d tx  %s↓%s%-12s %s↑%s%-12s\n",
					s, totals.Count, Fmt.Green, Fmt.Reset, fmtEUR(totals.In), Fmt.Red, Fmt.Reset, fmtEUR(totals.Out))
			}
		}
		// EUR summary line (when multiple EUR sources)
		if eurSourceCount > 1 {
			eurNet := grandTx.In - grandTx.Out
			fmt.Printf("\n    %s%-8s %4d tx  ↓%-12s ↑%-12s net %s%s\n",
				Fmt.Bold, "Total", eurTotalTx, fmtEUR(grandTx.In), fmtEUR(grandTx.Out), fmtEURSigned(eurNet), Fmt.Reset)
		}

		// EUR per-month breakdown
		if len(months) > 1 {
			fmt.Printf("\n  %sEUR By Month%s\n", Fmt.Bold, Fmt.Reset)
			for _, m := range months {
				ms, ok := monthEUR[m.label]
				if !ok || ms.Count == 0 {
					continue
				}
				mNet := ms.In - ms.Out
				fmt.Printf("    %-8s %4d tx  %s↓%s%-12s %s↑%s%-12s %snet %s%s\n",
					m.label, ms.Count,
					Fmt.Green, Fmt.Reset, fmtEUR(ms.In),
					Fmt.Red, Fmt.Reset, fmtEUR(ms.Out),
					Fmt.Dim, fmtEURSigned(mNet), Fmt.Reset,
				)
			}
		}

		// Category breakdown (EUR only)
		if len(categoryTotals) > 0 {
			settings, _ := LoadSettings()
			categorizer := NewCategorizer(settings)

			// Split into income and expense categories
			type catEntry struct {
				slug  string
				label string
				total *txSummary
			}
			var incomeEntries, expenseEntries []catEntry
			uncategorizedIn, uncategorizedOut := grandTx.In, grandTx.Out
			uncategorizedCount := 0

			for slug, total := range categoryTotals {
				dir := categorizer.CategoryDirection(slug)
				label := categorizer.CategoryLabel(slug)
				entry := catEntry{slug, label, total}
				if dir == "income" {
					incomeEntries = append(incomeEntries, entry)
					uncategorizedIn -= total.In
				} else {
					expenseEntries = append(expenseEntries, entry)
					uncategorizedOut -= total.Out
				}
			}

			// Count uncategorized
			for _, m := range months {
				ms, ok := monthEUR[m.label]
				if !ok {
					continue
				}
				_ = ms
			}

			fmt.Printf("\n  %sBy Category (EUR)%s\n", Fmt.Bold, Fmt.Reset)

			if len(incomeEntries) > 0 {
				sort.Slice(incomeEntries, func(i, j int) bool {
					return incomeEntries[i].total.In > incomeEntries[j].total.In
				})
				fmt.Printf("    %sIncome:%s\n", Fmt.Dim, Fmt.Reset)
				for _, e := range incomeEntries {
					fmt.Printf("      %-14s %4d tx  %s↓%s%-12s\n",
						e.label, e.total.Count, Fmt.Green, Fmt.Reset, fmtEUR(e.total.In))
				}
				if uncategorizedIn > 0.01 {
					uncategorizedCount++
					fmt.Printf("      %s%-14s          ↓%-12s%s\n",
						Fmt.Dim, "Uncategorized", fmtEUR(uncategorizedIn), Fmt.Reset)
				}
			}

			if len(expenseEntries) > 0 {
				sort.Slice(expenseEntries, func(i, j int) bool {
					return expenseEntries[i].total.Out > expenseEntries[j].total.Out
				})
				fmt.Printf("    %sExpenses:%s\n", Fmt.Dim, Fmt.Reset)
				for _, e := range expenseEntries {
					fmt.Printf("      %-14s %4d tx  %s↑%s%-12s\n",
						e.label, e.total.Count, Fmt.Red, Fmt.Reset, fmtEUR(e.total.Out))
				}
				if uncategorizedOut > 0.01 {
					uncategorizedCount++
					fmt.Printf("      %s%-14s          ↑%-12s%s\n",
						Fmt.Dim, "Uncategorized", fmtEUR(uncategorizedOut), Fmt.Reset)
				}
			}

			_ = uncategorizedCount
		}

		// Token sources (CHT, etc.)
		hasTokens := false
		for s := range sourceTotals {
			if !isEURCurrency(s) && s != "stripe" && s != "monerium" {
				hasTokens = true
				break
			}
		}
		if hasTokens {
			fmt.Printf("\n  %sTokens:%s\n", Fmt.Bold, Fmt.Reset)
			for s, totals := range sourceTotals {
				if !isEURCurrency(s) && s != "stripe" && s != "monerium" {
					parts := []string{}
					if totals.In > 0 {
						parts = append(parts, fmt.Sprintf("🪙 %s%s%s minted", Fmt.Green, fmtToken(totals.In, s), Fmt.Reset))
					}
					if totals.Out > 0 {
						parts = append(parts, fmt.Sprintf("🔥 %s%s%s burnt", Fmt.Red, fmtToken(totals.Out, s), Fmt.Reset))
					}
					if totals.Transfers > 0 {
						parts = append(parts, fmt.Sprintf("↔ %d transfers", totals.Transfers))
					}
					fmt.Printf("    %-8s %4d tx  %s\n",
						s, totals.Count, strings.Join(parts, "  "))
				}
			}

			// Token per-month breakdown
			if len(months) > 1 {
				// Collect all token symbols
				tokenSymbols := []string{}
				for s := range sourceTotals {
					if !isEURCurrency(s) && s != "stripe" && s != "monerium" {
						tokenSymbols = append(tokenSymbols, s)
					}
				}
				sort.Strings(tokenSymbols)

				for _, sym := range tokenSymbols {
					fmt.Printf("\n  %s%s By Month%s\n", Fmt.Bold, sym, Fmt.Reset)
					for _, m := range months {
						tokens, ok := monthToken[m.label]
						if !ok {
							continue
						}
						mt, ok := tokens[sym]
						if !ok || mt.Count == 0 {
							continue
						}
						parts := []string{}
						if mt.In > 0 {
							parts = append(parts, fmt.Sprintf("🪙 %-12s", fmtToken(mt.In, sym)))
						}
						if mt.Out > 0 {
							parts = append(parts, fmt.Sprintf("🔥 %-12s", fmtToken(mt.Out, sym)))
						}
						if mt.Transfers > 0 {
							parts = append(parts, fmt.Sprintf("↔ %d transfers", mt.Transfers))
						}
						fmt.Printf("    %-8s %4d tx  %s\n",
							m.label, mt.Count, strings.Join(parts, "  "))
					}
				}
			}
		}
	}

	// ── Messages & Images ──
	type monthMsgStats struct {
		messages int
		images   int
		imgSize  int64
	}
	totalMsg := monthMsgStats{}
	monthMsgs := map[string]*monthMsgStats{}

	for _, m := range months {
		ms := &monthMsgStats{}

		// Count messages from discord channel files
		discordDir := filepath.Join(m.path, "messages", "discord")
		if channels, err := os.ReadDir(discordDir); err == nil {
			for _, ch := range channels {
				if !ch.IsDir() || ch.Name() == "images" {
					continue
				}
				msgPath := filepath.Join(discordDir, ch.Name(), "messages.json")
				data, err := os.ReadFile(msgPath)
				if err != nil {
					continue
				}
				var msgFile struct {
					Messages []json.RawMessage `json:"messages"`
				}
				if json.Unmarshal(data, &msgFile) == nil {
					ms.messages += len(msgFile.Messages)
				}
			}
		}

		// Count images and their size
		imagesDir := filepath.Join(m.path, "messages", "discord", "images")
		if entries, err := os.ReadDir(imagesDir); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				ms.images++
				if info, err := e.Info(); err == nil {
					ms.imgSize += info.Size()
				}
			}
		}
		// Also count event cover images
		eventImagesDir := filepath.Join(m.path, "events", "images")
		if entries, err := os.ReadDir(eventImagesDir); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				ms.images++
				if info, err := e.Info(); err == nil {
					ms.imgSize += info.Size()
				}
			}
		}

		totalMsg.messages += ms.messages
		totalMsg.images += ms.images
		totalMsg.imgSize += ms.imgSize
		monthMsgs[m.label] = ms
	}

	if totalMsg.messages > 0 || totalMsg.images > 0 {
		fmt.Printf("\n%s💬 Messages%s\n", Fmt.Bold, Fmt.Reset)
		parts := []string{fmt.Sprintf("%d messages", totalMsg.messages)}
		if totalMsg.images > 0 {
			parts = append(parts, fmt.Sprintf("%d images (%s)", totalMsg.images, formatBytes(totalMsg.imgSize)))
		}
		fmt.Printf("    %s\n", strings.Join(parts, "  "))

		if len(months) > 1 {
			for _, m := range months {
				ms, ok := monthMsgs[m.label]
				if !ok || (ms.messages == 0 && ms.images == 0) {
					continue
				}
				parts := []string{fmt.Sprintf("%4d messages", ms.messages)}
				if ms.images > 0 {
					parts = append(parts, fmt.Sprintf("%3d images (%s)", ms.images, formatBytes(ms.imgSize)))
				}
				fmt.Printf("    %-8s  %s\n", m.label, strings.Join(parts, "  "))
			}
		}
	}

	// ── Bookings & Events ──
	type monthBookingStats struct {
		bookings int
		events   int
	}
	totalBookings := 0
	totalEvents := 0
	monthBookings := map[string]*monthBookingStats{}

	for _, m := range months {
		bs := &monthBookingStats{}

		// Count bookings from ICS files (one per room)
		icsDir := filepath.Join(m.path, "calendars", "ics")
		if files, err := os.ReadDir(icsDir); err == nil {
			for _, f := range files {
				if f.IsDir() || !strings.HasSuffix(f.Name(), ".ics") || f.Name() == "public.ics" {
					continue
				}
				icsData, err := os.ReadFile(filepath.Join(icsDir, f.Name()))
				if err != nil {
					continue
				}
				// Count VEVENT blocks
				bs.bookings += strings.Count(string(icsData), "BEGIN:VEVENT")
			}
		}

		// Count public events from events.json
		eventsPath := filepath.Join(m.path, "generated", "events.json")
		if data, err := os.ReadFile(eventsPath); err == nil {
			var evFile FullEventsFile
			if json.Unmarshal(data, &evFile) == nil {
				bs.events = len(evFile.Events)
			}
		}

		totalBookings += bs.bookings
		totalEvents += bs.events
		monthBookings[m.label] = bs
	}

	if totalBookings > 0 || totalEvents > 0 {
		parts := []string{fmt.Sprintf("%d bookings", totalBookings)}
		if totalEvents > 0 {
			parts = append(parts, fmt.Sprintf("%d public events", totalEvents))
		}
		fmt.Printf("\n%s📅 Bookings%s (%s)\n", Fmt.Bold, Fmt.Reset, strings.Join(parts, ", "))

		if len(months) > 1 {
			for _, m := range months {
				bs, ok := monthBookings[m.label]
				if !ok || (bs.bookings == 0 && bs.events == 0) {
					continue
				}
				if bs.events > 0 {
					fmt.Printf("    %-8s  %3d bookings (%d public events)\n", m.label, bs.bookings, bs.events)
				} else {
					fmt.Printf("    %-8s  %3d bookings\n", m.label, bs.bookings)
				}
			}
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
