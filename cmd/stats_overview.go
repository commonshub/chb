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

// StatsOverview shows a comprehensive summary for a year or month:
// CHT tokens issued/burned, financial in/out per account,
// messages per channel, photos count.
func StatsOverview(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		printStatsOverviewHelp()
		return
	}

	jsonOut := GetOption(args, "--format") == "json"
	dataDir := DataDir()

	posYear, posMonth, posFound := ParseYearMonthArg(args)

	// Collect year/month pairs to process
	type ym struct{ year, month string }
	var periods []ym

	if posFound && posMonth != "" {
		periods = append(periods, ym{posYear, posMonth})
	} else {
		// Scan data directory for all available months
		yearDirs, _ := os.ReadDir(dataDir)
		for _, yd := range yearDirs {
			if !yd.IsDir() || len(yd.Name()) != 4 {
				continue
			}
			year := yd.Name()
			if posFound && year != posYear {
				continue
			}
			monthDirs, _ := os.ReadDir(filepath.Join(dataDir, year))
			for _, md := range monthDirs {
				if !md.IsDir() || len(md.Name()) != 2 {
					continue
				}
				periods = append(periods, ym{year, md.Name()})
			}
		}
	}

	sort.Slice(periods, func(i, j int) bool {
		if periods[i].year != periods[j].year {
			return periods[i].year < periods[j].year
		}
		return periods[i].month < periods[j].month
	})

	if len(periods) == 0 {
		fmt.Printf("\n%sNo data found.%s Run %schb sync --history%s first.\n\n", Fmt.Dim, Fmt.Reset, Fmt.Cyan, Fmt.Reset)
		return
	}

	// Build channel name map
	channelNames := buildChannelNameMap()

	// ── Aggregate data ──────────────────────────────────────────────────

	type accountStats struct {
		Name string  `json:"name"`
		Slug string  `json:"slug"`
		In   float64 `json:"in"`
		Out  float64 `json:"out"`
	}

	type channelStats struct {
		Name     string `json:"name"`
		ID       string `json:"id"`
		Messages int    `json:"messages"`
	}

	type overviewResult struct {
		Period string `json:"period"`

		// CHT tokens
		CHTIssued    float64 `json:"chtIssued"`
		CHTBurned    float64 `json:"chtBurned"`
		CHTTransfers int     `json:"chtTransfers"`

		// Financial
		FinanceIn       float64         `json:"financeIn"`
		FinanceOut      float64         `json:"financeOut"`
		FinanceNet      float64         `json:"financeNet"`
		FinanceTxCount  int             `json:"financeTxCount"`
		AccountBreakdown []accountStats `json:"accounts"`

		// Messages
		TotalMessages   int             `json:"totalMessages"`
		ChannelBreakdown []channelStats `json:"channels"`

		// Photos
		TotalPhotos int `json:"totalPhotos"`

		// Events
		TotalEvents int `json:"totalEvents"`

		// Bookings
		TotalBookings int `json:"totalBookings"`
	}

	result := overviewResult{}
	if posFound && posMonth != "" {
		result.Period = posYear + "/" + posMonth
	} else if posFound {
		result.Period = posYear
	} else {
		result.Period = "all"
	}

	accountMap := map[string]*accountStats{}
	channelMap := map[string]*channelStats{}

	for _, p := range periods {
		// ── Transactions (from generated/transactions.json) ─────────
		txPath := filepath.Join(dataDir, p.year, p.month, "generated", "transactions.json")
		if data, err := os.ReadFile(txPath); err == nil {
			var txFile TransactionsFile
			if json.Unmarshal(data, &txFile) == nil {
				for _, tx := range txFile.Transactions {
					amount := tx.NormalizedAmount
					if amount == 0 {
						amount = tx.Amount
					}
					absAmount := math.Abs(amount)

					result.FinanceTxCount++

					if tx.Type == "CREDIT" || amount > 0 {
						result.FinanceIn += absAmount
					} else {
						result.FinanceOut += absAmount
					}

					// Per-account
					key := tx.AccountSlug
					if key == "" {
						key = tx.Provider
					}
					acc, ok := accountMap[key]
					if !ok {
						acc = &accountStats{Name: tx.AccountName, Slug: key}
						accountMap[key] = acc
					}
					if tx.Type == "CREDIT" || amount > 0 {
						acc.In += absAmount
					} else {
						acc.Out += absAmount
					}
				}
			}
		}

		// ── CHT tokens (from finance/celo/*.CHT.json) ───────────────
		celoDir := filepath.Join(dataDir, p.year, p.month, "finance", "celo")
		if entries, err := os.ReadDir(celoDir); err == nil {
			for _, e := range entries {
				if e.IsDir() || !strings.Contains(strings.ToUpper(e.Name()), "CHT") {
					continue
				}
				data, err := os.ReadFile(filepath.Join(celoDir, e.Name()))
				if err != nil {
					continue
				}
				var txFile struct {
					Transactions []struct {
						From         string `json:"from"`
						To           string `json:"to"`
						Value        string `json:"value"`
						TokenDecimal string `json:"tokenDecimal"`
					} `json:"transactions"`
				}
				if json.Unmarshal(data, &txFile) != nil {
					continue
				}

				zeroAddr := "0x0000000000000000000000000000000000000000"
				for _, tx := range txFile.Transactions {
					result.CHTTransfers++
					dec := 6
					if tx.TokenDecimal != "" {
						fmt.Sscanf(tx.TokenDecimal, "%d", &dec)
					}
					amount := parseTokenValue(tx.Value, dec)

					if strings.EqualFold(tx.From, zeroAddr) {
						result.CHTIssued += amount
					}
					if strings.EqualFold(tx.To, zeroAddr) {
						result.CHTBurned += amount
					}
				}
			}
		}

		// ── Messages (from messages/discord/*/messages.json) ────────
		discordDir := filepath.Join(dataDir, p.year, p.month, "messages", "discord")
		if entries, err := os.ReadDir(discordDir); err == nil {
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				channelID := e.Name()
				msgPath := filepath.Join(discordDir, channelID, "messages.json")
				data, err := os.ReadFile(msgPath)
				if err != nil {
					continue
				}
				var mf cachedMessageFile
				if json.Unmarshal(data, &mf) != nil {
					continue
				}
				n := len(mf.Messages)
				if n == 0 {
					continue
				}
				result.TotalMessages += n

				name := channelNames[channelID]
				if name == "" {
					name = channelID
				}
				ch, ok := channelMap[channelID]
				if !ok {
					ch = &channelStats{Name: name, ID: channelID}
					channelMap[channelID] = ch
				}
				ch.Messages += n
			}
		}

		// ── Photos (from generated/images.json) ─────────────────────
		imgPath := filepath.Join(dataDir, p.year, p.month, "generated", "images.json")
		if data, err := os.ReadFile(imgPath); err == nil {
			var imgFile ImagesFile
			if json.Unmarshal(data, &imgFile) == nil {
				result.TotalPhotos += imgFile.Count
			}
		}

		// ── Events (from generated/events.json) ─────────────────────
		eventsPath := filepath.Join(dataDir, p.year, p.month, "generated", "events.json")
		if data, err := os.ReadFile(eventsPath); err == nil {
			var evFile struct {
				Events []json.RawMessage `json:"events"`
			}
			if json.Unmarshal(data, &evFile) == nil {
				result.TotalEvents += len(evFile.Events)
			}
		}

		// ── Bookings (from calendars/*.json) ────────────────────────
		calDir := filepath.Join(dataDir, p.year, p.month, "calendars")
		if entries, err := os.ReadDir(calDir); err == nil {
			for _, e := range entries {
				if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
					continue
				}
				data, err := os.ReadFile(filepath.Join(calDir, e.Name()))
				if err != nil {
					continue
				}
				var bf struct {
					Events []json.RawMessage `json:"events"`
				}
				if json.Unmarshal(data, &bf) == nil {
					result.TotalBookings += len(bf.Events)
				}
			}
		}
	}

	result.FinanceNet = result.FinanceIn - result.FinanceOut

	// Build sorted slices
	for _, acc := range accountMap {
		result.AccountBreakdown = append(result.AccountBreakdown, *acc)
	}
	sort.Slice(result.AccountBreakdown, func(i, j int) bool {
		vi := result.AccountBreakdown[i].In + result.AccountBreakdown[i].Out
		vj := result.AccountBreakdown[j].In + result.AccountBreakdown[j].Out
		return vi > vj
	})

	for _, ch := range channelMap {
		result.ChannelBreakdown = append(result.ChannelBreakdown, *ch)
	}
	sort.Slice(result.ChannelBreakdown, func(i, j int) bool {
		return result.ChannelBreakdown[i].Messages > result.ChannelBreakdown[j].Messages
	})

	// ── Output ──────────────────────────────────────────────────────────

	if jsonOut {
		data, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(data))
		return
	}

	f := Fmt

	periodLabel := result.Period
	if posFound && posMonth != "" {
		periodLabel = posYear + "/" + posMonth
	} else if posFound {
		periodLabel = posYear
	} else {
		periodLabel = "all time"
	}

	fmt.Printf("\n%s📊 Stats: %s%s\n", f.Bold, periodLabel, f.Reset)

	// CHT Tokens
	if result.CHTIssued > 0 || result.CHTBurned > 0 || result.CHTTransfers > 0 {
		fmt.Printf("\n%s🪙  CHT Tokens%s\n", f.Bold, f.Reset)
		fmt.Printf("   %s↑ Issued:%s  %s CHT (%d transfers)\n", f.Green, f.Reset, fmtTokens(result.CHTIssued), result.CHTTransfers)
		if result.CHTBurned > 0 {
			fmt.Printf("   %s↓ Burned:%s  %s CHT\n", f.Red, f.Reset, fmtTokens(result.CHTBurned))
		}
		fmt.Printf("   %sNet:%s     %s CHT\n", f.Bold, f.Reset, fmtTokensSigned(result.CHTIssued-result.CHTBurned))
	} else {
		fmt.Printf("\n%s🪙  CHT Tokens%s\n", f.Bold, f.Reset)
		fmt.Printf("   %sNo CHT data synced.%s\n", f.Dim, f.Reset)
		fmt.Printf("   %sAdd CHT as a finance account or run: chb transactions sync%s\n", f.Dim, f.Reset)
	}

	// Finance
	fmt.Printf("\n%s💰 Finance%s  (%d transactions)\n", f.Bold, f.Reset, result.FinanceTxCount)
	fmt.Printf("   %s↑ In:%s   %s\n", f.Green, f.Reset, fmtEUR(result.FinanceIn))
	fmt.Printf("   %s↓ Out:%s  %s\n", f.Red, f.Reset, fmtEUR(result.FinanceOut))
	fmt.Printf("   %sNet:%s   %s\n", f.Bold, f.Reset, fmtEURSigned(result.FinanceNet))

	if len(result.AccountBreakdown) > 0 {
		fmt.Println()
		for _, acc := range result.AccountBreakdown {
			parts := []string{}
			if acc.In > 0 {
				parts = append(parts, fmt.Sprintf("%s↑%s%s", f.Green, f.Reset, fmtEUR(acc.In)))
			}
			if acc.Out > 0 {
				parts = append(parts, fmt.Sprintf("%s↓%s%s", f.Red, f.Reset, fmtEUR(acc.Out)))
			}
			name := acc.Name
			if name == "" {
				name = acc.Slug
			}
			fmt.Printf("   %-28s %s\n", Truncate(name, 28), strings.Join(parts, "  "))
		}
	}

	// Messages
	fmt.Printf("\n%s💬 Messages%s  %d total\n", f.Bold, f.Reset, result.TotalMessages)
	if len(result.ChannelBreakdown) > 0 {
		for _, ch := range result.ChannelBreakdown {
			bar := makeBar(int64(ch.Messages), int64(result.ChannelBreakdown[0].Messages), 15)
			fmt.Printf("   %-24s %s %4d\n", Truncate("#"+ch.Name, 24), bar, ch.Messages)
		}
	}

	// Photos
	fmt.Printf("\n%s📸 Photos%s   %d\n", f.Bold, f.Reset, result.TotalPhotos)

	// Events
	if result.TotalEvents > 0 {
		fmt.Printf("\n%s📅 Events%s   %d\n", f.Bold, f.Reset, result.TotalEvents)
	}

	// Bookings
	if result.TotalBookings > 0 {
		fmt.Printf("\n%s🏠 Bookings%s %d\n", f.Bold, f.Reset, result.TotalBookings)
	}

	fmt.Println()
}

// fmtTokens formats a token amount with 2 decimals and thousands separators
func fmtTokens(v float64) string {
	return fmtNumber(v)
}

// fmtTokensSigned formats with +/- prefix
func fmtTokensSigned(v float64) string {
	if v >= 0 {
		return "+" + fmtTokens(v)
	}
	return "-" + fmtTokens(-v)
}

func printStatsOverviewHelp() {
	f := Fmt
	fmt.Printf(`
%schb stats%s — Show comprehensive data overview

%sUSAGE%s
  %schb stats%s [year[/month]] [options]

Shows CHT tokens issued/burned, financial in/out per account,
messages per Discord channel, and photo count.

%sOPTIONS%s
  %s<year>%s              Show stats for a year (e.g. 2025)
  %s<year/month>%s        Show stats for a month (e.g. 2025/03)
  %s--format json%s       Output as JSON
  %s--help, -h%s          Show this help

%sEXAMPLES%s
  %schb stats%s                All-time overview
  %schb stats 2025%s           2025 overview
  %schb stats 2025/03%s        March 2025 overview
  %schb stats --format json%s  JSON output

%sNOTE%s
  Reads from synced + generated data. Run %schb sync --history --force%s first
  for a complete dataset.
`,
		f.Bold, f.Reset,
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
