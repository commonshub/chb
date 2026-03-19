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

// MembersFile represents members.json
type MembersFile struct {
	Summary struct {
		TotalMembers   int `json:"totalMembers"`
		ActiveMembers  int `json:"activeMembers"`
		MonthlyMembers int `json:"monthlyMembers"`
		YearlyMembers  int `json:"yearlyMembers"`
		MRR            struct {
			Value float64 `json:"value"`
		} `json:"mrr"`
	} `json:"summary"`
}

func Report(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printReportHelp()
		return nil
	}

	if len(args) == 0 {
		return fmt.Errorf("usage: chb report <YYYY/MM> or <YYYY>")
	}

	period := args[0]

	// Parse period: YYYY/MM or YYYY
	parts := strings.Split(period, "/")
	if len(parts) == 2 {
		return monthlyReport(parts[0], parts[1])
	} else if len(parts) == 1 && len(parts[0]) == 4 {
		return yearlyReport(parts[0])
	}

	return fmt.Errorf("invalid period format: %s (use YYYY/MM or YYYY)", period)
}

func monthlyReport(year, month string) error {
	// Pad month
	if len(month) == 1 {
		month = "0" + month
	}

	monthName := monthNameFromNumber(month)
	fmt.Printf("\n%s📊 Report for %s %s%s\n\n", Fmt.Bold, monthName, year, Fmt.Reset)

	dataDir := DataDir()

	// ── Events ──
	events := loadMonthEvents(dataDir, year, month)
	printEventsSummary(events)

	// ── Members ──
	printMembersSummary(dataDir, year, month)

	// ── Discord Messages ──
	printDiscordSummary(dataDir, year, month)

	// ── Transactions ──
	printTransactionsSummary(dataDir, year, month)

	fmt.Println()
	return nil
}

func yearlyReport(year string) error {
	fmt.Printf("\n%s📊 Report for %s%s\n\n", Fmt.Bold, year, Fmt.Reset)

	dataDir := DataDir()

	// Find all months with data
	yearDir := filepath.Join(dataDir, year)
	if !fileExists(yearDir) {
		return fmt.Errorf("no data for year %s", year)
	}

	entries, err := os.ReadDir(yearDir)
	if err != nil {
		return err
	}

	var months []string
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 2 {
			months = append(months, e.Name())
		}
	}
	sort.Strings(months)

	if len(months) == 0 {
		fmt.Println("No monthly data found.")
		return nil
	}

	// Aggregate
	totalEvents := 0
	totalAttendance := 0
	eventsWithAttendance := 0
	totalTicketRevenue := 0.0
	totalFridgeIncome := 0.0
	totalMessages := 0

	type monthData struct {
		month    string
		events   int
		attend   int
		messages int
		income   float64
		expenses float64
	}
	var breakdown []monthData

	// Track per-channel message counts across year
	yearlyChannelMessages := make(map[string]int)

	for _, month := range months {
		events := loadMonthEvents(dataDir, year, month)
		monthEvents := len(events)
		totalEvents += monthEvents

		monthAttendance := 0
		monthTicketRevenue := 0.0
		monthFridgeIncome := 0.0
		for _, e := range events {
			if e.Metadata.Attendance != nil && *e.Metadata.Attendance > 0 {
				monthAttendance += *e.Metadata.Attendance
				eventsWithAttendance++
			}
			if e.Metadata.TicketRevenue != nil {
				monthTicketRevenue += *e.Metadata.TicketRevenue
			}
			if e.Metadata.FridgeIncome != nil {
				monthFridgeIncome += *e.Metadata.FridgeIncome
			}
		}
		totalAttendance += monthAttendance
		totalTicketRevenue += monthTicketRevenue
		totalFridgeIncome += monthFridgeIncome

		msgCount, channelCounts := countDiscordMessages(dataDir, year, month)
		totalMessages += msgCount
		for ch, count := range channelCounts {
			yearlyChannelMessages[ch] += count
		}

		income, expenses := calculateMonthTransactions(dataDir, year, month)

		breakdown = append(breakdown, monthData{
			month:    month,
			events:   monthEvents,
			attend:   monthAttendance,
			messages: msgCount,
			income:   income,
			expenses: expenses,
		})
	}

	// Print aggregated summary
	fmt.Printf("%sEvents:%s %d", Fmt.Bold, Fmt.Reset, totalEvents)
	if eventsWithAttendance > 0 {
		fmt.Printf(" (%d with attendance data)", eventsWithAttendance)
	}
	fmt.Println()
	if totalAttendance > 0 {
		fmt.Printf("  Total attendance: %d\n", totalAttendance)
	}
	if totalTicketRevenue > 0 {
		fmt.Printf("  Ticket revenue: €%.2f\n", totalTicketRevenue)
	}
	if totalFridgeIncome > 0 {
		fmt.Printf("  Fridge income: €%.2f\n", totalFridgeIncome)
	}

	if totalMessages > 0 {
		fmt.Printf("\n%sDiscord Messages:%s %d\n", Fmt.Bold, Fmt.Reset, totalMessages)
		printChannelCounts(yearlyChannelMessages)
	}

	// Yearly totals
	var yearIncome, yearExpenses float64
	for _, m := range breakdown {
		yearIncome += m.income
		yearExpenses += m.expenses
	}
	if yearIncome > 0 || yearExpenses > 0 {
		fmt.Printf("\n%sTransactions:%s\n", Fmt.Bold, Fmt.Reset)
		fmt.Printf("  Income:   €%.2f\n", yearIncome)
		fmt.Printf("  Expenses: €%.2f\n", yearExpenses)
		fmt.Printf("  Net:      €%.2f\n", yearIncome-yearExpenses)
	}

	// Month-by-month breakdown
	fmt.Printf("\n%sMonth-by-Month:%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("  %s%-6s %6s %6s %8s %10s %10s%s\n",
		Fmt.Dim, "MONTH", "EVENTS", "ATTEND", "MESSAGES", "INCOME", "EXPENSES", Fmt.Reset)
	for _, m := range breakdown {
		monthName := monthNameShort(m.month)
		fmt.Printf("  %-6s %6d %6d %8d %10s %10s\n",
			monthName, m.events, m.attend, m.messages,
			formatEuro(m.income), formatEuro(m.expenses))
	}

	fmt.Println()
	return nil
}

// ── Data loading helpers ──

func loadMonthEvents(dataDir, year, month string) []EventEntry {
	eventsPath := filepath.Join(dataDir, year, month, "generated", "events.json")
	data, err := os.ReadFile(eventsPath)
	if err != nil {
		return nil
	}

	var ef EventsFile
	if err := json.Unmarshal(data, &ef); err != nil {
		return nil
	}
	return ef.Events
}

func printEventsSummary(events []EventEntry) {
	if len(events) == 0 {
		fmt.Printf("%sEvents:%s 0\n", Fmt.Bold, Fmt.Reset)
		return
	}

	withAttendance := 0
	totalAttendance := 0
	ticketRevenue := 0.0
	fridgeIncome := 0.0

	for _, e := range events {
		if e.Metadata.Attendance != nil && *e.Metadata.Attendance > 0 {
			withAttendance++
			totalAttendance += *e.Metadata.Attendance
		}
		if e.Metadata.TicketRevenue != nil {
			ticketRevenue += *e.Metadata.TicketRevenue
		}
		if e.Metadata.FridgeIncome != nil {
			fridgeIncome += *e.Metadata.FridgeIncome
		}
	}

	fmt.Printf("%sEvents:%s %d", Fmt.Bold, Fmt.Reset, len(events))
	if withAttendance > 0 {
		fmt.Printf(" (%d with attendance data)", withAttendance)
	}
	fmt.Println()

	if totalAttendance > 0 {
		fmt.Printf("  Total attendance: %d\n", totalAttendance)
	}
	if ticketRevenue > 0 {
		fmt.Printf("  Ticket revenue: €%.2f\n", ticketRevenue)
	}
	if fridgeIncome > 0 {
		fmt.Printf("  Fridge income: €%.2f\n", fridgeIncome)
	}
}

func printMembersSummary(dataDir, year, month string) {
	membersPath := filepath.Join(dataDir, year, month, "generated", "members.json")
	data, err := os.ReadFile(membersPath)
	if err != nil {
		return
	}

	var mf MembersFile
	if err := json.Unmarshal(data, &mf); err != nil {
		return
	}

	if mf.Summary.ActiveMembers > 0 {
		fmt.Printf("\n%sMembers:%s %d active", Fmt.Bold, Fmt.Reset, mf.Summary.ActiveMembers)
		if mf.Summary.MonthlyMembers > 0 || mf.Summary.YearlyMembers > 0 {
			fmt.Printf(" (%d monthly, %d yearly)", mf.Summary.MonthlyMembers, mf.Summary.YearlyMembers)
		}
		fmt.Println()
		if mf.Summary.MRR.Value > 0 {
			fmt.Printf("  MRR: €%.2f\n", mf.Summary.MRR.Value)
		}
	}
}

func printDiscordSummary(dataDir, year, month string) {
	total, channelCounts := countDiscordMessages(dataDir, year, month)
	if total == 0 {
		return
	}

	fmt.Printf("\n%sDiscord Messages:%s %d\n", Fmt.Bold, Fmt.Reset, total)
	printChannelCounts(channelCounts)
}

func countDiscordMessages(dataDir, year, month string) (int, map[string]int) {
	discordDir := filepath.Join(dataDir, year, month, "messages", "discord")
	channelCounts := make(map[string]int)

	if !fileExists(discordDir) {
		return 0, channelCounts
	}

	entries, err := os.ReadDir(discordDir)
	if err != nil {
		return 0, channelCounts
	}

	total := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		channelID := e.Name()
		messagesPath := filepath.Join(discordDir, channelID, "messages.json")

		data, err := os.ReadFile(messagesPath)
		if err != nil {
			continue
		}

		var cache struct {
			Messages []json.RawMessage `json:"messages"`
		}
		if err := json.Unmarshal(data, &cache); err != nil {
			continue
		}

		count := len(cache.Messages)
		channelCounts[channelID] = count
		total += count
	}

	return total, channelCounts
}

func printChannelCounts(channelCounts map[string]int) {
	// Load settings for channel name mapping
	channelNames := make(map[string]string)
	if settings, err := LoadSettings(); err == nil {
		channels := GetDiscordChannelIDs(settings)
		for name, id := range channels {
			channelNames[id] = name
		}
	}

	// Sort by count descending
	type channelCount struct {
		id    string
		name  string
		count int
	}
	var sorted []channelCount
	for id, count := range channelCounts {
		name := channelNames[id]
		if name == "" {
			name = id
		}
		sorted = append(sorted, channelCount{id, name, count})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})

	for _, ch := range sorted {
		fmt.Printf("  #%-25s %d\n", ch.name, ch.count)
	}
}

func printTransactionsSummary(dataDir, year, month string) {
	settings, err := LoadSettings()
	if err != nil {
		return
	}

	type accountSummary struct {
		name     string
		chain    string
		token    string
		income   float64
		expenses float64
		inCount  int
		outCount int
	}

	var summaries []accountSummary

	for _, acc := range settings.Finance.Accounts {
		if acc.Provider == "etherscan" && acc.Token != nil {
			filePath := filepath.Join(dataDir, year, month, "finance", acc.Chain,
				fmt.Sprintf("%s.%s.json", acc.Slug, acc.Token.Symbol))

			data, err := os.ReadFile(filePath)
			if err != nil {
				continue
			}

			var cache TransactionsCacheFile
			if err := json.Unmarshal(data, &cache); err != nil {
				continue
			}

			var income, expenses float64
			var inCount, outCount int
			addrLower := strings.ToLower(acc.Address)

			for _, tx := range cache.Transactions {
				val := parseTokenValue(tx.Value, acc.Token.Decimals)
				if strings.ToLower(tx.To) == addrLower {
					income += val
					inCount++
				} else if strings.ToLower(tx.From) == addrLower {
					expenses += val
					outCount++
				}
			}

			if inCount > 0 || outCount > 0 {
				summaries = append(summaries, accountSummary{
					name:     acc.Name,
					chain:    acc.Chain,
					token:    acc.Token.Symbol,
					income:   income,
					expenses: expenses,
					inCount:  inCount,
					outCount: outCount,
				})
			}
		}

		if acc.Provider == "stripe" {
			accountID := acc.AccountID
			if accountID == "" {
				accountID = acc.Slug
			}
			filePath := filepath.Join(dataDir, year, month, "finance", "stripe",
				fmt.Sprintf("%s.json", accountID))

			data, err := os.ReadFile(filePath)
			if err != nil {
				continue
			}

			var cache struct {
				Transactions []struct {
					Amount int `json:"amount"`
					Net    int `json:"net"`
					Fee    int `json:"fee"`
				} `json:"transactions"`
			}
			if err := json.Unmarshal(data, &cache); err != nil {
				continue
			}

			var income, expenses float64
			var inCount, outCount int
			for _, tx := range cache.Transactions {
				amountEur := float64(tx.Net) / 100.0
				if amountEur > 0 {
					income += amountEur
					inCount++
				} else {
					expenses += math.Abs(amountEur)
					outCount++
				}
			}

			if inCount > 0 || outCount > 0 {
				summaries = append(summaries, accountSummary{
					name:     acc.Name,
					chain:    "stripe",
					token:    "EUR",
					income:   income,
					expenses: expenses,
					inCount:  inCount,
					outCount: outCount,
				})
			}
		}
	}

	if len(summaries) == 0 {
		return
	}

	fmt.Printf("\n%sTransactions:%s\n", Fmt.Bold, Fmt.Reset)
	for _, s := range summaries {
		label := s.name
		if s.chain != "stripe" {
			label = fmt.Sprintf("%s/%s", s.chain, s.token)
		}
		fmt.Printf("  %s:\n", label)
		if s.inCount > 0 {
			fmt.Printf("    Income:   €%.2f (%d transactions)\n", s.income, s.inCount)
		}
		if s.outCount > 0 {
			fmt.Printf("    Expenses: €%.2f (%d transactions)\n", s.expenses, s.outCount)
		}
		fmt.Printf("    Net:      €%.2f\n", s.income-s.expenses)
	}
}

func calculateMonthTransactions(dataDir, year, month string) (income, expenses float64) {
	settings, err := LoadSettings()
	if err != nil {
		return 0, 0
	}

	for _, acc := range settings.Finance.Accounts {
		if acc.Provider == "etherscan" && acc.Token != nil {
			filePath := filepath.Join(dataDir, year, month, "finance", acc.Chain,
				fmt.Sprintf("%s.%s.json", acc.Slug, acc.Token.Symbol))

			data, err := os.ReadFile(filePath)
			if err != nil {
				continue
			}

			var cache TransactionsCacheFile
			if err := json.Unmarshal(data, &cache); err != nil {
				continue
			}

			addrLower := strings.ToLower(acc.Address)
			for _, tx := range cache.Transactions {
				val := parseTokenValue(tx.Value, acc.Token.Decimals)
				if strings.ToLower(tx.To) == addrLower {
					income += val
				} else if strings.ToLower(tx.From) == addrLower {
					expenses += val
				}
			}
		}

		if acc.Provider == "stripe" {
			accountID := acc.AccountID
			if accountID == "" {
				accountID = acc.Slug
			}
			filePath := filepath.Join(dataDir, year, month, "finance", "stripe",
				fmt.Sprintf("%s.json", accountID))

			data, err := os.ReadFile(filePath)
			if err != nil {
				continue
			}

			var cache struct {
				Transactions []struct {
					Net int `json:"net"`
				} `json:"transactions"`
			}
			if err := json.Unmarshal(data, &cache); err != nil {
				continue
			}

			for _, tx := range cache.Transactions {
				amountEur := float64(tx.Net) / 100.0
				if amountEur > 0 {
					income += amountEur
				} else {
					expenses += math.Abs(amountEur)
				}
			}
		}
	}

	return income, expenses
}

// ── Formatting helpers ──

func monthNameFromNumber(month string) string {
	m, _ := strconv.Atoi(month)
	if m < 1 || m > 12 {
		return month
	}
	return time.Month(m).String()
}

func monthNameShort(month string) string {
	m, _ := strconv.Atoi(month)
	if m < 1 || m > 12 {
		return month
	}
	return time.Month(m).String()[:3]
}

func formatEuro(v float64) string {
	if v == 0 {
		return "—"
	}
	return fmt.Sprintf("€%.0f", v)
}

func printReportHelp() {
	f := Fmt
	fmt.Printf(`
%schb report%s — Generate reports from local data

%sUSAGE%s
  %schb report%s <YYYY/MM>   Monthly report
  %schb report%s <YYYY>      Yearly report

%sEXAMPLES%s
  %s$ chb report 2025/11     # November 2025 report
  $ chb report 2025         # Full year 2025 report%s
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Dim, f.Reset,
	)
}
