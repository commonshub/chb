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

	discordsource "github.com/CommonsHub/chb/providers/discord"
	etherscansource "github.com/CommonsHub/chb/providers/etherscan"
	stripesource "github.com/CommonsHub/chb/providers/stripe"
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
		return fmt.Errorf("usage: chb report <YYYY/MM>, <YYYYMM>, or <YYYY>")
	}

	year, month, found := ParseYearMonthArg(args)
	if found {
		if month != "" {
			return monthlyReport(year, month)
		}
		return yearlyReport(year)
	}

	return fmt.Errorf("invalid period format: %s (use YYYY/MM, YYYYMM, or YYYY)", args[0])
}

func monthlyReport(year, month string) error {
	// Pad month
	if len(month) == 1 {
		month = "0" + month
	}

	monthName := monthNameFromNumber(month)
	fmt.Printf("\n%s📊 Report for %s %s%s\n\n", Fmt.Bold, monthName, year, Fmt.Reset)

	dataDir := DataDir()

	if report := loadGeneratedMonthlyReport(dataDir, year, month); report != nil {
		printGeneratedMonthlyReport(*report, dataDir, year, month)
		fmt.Println()
		return nil
	}

	fmt.Printf("%sNo generated/summary.json found; using legacy local summary. Run `chb generate %s/%s` to refresh the monthly summary.%s\n\n",
		Fmt.Dim, year, month, Fmt.Reset)

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

func loadGeneratedMonthlyReport(dataDir, year, month string) *MonthlyReportFile {
	path := filepath.Join(dataDir, year, month, "generated", "summary.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var report MonthlyReportFile
	if err := json.Unmarshal(data, &report); err != nil {
		return nil
	}
	return &report
}

func printGeneratedMonthlyReport(report MonthlyReportFile, dataDir, year, month string) {
	fmt.Printf("%s👥 Contributors%s  %s%d%s\n", Fmt.Bold, Fmt.Reset, Fmt.Cyan, report.Summary.Contributors, Fmt.Reset)

	settings, _ := LoadSettings()
	tokenSymbols := tokenSymbolSet(settings, report.Tokens)

	tokens := report.Tokens
	fmt.Printf("\n%s🪙 Tokens%s\n", Fmt.Bold, Fmt.Reset)
	if len(tokens) == 0 {
		fmt.Printf("  %sNo token activity%s\n", Fmt.Dim, Fmt.Reset)
	} else {
		for _, token := range tokens {
			fmt.Printf("  %-8s minted %s  burnt %s  txs %d  supply %s  holders %d  active %d\n",
				token.Symbol,
				colorReportAmount(token.Minted, token.Symbol, Fmt.Green),
				colorReportAmount(token.Burnt, token.Symbol, Fmt.Red),
				token.Transactions,
				formatReportAmount(token.TotalSupply, token.Symbol),
				token.TokenHolders,
				token.ActiveTokenHolders)

			receivers, spenders := tokenTopFlows(dataDir, year, month, token, settings)
			printTopFlows("Top receivers", receivers, token.Symbol)
			printTopFlows("Top spenders", spenders, token.Symbol)
			if len(receivers) > 0 || len(spenders) > 0 {
				fmt.Println()
			}
		}
	}

	visibleCurrencies, currencyMembers := filterAndRegroupCurrencies(report.Currencies, tokenSymbols)

	fmt.Printf("\n%s💶 Currencies%s\n", Fmt.Bold, Fmt.Reset)
	if len(visibleCurrencies) == 0 {
		fmt.Printf("  %sNo currency activity%s\n", Fmt.Dim, Fmt.Reset)
	} else {
		for _, cur := range visibleCurrencies {
			fmt.Printf("  %-8s total in %s  out %s  net %s\n",
				cur.Currency,
				colorReportAmount(cur.In, cur.Currency, Fmt.Green),
				colorReportAmount(cur.Out, cur.Currency, Fmt.Red),
				colorNetReportAmount(cur.Net, cur.Currency))

			customers, vendors := currencyTopFlows(dataDir, year, month, currencyMembers[cur.Currency])
			printTopFlows("Top customers", customers, cur.Currency)
			printTopFlows("Top vendors", vendors, cur.Currency)
			if len(customers) > 0 || len(vendors) > 0 {
				fmt.Println()
			}
		}
	}

	printTaggedFlowSection("🤝 Collectives", report.Collectives)
	printTaggedFlowSection("🏷  Categories", report.Categories)

	fmt.Printf("\n%s🏦 Accounts%s\n", Fmt.Bold, Fmt.Reset)
	if len(report.Accounts) > 0 {
		for _, acct := range report.Accounts {
			slug, name := monthlyReportAccountSlugAndName(acct)
			fmt.Printf("  %s%s%s", Fmt.Bold, slug, Fmt.Reset)
			if name != "" && name != slug {
				fmt.Printf("  %s%s%s", Fmt.Dim, name, Fmt.Reset)
			}
			fmt.Printf("  in %s  out %s  net %s  start %s  end %s\n",
				colorReportAmount(acct.Amounts.In, acct.Currency, Fmt.Green),
				colorReportAmount(acct.Amounts.Out, acct.Currency, Fmt.Red),
				colorNetReportAmount(acct.Amounts.Net, acct.Currency),
				formatOptionalReportAmount(acct.Balance.Opening, acct.Currency),
				formatOptionalReportAmount(acct.Balance.Ending, acct.Currency))
		}
	} else {
		fmt.Printf("  %sNo account activity%s\n", Fmt.Dim, Fmt.Reset)
	}
}

func monthlyReportAccountSlugAndName(acct MonthlyReportAccount) (string, string) {
	slug := acct.AccountSlug
	if slug == "" {
		slug = acct.Account
	}
	if slug == "" && acct.Token != nil {
		slug = acct.Token.Symbol
	}
	if slug == "" {
		slug = acct.Source
	}
	name := acct.AccountName
	return slug, name
}

func tokenSymbolSet(settings *Settings, reportTokens []MonthlyReportTokenData) map[string]bool {
	out := map[string]bool{}
	if settings != nil {
		for _, t := range settings.Tokens {
			sym := strings.ToUpper(strings.TrimSpace(t.Symbol))
			if sym != "" {
				out[sym] = true
			}
		}
	}
	for _, t := range reportTokens {
		sym := strings.ToUpper(strings.TrimSpace(t.Symbol))
		if sym != "" {
			out[sym] = true
		}
	}
	return out
}

func filterAndRegroupCurrencies(currencies []MonthlyReportCurrency, tokenSymbols map[string]bool) ([]MonthlyReportCurrency, map[string][]string) {
	members := map[string][]string{}
	var eurAgg MonthlyReportCurrency
	eurAgg.Currency = "EUR"
	var eurMembers []string
	others := []MonthlyReportCurrency{}

	for _, c := range currencies {
		upper := strings.ToUpper(strings.TrimSpace(c.Currency))
		if tokenSymbols[upper] {
			continue
		}
		if isEURCurrency(c.Currency) {
			eurAgg.Transactions += c.Transactions
			eurAgg.In = roundReportAmount(eurAgg.In + c.In)
			eurAgg.Out = roundReportAmount(eurAgg.Out + c.Out)
			eurAgg.Fees = roundReportAmount(eurAgg.Fees + c.Fees)
			eurMembers = append(eurMembers, c.Currency)
			continue
		}
		others = append(others, c)
		members[c.Currency] = []string{c.Currency}
	}

	out := []MonthlyReportCurrency{}
	if len(eurMembers) > 0 {
		eurAgg.Net = roundReportAmount(eurAgg.In - eurAgg.Out - eurAgg.Fees)
		out = append(out, eurAgg)
		members["EUR"] = eurMembers
	}
	out = append(out, others...)
	return out, members
}

type counterpartyAmount struct {
	Name   string
	Amount float64
}

func currencyTopFlows(dataDir, year, month string, currencyMembers []string) (customers, vendors []counterpartyAmount) {
	if len(currencyMembers) == 0 {
		return nil, nil
	}
	ccySet := map[string]bool{}
	for _, c := range currencyMembers {
		ccySet[strings.ToUpper(strings.TrimSpace(c))] = true
	}
	txFile := LoadTransactionsWithPII(dataDir, year, month)
	if txFile == nil {
		return nil, nil
	}
	credits := map[string]float64{}
	debits := map[string]float64{}
	for _, tx := range txFile.Transactions {
		ccy := strings.ToUpper(strings.TrimSpace(tx.Currency))
		if !ccySet[ccy] {
			continue
		}
		cp := strings.TrimSpace(tx.Counterparty)
		if cp == "" {
			continue
		}
		amount := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
		switch strings.ToUpper(tx.Type) {
		case "CREDIT":
			credits[cp] = roundReportAmount(credits[cp] + amount)
		case "DEBIT":
			debits[cp] = roundReportAmount(debits[cp] + amount)
		}
	}
	return topNCounterparties(credits, 10), topNCounterparties(debits, 10)
}

type addressBucket struct {
	amount float64
	label  string
}

func tokenTopFlows(dataDir, year, month string, token MonthlyReportTokenData, _ *Settings) (receivers, spenders []counterpartyAmount) {
	txFile := LoadTransactionsWithPII(dataDir, year, month)
	if txFile == nil {
		return nil, nil
	}

	zeroAddr := "0x0000000000000000000000000000000000000000"
	receivedBy := map[string]*addressBucket{}
	sentBy := map[string]*addressBucket{}

	accumulate := func(m map[string]*addressBucket, addr, name string, amount float64) {
		if addr == "" || strings.EqualFold(addr, zeroAddr) {
			return
		}
		key := strings.ToLower(addr)
		b := m[key]
		if b == nil {
			b = &addressBucket{}
			m[key] = b
		}
		b.amount = roundReportAmount(b.amount + amount)
		if name != "" {
			b.label = name
		}
	}

	for _, tx := range txFile.Transactions {
		// Token-wide tracking only — wallet-specific txs are accounted under
		// their account, not under the token-level top flows. The schema for
		// token-wide entries:
		//   accountId / Account              = sender address
		//   accountName                      = sender label (or fallback)
		//   counterpartyId / Counterparty    = receiver address or label
		// Sender/receiver addresses come from the URI; names live on
		// AccountName / Counterparty when Nostr address metadata is known.
		if !strings.EqualFold(tx.Currency, token.Symbol) || strings.TrimSpace(tx.Account) != "" {
			continue
		}
		from := strings.ToLower(strings.TrimSpace(tx.Account))
		fromName := tx.AccountName
		// AccountName falls back to the generic "⛓️ Gnosis CHT" label when
		// no nostr name was resolved — don't pass that through as a
		// person's label, the address is more useful.
		if strings.HasPrefix(fromName, "⛓️") {
			fromName = ""
		}
		to := strings.ToLower(strings.TrimSpace(tx.Counterparty))
		toName := ""
		// If Counterparty looks like a hex address it's the raw address;
		// otherwise it's a resolved label, so split them.
		if !strings.HasPrefix(to, "0x") {
			toName = tx.Counterparty
			to = ""
		}
		amount := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
		accumulate(receivedBy, to, toName, amount)
		accumulate(sentBy, from, fromName, amount)
	}
	return topNFromBuckets(receivedBy, 10), topNFromBuckets(sentBy, 10)
}

func topNFromBuckets(m map[string]*addressBucket, n int) []counterpartyAmount {
	out := make([]counterpartyAmount, 0, len(m))
	for addr, b := range m {
		if b.amount <= 0 {
			continue
		}
		display := b.label
		if display == "" {
			display = addr
		}
		out = append(out, counterpartyAmount{Name: display, Amount: b.amount})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Amount != out[j].Amount {
			return out[i].Amount > out[j].Amount
		}
		return out[i].Name < out[j].Name
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

func topNCounterparties(m map[string]float64, n int) []counterpartyAmount {
	out := make([]counterpartyAmount, 0, len(m))
	for k, v := range m {
		if v <= 0 {
			continue
		}
		out = append(out, counterpartyAmount{Name: k, Amount: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Amount != out[j].Amount {
			return out[i].Amount > out[j].Amount
		}
		return out[i].Name < out[j].Name
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

func printTaggedFlowSection(header string, entries []TaggedSummary) {
	fmt.Printf("\n%s%s%s\n", Fmt.Bold, header, Fmt.Reset)
	if len(entries) == 0 {
		fmt.Printf("  %sNone%s\n", Fmt.Dim, Fmt.Reset)
		return
	}
	tagWidth := 12
	for _, e := range entries {
		if l := len(e.Slug); l > tagWidth {
			tagWidth = l
		}
	}
	for _, e := range entries {
		for i, c := range e.Currencies {
			slug := ""
			if i == 0 {
				slug = e.Slug
			}
			extras := ""
			if c.Fees > 0 {
				extras += fmt.Sprintf("  fees %s", colorReportAmount(c.Fees, c.Currency, Fmt.Yellow))
			}
			if c.VAT > 0 {
				extras += fmt.Sprintf("  vat %s", colorReportAmount(c.VAT, c.Currency, Fmt.Yellow))
			}
			fmt.Printf("  %-*s  %-4s in %s  out %s%s  net %s\n",
				tagWidth, slug, c.Currency,
				colorReportAmount(c.In, c.Currency, Fmt.Green),
				colorReportAmount(c.Out, c.Currency, Fmt.Red),
				extras,
				colorNetReportAmount(c.Net, c.Currency))
		}
	}
}

func printTopFlows(label string, flows []counterpartyAmount, currency string) {
	if len(flows) == 0 {
		return
	}
	fmt.Printf("    %s%s%s\n", Fmt.Dim, label, Fmt.Reset)
	for _, f := range flows {
		fmt.Printf("      %-42s %s\n", displayCounterpartyName(f.Name), formatReportAmount(f.Amount, currency))
	}
}

func displayCounterpartyName(name string) string {
	trimmed := strings.TrimSpace(name)
	if strings.HasPrefix(strings.ToLower(trimmed), "0x") && len(trimmed) == 42 {
		return truncateAddr(trimmed)
	}
	if len(trimmed) > 42 {
		return trimmed[:39] + "..."
	}
	return trimmed
}

func printMonthlyReportSource(src MonthlyReportSource) {
	fmt.Printf("  %-10s records: %d", src.Source, src.Records)
	if src.Attachments > 0 {
		fmt.Printf("  attachments: %d", src.Attachments)
	}
	if len(src.Summary) > 0 {
		fmt.Printf("  %s", monthlyReportSourceSummary(src.Summary, "byCalendar"))
	}
	fmt.Println()
	if src.Source == "ics" {
		printMonthlyReportCalendarBreakdown(src.Summary)
	}
}

func monthlyReportAccountLabel(acct MonthlyReportAccount) string {
	parts := []string{}
	if acct.AccountName != "" {
		parts = append(parts, acct.AccountName)
	} else if acct.AccountSlug != "" {
		parts = append(parts, acct.AccountSlug)
	} else if acct.Account != "" {
		parts = append(parts, acct.Account)
	}
	if acct.Chain != "" {
		parts = append(parts, acct.Chain)
	}
	if acct.Currency != "" {
		parts = append(parts, acct.Currency)
	}
	if len(parts) == 0 {
		return acct.Source
	}
	return fmt.Sprintf("%s (%s)", strings.Join(parts, " / "), acct.Source)
}

func monthlyReportCountsLabel(counts MonthlyReportCounts) string {
	parts := []string{}
	add := func(label string, n int) {
		if n > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", label, n))
		}
	}
	add("credits", counts.Credits)
	add("debits", counts.Debits)
	add("internal", counts.Internal)
	add("mints", counts.Mints)
	add("burns", counts.Burns)
	add("transfers", counts.Transfers)
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

func monthlyReportSourceSummary(summary map[string]interface{}, skipKeys ...string) string {
	skip := map[string]bool{}
	for _, key := range skipKeys {
		skip[key] = true
	}
	keys := make([]string, 0, len(summary))
	for k := range summary {
		if skip[k] {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", key, summary[key]))
	}
	return strings.Join(parts, " ")
}

func printMonthlyReportCalendarBreakdown(summary map[string]interface{}) {
	raw, ok := summary["byCalendar"]
	if !ok {
		return
	}
	calendars := map[string]map[string]int{}
	switch v := raw.(type) {
	case map[string]map[string]int:
		calendars = v
	case map[string]interface{}:
		for slug, val := range v {
			counts := map[string]int{}
			if m, ok := val.(map[string]interface{}); ok {
				for key, n := range m {
					switch x := n.(type) {
					case float64:
						counts[key] = int(x)
					case int:
						counts[key] = x
					}
				}
			}
			calendars[slug] = counts
		}
	default:
		return
	}
	if len(calendars) == 0 {
		return
	}
	keys := make([]string, 0, len(calendars))
	for slug := range calendars {
		keys = append(keys, slug)
	}
	sort.Strings(keys)
	for _, slug := range keys {
		counts := calendars[slug]
		fmt.Printf("    %-20s events: %d  bookings: %d\n", slug, counts["events"], counts["bookings"])
	}
}

func formatOptionalReportAmount(v *float64, currency string) string {
	if v == nil {
		return "n/a"
	}
	return formatReportAmount(*v, currency)
}

func colorReportAmount(v float64, currency, color string) string {
	return color + formatReportAmount(v, currency) + Fmt.Reset
}

func colorNetReportAmount(v float64, currency string) string {
	color := Fmt.Green
	if v < 0 {
		color = Fmt.Red
	} else if v == 0 {
		color = Fmt.Dim
	}
	return colorReportAmount(v, currency, color)
}

func formatReportAmount(v float64, currency string) string {
	if strings.EqualFold(currency, "EUR") || strings.EqualFold(currency, "EURe") || strings.EqualFold(currency, "EURb") {
		return fmt.Sprintf("%.2f EUR", v)
	}
	if currency == "" {
		return fmt.Sprintf("%.2f", v)
	}
	return fmt.Sprintf("%.8g %s", v, currency)
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
		tickets  int
		messages int
		income   float64
		expenses float64
	}
	var breakdown []monthData
	totalTickets := 0

	// Track per-channel message counts across year
	yearlyChannelMessages := make(map[string]int)

	for _, month := range months {
		events := loadMonthEvents(dataDir, year, month)
		monthEvents := len(events)
		totalEvents += monthEvents

		monthAttendance := 0
		monthTickets := 0
		monthTicketRevenue := 0.0
		monthFridgeIncome := 0.0
		for _, e := range events {
			if e.Metadata.Attendance != nil && *e.Metadata.Attendance > 0 {
				monthAttendance += *e.Metadata.Attendance
				eventsWithAttendance++
			}
			if e.Metadata.TicketsSold != nil && *e.Metadata.TicketsSold > 0 {
				monthTickets += *e.Metadata.TicketsSold
			}
			if e.Metadata.TicketRevenue != nil {
				monthTicketRevenue += *e.Metadata.TicketRevenue
			}
			if e.Metadata.FridgeIncome != nil {
				monthFridgeIncome += *e.Metadata.FridgeIncome
			}
		}
		totalAttendance += monthAttendance
		totalTickets += monthTickets
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
			tickets:  monthTickets,
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
	if totalTickets > 0 {
		fmt.Printf("  Tickets sold: %d\n", totalTickets)
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
	fmt.Printf("  %s%-6s %6s %7s %8s %10s %10s%s\n",
		Fmt.Dim, "MONTH", "EVENTS", "TICKETS", "MESSAGES", "INCOME", "EXPENSES", Fmt.Reset)
	for _, m := range breakdown {
		monthName := monthNameShort(m.month)
		fmt.Printf("  %-6s %6d %7d %8d %10s %10s\n",
			monthName, m.events, m.tickets, m.messages,
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
	discordDir := discordsource.Path(dataDir, year, month)
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
		messagesPath := filepath.Join(discordDir, channelID, discordsource.MessagesFile)

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
			filePath, found := etherscansource.FindFileForAddr(dataDir, year, month, acc.Chain, acc.Slug, acc.Address, acc.Token.Symbol)
			if !found {
				continue
			}

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
			filePath := stripesource.TransactionCachePath(dataDir, year, month)
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

// calculateMonthTransactions returns EUR-family income/expense totals for a
// month, sourced from the generated transactions.json so that tx.Type is
// honored. INTERNAL rows (movements between accounts we own) are excluded so a
// Gnosis→Polygon EURe transfer doesn't get double-counted as both income and
// expense — matching the policy used by `chb transactions stats`
// (see cmd/transactions_stats.go).
func calculateMonthTransactions(dataDir, year, month string) (income, expenses float64) {
	txFile := LoadTransactionsWithPII(dataDir, year, month)
	if txFile == nil {
		return 0, 0
	}
	for _, tx := range txFile.Transactions {
		if tx.Type == "INTERNAL" {
			continue
		}
		if !isEURCurrency(tx.Currency) {
			continue
		}
		amount := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
		if amount == 0 {
			continue
		}
		switch {
		case tx.IsIncoming():
			income += amount
		case tx.IsOutgoing():
			expenses += amount
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
	return fmt.Sprintf("%.0f EUR", v)
}

func printReportHelp() {
	f := Fmt
	fmt.Printf(`
%schb report%s — Generate reports from local data

%sUSAGE%s
  %schb report%s <YYYY/MM>   Monthly report
  %schb report%s <YYYYMM>    Monthly report
  %schb report%s <YYYY>      Yearly report

%sEXAMPLES%s
  %s$ chb report 2025/11     # November 2025 report
  $ chb report 202511      # November 2025 report
  $ chb report 2025         # Full year 2025 report%s
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Dim, f.Reset,
	)
}
