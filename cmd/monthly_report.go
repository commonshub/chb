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

	"github.com/CommonsHub/chb/ical"
	discordsource "github.com/CommonsHub/chb/sources/discord"
	etherscansource "github.com/CommonsHub/chb/sources/etherscan"
	icssource "github.com/CommonsHub/chb/sources/ics"
	moneriumsource "github.com/CommonsHub/chb/sources/monerium"
	nostrsource "github.com/CommonsHub/chb/sources/nostr"
	odoosource "github.com/CommonsHub/chb/sources/odoo"
	stripesource "github.com/CommonsHub/chb/sources/stripe"
)

type MonthlyReportFile struct {
	Year        string                      `json:"year"`
	Month       string                      `json:"month"`
	GeneratedAt string                      `json:"generatedAt"`
	Summary     MonthlyReportSummary        `json:"summary"`
	Accounts    []MonthlyReportAccount      `json:"accounts"`
	Tokens      []MonthlyReportTokenData    `json:"tokens,omitempty"`
	Currencies  []MonthlyReportCurrency     `json:"currencies,omitempty"`
	Collectives []MonthlyReportTaggedFlow   `json:"collectives,omitempty"`
	Categories  []MonthlyReportTaggedFlow   `json:"categories,omitempty"`
	Sources     []MonthlyReportSource       `json:"sources"`
	Notes       []string                    `json:"notes,omitempty"`
}

// MonthlyReportTaggedFlow summarizes credits/debits grouped by a tag (collective
// or category) per regrouped currency. EUR-family currencies are merged into
// "EUR"; other currencies are kept separate so token-denominated rows surface.
type MonthlyReportTaggedFlow struct {
	Tag          string  `json:"tag"`
	Currency     string  `json:"currency"`
	Transactions int     `json:"transactions"`
	In           float64 `json:"in"`
	Out          float64 `json:"out"`
	Net          float64 `json:"net"`
}

type MonthlyReportSummary struct {
	Contributors int `json:"contributors"`
	Images       int `json:"images"`
	Transactions int `json:"transactions"`
	Events       int `json:"events"`
	Bookings     int `json:"bookings"`
}

type MonthlyReportAccount struct {
	Source      string               `json:"source"`
	Account     string               `json:"account,omitempty"`
	AccountSlug string               `json:"accountSlug,omitempty"`
	AccountName string               `json:"accountName,omitempty"`
	Chain       string               `json:"chain,omitempty"`
	Currency    string               `json:"currency"`
	Token       *MonthlyReportToken  `json:"token,omitempty"`
	Counts      MonthlyReportCounts  `json:"counts"`
	Amounts     MonthlyReportAmounts `json:"amounts"`
	Balance     MonthlyReportBalance `json:"balance"`
}

type MonthlyReportToken struct {
	Symbol string `json:"symbol"`
	Chain  string `json:"chain,omitempty"`
}

type MonthlyReportTokenData struct {
	Slug                string  `json:"slug"`
	Name                string  `json:"name,omitempty"`
	Symbol              string  `json:"symbol"`
	Chain               string  `json:"chain,omitempty"`
	Mints               int     `json:"mints"`
	Burns               int     `json:"burns"`
	Transfers           int     `json:"transfers"`
	Transactions        int     `json:"transactions"`
	Minted              float64 `json:"minted"`
	Burnt               float64 `json:"burnt"`
	TotalSupply         float64 `json:"totalSupply"`
	TokenHolders        int     `json:"tokenHolders"`
	ActiveTokenHolders  int     `json:"activeTokenHolders"`
	ComputedFromHistory bool    `json:"computedFromHistory"`
}

type MonthlyReportCounts struct {
	Credits   int `json:"credits"`
	Debits    int `json:"debits"`
	Internal  int `json:"internal"`
	Mints     int `json:"mints"`
	Burns     int `json:"burns"`
	Transfers int `json:"transfers"`
}

type MonthlyReportAmounts struct {
	In   float64 `json:"in"`
	Out  float64 `json:"out"`
	Net  float64 `json:"net"`
	Fees float64 `json:"fees,omitempty"`
}

type MonthlyReportBalance struct {
	Opening  *float64 `json:"opening,omitempty"`
	Ending   *float64 `json:"ending,omitempty"`
	Delta    float64  `json:"delta"`
	Computed bool     `json:"computed"`
	Verified bool     `json:"verified"`
}

type MonthlyReportCurrency struct {
	Currency     string  `json:"currency"`
	Transactions int     `json:"transactions"`
	In           float64 `json:"in"`
	Out          float64 `json:"out"`
	Net          float64 `json:"net"`
	Fees         float64 `json:"fees,omitempty"`
}

type MonthlyReportSource struct {
	Source      string                 `json:"source"`
	Records     int                    `json:"records"`
	Attachments int                    `json:"attachments"`
	Summary     map[string]interface{} `json:"summary,omitempty"`
}

type MonthlyReportScope struct {
	Year  string
	Month string
}

type MonthlyReportContext struct {
	DataDir  string
	Settings *Settings
}

type MonthlyReportContributor interface {
	Source() string
	MonthlyReport(ctx MonthlyReportContext, scope MonthlyReportScope) (MonthlyReportSource, error)
}

func generateMonthlyReportGo(dataDir, year, month string, settings *Settings) bool {
	if year == "latest" || month == "" {
		return false
	}

	collectives, categories := buildMonthlyReportTaggedFlows(dataDir, year, month)
	report := MonthlyReportFile{
		Year:        year,
		Month:       month,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Summary:     readMonthlyReportSummary(dataDir, year, month),
		Accounts:    buildMonthlyReportAccounts(dataDir, year, month),
		Tokens:      buildMonthlyReportTokens(dataDir, year, month, settings),
		Currencies:  buildMonthlyReportCurrencies(dataDir, year, month),
		Collectives: collectives,
		Categories:  categories,
		Notes: []string{
			"opening and ending balances are omitted until a source provides full-history, live-balance verification",
		},
	}

	ctx := MonthlyReportContext{DataDir: dataDir, Settings: settings}
	scope := MonthlyReportScope{Year: year, Month: month}
	for _, contributor := range monthlyReportContributors() {
		src, err := contributor.MonthlyReport(ctx, scope)
		if err != nil || src.Records == 0 && src.Attachments == 0 && len(src.Summary) == 0 {
			continue
		}
		report.Sources = append(report.Sources, src)
	}
	sort.Slice(report.Sources, func(i, j int) bool {
		return report.Sources[i].Source < report.Sources[j].Source
	})

	data, err := marshalIndentedNoHTMLEscape(report)
	if err != nil {
		return false
	}
	return writeMonthFile(dataDir, year, month, filepath.Join("generated", "report.json"), data) == nil
}

func monthlyReportContributors() []MonthlyReportContributor {
	return []MonthlyReportContributor{
		discordMonthlyReportContributor{},
		icsMonthlyReportContributor{},
		stripeMonthlyReportContributor{},
		etherscanMonthlyReportContributor{},
		moneriumMonthlyReportContributor{},
		odooMonthlyReportContributor{},
		nostrMonthlyReportContributor{},
	}
}

func readMonthlyReportSummary(dataDir, year, month string) MonthlyReportSummary {
	var summary MonthlyReportSummary
	if data, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "contributors.json")); err == nil {
		var f MonthlyContributorsFile
		if json.Unmarshal(data, &f) == nil {
			summary.Contributors = f.Summary.TotalContributors
			if summary.Contributors == 0 {
				summary.Contributors = len(f.Contributors)
			}
		}
	}
	if data, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "images.json")); err == nil {
		var f ImagesFile
		if json.Unmarshal(data, &f) == nil {
			summary.Images = f.Count
			if summary.Images == 0 {
				summary.Images = len(f.Images)
			}
		}
	}
	if data, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "transactions.json")); err == nil {
		var f TransactionsFile
		if json.Unmarshal(data, &f) == nil {
			summary.Transactions = len(f.Transactions)
		}
	}
	if data, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "events.json")); err == nil {
		var f FullEventsFile
		if json.Unmarshal(data, &f) == nil {
			summary.Events = len(f.Events)
		}
	}
	summary.Bookings, _ = countICSBookingsByCalendar(dataDir, year, month)
	return summary
}

func buildMonthlyReportCurrencies(dataDir, year, month string) []MonthlyReportCurrency {
	data, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "transactions.json"))
	if err != nil {
		return nil
	}
	var f TransactionsFile
	if json.Unmarshal(data, &f) != nil {
		return nil
	}

	currencies := map[string]*MonthlyReportCurrency{}
	for _, tx := range f.Transactions {
		currency := strings.ToUpper(strings.TrimSpace(tx.Currency))
		if currency == "" {
			currency = "UNKNOWN"
		}
		cur := currencies[currency]
		if cur == nil {
			cur = &MonthlyReportCurrency{Currency: currency}
			currencies[currency] = cur
		}
		cur.Transactions++
		amount := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
		switch strings.ToUpper(tx.Type) {
		case "CREDIT":
			cur.In = roundReportAmount(cur.In + amount)
		case "DEBIT":
			cur.Out = roundReportAmount(cur.Out + amount)
		}
		cur.Fees = roundReportAmount(cur.Fees + math.Abs(tx.Fee))
	}

	out := make([]MonthlyReportCurrency, 0, len(currencies))
	for _, cur := range currencies {
		cur.Net = roundReportAmount(cur.In - cur.Out - cur.Fees)
		out = append(out, *cur)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Currency < out[j].Currency
	})
	return out
}

func buildMonthlyReportTaggedFlows(dataDir, year, month string) (collectives, categories []MonthlyReportTaggedFlow) {
	data, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "transactions.json"))
	if err != nil {
		return nil, nil
	}
	var f TransactionsFile
	if json.Unmarshal(data, &f) != nil {
		return nil, nil
	}
	thisYM := year + "-" + month

	type key struct{ tag, currency string }
	colAgg := map[key]*MonthlyReportTaggedFlow{}
	catAgg := map[key]*MonthlyReportTaggedFlow{}

	bump := func(m map[key]*MonthlyReportTaggedFlow, tag, currency, txType string, amount float64) {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			tag = "(untagged)"
		}
		ccy := strings.ToUpper(strings.TrimSpace(currency))
		if ccy == "" {
			ccy = "UNKNOWN"
		}
		if isEURCurrency(currency) {
			ccy = "EUR"
		}
		k := key{tag, ccy}
		row := m[k]
		if row == nil {
			row = &MonthlyReportTaggedFlow{Tag: tag, Currency: ccy}
			m[k] = row
		}
		row.Transactions++
		switch strings.ToUpper(txType) {
		case "CREDIT":
			row.In = roundReportAmount(row.In + amount)
		case "DEBIT":
			row.Out = roundReportAmount(row.Out + amount)
		}
	}

	bumpSigned := func(m map[key]*MonthlyReportTaggedFlow, tag, currency string, signed float64) {
		if signed == 0 {
			return
		}
		if signed > 0 {
			bump(m, tag, currency, "CREDIT", signed)
		} else {
			bump(m, tag, currency, "DEBIT", -signed)
		}
	}

	// Natural-month transactions: a tx with a spread contributes only its
	// allocation for thisYM (which may be 0 — meaning the cash arrived here
	// but the cost is recognized elsewhere). A tx without a spread contributes
	// its full amount as before.
	for _, tx := range f.Transactions {
		if len(tx.Spread) > 0 {
			alloc, ok := spreadAllocationForMonth(tx.Spread, thisYM)
			if !ok || alloc == 0 {
				continue
			}
			bumpSigned(colAgg, tx.Collective, tx.Currency, alloc)
			bumpSigned(catAgg, tx.Category, tx.Currency, alloc)
			continue
		}
		amount := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
		bump(colAgg, tx.Collective, tx.Currency, tx.Type, amount)
		bump(catAgg, tx.Category, tx.Currency, tx.Type, amount)
	}

	// Inbound spreads — projections from txs whose natural month is elsewhere
	// but which allocate a portion to thisYM.
	for _, in := range LoadInboundSpreads(dataDir, year, month) {
		v, err := strconv.ParseFloat(in.Amount, 64)
		if err != nil {
			continue
		}
		bumpSigned(colAgg, in.Collective, in.Currency, v)
		bumpSigned(catAgg, in.Category, in.Currency, v)
	}

	flatten := func(m map[key]*MonthlyReportTaggedFlow) []MonthlyReportTaggedFlow {
		out := make([]MonthlyReportTaggedFlow, 0, len(m))
		for _, row := range m {
			row.Net = roundReportAmount(row.In - row.Out)
			if row.In == 0 && row.Out == 0 {
				continue
			}
			out = append(out, *row)
		}
		sort.Slice(out, func(i, j int) bool {
			iUntagged := out[i].Tag == "(untagged)"
			jUntagged := out[j].Tag == "(untagged)"
			if iUntagged != jUntagged {
				return jUntagged
			}
			ai := math.Abs(out[i].Net)
			aj := math.Abs(out[j].Net)
			if ai != aj {
				return ai > aj
			}
			if out[i].Tag != out[j].Tag {
				return out[i].Tag < out[j].Tag
			}
			return out[i].Currency < out[j].Currency
		})
		return out
	}

	return flatten(colAgg), flatten(catAgg)
}

func buildMonthlyReportAccounts(dataDir, year, month string) []MonthlyReportAccount {
	data, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "transactions.json"))
	if err != nil {
		return nil
	}
	var f TransactionsFile
	if json.Unmarshal(data, &f) != nil {
		return nil
	}

	accounts := map[string]*MonthlyReportAccount{}
	for _, tx := range f.Transactions {
		if tx.Provider == etherscansource.Source && strings.TrimSpace(tx.Account) == "" {
			continue
		}
		chain := ""
		if tx.Chain != nil {
			chain = *tx.Chain
		}
		source := tx.Provider
		if source == "" {
			source = "unknown"
		}
		currency := strings.ToUpper(strings.TrimSpace(tx.Currency))
		if currency == "" {
			currency = "UNKNOWN"
		}
		key := strings.Join([]string{source, chain, tx.AccountSlug, tx.Account, currency}, "\x00")
		acct := accounts[key]
		if acct == nil {
			acct = &MonthlyReportAccount{
				Source:      source,
				Account:     tx.Account,
				AccountSlug: tx.AccountSlug,
				AccountName: tx.AccountName,
				Chain:       chain,
				Currency:    currency,
				Balance:     MonthlyReportBalance{Computed: false, Verified: false},
			}
			if chain != "" || source == etherscansource.Source {
				acct.Token = &MonthlyReportToken{Symbol: currency, Chain: chain}
			}
			accounts[key] = acct
		}

		amount := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
		switch strings.ToUpper(tx.Type) {
		case "CREDIT":
			acct.Counts.Credits++
			acct.Amounts.In = roundReportAmount(acct.Amounts.In + amount)
			if source == etherscansource.Source && tx.Account == "" {
				acct.Counts.Mints++
			}
		case "DEBIT":
			acct.Counts.Debits++
			acct.Amounts.Out = roundReportAmount(acct.Amounts.Out + amount)
			if source == etherscansource.Source && tx.Account == "" {
				acct.Counts.Burns++
			}
		case "INTERNAL":
			acct.Counts.Internal++
		case "TRANSFER":
			acct.Counts.Transfers++
		default:
			acct.Counts.Transfers++
		}
		acct.Amounts.Fees = roundReportAmount(acct.Amounts.Fees + math.Abs(tx.Fee))
	}

	out := make([]MonthlyReportAccount, 0, len(accounts))
	for _, acct := range accounts {
		acct.Amounts.Net = roundReportAmount(acct.Amounts.In - acct.Amounts.Out - acct.Amounts.Fees)
		acct.Balance.Delta = acct.Amounts.Net
		out = append(out, *acct)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}
		if out[i].Chain != out[j].Chain {
			return out[i].Chain < out[j].Chain
		}
		if out[i].AccountSlug != out[j].AccountSlug {
			return out[i].AccountSlug < out[j].AccountSlug
		}
		return out[i].Currency < out[j].Currency
	})
	return out
}

func buildMonthlyReportTokens(dataDir, year, month string, settings *Settings) []MonthlyReportTokenData {
	if settings == nil || len(settings.Tokens) == 0 {
		return nil
	}
	out := make([]MonthlyReportTokenData, 0, len(settings.Tokens))
	for _, token := range dedupeTokenConfigs(settings.Tokens) {
		if !token.Mintable && !token.Burnable {
			continue
		}
		summary, ok := buildMonthlyReportToken(dataDir, year, month, token)
		if ok {
			out = append(out, summary)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Chain != out[j].Chain {
			return out[i].Chain < out[j].Chain
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out
}

func buildMonthlyReportToken(dataDir, year, month string, token TokenConfig) (MonthlyReportTokenData, bool) {
	summary := MonthlyReportTokenData{
		Slug:   token.Slug,
		Name:   token.Name,
		Symbol: token.Symbol,
		Chain:  token.Chain,
	}
	if summary.Symbol == "" {
		return summary, false
	}
	zeroAddr := "0x0000000000000000000000000000000000000000"
	targetYM := year + "-" + month
	monthYM := targetYM
	active := map[string]bool{}
	balances := map[string]float64{}
	hadHistory := false

	for _, ym := range tokenReportMonths(dataDir, targetYM) {
		parts := strings.Split(ym, "-")
		if len(parts) != 2 {
			continue
		}
		path := etherscansource.Path(dataDir, parts[0], parts[1], token.Chain, etherscansource.FileName(token.Slug, token.Symbol))
		cache, ok := etherscansource.LoadCache(path)
		if !ok {
			continue
		}
		if cache.Account != "" {
			continue
		}
		hadHistory = true
		isMonth := ym == monthYM
		for _, tx := range cache.Transactions {
			dec := token.Decimals
			if tx.TokenDecimal != "" {
				fmt.Sscanf(tx.TokenDecimal, "%d", &dec)
			}
			amount := etherscansource.ParseTokenValue(tx.Value, dec)
			from := strings.ToLower(tx.From)
			to := strings.ToLower(tx.To)
			isMint := strings.EqualFold(from, zeroAddr)
			isBurn := strings.EqualFold(to, zeroAddr)

			if !isMint {
				balances[from] = roundReportAmount(balances[from] - amount)
			}
			if !isBurn {
				balances[to] = roundReportAmount(balances[to] + amount)
			}
			if isMint {
				summary.TotalSupply = roundReportAmount(summary.TotalSupply + amount)
			}
			if isBurn {
				summary.TotalSupply = roundReportAmount(summary.TotalSupply - amount)
			}

			if !isMonth {
				continue
			}
			summary.Transactions++
			if isMint {
				summary.Mints++
				summary.Minted = roundReportAmount(summary.Minted + amount)
			} else if isBurn {
				summary.Burns++
				summary.Burnt = roundReportAmount(summary.Burnt + amount)
			} else {
				summary.Transfers++
			}
			if from != "" && !strings.EqualFold(from, zeroAddr) {
				active[from] = true
			}
			if to != "" && !strings.EqualFold(to, zeroAddr) {
				active[to] = true
			}
		}
	}

	for addr, balance := range balances {
		if addr == "" || strings.EqualFold(addr, zeroAddr) {
			continue
		}
		if balance > 0 {
			summary.TokenHolders++
		}
	}
	summary.ActiveTokenHolders = len(active)
	summary.ComputedFromHistory = hadHistory
	return summary, hadHistory || summary.Transactions > 0
}

func tokenReportMonths(dataDir, targetYM string) []string {
	years, err := os.ReadDir(dataDir)
	if err != nil {
		return nil
	}
	var months []string
	for _, yearEntry := range years {
		if !yearEntry.IsDir() || len(yearEntry.Name()) != 4 {
			continue
		}
		monthEntries, _ := os.ReadDir(filepath.Join(dataDir, yearEntry.Name()))
		for _, monthEntry := range monthEntries {
			if !monthEntry.IsDir() || len(monthEntry.Name()) != 2 {
				continue
			}
			ym := yearEntry.Name() + "-" + monthEntry.Name()
			if ym <= targetYM {
				months = append(months, ym)
			}
		}
	}
	sort.Strings(months)
	return months
}

func firstNonZeroFloat(vals ...float64) float64 {
	for _, v := range vals {
		if v != 0 {
			return v
		}
	}
	return 0
}

func roundReportAmount(v float64) float64 {
	return math.Round(v*1e8) / 1e8
}

type discordMonthlyReportContributor struct{}

func (discordMonthlyReportContributor) Source() string { return discordsource.Source }

func (discordMonthlyReportContributor) MonthlyReport(ctx MonthlyReportContext, scope MonthlyReportScope) (MonthlyReportSource, error) {
	root := discordsource.Path(ctx.DataDir, scope.Year, scope.Month)
	entries, err := os.ReadDir(root)
	if err != nil {
		return MonthlyReportSource{}, nil
	}
	out := MonthlyReportSource{Source: discordsource.Source, Summary: map[string]interface{}{}}
	channels := 0
	images := 0
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "images" {
			continue
		}
		channels++
		data, err := os.ReadFile(discordsource.ChannelPath(ctx.DataDir, scope.Year, scope.Month, entry.Name()))
		if err != nil {
			continue
		}
		var cache discordsource.CacheFile
		if json.Unmarshal(data, &cache) != nil {
			continue
		}
		out.Records += len(cache.Messages)
		for _, msg := range cache.Messages {
			out.Attachments += len(msg.Attachments)
			for _, att := range msg.Attachments {
				if isDiscordImageAttachment(att.ContentType, att.URL) {
					images++
				}
			}
		}
	}
	out.Summary["channels"] = channels
	out.Summary["images"] = images
	return out, nil
}

type icsMonthlyReportContributor struct{}

func (icsMonthlyReportContributor) Source() string { return icssource.Source }

func (icsMonthlyReportContributor) MonthlyReport(ctx MonthlyReportContext, scope MonthlyReportScope) (MonthlyReportSource, error) {
	bookings, byCalendar := countICSBookingsByCalendar(ctx.DataDir, scope.Year, scope.Month)
	events, eventsByCalendar := countGeneratedEventsByCalendar(ctx.DataDir, scope.Year, scope.Month)
	if len(byCalendar) == 0 && len(eventsByCalendar) == 0 {
		return MonthlyReportSource{}, nil
	}

	calendarSet := map[string]bool{}
	for slug := range byCalendar {
		calendarSet[slug] = true
	}
	for slug := range eventsByCalendar {
		calendarSet[slug] = true
	}
	breakdown := map[string]map[string]int{}
	for slug := range calendarSet {
		breakdown[slug] = map[string]int{
			"bookings": byCalendar[slug],
			"events":   eventsByCalendar[slug],
		}
	}

	return MonthlyReportSource{
		Source:  icssource.Source,
		Records: bookings,
		Summary: map[string]interface{}{
			"calendars":  len(calendarSet),
			"bookings":   bookings,
			"events":     events,
			"byCalendar": breakdown,
		},
	}, nil
}

func countICSBookingsByCalendar(dataDir, year, month string) (int, map[string]int) {
	root := icssource.Path(dataDir, year, month)
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, map[string]int{}
	}
	total := 0
	byCalendar := map[string]int{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".ics") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, entry.Name()))
		if err != nil {
			continue
		}
		events, err := ical.ParseICS(string(data))
		if err != nil {
			continue
		}
		slug := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		byCalendar[slug] += len(events)
		total += len(events)
	}
	return total, byCalendar
}

func countGeneratedEventsByCalendar(dataDir, year, month string) (int, map[string]int) {
	data, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "events.json"))
	if err != nil {
		return 0, map[string]int{}
	}
	var f FullEventsFile
	if json.Unmarshal(data, &f) != nil {
		return 0, map[string]int{}
	}
	byCalendar := map[string]int{}
	for _, event := range f.Events {
		slug := event.CalendarSource
		if slug == "" {
			slug = event.Source
		}
		if slug == "" {
			slug = "unknown"
		}
		byCalendar[slug]++
	}
	return len(f.Events), byCalendar
}

type stripeMonthlyReportContributor struct{}

func (stripeMonthlyReportContributor) Source() string { return stripesource.Source }

func (stripeMonthlyReportContributor) MonthlyReport(ctx MonthlyReportContext, scope MonthlyReportScope) (MonthlyReportSource, error) {
	out := MonthlyReportSource{Source: stripesource.Source, Summary: map[string]interface{}{}}
	if cache, ok := stripesource.LoadCache(stripesource.TransactionCachePath(ctx.DataDir, scope.Year, scope.Month)); ok {
		out.Records += len(cache.Transactions)
		out.Summary["balanceTransactions"] = len(cache.Transactions)
	}
	if data, err := os.ReadFile(stripesource.Path(ctx.DataDir, scope.Year, scope.Month, stripesource.ChargesFile)); err == nil {
		var charges stripesource.ChargeData
		if json.Unmarshal(data, &charges) == nil {
			out.Records += len(charges.Charges)
			out.Summary["charges"] = len(charges.Charges)
		}
	}
	if data, err := os.ReadFile(stripesource.Path(ctx.DataDir, scope.Year, scope.Month, stripesource.SubscriptionsFile)); err == nil {
		var snap providerSnapshot
		if json.Unmarshal(data, &snap) == nil {
			out.Records += len(snap.Subscriptions)
			out.Summary["subscriptions"] = len(snap.Subscriptions)
		}
	}
	return out, nil
}

type etherscanMonthlyReportContributor struct{}

func (etherscanMonthlyReportContributor) Source() string { return etherscansource.Source }

func (etherscanMonthlyReportContributor) MonthlyReport(ctx MonthlyReportContext, scope MonthlyReportScope) (MonthlyReportSource, error) {
	root := filepath.Join(ctx.DataDir, scope.Year, scope.Month, etherscansource.RelPath(""))
	out := MonthlyReportSource{Source: etherscansource.Source, Summary: map[string]interface{}{}}
	byChain := map[string]int{}
	entries, err := os.ReadDir(root)
	if err != nil {
		return out, nil
	}
	for _, chainEntry := range entries {
		if !chainEntry.IsDir() {
			continue
		}
		chain := chainEntry.Name()
		files, _ := os.ReadDir(filepath.Join(root, chain))
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {
				continue
			}
			cache, ok := etherscansource.LoadCache(filepath.Join(root, chain, file.Name()))
			if !ok {
				continue
			}
			out.Records += len(cache.Transactions)
			byChain[chain] += len(cache.Transactions)
		}
	}
	if len(byChain) > 0 {
		out.Summary["byChain"] = byChain
	}
	return out, nil
}

type moneriumMonthlyReportContributor struct{}

func (moneriumMonthlyReportContributor) Source() string { return moneriumsource.Source }

func (moneriumMonthlyReportContributor) MonthlyReport(ctx MonthlyReportContext, scope MonthlyReportScope) (MonthlyReportSource, error) {
	root := moneriumsource.Path(ctx.DataDir, scope.Year, scope.Month)
	out := MonthlyReportSource{Source: moneriumsource.Source, Summary: map[string]interface{}{}}
	files, err := os.ReadDir(root)
	if err != nil {
		return out, nil
	}
	ordersByState := map[string]int{}
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		cache, ok := moneriumsource.LoadCache(filepath.Join(root, file.Name()))
		if !ok {
			continue
		}
		out.Records += len(cache.Orders)
		for _, order := range cache.Orders {
			ordersByState[order.State]++
		}
	}
	if len(ordersByState) > 0 {
		out.Summary["ordersByState"] = ordersByState
	}
	return out, nil
}

type odooMonthlyReportContributor struct{}

func (odooMonthlyReportContributor) Source() string { return odoosource.Source }

func (odooMonthlyReportContributor) MonthlyReport(ctx MonthlyReportContext, scope MonthlyReportScope) (MonthlyReportSource, error) {
	out := MonthlyReportSource{Source: odoosource.Source, Summary: map[string]interface{}{}}
	if data, err := os.ReadFile(odoosource.Path(ctx.DataDir, scope.Year, scope.Month, odoosource.InvoicesFile)); err == nil {
		var f OdooOutgoingInvoicesFile
		if json.Unmarshal(data, &f) == nil {
			out.Records += len(f.Invoices)
			out.Summary["invoices"] = len(f.Invoices)
		}
	}
	if data, err := os.ReadFile(odoosource.Path(ctx.DataDir, scope.Year, scope.Month, odoosource.BillsFile)); err == nil {
		var f OdooVendorBillsFile
		if json.Unmarshal(data, &f) == nil {
			out.Records += len(f.Bills)
			out.Summary["bills"] = len(f.Bills)
		}
	}
	if data, err := os.ReadFile(odoosource.Path(ctx.DataDir, scope.Year, scope.Month, odoosource.SubscriptionsFile)); err == nil {
		var snap providerSnapshot
		if json.Unmarshal(data, &snap) == nil {
			out.Records += len(snap.Subscriptions)
			out.Summary["subscriptions"] = len(snap.Subscriptions)
		}
	}
	if data, err := os.ReadFile(odoosource.Path(ctx.DataDir, scope.Year, scope.Month, odoosource.AnalyticEnrichmentFile)); err == nil {
		var enrichment OdooAnalyticEnrichment
		if json.Unmarshal(data, &enrichment) == nil {
			out.Records += len(enrichment.Mappings)
			out.Summary["analyticMappings"] = len(enrichment.Mappings)
		}
	}
	out.Attachments = countOdooSourceAttachments(ctx.DataDir, scope.Year, scope.Month)
	if out.Attachments > 0 {
		out.Summary["attachments"] = out.Attachments
	}
	return out, nil
}

func countOdooSourceAttachments(dataDir, year, month string) int {
	seen := map[string]bool{}
	addDocAttachments := func(path, kind string) {
		data, err := os.ReadFile(path)
		if err != nil {
			return
		}
		if kind == "invoices" {
			var f OdooOutgoingInvoicesPrivateFile
			if json.Unmarshal(data, &f) != nil {
				return
			}
			for _, doc := range f.Invoices {
				for _, att := range doc.Attachments {
					seen[fmt.Sprintf("%s:%d:%d:%s", kind, doc.ID, att.ID, att.Name)] = true
				}
			}
			return
		}
		var f OdooVendorBillsPrivateFile
		if json.Unmarshal(data, &f) != nil {
			return
		}
		for _, doc := range f.Bills {
			for _, att := range doc.Attachments {
				seen[fmt.Sprintf("%s:%d:%d:%s", kind, doc.ID, att.ID, att.Name)] = true
			}
		}
	}
	addDocAttachments(odoosource.PrivatePath(dataDir, year, month, odoosource.InvoicesFile), "invoices")
	addDocAttachments(odoosource.PrivatePath(dataDir, year, month, odoosource.BillsFile), "bills")
	return len(seen)
}

type nostrMonthlyReportContributor struct{}

func (nostrMonthlyReportContributor) Source() string { return nostrsource.Source }

func (nostrMonthlyReportContributor) MonthlyReport(ctx MonthlyReportContext, scope MonthlyReportScope) (MonthlyReportSource, error) {
	root := nostrsource.Path(ctx.DataDir, scope.Year, scope.Month)
	out := MonthlyReportSource{Source: nostrsource.Source, Summary: map[string]interface{}{}}
	entries, err := os.ReadDir(root)
	if err != nil {
		return out, nil
	}
	metadataRecords := 0
	annotationRecords := 0
	stripeAnnotationRecords := 0
	for _, entry := range entries {
		path := filepath.Join(root, entry.Name())
		if entry.IsDir() {
			data, err := os.ReadFile(filepath.Join(path, nostrsource.MetadataFile))
			if err != nil {
				continue
			}
			var cache NostrMetadataCache
			if json.Unmarshal(data, &cache) == nil {
				metadataRecords += len(cache.Transactions) + len(cache.Addresses)
			}
			continue
		}
		switch entry.Name() {
		case nostrsource.AnnotationsFile:
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var cache NostrAnnotationCache
			if json.Unmarshal(data, &cache) == nil {
				annotationRecords = len(cache.Annotations)
			}
		case nostrsource.StripeAnnotationsFile:
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var cache NostrAnnotationCache
			if json.Unmarshal(data, &cache) == nil {
				stripeAnnotationRecords = len(cache.Annotations)
			}
		}
	}
	if metadataRecords > 0 {
		out.Records += metadataRecords
		out.Summary["metadata"] = metadataRecords
	}
	if annotationRecords > 0 {
		out.Records += annotationRecords
		out.Summary["annotations"] = annotationRecords
	} else if stripeAnnotationRecords > 0 {
		out.Records += stripeAnnotationRecords
		out.Summary["stripeAnnotations"] = stripeAnnotationRecords
	}
	return out, nil
}
