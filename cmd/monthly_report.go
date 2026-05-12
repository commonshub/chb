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
	Collectives []TaggedSummary             `json:"collectives,omitempty"`
	Categories  []TaggedSummary             `json:"categories,omitempty"`
	Sources     []MonthlyReportSource       `json:"sources"`
	Notes       []string                    `json:"notes,omitempty"`
}

// TaggedSummary groups currency flows under a single tag (collective or
// category slug). One slug can hold multiple currencies (e.g. a collective
// receiving both EUR and CHT).
type TaggedSummary struct {
	Slug       string         `json:"slug"`
	Currencies []CurrencyFlow `json:"currencies,omitempty"`
}

// CurrencyFlow summarizes credits/debits for one currency within a tag.
// EUR-family currencies are merged into "EUR"; other currencies (CHT, etc.)
// are kept distinct. FirstTx/LastTx mark the activity span (within the month
// for per-month summaries, across all history for the global rollup).
// StartBalance is the cumulative balance entering the period; EndBalance
// leaving it. Balances are filled only for collective rows by the
// cross-month rollup pass.
type CurrencyFlow struct {
	Currency     string  `json:"currency"`
	Transactions int     `json:"transactions"`
	In           float64 `json:"in"`
	Out          float64 `json:"out"`
	Fees         float64 `json:"fees,omitempty"`
	VAT          float64 `json:"vat,omitempty"`
	Net          float64 `json:"net"`
	FirstTx      string  `json:"firstTx,omitempty"`
	LastTx       string  `json:"lastTx,omitempty"`
	StartBalance float64 `json:"startBalance,omitempty"`
	EndBalance   float64 `json:"endBalance,omitempty"`
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
	return writeMonthFile(dataDir, year, month, filepath.Join("generated", "summary.json"), data) == nil
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

func buildMonthlyReportTaggedFlows(dataDir, year, month string) (collectives, categories []TaggedSummary) {
	data, err := os.ReadFile(filepath.Join(dataDir, year, month, "generated", "transactions.json"))
	if err != nil {
		return nil, nil
	}
	var f TransactionsFile
	if json.Unmarshal(data, &f) != nil {
		return nil, nil
	}
	thisYM := year + "-" + month

	colAgg := map[tagKey]*CurrencyFlow{}
	colTag := map[tagKey]string{}
	catAgg := map[tagKey]*CurrencyFlow{}
	catTag := map[tagKey]string{}

	// flowDelta describes one transaction's contribution to a (tag, currency)
	// flow. Direction is encoded by the sign of `gross` (>0 = incoming,
	// <0 = outgoing). fees and vat are always non-negative and always
	// counted into `out` so net = in - out captures the available balance.
	type flowDelta struct {
		gross float64 // signed: + for incoming, - for outgoing
		fees  float64 // always non-negative
		vat   float64 // always non-negative; incoming-only
		ts    int64
	}

	bumpFlow := func(agg map[tagKey]*CurrencyFlow, tagMap map[tagKey]string, tag, currency string, d flowDelta) {
		if d.gross == 0 && d.fees == 0 && d.vat == 0 {
			return
		}
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
		k := tagKey{tag, ccy}
		row := agg[k]
		if row == nil {
			row = &CurrencyFlow{Currency: ccy}
			agg[k] = row
			tagMap[k] = tag
		}
		row.Transactions++
		if d.gross > 0 {
			row.In = roundReportAmount(row.In + d.gross)
		} else if d.gross < 0 {
			row.Out = roundReportAmount(row.Out + -d.gross)
		}
		if d.fees > 0 {
			row.Fees = roundReportAmount(row.Fees + d.fees)
			row.Out = roundReportAmount(row.Out + d.fees)
		}
		if d.vat > 0 {
			row.VAT = roundReportAmount(row.VAT + d.vat)
			row.Out = roundReportAmount(row.Out + d.vat)
		}
		if d.ts > 0 {
			day := time.Unix(d.ts, 0).UTC().Format("2006-01-02")
			if row.FirstTx == "" || day < row.FirstTx {
				row.FirstTx = day
			}
			if day > row.LastTx {
				row.LastTx = day
			}
		}
	}

	// txDelta computes a tx's contribution. amount is the unsigned share for
	// the month (full gross for non-spread txs, the spread allocation for
	// spread txs). For spread txs the fee/vat are pro-rated proportionally.
	txDelta := func(tx TransactionEntry, amount float64) flowDelta {
		d := flowDelta{ts: tx.Timestamp}
		if amount == 0 {
			return d
		}
		fullGross := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
		ratio := 1.0
		if fullGross > 0 && fullGross != amount {
			ratio = amount / fullGross
		}
		signed := amount
		if tx.IsOutgoing() {
			signed = -amount
		}
		d.gross = signed
		if tx.Fee != 0 {
			d.fees = roundReportAmount(math.Abs(tx.Fee) * ratio)
		}
		if tx.IsIncoming() {
			if v := floatMetadata(tx.Metadata, "vatAmount"); v > 0 {
				d.vat = roundReportAmount(v * ratio)
			}
		}
		return d
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
			d := txDelta(tx, math.Abs(alloc))
			// Spread amount carries its own sign; honor it.
			if alloc < 0 {
				d.gross = -math.Abs(d.gross)
			} else {
				d.gross = math.Abs(d.gross)
			}
			bumpFlow(colAgg, colTag, tx.Collective, tx.Currency, d)
			bumpFlow(catAgg, catTag, tx.Category, tx.Currency, d)
			continue
		}
		amount := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
		d := txDelta(tx, amount)
		bumpFlow(colAgg, colTag, tx.Collective, tx.Currency, d)
		bumpFlow(catAgg, catTag, tx.Category, tx.Currency, d)
	}

	// Inbound spreads — projections from txs whose natural month is elsewhere
	// but which allocate a portion to thisYM. We don't have a tx timestamp
	// for these, so omit firstTx/lastTx contributions.
	for _, in := range LoadInboundSpreads(dataDir, year, month) {
		v, err := strconv.ParseFloat(in.Amount, 64)
		if err != nil || v == 0 {
			continue
		}
		bumpFlow(colAgg, colTag, in.Collective, in.Currency, flowDelta{gross: v})
		bumpFlow(catAgg, catTag, in.Category, in.Currency, flowDelta{gross: v})
	}

	// Fiscal-host commissions — synthetic 10% transfer from each collective
	// to commonshub, derived from this month's gross income per (collective,
	// currency). Generated upstream into generated/commissions.json.
	for _, c := range LoadCommissions(dataDir, year, month) {
		v, err := strconv.ParseFloat(c.Amount, 64)
		if err != nil || v == 0 {
			continue
		}
		// DEBIT on source collective
		bumpFlow(colAgg, colTag, c.Collective, c.Currency, flowDelta{gross: -v})
		// CREDIT on commonshub
		bumpFlow(colAgg, colTag, commissionHostSlug, c.Currency, flowDelta{gross: v})
		// Categorize the commission for visibility on the host side
		bumpFlow(catAgg, catTag, commissionCategorySlug, c.Currency, flowDelta{gross: v})
	}

	return groupByTag(colAgg, colTag), groupByTag(catAgg, catTag)
}

// tagKey is the (tag, currency) composite key used to aggregate flows.
type tagKey struct {
	tag, currency string
}

// groupByTag flattens a (tag, currency) aggregate into one TaggedSummary per
// tag, with sorted Currencies inside. Empty rows (no in/out) are dropped.
func groupByTag(agg map[tagKey]*CurrencyFlow, tagMap map[tagKey]string) []TaggedSummary {
	bySlug := map[string]*TaggedSummary{}
	for k, row := range agg {
		row.Net = roundReportAmount(row.In - row.Out)
		if row.In == 0 && row.Out == 0 {
			continue
		}
		slug := tagMap[k]
		entry, ok := bySlug[slug]
		if !ok {
			entry = &TaggedSummary{Slug: slug}
			bySlug[slug] = entry
		}
		entry.Currencies = append(entry.Currencies, *row)
	}
	out := make([]TaggedSummary, 0, len(bySlug))
	for _, e := range bySlug {
		sort.Slice(e.Currencies, func(i, j int) bool {
			return e.Currencies[i].Currency < e.Currencies[j].Currency
		})
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		iUntagged := out[i].Slug == "(untagged)"
		jUntagged := out[j].Slug == "(untagged)"
		if iUntagged != jUntagged {
			return jUntagged
		}
		// Order by absolute net of the first (largest) currency row.
		ai, aj := 0.0, 0.0
		if len(out[i].Currencies) > 0 {
			ai = math.Abs(out[i].Currencies[0].Net)
		}
		if len(out[j].Currencies) > 0 {
			aj = math.Abs(out[j].Currencies[0].Net)
		}
		if ai != aj {
			return ai > aj
		}
		return out[i].Slug < out[j].Slug
	})
	return out
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

// ── Cross-month rollup (per-collective balances + global summary) ───────────

// GlobalSummaryFile is the rollup written to latest/generated/summary.json.
// One entry per collective, each with a per-currency breakdown carrying
// lifetime totals, activity span, and final balance.
type GlobalSummaryFile struct {
	GeneratedAt string          `json:"generatedAt"`
	FirstMonth  string          `json:"firstMonth,omitempty"` // YYYY-MM
	LastMonth   string          `json:"lastMonth,omitempty"`  // YYYY-MM
	Collectives []TaggedSummary `json:"collectives,omitempty"`
}

// rebuildSummaryRollup walks every month's summary.json in chronological
// order, fills in StartBalance / EndBalance for each collective row, and
// writes latest/generated/summary.json with the lifetime aggregate. Returns
// the number of collective entries in the global file.
func rebuildSummaryRollup(dataDir string) (int, error) {
	type monthRef struct {
		year, month, ym string
	}
	var months []monthRef
	for _, year := range getAvailableYears(dataDir) {
		entries, err := os.ReadDir(filepath.Join(dataDir, year))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() || len(e.Name()) != 2 {
				continue
			}
			months = append(months, monthRef{year, e.Name(), year + "-" + e.Name()})
		}
	}
	sort.Slice(months, func(i, j int) bool { return months[i].ym < months[j].ym })

	// running cumulative balance keyed by (slug, currency)
	balance := map[tagKey]float64{}

	// global aggregate keyed by (slug, currency)
	global := map[tagKey]*CurrencyFlow{}
	globalSlug := map[tagKey]string{}

	for _, m := range months {
		path := filepath.Join(dataDir, m.year, m.month, "generated", "summary.json")
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var file MonthlyReportFile
		if err := json.Unmarshal(data, &file); err != nil {
			continue
		}
		changed := false
		for i := range file.Collectives {
			entry := &file.Collectives[i]
			for j := range entry.Currencies {
				row := &entry.Currencies[j]
				k := tagKey{entry.Slug, row.Currency}
				start := balance[k]
				end := roundReportAmount(start + row.Net)
				if row.StartBalance != start || row.EndBalance != end {
					row.StartBalance = start
					row.EndBalance = end
					changed = true
				}
				balance[k] = end

				// fold into global aggregate
				g := global[k]
				if g == nil {
					g = &CurrencyFlow{Currency: row.Currency}
					global[k] = g
					globalSlug[k] = entry.Slug
				}
				g.Transactions += row.Transactions
				g.In = roundReportAmount(g.In + row.In)
				g.Out = roundReportAmount(g.Out + row.Out)
				g.Fees = roundReportAmount(g.Fees + row.Fees)
				g.VAT = roundReportAmount(g.VAT + row.VAT)
				g.Net = roundReportAmount(g.In - g.Out)
				if row.FirstTx != "" && (g.FirstTx == "" || row.FirstTx < g.FirstTx) {
					g.FirstTx = row.FirstTx
				}
				if row.LastTx != "" && row.LastTx > g.LastTx {
					g.LastTx = row.LastTx
				}
				g.EndBalance = end // running global balance equals running per-month balance
			}
		}
		if changed {
			out, err := marshalIndentedNoHTMLEscape(file)
			if err == nil {
				_ = writeMonthFile(dataDir, m.year, m.month, filepath.Join("generated", "summary.json"), out)
			}
		}
	}

	// regroup global rows by slug, with sorted currencies under each
	bySlug := map[string]*TaggedSummary{}
	for k, row := range global {
		slug := globalSlug[k]
		entry, ok := bySlug[slug]
		if !ok {
			entry = &TaggedSummary{Slug: slug}
			bySlug[slug] = entry
		}
		entry.Currencies = append(entry.Currencies, *row)
	}
	rows := make([]TaggedSummary, 0, len(bySlug))
	for _, e := range bySlug {
		sort.Slice(e.Currencies, func(i, j int) bool {
			ai := math.Abs(e.Currencies[i].Net)
			aj := math.Abs(e.Currencies[j].Net)
			if ai != aj {
				return ai > aj
			}
			return e.Currencies[i].Currency < e.Currencies[j].Currency
		})
		rows = append(rows, *e)
	}
	sort.Slice(rows, func(i, j int) bool {
		iUntagged := rows[i].Slug == "(untagged)"
		jUntagged := rows[j].Slug == "(untagged)"
		if iUntagged != jUntagged {
			return jUntagged
		}
		var ai, aj float64
		if len(rows[i].Currencies) > 0 {
			ai = math.Abs(rows[i].Currencies[0].Net)
		}
		if len(rows[j].Currencies) > 0 {
			aj = math.Abs(rows[j].Currencies[0].Net)
		}
		if ai != aj {
			return ai > aj
		}
		return rows[i].Slug < rows[j].Slug
	})

	gs := GlobalSummaryFile{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Collectives: rows,
	}
	if len(months) > 0 {
		gs.FirstMonth = months[0].ym
		gs.LastMonth = months[len(months)-1].ym
	}
	data, err := marshalIndentedNoHTMLEscape(gs)
	if err != nil {
		return 0, err
	}
	dest := filepath.Join(dataDir, "latest", "generated", "summary.json")
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return 0, err
	}
	if err := os.WriteFile(dest, data, 0644); err != nil {
		return 0, err
	}
	return len(rows), nil
}
