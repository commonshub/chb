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

	stickertable "github.com/76creates/stickers/table"
	nostrsource "github.com/CommonsHub/chb/providers/nostr"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/nbd-wtf/go-nostr"
	overlay "github.com/rmhubbert/bubbletea-overlay"
)

// ── Styles ──

var (
	styleGreen = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleRed   = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
)

// ── Helpers ──

// txAmount returns the signed gross amount — the customer-facing number,
// before any processing fees the provider deducted (Stripe especially).
// For providers that don't separate gross from net (etherscan, kbc),
// GrossAmount, Amount, and NormalizedAmount all carry the same value so
// the order of fallbacks doesn't matter.
//
// IMPORTANT: prefer GrossAmount first. Earlier versions started with
// NormalizedAmount which is the balance-impact (post-fee) value for
// Stripe — that's the wrong number to display in the table, since "show
// me my €100 customer payment" should show €100 not €97.10.
func txAmount(tx TransactionEntry) float64 {
	// Tx Type ("CREDIT"/"MINT" vs "DEBIT"/"BURN") is the authoritative
	// direction signal across providers: Stripe has signed amounts but
	// blockchain providers (Etherscan / Monerium) store the token magnitude
	// as a positive number and rely on Type to encode direction. So we
	// take the magnitude from whichever amount field is populated and
	// re-apply the sign from IsOutgoing().
	mag := math.Abs(tx.GrossAmount)
	if mag == 0 {
		mag = math.Abs(tx.Amount)
	}
	if mag == 0 {
		mag = math.Abs(tx.NormalizedAmount)
	}
	if tx.IsOutgoing() {
		return -mag
	}
	if tx.IsIncoming() {
		return mag
	}
	// Neither flagged (TRANSFER / INTERNAL / etc.): keep the original
	// signed value so callers see whichever sign the source supplied.
	if tx.Amount != 0 {
		return tx.Amount
	}
	if tx.NormalizedAmount != 0 {
		return tx.NormalizedAmount
	}
	return tx.GrossAmount
}

// txFee returns the absolute processing fee for this transaction, in the
// transaction's currency. Zero for providers that don't book fees on the
// tx line (etherscan, kbc).
func txFee(tx TransactionEntry) float64 {
	return math.Abs(tx.Fee)
}

func txAmountCell(tx TransactionEntry, styled bool) string {
	amt := txAmount(tx)
	absAmt := math.Abs(amt)
	positive := tx.IsIncoming()
	if !isEURCurrency(tx.Currency) && tx.Type == "DEBIT" {
		// Token-wide transfer between two non-tracked addresses.
		// Display as a positive token movement rather than an outflow.
		positive = true
	}

	var out string
	if isEURCurrency(tx.Currency) {
		if positive {
			out = fmt.Sprintf("+€%.2f", absAmt)
		} else {
			out = fmt.Sprintf("-€%.2f", absAmt)
		}
	} else {
		if positive {
			out = fmt.Sprintf("+%.2f %s", absAmt, tx.Currency)
		} else {
			out = fmt.Sprintf("-%.2f %s", absAmt, tx.Currency)
		}
	}
	if !styled {
		return out
	}
	if positive {
		return styleGreen.Render(out)
	}
	return styleRed.Render(out)
}

func parseTransactionAmountCell(s string) float64 {
	sign := 1.0
	sawSign := false
	var b strings.Builder
	started := false
	inANSI := false
	inANSICSI := false
	for _, r := range s {
		if inANSI {
			if !inANSICSI && r == '[' {
				inANSICSI = true
				continue
			}
			if !inANSICSI || r >= '@' && r <= '~' {
				inANSI = false
				inANSICSI = false
			}
			continue
		}
		if r == '\x1b' {
			inANSI = true
			continue
		}
		if !started && !sawSign && (r == '+' || r == '-') {
			sawSign = true
			if r == '-' {
				sign = -1
			}
			continue
		}
		if (r >= '0' && r <= '9') || r == '.' || r == ',' {
			started = true
			if r != ',' {
				b.WriteRune(r)
			}
			continue
		}
		if started {
			break
		}
	}
	v, err := strconv.ParseFloat(b.String(), 64)
	if err != nil {
		return 0
	}
	return sign * v
}

func txDisplayCounterparty(tx TransactionEntry) string {
	cp := tx.Counterparty
	if tx.Provider == "stripe" {
		if desc, ok := tx.Metadata["description"]; ok {
			if s, ok := desc.(string); ok && cp != s && cp != "" {
				return cp
			}
		}
		return "Stripe"
	}
	return shortAddr(cp)
}

func txDisplayDescription(tx TransactionEntry) string {
	if desc, ok := tx.Metadata["description"]; ok {
		if s, ok := desc.(string); ok && s != "" {
			return s
		}
	}
	if tx.Provider == "stripe" {
		return tx.Counterparty
	}
	return ""
}

func txDisplayCategory(tx TransactionEntry) string {
	if tx.Category != "" {
		return tx.Category
	}
	if category := firstTransactionTagValue(tx, "category"); category != "" {
		return category
	}
	if tx.Metadata != nil {
		if category, ok := tx.Metadata["category"].(string); ok && category != "" {
			return normalizeTransactionTagSlug(category)
		}
	}
	return ""
}

func txDisplayCollective(tx TransactionEntry) string {
	if tx.Collective != "" {
		return tx.Collective
	}
	if collective := firstTransactionTagValue(tx, "collective"); collective != "" {
		return collective
	}
	if tx.Metadata != nil {
		for _, key := range []string{"collective", "stripe_collective"} {
			if collective, ok := tx.Metadata[key].(string); ok && collective != "" {
				return normalizeTransactionTagSlug(collective)
			}
		}
	}
	return ""
}

func txSource(tx TransactionEntry) string {
	if tx.Provider == "stripe" {
		return "Stripe"
	}
	if tx.Provider == "etherscan" {
		return tx.AccountSlug
	}
	if tx.Provider == "monerium" {
		return "Monerium"
	}
	return tx.Provider
}

func shortAddr(s string) string {
	if strings.HasPrefix(s, "0x") && len(s) > 14 {
		return s[:6] + "..." + s[len(s)-4:]
	}
	return s
}

// ── Data loading ──

// TxFilter narrows the set of transactions returned by loadFilteredTransactions.
// Zero-valued fields are treated as "no filter".
type TxFilter struct {
	AccountSlug string     // matches AccountSlug or Slug-like account fields
	Currency    string     // "EUR" matches the EUR family; other codes are exact
	Since       time.Time  // inclusive lower bound
	Until       time.Time  // inclusive upper bound (end-of-day handled by caller)
	Tags        [][]string // all Nostr-style tags must match (AND across keys)
	// AnyTags holds OR-groups: a tx passes if for each group at least one
	// tag in that group matches. Powers `--category foo,bar,baz` where the
	// tx need only match ONE of the listed categories.
	AnyTags [][][]string
	// SearchAny: each entry is a (lower-cased) substring; a tx passes if ANY
	// entry matches against the tx's searchable text (counterparty +
	// description + payment refs). Repeated --search flags accumulate here.
	SearchAny []string
	// DescriptionAny: each entry is a (lower-cased) substring; a tx passes
	// if ANY entry matches against the tx's description field only — the
	// narrower companion to SearchAny when counterparty/category noise is
	// triggering false matches. Repeated --description flags accumulate.
	DescriptionAny []string
	// Amount filters by absolute gross magnitude with an optional operator
	// (==, >, <, >=, <=). nil = no filter. Magnitude not signed amount —
	// operators think "€100 transactions", not "+€100 vs -€100"; direction
	// is what --direction is for.
	Amount *AmountFilter
	// Direction filters by sign of the signed amount: "in" (positive) or
	// "out" (negative). Empty = no filter. Zero-amount txs match neither.
	Direction string
	// NoCategory keeps only transactions with no category set anywhere
	// (Category field, "category" tag, metadata["category"]). Same shape
	// for NoCollective.
	NoCategory  bool
	NoCollective bool
}

// AmountFilter is one comparison constraint on the absolute gross of a tx.
type AmountFilter struct {
	Op    string  // "==", ">", "<", ">=", "<="
	Value float64
}

func loadAllTransactions(currencyFilter string) []TransactionEntry {
	return loadFilteredTransactions(TxFilter{Currency: currencyFilter})
}

func loadFilteredTransactions(f TxFilter) []TransactionEntry {
	return loadFilteredTransactionsWithPII(f, true)
}

func loadPublicFilteredTransactions(f TxFilter) []TransactionEntry {
	return loadFilteredTransactionsWithPII(f, false)
}

func loadFilteredTransactionsWithPII(f TxFilter, includePII bool) []TransactionEntry {
	dataDir := DataDir()
	var all []TransactionEntry

	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			txFile := loadTransactionsFile(dataDir, yd.Name(), md.Name(), includePII)
			if txFile == nil {
				continue
			}
			for _, tx := range txFile.Transactions {
				if tx.Type == "INTERNAL" {
					continue
				}
				if !f.matches(tx) {
					continue
				}
				all = append(all, tx)
			}
		}
	}

	all = append(all, virtualSpreadRows(dataDir, f)...)

	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp > all[j].Timestamp
	})
	return all
}

// virtualSpreadRows synthesizes one TransactionEntry per inbound-spread
// allocation that lands in the filter's date range. They are clearly marked
// (Metadata["virtualSource"]) so renderers can show them with a ↳ prefix and
// edit paths can refuse to mutate them. Only emitted when the filter has a
// bounded Since/Until — otherwise we'd flood the table.
func virtualSpreadRows(dataDir string, f TxFilter) []TransactionEntry {
	if f.Since.IsZero() || f.Until.IsZero() {
		return nil
	}
	tz := BrusselsTZ()
	start := f.Since.In(tz)
	end := f.Until.In(tz)
	if end.Before(start) {
		return nil
	}
	var out []TransactionEntry
	for cur := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, tz); !cur.After(end); cur = cur.AddDate(0, 1, 0) {
		year := fmt.Sprintf("%04d", cur.Year())
		month := fmt.Sprintf("%02d", cur.Month())
		for _, in := range LoadInboundSpreads(dataDir, year, month) {
			tx := virtualSpreadEntry(in, year, month, tz)
			if !f.matches(tx) {
				continue
			}
			out = append(out, tx)
		}
	}
	return out
}

func virtualSpreadEntry(in InboundSpread, year, month string, tz *time.Location) TransactionEntry {
	y, _ := strconv.Atoi(year)
	mo, _ := strconv.Atoi(month)
	ts := time.Date(y, time.Month(mo), 1, 0, 0, 0, 0, tz).Unix()

	amount, _ := strconv.ParseFloat(in.Amount, 64)
	txType := "CREDIT"
	if amount < 0 {
		txType = "DEBIT"
	}
	if in.Type != "" && (strings.EqualFold(in.Type, "DEBIT") || strings.EqualFold(in.Type, "CREDIT")) {
		txType = strings.ToUpper(in.Type)
	}

	tx := TransactionEntry{
		ID:               "virtual:" + in.URI + ":" + year + "-" + month,
		TxHash:           "",
		Provider:         "spread",
		Account:          "",
		AccountSlug:      "",
		AccountName:      "",
		Currency:         in.Currency,
		Amount:           absFloat(amount),
		NormalizedAmount: amount,
		GrossAmount:      absFloat(amount),
		Type:             txType,
		Counterparty:     in.Counterparty,
		Timestamp:        ts,
		Category:         in.Category,
		Collective:       in.Collective,
		Metadata: map[string]interface{}{
			"virtualSource":    true,
			"sourceURI":        in.URI,
			"sourceNaturalYM":  in.NaturalYM,
			"sourceTxID":       in.TxID,
			"spreadAllocation": in.Amount,
			"spreadTotal":      in.Total,
			"description":      in.Description,
		},
	}
	return tx
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func loadTransactionsFile(dataDir, year, month string, includePII bool) *TransactionsFile {
	if includePII {
		return LoadTransactionsWithPII(dataDir, year, month)
	}
	txPath := filepath.Join(dataDir, year, month, "generated", "transactions.json")
	data, err := os.ReadFile(txPath)
	if err != nil {
		return nil
	}
	var txFile TransactionsFile
	if json.Unmarshal(data, &txFile) != nil {
		return nil
	}
	return &txFile
}

func (f TxFilter) matches(tx TransactionEntry) bool {
	if f.Currency != "" {
		if strings.EqualFold(f.Currency, "EUR") {
			if !isEURCurrency(tx.Currency) {
				return false
			}
		} else if !strings.EqualFold(tx.Currency, f.Currency) {
			return false
		}
	}
	if f.AccountSlug != "" && !accountSlugMatchesTx(f.AccountSlug, tx) {
		return false
	}
	if !f.Since.IsZero() && tx.Timestamp < f.Since.Unix() {
		return false
	}
	if !f.Until.IsZero() && tx.Timestamp > f.Until.Unix() {
		return false
	}
	for _, tag := range f.Tags {
		if !transactionHasTag(tx, tag) {
			return false
		}
	}
	for _, group := range f.AnyTags {
		anyMatch := false
		for _, tag := range group {
			if transactionHasTag(tx, tag) {
				anyMatch = true
				break
			}
		}
		if !anyMatch {
			return false
		}
	}
	if len(f.SearchAny) > 0 {
		hay := txSearchableText(tx)
		anyMatch := false
		for _, needle := range f.SearchAny {
			if strings.Contains(hay, needle) {
				anyMatch = true
				break
			}
		}
		if !anyMatch {
			return false
		}
	}
	if len(f.DescriptionAny) > 0 {
		desc := strings.ToLower(txDisplayDescription(tx))
		anyMatch := false
		for _, needle := range f.DescriptionAny {
			if strings.Contains(desc, needle) {
				anyMatch = true
				break
			}
		}
		if !anyMatch {
			return false
		}
	}
	if f.Direction != "" {
		amt := txAmount(tx)
		switch f.Direction {
		case "in":
			if amt <= 0 {
				return false
			}
		case "out":
			if amt >= 0 {
				return false
			}
		}
	}
	if f.NoCategory && txDisplayCategory(tx) != "" {
		return false
	}
	if f.NoCollective && txDisplayCollective(tx) != "" {
		return false
	}
	if f.Amount != nil {
		abs := roundCents(math.Abs(txAmount(tx)))
		v := roundCents(f.Amount.Value)
		switch f.Amount.Op {
		case "==", "":
			if abs != v {
				return false
			}
		case ">":
			if abs <= v {
				return false
			}
		case ">=":
			if abs < v {
				return false
			}
		case "<":
			if abs >= v {
				return false
			}
		case "<=":
			if abs > v {
				return false
			}
		}
	}
	return true
}

// parseAmountFilter accepts "100", "=100", "==100", ">10", ">=10", "<5",
// "<=5" (whitespace around the operator is optional). Returns an
// AmountFilter or an error suitable for printing.
func parseAmountFilter(raw string) (*AmountFilter, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return nil, nil
	}
	op := "=="
	switch {
	case strings.HasPrefix(s, ">="):
		op = ">="
		s = s[2:]
	case strings.HasPrefix(s, "<="):
		op = "<="
		s = s[2:]
	case strings.HasPrefix(s, "=="):
		s = s[2:]
	case strings.HasPrefix(s, ">"):
		op = ">"
		s = s[1:]
	case strings.HasPrefix(s, "<"):
		op = "<"
		s = s[1:]
	case strings.HasPrefix(s, "="):
		s = s[1:]
	}
	s = strings.TrimSpace(s)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid --amount value %q (expected: 100, >10, <=1000.40, etc.)", raw)
	}
	return &AmountFilter{Op: op, Value: v}, nil
}

// txSearchableText returns the concatenated lower-cased text fields a
// --search query runs against. Includes the visible cells (counterparty,
// description, category, collective) plus the raw payment-ref and Stripe
// customer name when present — so a search for "openletter" hits both
// "donation openletter" descriptions and any tagged collective.
func txSearchableText(tx TransactionEntry) string {
	parts := []string{
		tx.Counterparty,
		txDisplayCounterparty(tx),
		txDisplayDescription(tx),
		tx.Category,
		tx.Collective,
		tx.Event,
		tx.Application,
	}
	if desc := stringMetadata(tx.Metadata, "description"); desc != "" {
		parts = append(parts, desc)
	}
	if pr := stringMetadata(tx.Metadata, "paymentRef"); pr != "" {
		parts = append(parts, pr)
	}
	if name := stringMetadata(tx.Metadata, "customerName"); name != "" {
		parts = append(parts, name)
	}
	return strings.ToLower(strings.Join(parts, " "))
}

// accountSlugMatchesTx returns true when a user-supplied --account value
// matches the transaction. The literal match against tx.AccountSlug
// handles the common case (e.g. "savings"), but Stripe transactions
// store the Stripe account ID in AccountSlug — so we also consult
// accounts.json: if the user-supplied slug names a configured account,
// the configured AccountID (and address) is accepted too.
func accountSlugMatchesTx(slug string, tx TransactionEntry) bool {
	if slug == "" {
		return true
	}
	if strings.EqualFold(tx.AccountSlug, slug) {
		return true
	}
	acc := findAccountConfigBySlug(slug)
	if acc == nil {
		return false
	}
	if acc.AccountID != "" && strings.EqualFold(tx.AccountSlug, acc.AccountID) {
		return true
	}
	if acc.Address != "" && strings.EqualFold(tx.Account, acc.Address) {
		return true
	}
	return false
}

// findAccountConfigBySlug returns a pointer to the configured account
// whose slug matches (case-insensitive), or nil. Loaded fresh each call —
// transactions filtering is short-lived, and accounts.json is small.
func findAccountConfigBySlug(slug string) *AccountConfig {
	if slug == "" {
		return nil
	}
	for _, acc := range LoadAccountConfigs() {
		if strings.EqualFold(acc.Slug, slug) {
			return &acc
		}
	}
	return nil
}

// ── Build table rows ──

func buildStickerRows(txs []TransactionEntry) [][]string {
	tz := BrusselsTZ()
	rows := make([][]string, len(txs))
	for i, tx := range txs {
		t := time.Unix(tx.Timestamp, 0).In(tz)

		rows[i] = []string{
			fmt.Sprintf(" %s", t.Format("02/01")),
			fmt.Sprintf(" %s", txSource(tx)),
			fmt.Sprintf(" %s", txDisplayCollective(tx)),
			fmt.Sprintf(" %s", txDisplayCategory(tx)),
			fmt.Sprintf(" %s", txDisplayCounterparty(tx)),
			fmt.Sprintf(" %s", txDisplayDescription(tx)),
			fmt.Sprintf(" %s", txAmountCell(tx, true)),
		}
	}
	return rows
}

var columnHeaders = []string{"Date", "Source", "Collective", "Category", "Counterparty", "Description", "Amount"}

const amountColumnIndex = 6

type txTableColumnKind string

const (
	txColumnSelection    txTableColumnKind = "selection"
	txColumnDate         txTableColumnKind = "date"
	txColumnSource       txTableColumnKind = "source"
	txColumnCollective   txTableColumnKind = "collective"
	txColumnCategory     txTableColumnKind = "category"
	txColumnCounterparty txTableColumnKind = "counterparty"
	txColumnDescription  txTableColumnKind = "description"
	txColumnAmount       txTableColumnKind = "amount"
	txColumnReconciled   txTableColumnKind = "reconciled"
)

type txTableColumn struct {
	Header   string
	Kind     txTableColumnKind
	Ratio    int
	MinWidth int
}

func transactionTableColumns(showAccount bool, includeSelection bool) []txTableColumn {
	cols := make([]txTableColumn, 0, 8)
	if includeSelection {
		cols = append(cols, txTableColumn{Header: "Sel", Kind: txColumnSelection, Ratio: 1, MinWidth: 5})
	}
	// Date renders as "↳ 02/01" worst case (virtual rows) — 7 columns.
	// Pin the column there: Ratio=0 means flexbox gives it no share of
	// the leftover width, MinWidth=7 holds it at exactly that. The
	// freed width gets redistributed to Counterparty / Description /
	// Collective, which are the columns that actually benefit from
	// extra space.
	cols = append(cols, txTableColumn{Header: "Date", Kind: txColumnDate, Ratio: 0, MinWidth: 7})
	if showAccount {
		cols = append(cols, txTableColumn{Header: "Account", Kind: txColumnSource, Ratio: 3, MinWidth: 8})
	}
	cols = append(cols,
		txTableColumn{Header: "Collective", Kind: txColumnCollective, Ratio: 4, MinWidth: 12},
		txTableColumn{Header: "Category", Kind: txColumnCategory, Ratio: 4, MinWidth: 10},
		txTableColumn{Header: "Counterparty", Kind: txColumnCounterparty, Ratio: 5, MinWidth: 12},
		txTableColumn{Header: "Description", Kind: txColumnDescription, Ratio: 8, MinWidth: 12},
		txTableColumn{Header: "Amount", Kind: txColumnAmount, Ratio: 4, MinWidth: 9},
		txTableColumn{Header: "Reconciled", Kind: txColumnReconciled, Ratio: 3, MinWidth: 10},
	)
	return cols
}

func legacyTransactionTableColumns() []txTableColumn {
	return transactionTableColumns(true, false)
}

func txColumnHeaders(cols []txTableColumn) []string {
	headers := make([]string, len(cols))
	for i, col := range cols {
		headers[i] = col.Header
	}
	return headers
}

func txColumnRatios(cols []txTableColumn) []int {
	ratios := make([]int, len(cols))
	for i, col := range cols {
		ratios[i] = col.Ratio
	}
	return ratios
}

func txColumnMinWidths(cols []txTableColumn) []int {
	widths := make([]int, len(cols))
	for i, col := range cols {
		widths[i] = col.MinWidth
	}
	return widths
}

func txColumnIndex(cols []txTableColumn, kind txTableColumnKind) int {
	for i, col := range cols {
		if col.Kind == kind {
			return i
		}
	}
	return -1
}

func selectedTxMarker(selected map[string]bool, tx TransactionEntry) string {
	if selected != nil && selected[tx.ID] {
		return "[x]"
	}
	return "[ ]"
}

func transactionCellValue(tx TransactionEntry, kind txTableColumnKind, selected map[string]bool, styled bool) string {
	virtual := isVirtualSpreadTx(tx)
	switch kind {
	case txColumnSelection:
		if virtual {
			return ""
		}
		return selectedTxMarker(selected, tx)
	case txColumnDate:
		date := time.Unix(tx.Timestamp, 0).In(BrusselsTZ()).Format("02/01")
		if virtual {
			return "↳ " + date
		}
		return date
	case txColumnSource:
		return txSource(tx)
	case txColumnCollective:
		return txDisplayCollective(tx)
	case txColumnCategory:
		return txDisplayCategory(tx)
	case txColumnCounterparty:
		return txDisplayCounterparty(tx)
	case txColumnDescription:
		if virtual {
			ym, _ := tx.Metadata["sourceNaturalYM"].(string)
			if ym != "" {
				return "from " + ym
			}
			return "spread"
		}
		return txDisplayDescription(tx)
	case txColumnAmount:
		if virtual {
			return virtualAmountCell(tx, styled)
		}
		return txAmountCell(tx, styled)
	case txColumnReconciled:
		if virtual {
			return ""
		}
		return getTxReconciliationLookup().ReconciledRef(tx)
	default:
		return ""
	}
}

func isVirtualSpreadTx(tx TransactionEntry) bool {
	if tx.Metadata == nil {
		return false
	}
	v, _ := tx.Metadata["virtualSource"].(bool)
	return v
}

func virtualAmountCell(tx TransactionEntry, styled bool) string {
	allocStr, _ := tx.Metadata["spreadAllocation"].(string)
	totalStr, _ := tx.Metadata["spreadTotal"].(string)
	alloc, _ := strconv.ParseFloat(allocStr, 64)
	total, _ := strconv.ParseFloat(totalStr, 64)
	out := formatVirtualAmount(alloc, tx.Currency) + "/" + formatVirtualAmount(total, tx.Currency)
	if !styled {
		return out
	}
	style := styleGreen
	if alloc < 0 {
		style = styleRed
	}
	return style.Render(out)
}

func formatVirtualAmount(v float64, currency string) string {
	abs := absFloat(v)
	sign := "+"
	if v < 0 {
		sign = "-"
	}
	if v == 0 {
		sign = ""
	}
	if isEURCurrency(currency) {
		return fmt.Sprintf("%s€%.2f", sign, abs)
	}
	return fmt.Sprintf("%s%.2f %s", sign, abs, currency)
}

func buildStickerRowsForTable(txs []TransactionEntry, cols []txTableColumn, selected map[string]bool) [][]string {
	rows := make([][]string, len(txs))
	for i, tx := range txs {
		row := make([]string, len(cols))
		for j, col := range cols {
			row[j] = fmt.Sprintf(" %s", transactionCellValue(tx, col.Kind, selected, true))
		}
		rows[i] = row
	}
	return rows
}

func newStickerTable(txs []TransactionEntry, w, h int) *stickertable.Table {
	return newStickerTableForColumns(txs, w, h, legacyTransactionTableColumns(), nil)
}

func newStickerTableForColumns(txs []TransactionEntry, w, h int, cols []txTableColumn, selected map[string]bool) *stickertable.Table {
	t := stickertable.NewTable(w, h, txColumnHeaders(cols))
	t.SetRatio(txColumnRatios(cols))
	t.SetMinWidth(txColumnMinWidths(cols))
	t.SetStylePassing(true)

	t.SetStyles(map[stickertable.StyleKey]lipgloss.Style{
		stickertable.StyleKeyHeader: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("252")).
			Background(lipgloss.Color("238")),
		stickertable.StyleKeyRows: lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")),
		stickertable.StyleKeyRowsSubsequent: lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")),
		// Selected row
		stickertable.StyleKeyRowsCursor: lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Background(lipgloss.Color("236")).
			Bold(true),
		// Same as row cursor (column selection is in the header bar, not in cells)
		stickertable.StyleKeyCellCursor: lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Background(lipgloss.Color("236")).
			Bold(true),
		stickertable.StyleKeyFooter: lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Height(1),
	})

	rows := buildStickerRowsForTable(txs, cols, selected)
	for _, row := range rows {
		anyRow := make([]any, len(row))
		for j, v := range row {
			anyRow[j] = v
		}
		t.MustAddRows([][]any{anyRow})
	}

	return t
}

func (m *txBrowserModel) rebuildTableRows() {
	if m.sortCol >= 0 {
		sortTransactionsForTableColumn(m.txs, m.columns, m.sortCol, m.sortAsc)
		tableHeight := 0
		if m.height > 4 {
			tableHeight = m.height - 4
		}
		m.table = newStickerTableForColumns(m.txs, m.width, tableHeight, m.columns, m.selectedTxIDs)
		if m.filterStr != "" {
			m.table.SetFilter(m.selectedCol, m.filterStr)
		}
		return
	}
	m.table.ClearRows()
	rows := buildStickerRowsForTable(m.txs, m.columns, m.selectedTxIDs)
	for _, row := range rows {
		anyRow := make([]any, len(row))
		for j, v := range row {
			anyRow[j] = v
		}
		m.table.MustAddRows([][]any{anyRow})
	}
	if m.filterStr != "" {
		m.table.SetFilter(m.selectedCol, m.filterStr)
	}
}

func sortTransactionsForColumn(txs []TransactionEntry, column int, ascending bool) {
	sortTransactionsForTableColumn(txs, legacyTransactionTableColumns(), column, ascending)
}

func sortTransactionsForTableColumn(txs []TransactionEntry, cols []txTableColumn, column int, ascending bool) {
	if column < 0 || column >= len(cols) || cols[column].Kind == txColumnSelection {
		return
	}
	sort.SliceStable(txs, func(i, j int) bool {
		if cols[column].Kind == txColumnAmount {
			left := transactionSortAmountValue(txs[i])
			right := transactionSortAmountValue(txs[j])
			if left == right {
				return txs[i].Timestamp > txs[j].Timestamp
			}
			if ascending {
				return left < right
			}
			return left > right
		}
		left := transactionSortValueForKind(txs[i], cols[column].Kind)
		right := transactionSortValueForKind(txs[j], cols[column].Kind)
		if left == right {
			return txs[i].Timestamp > txs[j].Timestamp
		}
		if ascending {
			return left < right
		}
		return left > right
	})
}

func transactionSortValue(tx TransactionEntry, column int) string {
	cols := legacyTransactionTableColumns()
	if column < 0 || column >= len(cols) {
		return ""
	}
	return transactionSortValueForKind(tx, cols[column].Kind)
}

func transactionSortValueForKind(tx TransactionEntry, kind txTableColumnKind) string {
	switch kind {
	case txColumnDate:
		return fmt.Sprintf("%020d", tx.Timestamp)
	case txColumnSource:
		return strings.ToLower(txSource(tx))
	case txColumnCollective:
		return strings.ToLower(txDisplayCollective(tx))
	case txColumnCategory:
		return strings.ToLower(txDisplayCategory(tx))
	case txColumnCounterparty:
		return strings.ToLower(txDisplayCounterparty(tx))
	case txColumnDescription:
		return strings.ToLower(txDisplayDescription(tx))
	case txColumnAmount:
		return fmt.Sprintf("%.8f", transactionSortAmountValue(tx))
	default:
		return ""
	}
}

func transactionSortAmountValue(tx TransactionEntry) float64 {
	return parseTransactionAmountCell(txAmountCell(tx, false))
}

// ── Browser modes and actions ──

type browserMode int

const (
	modeTable browserMode = iota
	modeDetail
	modeEditCollective
	modeEditCategory
	modeEditAssignment
	modeEditDate
)

type browserAction int

const (
	browserNone browserAction = iota
	browserQuit
	browserCreateRule
	browserSaved
)

// ── Detail panel (implements overlay.Viewable) ──

type detailPanel struct {
	content string
}

func (d *detailPanel) View() string { return d.content }

// ── Table view wrapper (implements overlay.Viewable) ──

type tableView struct {
	content string
}

func (t *tableView) View() string { return t.content }

// ── Bubbletea model ──

type txBrowserModel struct {
	table         *stickertable.Table
	columns       []txTableColumn
	txs           []TransactionEntry
	currency      string
	account       string
	quitting      bool
	action        browserAction
	mode          browserMode
	detailTx      *TransactionEntry
	detailIdx     int
	bulkEdit      bool
	selectedTxIDs map[string]bool
	// Inline edit fields
	editInput             string   // current text input for inline edit
	editOptions           []string // available options for autocomplete
	editCursor            int      // cursor position in filtered options
	editCollectiveInput   string
	editCategoryInput     string
	editCollectiveOptions []string
	editCategoryOptions   []string
	editField             int
	editCollectiveCursor  int
	editCategoryCursor    int
	// Column selection + filter
	selectedCol int    // which column is highlighted in the header
	filterStr   string // active filter text (empty = not filtering)
	filtering   bool   // true = in filter input mode (typing filters)
	sortCol     int
	sortAsc     bool
	statusText  string
	statusError bool
	statusSeq   int
	// Layout
	width  int
	height int
}

func (m txBrowserModel) Init() tea.Cmd { return nil }

type txPublishResultMsg struct {
	Seq       int
	Events    int
	Relays    int
	Published int
	Err       error
}

type txClearStatusMsg struct {
	Seq int
}

func clearTxStatusAfter(seq int, d time.Duration) tea.Cmd {
	return tea.Tick(d, func(time.Time) tea.Msg {
		return txClearStatusMsg{Seq: seq}
	})
}

func (m txBrowserModel) selectedCount() int {
	return len(m.selectedTxIDs)
}

func (m txBrowserModel) currentFilteredTx() (TransactionEntry, bool) {
	_, y := m.table.GetCursorLocation()
	filtered := m.getFilteredTxs()
	if y < 0 || y >= len(filtered) {
		return TransactionEntry{}, false
	}
	return filtered[y], true
}

func (m txBrowserModel) selectedTransactions() []TransactionEntry {
	if len(m.selectedTxIDs) == 0 {
		return nil
	}
	out := make([]TransactionEntry, 0, len(m.selectedTxIDs))
	for _, tx := range m.txs {
		if m.selectedTxIDs[tx.ID] {
			out = append(out, tx)
		}
	}
	return out
}

func (m *txBrowserModel) toggleCurrentSelection() {
	tx, ok := m.currentFilteredTx()
	if !ok {
		return
	}
	if m.selectedTxIDs == nil {
		m.selectedTxIDs = map[string]bool{}
	}
	if m.selectedTxIDs[tx.ID] {
		delete(m.selectedTxIDs, tx.ID)
	} else {
		m.selectedTxIDs[tx.ID] = true
	}
	m.rebuildTableRows()
}

func (m *txBrowserModel) toggleAllFilteredSelection() {
	filtered := m.getFilteredTxs()
	if len(filtered) == 0 {
		return
	}
	if m.selectedTxIDs == nil {
		m.selectedTxIDs = map[string]bool{}
	}
	allSelected := true
	for _, tx := range filtered {
		if !m.selectedTxIDs[tx.ID] {
			allSelected = false
			break
		}
	}
	if allSelected {
		for _, tx := range filtered {
			delete(m.selectedTxIDs, tx.ID)
		}
	} else {
		for _, tx := range filtered {
			m.selectedTxIDs[tx.ID] = true
		}
	}
	m.rebuildTableRows()
}

func (m txBrowserModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.table.SetWidth(msg.Width)
		m.table.SetHeight(msg.Height - 4) // title + footer
	case txPublishResultMsg:
		if msg.Seq != m.statusSeq {
			return m, nil
		}
		if msg.Err != nil {
			m.statusText = fmt.Sprintf("Nostr publish failed: %v", msg.Err)
			m.statusError = true
			return m, clearTxStatusAfter(msg.Seq, 10*time.Second)
		}
		m.statusText = fmt.Sprintf("Posted %s to %s", Pluralize(msg.Published, "Nostr event", ""), Pluralize(msg.Relays, "relay", ""))
		m.statusError = false
		return m, clearTxStatusAfter(msg.Seq, 4*time.Second)
	case txClearStatusMsg:
		if msg.Seq == m.statusSeq {
			m.statusText = ""
			m.statusError = false
		}
		return m, nil
	}

	switch m.mode {
	case modeTable:
		return m.updateTable(msg)
	case modeDetail:
		return m.updateDetail(msg)
	case modeEditCollective, modeEditCategory, modeEditAssignment, modeEditDate:
		return m.updateInlineEdit(msg)
	}

	return m, nil
}

func (m txBrowserModel) updateTable(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		key := keyMsg.String()

		// Filter input mode: typing goes to filter
		if m.filtering {
			switch key {
			case "esc":
				m.filtering = false
				m.filterStr = ""
				m.table.UnsetFilter()
			case "backspace":
				if len(m.filterStr) > 1 {
					m.filterStr = m.filterStr[:len(m.filterStr)-1]
					m.table.SetFilter(m.selectedCol, m.filterStr)
				} else if len(m.filterStr) == 1 {
					m.filterStr = ""
					m.table.UnsetFilter()
				} else {
					m.filtering = false
				}
			case "enter":
				m.filtering = false // keep filter active, exit typing mode
			case "down", "j":
				m.table.CursorDown()
			case "up", "k":
				m.table.CursorUp()
			case "left":
				if m.selectedCol > 0 {
					m.selectedCol--
					if m.filterStr != "" {
						m.table.SetFilter(m.selectedCol, m.filterStr)
					}
				}
			case "right":
				if m.selectedCol < len(m.columns)-1 {
					m.selectedCol++
					if m.filterStr != "" {
						m.table.SetFilter(m.selectedCol, m.filterStr)
					}
				}
			default:
				if len(key) == 1 && key >= " " && key <= "~" {
					m.filterStr += key
					m.table.SetFilter(m.selectedCol, m.filterStr)
				}
			}
			return m, nil
		}

		// Normal mode
		switch key {
		case "q", "ctrl+c":
			m.quitting = true
			m.action = browserQuit
			return m, tea.Quit
		case "esc":
			m.filtering = false
			if m.filterStr != "" {
				m.filterStr = ""
				m.table.UnsetFilter()
			}
		case "/":
			m.filtering = true
			m.filterStr = ""
		case "down", "j":
			m.table.CursorDown()
		case "up", "k":
			m.table.CursorUp()
		case "left":
			if m.selectedCol > 0 {
				m.selectedCol--
			}
		case "right":
			if m.selectedCol < len(m.columns)-1 {
				m.selectedCol++
			}
		case "pgdown":
			pageSize := m.height - 6
			if pageSize < 5 {
				pageSize = 5
			}
			for i := 0; i < pageSize; i++ {
				m.table.CursorDown()
			}
		case "pgup":
			pageSize := m.height - 6
			if pageSize < 5 {
				pageSize = 5
			}
			for i := 0; i < pageSize; i++ {
				m.table.CursorUp()
			}
		case "home":
			_, y := m.table.GetCursorLocation()
			for i := 0; i < y; i++ {
				m.table.CursorUp()
			}
		case "end":
			for i := 0; i < len(m.txs); i++ {
				m.table.CursorDown()
			}
		case "enter":
			if tx, ok := m.currentFilteredTx(); ok {
				m.detailTx = &tx
				_, m.detailIdx = m.table.GetCursorLocation()
				m.mode = modeDetail
			}
		case "x", " ", "space":
			m.toggleCurrentSelection()
		case "A":
			m.toggleAllFilteredSelection()
		case "C":
			if len(m.selectedTxIDs) > 0 {
				m.startEditCollectiveForSelection()
			} else if tx, ok := m.currentFilteredTx(); ok {
				m.detailTx = &tx
				_, m.detailIdx = m.table.GetCursorLocation()
				m.startEditCollective()
			}
		case "c":
			if len(m.selectedTxIDs) > 0 {
				m.startEditCategoryForSelection()
			} else if tx, ok := m.currentFilteredTx(); ok {
				m.detailTx = &tx
				_, m.detailIdx = m.table.GetCursorLocation()
				m.startEditCategory()
			}
		case "e":
			if len(m.selectedTxIDs) > 0 {
				m.startEditAssignmentForSelection()
			} else if tx, ok := m.currentFilteredTx(); ok {
				m.detailTx = &tx
				_, m.detailIdx = m.table.GetCursorLocation()
				m.startEditAssignment()
			}
		case "d":
			if tx, ok := m.currentFilteredTx(); ok {
				m.detailTx = &tx
				_, m.detailIdx = m.table.GetCursorLocation()
				m.startEditDate()
			}
		case "s":
			ascending := true
			if m.sortCol == m.selectedCol {
				ascending = !m.sortAsc
			}
			m.sortCol = m.selectedCol
			m.sortAsc = ascending
			m.rebuildTableRows()
		case "r":
			m.action = browserCreateRule
			return m, tea.Quit
		}
	}

	return m, nil
}

func (m txBrowserModel) updateDetail(msg tea.Msg) (tea.Model, tea.Cmd) {
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "esc", "q", "enter":
			m.mode = modeTable
			m.detailTx = nil
			// Re-sync filter state with stickers table
			if m.filterStr != "" {
				m.table.SetFilter(m.selectedCol, m.filterStr)
			}
		case "C":
			m.startEditCollective()
		case "c":
			m.startEditCategory()
		case "e":
			m.startEditAssignment()
		case "d":
			m.startEditDate()
		}
	}
	return m, nil
}

// ── Inline edit helpers ──

func (m txBrowserModel) filteredEditOptions() []string {
	if m.editInput == "" {
		return m.editOptions
	}
	filter := strings.ToLower(m.editInput)
	var result []string
	for _, o := range m.editOptions {
		if strings.Contains(strings.ToLower(o), filter) {
			result = append(result, o)
		}
	}
	return result
}

func filterEditOptions(options []string, input string) []string {
	if input == "" {
		return options
	}
	filter := strings.ToLower(input)
	var result []string
	for _, option := range options {
		if strings.Contains(strings.ToLower(option), filter) {
			result = append(result, option)
		}
	}
	return result
}

func (m txBrowserModel) filteredCollectiveOptions() []string {
	return filterEditOptions(m.editCollectiveOptions, m.editCollectiveInput)
}

func (m txBrowserModel) filteredCategoryOptions() []string {
	return filterEditOptions(m.editCategoryOptions, m.editCategoryInput)
}

func (m *txBrowserModel) startEditCollective() {
	m.bulkEdit = false
	m.editOptions = CollectiveSlugs()
	sort.Strings(m.editOptions)
	m.editInput = txDisplayCollective(*m.detailTx)
	m.editCursor = 0
	m.mode = modeEditCollective
}

func (m *txBrowserModel) startEditCollectiveForSelection() {
	m.bulkEdit = true
	m.detailTx = nil
	m.editOptions = CollectiveSlugs()
	sort.Strings(m.editOptions)
	m.editInput = commonCollectiveValue(m.selectedTransactions())
	m.editCursor = 0
	m.mode = modeEditCollective
}

func (m *txBrowserModel) startEditCategory() {
	m.bulkEdit = false
	m.editOptions = categoryOptionsForTransaction(*m.detailTx, LoadCategories())
	m.editInput = txDisplayCategory(*m.detailTx)
	m.editCursor = 0
	m.mode = modeEditCategory
}

func (m *txBrowserModel) startEditCategoryForSelection() {
	m.bulkEdit = true
	m.detailTx = nil
	targets := m.selectedTransactions()
	m.editOptions = categoryOptionsForTransactions(targets, LoadCategories())
	m.editInput = commonCategoryValue(targets)
	m.editCursor = 0
	m.mode = modeEditCategory
}

func (m *txBrowserModel) startEditAssignment() {
	m.bulkEdit = false
	m.editCollectiveOptions = CollectiveSlugs()
	sort.Strings(m.editCollectiveOptions)
	m.editCategoryOptions = categoryOptionsForTransaction(*m.detailTx, LoadCategories())
	m.editCollectiveInput = txDisplayCollective(*m.detailTx)
	m.editCategoryInput = txDisplayCategory(*m.detailTx)
	m.editField = 0
	m.editCollectiveCursor = 0
	m.editCategoryCursor = 0
	m.mode = modeEditAssignment
}

func (m *txBrowserModel) startEditAssignmentForSelection() {
	m.bulkEdit = true
	m.detailTx = nil
	targets := m.selectedTransactions()
	m.editCollectiveOptions = CollectiveSlugs()
	sort.Strings(m.editCollectiveOptions)
	m.editCategoryOptions = categoryOptionsForTransactions(targets, LoadCategories())
	m.editCollectiveInput = commonCollectiveValue(targets)
	m.editCategoryInput = commonCategoryValue(targets)
	m.editField = 0
	m.editCollectiveCursor = 0
	m.editCategoryCursor = 0
	m.mode = modeEditAssignment
}

func categoryOptionsForTransaction(tx TransactionEntry, cats []CategoryDef) []string {
	return categoryOptionsForTransactions([]TransactionEntry{tx}, cats)
}

func categoryOptionsForTransactions(txs []TransactionEntry, cats []CategoryDef) []string {
	directions := map[string]bool{}
	for _, tx := range txs {
		if direction := categoryDirectionForTransaction(tx); direction != "" {
			directions[direction] = true
		}
	}
	seen := map[string]bool{}
	options := make([]string, 0, len(cats))
	for _, cat := range cats {
		slug := strings.TrimSpace(cat.Slug)
		if slug == "" {
			continue
		}
		catDirection := strings.ToLower(strings.TrimSpace(cat.Direction))
		if len(directions) > 0 && !directions[catDirection] {
			continue
		}
		key := strings.ToLower(slug)
		if seen[key] {
			continue
		}
		seen[key] = true
		options = append(options, slug)
	}
	sort.Strings(options)
	return options
}

func commonCollectiveValue(txs []TransactionEntry) string {
	return commonTransactionAssignmentValue(txs, txDisplayCollective)
}

func commonCategoryValue(txs []TransactionEntry) string {
	return commonTransactionAssignmentValue(txs, txDisplayCategory)
}

func commonTransactionAssignmentValue(txs []TransactionEntry, value func(TransactionEntry) string) string {
	if len(txs) == 0 {
		return ""
	}
	first := value(txs[0])
	for _, tx := range txs[1:] {
		if value(tx) != first {
			return ""
		}
	}
	return first
}

func categoryDirectionForTransaction(tx TransactionEntry) string {
	amount := parseTransactionAmountCell(txAmountCell(tx, false))
	if amount < 0 {
		return "expense"
	}
	if amount > 0 {
		return "income"
	}
	return ""
}

func (m *txBrowserModel) startEditDate() {
	m.bulkEdit = false
	if existing := compactSpreadInput(m.detailTx.Spread); existing != "" {
		m.editInput = existing
	} else {
		tz := BrusselsTZ()
		t := time.Unix(m.detailTx.Timestamp, 0).In(tz)
		m.editInput = fmt.Sprintf("%d-%02d", t.Year(), t.Month())
	}
	m.editOptions = nil
	m.editCursor = 0
	m.mode = modeEditDate
}

// compactSpreadInput renders a list of spread entries back as the most compact
// shorthand the parser accepts, so the editor prefills with what the user
// previously typed (or close to it).
func compactSpreadInput(spread []SpreadEntry) string {
	if len(spread) == 0 {
		return ""
	}
	months := make([]string, len(spread))
	for i, s := range spread {
		months[i] = s.Month
	}
	return compactMonthList(months)
}

func compactMonthList(months []string) string {
	if len(months) == 0 {
		return ""
	}
	if len(months) == 1 {
		return months[0]
	}
	// Detect contiguous range to compress.
	if isContiguousMonthRange(months) {
		return months[0] + "-" + months[len(months)-1]
	}
	return strings.Join(months, ",")
}

func isContiguousMonthRange(months []string) bool {
	if len(months) < 2 {
		return false
	}
	cur, err := time.Parse("2006-01", months[0])
	if err != nil {
		return false
	}
	for i := 1; i < len(months); i++ {
		next := cur.AddDate(0, 1, 0)
		got, err := time.Parse("2006-01", months[i])
		if err != nil || !got.Equal(next) {
			return false
		}
		cur = got
	}
	return true
}

func (m *txBrowserModel) commitInlineEdit() tea.Cmd {
	if m.bulkEdit {
		return m.commitBulkInlineEdit()
	}
	if m.detailTx == nil {
		return nil
	}
	if isVirtualSpreadTx(*m.detailTx) {
		// Virtual rows are projections of real txs in another month — edit
		// the source there.
		return nil
	}
	shouldPublish := m.mode == modeEditCollective || m.mode == modeEditCategory
	switch m.mode {
	case modeEditAssignment:
		m.editCollectiveInput = normalizeTransactionTagSlug(m.editCollectiveInput)
		m.editCategoryInput = normalizeTransactionTagSlug(m.editCategoryInput)
		m.detailTx.Collective = m.editCollectiveInput
		m.detailTx.Category = m.editCategoryInput
		for i := range m.txs {
			if m.txs[i].ID == m.detailTx.ID {
				m.txs[i].Collective = m.editCollectiveInput
				m.txs[i].Category = m.editCategoryInput
				syncTransactionTags(&m.txs[i])
				break
			}
		}
		shouldPublish = true
	case modeEditCollective:
		m.editInput = normalizeTransactionTagSlug(m.editInput)
		m.detailTx.Collective = m.editInput
		for i := range m.txs {
			if m.txs[i].ID == m.detailTx.ID {
				m.txs[i].Collective = m.editInput
				syncTransactionTags(&m.txs[i])
				break
			}
		}
	case modeEditCategory:
		m.editInput = normalizeTransactionTagSlug(m.editInput)
		m.detailTx.Category = m.editInput
		for i := range m.txs {
			if m.txs[i].ID == m.detailTx.ID {
				m.txs[i].Category = m.editInput
				syncTransactionTags(&m.txs[i])
				break
			}
		}
	case modeEditDate:
		// Parse the date input into a list of months and rebuild the spread.
		// On parse error, leave the existing spread untouched.
		months, err := ParseSpreadInput(m.editInput)
		if err == nil {
			total := txSpreadTotal(*m.detailTx)
			m.detailTx.Spread = BuildSpreadEntries(months, total)
			for i := range m.txs {
				if m.txs[i].ID == m.detailTx.ID {
					m.txs[i].Spread = m.detailTx.Spread
					break
				}
			}
			shouldPublish = true
		}
	}
	syncTransactionTags(m.detailTx)
	ensureTransactionAssignmentSettings(*m.detailTx)
	saveTransactionUpdate(m.detailTx)
	// Rebuild table
	m.rebuildTableRows()
	if shouldPublish {
		return m.startNostrPublish([]TransactionEntry{*m.detailTx})
	}
	return nil
}

// txSpreadTotal returns the signed total amount used as the basis for spread
// distribution. DEBIT amounts are negated so distribution preserves direction.
func txSpreadTotal(tx TransactionEntry) float64 {
	total := firstNonZeroFloat(tx.NormalizedAmount, tx.Amount, tx.GrossAmount, tx.NetAmount)
	if total < 0 {
		total = -total
	}
	if tx.IsOutgoing() {
		total = -total
	}
	return total
}

// spreadPreviewLines is the live feedback shown under the input while the user
// is typing in modeEditDate. It tells them how many months will be created and
// the per-month amount, or surfaces a parse error.
func (m txBrowserModel) spreadPreviewLines() []string {
	if m.detailTx == nil {
		return nil
	}
	input := strings.TrimSpace(m.editInput)
	if input == "" {
		return []string{"(no spread — natural month)"}
	}
	months, err := ParseSpreadInput(input)
	if err != nil {
		return []string{"⚠ " + err.Error()}
	}
	if len(months) == 0 {
		return []string{"(no spread)"}
	}
	total := txSpreadTotal(*m.detailTx)
	entries := BuildSpreadEntries(months, total)
	currency := m.detailTx.Currency
	if currency == "" {
		currency = "EUR"
	}
	totalLabel := formatSpreadAmount(total, currency)
	headline := fmt.Sprintf("%s × %s = %s",
		Pluralize(len(entries), "month", ""),
		formatSpreadEntry(entries[0], currency),
		totalLabel)
	out := []string{headline}

	monthList := compactMonthList(months)
	if len(monthList) > 60 {
		monthList = months[0] + " … " + months[len(months)-1]
	}
	out = append(out, monthList)
	return out
}

func formatSpreadEntry(s SpreadEntry, currency string) string {
	v, err := strconv.ParseFloat(s.Amount, 64)
	if err != nil {
		return s.Amount + " " + currency
	}
	return formatSpreadAmount(v, currency)
}

func formatSpreadAmount(v float64, currency string) string {
	if isEURCurrency(currency) {
		if v < 0 {
			return "-" + fmtNumber(-v) + " EUR"
		}
		return fmtNumber(v) + " EUR"
	}
	return fmt.Sprintf("%.2f %s", v, currency)
}

func (m *txBrowserModel) commitBulkInlineEdit() tea.Cmd {
	targets := m.selectedTransactions()
	if len(targets) == 0 {
		m.bulkEdit = false
		return nil
	}
	switch m.mode {
	case modeEditCollective:
		m.editInput = normalizeTransactionTagSlug(m.editInput)
	case modeEditCategory:
		m.editInput = normalizeTransactionTagSlug(m.editInput)
	case modeEditAssignment:
		m.editCollectiveInput = normalizeTransactionTagSlug(m.editCollectiveInput)
		m.editCategoryInput = normalizeTransactionTagSlug(m.editCategoryInput)
	default:
		m.bulkEdit = false
		return nil
	}
	if m.mode != modeEditAssignment && m.editInput == "" {
		m.bulkEdit = false
		return nil
	}
	if m.mode == modeEditAssignment && m.editCollectiveInput == "" && m.editCategoryInput == "" {
		m.bulkEdit = false
		return nil
	}

	changed := make([]TransactionEntry, 0, len(targets))
	for i := range m.txs {
		if !m.selectedTxIDs[m.txs[i].ID] {
			continue
		}
		switch m.mode {
		case modeEditCollective:
			m.txs[i].Collective = m.editInput
		case modeEditCategory:
			m.txs[i].Category = m.editInput
		case modeEditAssignment:
			m.txs[i].Collective = m.editCollectiveInput
			m.txs[i].Category = m.editCategoryInput
		}
		syncTransactionTags(&m.txs[i])
		ensureTransactionAssignmentSettings(m.txs[i])
		saveTransactionUpdate(&m.txs[i])
		changed = append(changed, m.txs[i])
	}
	m.bulkEdit = false
	m.rebuildTableRows()
	return m.startNostrPublish(changed)
}

func (m *txBrowserModel) startNostrPublish(txs []TransactionEntry) tea.Cmd {
	events := countPublishableTransactionAnnotations(txs)
	if events == 0 {
		return nil
	}
	relays := nostrRelayCountForPosting()
	m.statusSeq++
	seq := m.statusSeq
	m.statusText = fmt.Sprintf("Posting %s to %s...", Pluralize(events, "Nostr event", ""), Pluralize(relays, "relay", ""))
	m.statusError = false
	return publishTransactionAnnotationsCmd(seq, txs)
}

func ensureTransactionAssignmentSettings(tx TransactionEntry) {
	if category := txDisplayCategory(tx); category != "" {
		direction := categoryDirectionForTransaction(tx)
		AddCategory(CategoryDef{
			Slug:      category,
			Label:     labelFromSlug(category),
			Direction: direction,
		})
	}
	if collective := txDisplayCollective(tx); collective != "" {
		AddCollective(collective)
	}
}

func labelFromSlug(slug string) string {
	parts := strings.Fields(strings.ReplaceAll(strings.TrimSpace(slug), "-", " "))
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func nostrRelayCountForPosting() int {
	keys := LoadNostrKeys()
	if keys == nil {
		return 0
	}
	if len(keys.Relays) > 0 {
		return len(keys.Relays)
	}
	return len(nostrRelays)
}

func countPublishableTransactionAnnotations(txs []TransactionEntry) int {
	count := 0
	for _, tx := range txs {
		if buildTransactionAnnotationEvent(tx) != nil {
			count++
		}
	}
	return count
}

func publishTransactionAnnotationsCmd(seq int, txs []TransactionEntry) tea.Cmd {
	return func() tea.Msg {
		events, relays, published, err := publishTransactionAnnotationsFromTUI(txs)
		return txPublishResultMsg{
			Seq:       seq,
			Events:    events,
			Relays:    relays,
			Published: published,
			Err:       err,
		}
	}
}

func publishTransactionAnnotationsFromTUI(txs []TransactionEntry) (events int, relays int, published int, err error) {
	keys := LoadNostrKeys()
	if keys != nil {
		if len(keys.Relays) > 0 {
			relays = len(keys.Relays)
		} else {
			relays = len(nostrRelays)
		}
	}
	if keys == nil {
		for _, tx := range txs {
			if buildTransactionAnnotationEvent(tx) == nil {
				continue
			}
			events++
			persistTransactionAnnotationToNostrSource(tx, "", "")
		}
		if events == 0 {
			return 0, 0, 0, nil
		}
		return events, 0, 0, fmt.Errorf("no Nostr identity configured")
	}

	failures := 0
	firstFailure := ""
	for _, tx := range txs {
		ev := buildTransactionAnnotationEvent(tx)
		if ev == nil {
			continue
		}
		events++
		uri := txURI(tx)
		_, publishErr := publishNostrEventWithOutbox(keys, uri, ev)
		if publishErr != nil {
			persistTransactionAnnotationToNostrSource(tx, "", keys.PubHex)
			failures++
			if firstFailure == "" {
				firstFailure = fmt.Sprintf("%s: %v", tx.ID, publishErr)
			}
			continue
		}
		persistTransactionAnnotationToNostrSource(tx, ev.ID, keys.PubHex)
		published++
	}
	if failures > 0 {
		return events, relays, published, fmt.Errorf("%d/%s failed; first error: %s", failures, Pluralize(events, "event", ""), firstFailure)
	}
	return events, relays, published, nil
}

func buildTransactionAnnotationEvent(tx TransactionEntry) *nostr.Event {
	uri := txURI(tx)
	if uri == "" {
		return nil
	}
	category := txDisplayCategory(tx)
	collective := txDisplayCollective(tx)
	if category == "" && collective == "" && tx.Event == "" && len(tx.Spread) == 0 {
		return nil
	}

	tags := nostr.Tags{
		{"I", uri},
		{"K", uriKind(uri)},
		{"i", uri},
		{"k", uriKind(uri)},
	}
	if category != "" {
		tags = append(tags, nostr.Tag{"category", category})
	}
	if collective != "" {
		tags = append(tags, nostr.Tag{"collective", collective})
	}
	if tx.Event != "" {
		tags = append(tags, nostr.Tag{"event", tx.Event})
	}

	amount := tx.Amount
	if tx.NormalizedAmount != 0 {
		amount = tx.NormalizedAmount
	}
	if amount != 0 && tx.Currency != "" {
		tags = append(tags, nostr.Tag{"amount", fmt.Sprintf("%.2f", amount), tx.Currency})
	}

	for _, s := range tx.Spread {
		if s.Month == "" {
			continue
		}
		tags = append(tags, nostr.Tag{"spread", s.Month, s.Amount})
	}

	return &nostr.Event{
		Kind:    1111,
		Tags:    tags,
		Content: "",
	}
}

func persistTransactionAnnotationToNostrSource(tx TransactionEntry, eventID, author string) {
	uri := txURI(tx)
	if uri == "" {
		return
	}
	category := txDisplayCategory(tx)
	collective := txDisplayCollective(tx)
	if category == "" && collective == "" && tx.Event == "" && len(tx.Tags) == 0 && len(tx.Spread) == 0 {
		return
	}

	dataDir := DataDir()
	t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
	year := fmt.Sprintf("%d", t.Year())
	month := fmt.Sprintf("%02d", t.Month())
	path := nostrsource.Path(dataDir, year, month, nostrsource.AnnotationsFile)

	cache := NostrAnnotationCache{Annotations: map[string]*TxAnnotation{}}
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &cache)
	}
	if cache.Annotations == nil {
		cache.Annotations = map[string]*TxAnnotation{}
	}
	cache.FetchedAt = time.Now().UTC().Format(time.RFC3339)
	cache.Annotations[uri] = &TxAnnotation{
		URI:          uri,
		Category:     category,
		Collective:   collective,
		Event:        tx.Event,
		Tags:         normalizeTransactionTags(tx.Tags),
		Spread:       tx.Spread,
		NostrEventID: eventID,
		Author:       author,
		CreatedAt:    time.Now().Unix(),
	}
	_ = nostrsource.WriteJSON(dataDir, year, month, cache, nostrsource.AnnotationsFile)
}

func (m txBrowserModel) updateInlineEdit(msg tea.Msg) (tea.Model, tea.Cmd) {
	if m.mode == modeEditAssignment {
		return m.updateAssignmentEdit(msg)
	}
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		key := keyMsg.String()
		switch key {
		case "esc":
			if m.mode == modeEditDate {
				m.mode = modeDetail
			} else {
				m.mode = modeTable
				m.detailTx = nil
				m.bulkEdit = false
			}
			return m, nil
		case "enter":
			editMode := m.mode
			// If there are filtered options and one is selected, use it
			if m.mode != modeEditDate {
				filtered := m.filteredEditOptions()
				if len(filtered) > 0 && m.editCursor < len(filtered) {
					m.editInput = filtered[m.editCursor]
				}
			}
			cmd := m.commitInlineEdit()
			if editMode == modeEditDate {
				m.mode = modeDetail
			} else {
				m.mode = modeTable
				m.detailTx = nil
			}
			return m, cmd
		case "tab":
			// Autocomplete: fill input with selected option
			if m.mode != modeEditDate {
				filtered := m.filteredEditOptions()
				if len(filtered) > 0 && m.editCursor < len(filtered) {
					m.editInput = filtered[m.editCursor]
				}
			}
			return m, nil
		case "up":
			if m.editCursor > 0 {
				m.editCursor--
			}
		case "down":
			filtered := m.filteredEditOptions()
			if m.editCursor < len(filtered)-1 {
				m.editCursor++
			}
		case "backspace":
			if len(m.editInput) > 0 {
				m.editInput = m.editInput[:len(m.editInput)-1]
				m.editCursor = 0
			}
		default:
			if len(key) == 1 && key >= " " && key <= "~" {
				m.editInput += key
				m.editCursor = 0
			}
		}
	}
	return m, nil
}

func (m txBrowserModel) updateAssignmentEdit(msg tea.Msg) (tea.Model, tea.Cmd) {
	keyMsg, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	key := keyMsg.String()
	activeInput := func() *string {
		if m.editField == 0 {
			return &m.editCollectiveInput
		}
		return &m.editCategoryInput
	}
	activeCursor := func() *int {
		if m.editField == 0 {
			return &m.editCollectiveCursor
		}
		return &m.editCategoryCursor
	}
	activeFiltered := func() []string {
		if m.editField == 0 {
			return m.filteredCollectiveOptions()
		}
		return m.filteredCategoryOptions()
	}
	completeActive := func() {
		filtered := activeFiltered()
		cursor := *activeCursor()
		if len(filtered) > 0 && cursor < len(filtered) {
			*activeInput() = filtered[cursor]
		}
	}

	switch key {
	case "esc":
		m.mode = modeTable
		m.detailTx = nil
		m.bulkEdit = false
		return m, nil
	case "enter":
		completeActive()
		cmd := m.commitInlineEdit()
		m.mode = modeTable
		m.detailTx = nil
		return m, cmd
	case "tab":
		completeActive()
		m.editField = (m.editField + 1) % 2
		return m, nil
	case "shift+tab":
		if m.editField == 0 {
			m.editField = 1
		} else {
			m.editField = 0
		}
		return m, nil
	case "up":
		cursor := activeCursor()
		if *cursor > 0 {
			*cursor--
		}
	case "down":
		filtered := activeFiltered()
		cursor := activeCursor()
		if *cursor < len(filtered)-1 {
			*cursor++
		}
	case "backspace":
		input := activeInput()
		if len(*input) > 0 {
			*input = (*input)[:len(*input)-1]
			*activeCursor() = 0
		}
	default:
		if len(key) == 1 && key >= " " && key <= "~" {
			input := activeInput()
			*input += key
			*activeCursor() = 0
		}
	}
	return m, nil
}

func (m txBrowserModel) View() string {
	if m.quitting {
		return ""
	}

	switch m.mode {
	case modeDetail:
		// Overlay the detail panel on top of the table
		bgView := &tableView{content: m.renderTable()}
		fg := &detailPanel{content: m.renderDetailBox()}
		ov := overlay.New(fg, bgView, overlay.Center, overlay.Center, 0, 0)
		return ov.View()
	case modeEditCollective, modeEditCategory, modeEditAssignment, modeEditDate:
		bgView := &tableView{content: m.renderTable()}
		content := m.renderInlineEditBox()
		if m.mode == modeEditAssignment {
			content = m.renderAssignmentEditBox()
		}
		fg := &detailPanel{content: content}
		ov := overlay.New(fg, bgView, overlay.Center, overlay.Center, 0, 0)
		return ov.View()
	default:
		return m.renderTable()
	}
}

func (m txBrowserModel) renderTable() string {
	var b strings.Builder

	title := "💰 Transactions"
	if m.account != "" {
		title += " · " + m.account
	}
	if m.currency != "" {
		title += " (" + m.currency + ")"
	}
	colName := ""
	if m.selectedCol >= 0 && m.selectedCol < len(m.columns) {
		colName = m.columns[m.selectedCol].Header
	}
	b.WriteString(lipgloss.NewStyle().Bold(true).Render(title))

	// Show selected column + filter info
	if m.filtering {
		filterVal := m.filterStr
		if filterVal == "" {
			filterVal = "…"
		}
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Render(
			fmt.Sprintf("  🔍 %s: %s", colName, filterVal)))
	} else if m.filterStr != "" {
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Render(
			fmt.Sprintf("  🔍 %s: %s", colName, m.filterStr)))
	} else {
		b.WriteString(lipgloss.NewStyle().Faint(true).Render(
			fmt.Sprintf("  [%s]", colName)))
	}
	b.WriteString("\n")

	// Render table and strip the built-in footer (last line)
	rendered := m.table.Render()
	lines := strings.Split(rendered, "\n")
	if len(lines) > 1 {
		lines = lines[:len(lines)-1]
	}
	b.WriteString(strings.Join(lines, "\n"))
	b.WriteString("\n")

	// Custom footer — compute from filtered set
	filteredTxs := m.getFilteredTxs()
	_, cursorY := m.table.GetCursorLocation()
	pageSize := m.height - 8
	if pageSize < 5 {
		pageSize = 20
	}
	currentPage := 1
	totalPages := 1
	if pageSize > 0 && len(filteredTxs) > 0 {
		currentPage = (cursorY / pageSize) + 1
		totalPages = (len(filteredTxs) + pageSize - 1) / pageSize
	}

	var totalIn, totalOut float64
	for _, tx := range filteredTxs {
		amt := math.Abs(txAmount(tx))
		if isEURCurrency(tx.Currency) {
			if tx.IsIncoming() {
				totalIn += amt
			} else if tx.IsOutgoing() {
				totalOut += amt
			}
		}
	}

	countStr := fmt.Sprintf("%d transactions", len(filteredTxs))
	if len(filteredTxs) != len(m.txs) {
		countStr = fmt.Sprintf("%d of %d transactions", len(filteredTxs), len(m.txs))
	}
	footerInfo := fmt.Sprintf("  %s — Page %d/%d — In: %s  Out: %s  Net: %s",
		countStr, currentPage, totalPages,
		styleGreen.Render(fmtEUR(totalIn)),
		styleRed.Render(fmtEUR(totalOut)),
		fmtEURSigned(totalIn-totalOut))
	if selected := m.selectedCount(); selected > 0 {
		footerInfo += fmt.Sprintf(" — %d selected", selected)
	}
	b.WriteString(lipgloss.NewStyle().Faint(true).Render(footerInfo))
	b.WriteString("\n")

	if m.statusText != "" {
		statusStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
		if m.statusError {
			statusStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
		}
		b.WriteString(statusStyle.Render("  " + m.statusText))
		b.WriteString("\n")
	}

	var keys string
	if m.filtering {
		keys = "  Type to filter  [←→] Column  [Esc] Clear  [Enter] Done  [↑↓] Navigate"
	} else {
		keys = "  [x/Space] Select  [A] All  [e] Edit  [c] Category  [C] Collective  [/] Filter  [s] Sort  [Enter] Details  [q] Quit"
	}
	b.WriteString(lipgloss.NewStyle().Faint(true).Render(keys))
	b.WriteString("\n")

	return b.String()
}

func (m txBrowserModel) renderDetailBox() string {
	tx := m.detailTx
	tz := BrusselsTZ()
	t := time.Unix(tx.Timestamp, 0).In(tz)

	bg := lipgloss.Color("235")
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6")).Background(bg).Width(16)

	valueStyle := lipgloss.NewStyle().Background(bg)

	var lines []string
	add := func(label, value string) {
		if value != "" {
			lines = append(lines, fmt.Sprintf("%s %s", labelStyle.Render(label), valueStyle.Render(value)))
		}
	}

	add("Date", t.Format("02/01/2006 15:04"))
	add("Type", tx.Type)
	add("Provider", tx.Provider)
	if tx.Chain != nil {
		add("Chain", *tx.Chain)
	}
	add("Account", tx.AccountName)
	if tx.Account != "" && tx.Account != tx.AccountName {
		add("Address", shortAddr(tx.Account))
	}
	add("Currency", tx.Currency)

	amt := txAmount(*tx)
	if tx.Type == "CREDIT" {
		add("Amount", styleGreen.Render(fmt.Sprintf("+%.2f", math.Abs(amt))))
	} else {
		add("Amount", styleRed.Render(fmt.Sprintf("-%.2f", math.Abs(amt))))
	}
	if tx.Fee > 0 {
		add("Fee", fmt.Sprintf("%.2f", tx.Fee))
	}
	add("Counterparty", txDisplayCounterparty(*tx))
	add("Description", txDisplayDescription(*tx))
	add("Category", txDisplayCategory(*tx))
	add("Collective", txDisplayCollective(*tx))
	if tx.Event != "" {
		add("Event", tx.Event)
	}
	if tx.Application != "" {
		add("Application", tx.Application)
	}
	if app, ok := tx.Metadata["application"]; ok {
		if s, ok := app.(string); ok && s != "" && s != tx.Application {
			add("Application", s)
		}
	}
	if email, ok := tx.Metadata["email"]; ok {
		if s, ok := email.(string); ok && s != "" {
			add("Email", s)
		}
	}
	for k, v := range tx.Metadata {
		if strings.HasPrefix(k, "custom_") {
			if s, ok := v.(string); ok && s != "" {
				add(strings.TrimPrefix(k, "custom_"), s)
			}
		}
	}
	// Show Nostr/custom tags (metadata keys that aren't standard enrichment fields)
	standardKeys := map[string]bool{
		"category": true, "description": true, "application": true,
		"email": true, "paymentLink": true,
		"memo": true, "state": true, "accountSlug": true,
	}
	var tagLines []string
	for _, tag := range tx.Tags {
		if label := formatTransactionTag(tag); label != "" {
			tagLines = append(tagLines, lipgloss.NewStyle().Foreground(lipgloss.Color("5")).Background(bg).Render(label))
		}
	}
	for k, v := range tx.Metadata {
		if standardKeys[k] || strings.HasPrefix(k, "stripe_") || strings.HasPrefix(k, "custom_") {
			continue
		}
		if s, ok := v.(string); ok && s != "" && len(k) > 0 {
			tagLines = append(tagLines, fmt.Sprintf("%s %s",
				labelStyle.Render(k),
				lipgloss.NewStyle().Foreground(lipgloss.Color("5")).Background(bg).Render(s)))
		}
	}
	if len(tagLines) > 0 {
		sort.Strings(tagLines)
		lines = append(lines, "")
		lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("Tags"))
		lines = append(lines, tagLines...)
	}

	add("TX Hash", shortAddr(tx.TxHash))

	lines = append(lines, "")
	lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("[Enter/Esc] Back  [e] Edit  [c] Category  [C] Collective  [d] Date"))

	boxWidth := 56
	if m.width > 20 {
		boxWidth = m.width / 2
		if boxWidth > 70 {
			boxWidth = 70
		}
	}

	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62")).
		Background(lipgloss.Color("235")).
		Padding(1, 2).
		Width(boxWidth).
		Render(lipgloss.NewStyle().Bold(true).Background(bg).Render("Transaction Detail") + "\n\n" + strings.Join(lines, "\n"))
}

func (m txBrowserModel) renderInlineEditBox() string {
	bg := lipgloss.Color("235")
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6")).Background(bg)
	selectedStyle := lipgloss.NewStyle().Background(lipgloss.Color("62")).Foreground(lipgloss.Color("255"))
	optionStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Background(bg)

	var title string
	switch m.mode {
	case modeEditCollective:
		title = "Assign Collective"
	case modeEditCategory:
		title = "Assign Category"
	case modeEditDate:
		title = "Set Accounting Date / Spread"
	}
	if m.bulkEdit {
		title = fmt.Sprintf("%s (%d txs)", title, m.selectedCount())
	}

	var lines []string
	lines = append(lines, lipgloss.NewStyle().Bold(true).Background(bg).Render(title))
	lines = append(lines, "")

	// Input field
	inputDisplay := m.editInput
	if inputDisplay == "" {
		inputDisplay = "…"
	}
	lines = append(lines, labelStyle.Render("> ")+lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Background(bg).Render(inputDisplay+"▎"))

	// Autocomplete options (not for date)
	if m.mode != modeEditDate {
		filtered := m.filteredEditOptions()
		lines = append(lines, "")
		maxShow := 10
		if len(filtered) < maxShow {
			maxShow = len(filtered)
		}
		// Scroll window around cursor
		start := 0
		if m.editCursor >= maxShow {
			start = m.editCursor - maxShow + 1
		}
		end := start + maxShow
		if end > len(filtered) {
			end = len(filtered)
			start = end - maxShow
			if start < 0 {
				start = 0
			}
		}
		for i := start; i < end; i++ {
			if i == m.editCursor {
				lines = append(lines, selectedStyle.Render(" > "+filtered[i]+" "))
			} else {
				lines = append(lines, optionStyle.Render("   "+filtered[i]))
			}
		}
		if len(filtered) == 0 {
			lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("   (new: "+m.editInput+")"))
		}
	} else {
		lines = append(lines, "")
		for _, l := range m.spreadPreviewLines() {
			lines = append(lines, optionStyle.Render(l))
		}
	}

	lines = append(lines, "")
	if m.mode == modeEditDate {
		lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("Forms: 2025 · 2024-2025 · 202401-202506 · 2024-12,2025-03"))
		lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("[Enter] Save  [Esc] Cancel"))
	} else {
		lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("[Enter] Save  [Tab] Complete  [Esc] Cancel"))
	}

	boxWidth := 40
	if m.width > 20 {
		boxWidth = m.width / 3
		if boxWidth < 40 {
			boxWidth = 40
		}
		if boxWidth > 60 {
			boxWidth = 60
		}
	}

	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62")).
		Background(bg).
		Padding(1, 2).
		Width(boxWidth).
		Render(strings.Join(lines, "\n"))
}

func (m txBrowserModel) renderAssignmentEditBox() string {
	bg := lipgloss.Color("235")
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("6")).Background(bg)
	activeStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Background(bg).Bold(true)
	valueStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Background(bg)
	selectedStyle := lipgloss.NewStyle().Background(lipgloss.Color("62")).Foreground(lipgloss.Color("255"))
	optionStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Background(bg)

	title := "Edit Assignment"
	if m.bulkEdit {
		title = fmt.Sprintf("%s (%d txs)", title, m.selectedCount())
	}

	var lines []string
	lines = append(lines, lipgloss.NewStyle().Bold(true).Background(bg).Render(title))
	lines = append(lines, "")

	renderInput := func(index int, label, value string) {
		style := valueStyle
		if index == m.editField {
			style = activeStyle
		}
		if value == "" {
			value = "…"
		}
		cursor := ""
		if index == m.editField {
			cursor = "▎"
		}
		lines = append(lines, labelStyle.Render(label+": ")+style.Render(value+cursor))
	}
	renderInput(0, "Collective", m.editCollectiveInput)
	renderInput(1, "Category", m.editCategoryInput)

	lines = append(lines, "")
	var filtered []string
	cursor := 0
	if m.editField == 0 {
		filtered = m.filteredCollectiveOptions()
		cursor = m.editCollectiveCursor
	} else {
		filtered = m.filteredCategoryOptions()
		cursor = m.editCategoryCursor
	}
	maxShow := 8
	if len(filtered) < maxShow {
		maxShow = len(filtered)
	}
	start := 0
	if cursor >= maxShow {
		start = cursor - maxShow + 1
	}
	end := start + maxShow
	if end > len(filtered) {
		end = len(filtered)
		start = end - maxShow
		if start < 0 {
			start = 0
		}
	}
	for i := start; i < end; i++ {
		if i == cursor {
			lines = append(lines, selectedStyle.Render(" > "+filtered[i]+" "))
		} else {
			lines = append(lines, optionStyle.Render("   "+filtered[i]))
		}
	}
	if len(filtered) == 0 {
		value := m.editCollectiveInput
		if m.editField == 1 {
			value = m.editCategoryInput
		}
		lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("   (new: "+value+")"))
	}

	lines = append(lines, "")
	lines = append(lines, lipgloss.NewStyle().Faint(true).Background(bg).Render("[Enter] Save  [Tab] Switch/Complete  [Esc] Cancel"))

	boxWidth := 46
	if m.width > 20 {
		boxWidth = m.width / 3
		if boxWidth < 46 {
			boxWidth = 46
		}
		if boxWidth > 70 {
			boxWidth = 70
		}
	}

	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62")).
		Background(bg).
		Padding(1, 2).
		Width(boxWidth).
		Render(strings.Join(lines, "\n"))
}

// getFilteredTxs returns the transactions matching the current filter.
// Mirrors the stickers filter logic (case-insensitive substring on column).
func (m txBrowserModel) getFilteredTxs() []TransactionEntry {
	if m.filterStr == "" {
		return m.txs
	}
	filter := strings.ToLower(m.filterStr)
	var result []TransactionEntry
	if m.selectedCol < 0 || m.selectedCol >= len(m.columns) {
		return result
	}
	kind := m.columns[m.selectedCol].Kind
	for _, tx := range m.txs {
		cellValue := transactionCellValue(tx, kind, m.selectedTxIDs, false)
		if strings.Contains(strings.ToLower(cellValue), filter) {
			result = append(result, tx)
		}
	}
	return result
}

// ── Save ──

func saveTransactionUpdate(tx *TransactionEntry) bool {
	dataDir := DataDir()
	t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
	year := fmt.Sprintf("%d", t.Year())
	month := fmt.Sprintf("%02d", t.Month())

	txPath := filepath.Join(dataDir, year, month, "generated", "transactions.json")
	data, err := os.ReadFile(txPath)
	if err != nil {
		return false
	}
	var txFile TransactionsFile
	if json.Unmarshal(data, &txFile) != nil {
		return false
	}
	for i := range txFile.Transactions {
		if txFile.Transactions[i].ID == tx.ID {
			txFile.Transactions[i].Category = tx.Category
			txFile.Transactions[i].Collective = tx.Collective
			txFile.Transactions[i].Spread = tx.Spread
			syncTransactionTags(&txFile.Transactions[i])
			out, _ := json.MarshalIndent(txFile, "", "  ")
			writeMonthFile(dataDir, year, month, filepath.Join("generated", "transactions.json"), out)
			return true
		}
	}
	return false
}

// ── Create rule ──

func createRuleFromBrowser(allTxs []TransactionEntry) {
	em := newRuleEditor(nil, allTxs)
	ep := tea.NewProgram(em, tea.WithAltScreen())
	ep.Run()
}

// ── Command ──

// printTransactionsCSV emits one row per transaction with the same
// columns as the table (date, account, collective, category,
// counterparty, description, amount, currency).
func printTransactionsCSV(txs []TransactionEntry) {
	fmt.Println("date,account,collective,category,counterparty,description,amount,currency")
	tz := BrusselsTZ()
	for _, tx := range txs {
		date := time.Unix(tx.Timestamp, 0).In(tz).Format("2006-01-02")
		amt := txAmount(tx)
		if tx.IsOutgoing() {
			amt = -math.Abs(amt)
		} else {
			amt = math.Abs(amt)
		}
		fmt.Printf("%s,%s,%s,%s,%s,%s,%.2f,%s\n",
			csvCell(date),
			csvCell(txSource(tx)),
			csvCell(txDisplayCollective(tx)),
			csvCell(txDisplayCategory(tx)),
			csvCell(txDisplayCounterparty(tx)),
			csvCell(txDisplayDescription(tx)),
			amt,
			csvCell(tx.Currency),
		)
	}
}

// printTransactionsTable renders a non-interactive, plain-text view to
// stdout — the default output for `chb transactions`. The layout is:
//
//   1. one-line per-currency totals (in / out / fees / net)
//   2. per-category gross breakdown
//   3. the actual rows table
//
// Putting the summary on top means an operator scanning a wide period can
// answer "how much did we make this quarter?" without scrolling past
// hundreds of rows. The breakdown is the natural drill-down before going
// row by row.
func printTransactionsTable(filter TxFilter, txs []TransactionEntry) {
	if len(txs) == 0 {
		fmt.Printf("\n%sNo transactions match the filter%s\n\n", Fmt.Dim, Fmt.Reset)
		return
	}

	// Aggregate first so the header has totals + category breakdown.
	// pos/neg track gross flow (signed gross from txAmount); fees track
	// what providers deducted on top. "net" in the header = pos + neg -
	// fees, i.e. the actual balance impact — computed inline below so
	// "net" stays honest after switching txAmount to return gross.
	type sums struct{ pos, neg, fees float64 }
	byCur := map[string]*sums{}
	curOrder := []string{}
	// Category breakdown bucket: (currency, category) → signed gross sum.
	// Tracked per-currency so multi-currency views don't conflate amounts;
	// for the common single-currency case it collapses to one row each.
	type catSum struct {
		category string
		currency string
		gross    float64
		n        int
	}
	catBuckets := map[string]*catSum{} // key: currency + "|" + category
	catOrder := []string{}

	for _, tx := range txs {
		c := tx.Currency
		if c == "" {
			c = "EUR"
		}
		s, ok := byCur[c]
		if !ok {
			s = &sums{}
			byCur[c] = s
			curOrder = append(curOrder, c)
		}
		amt := txAmount(tx) // signed gross
		if amt >= 0 {
			s.pos += amt
		} else {
			s.neg += amt
		}
		s.fees += txFee(tx)

		cat := txDisplayCategory(tx)
		if cat == "" || cat == "—" {
			cat = "(uncategorised)"
		}
		key := c + "|" + cat
		b, ok := catBuckets[key]
		if !ok {
			b = &catSum{category: cat, currency: c}
			catBuckets[key] = b
			catOrder = append(catOrder, key)
		}
		b.gross += amt
		b.n++
	}

	// ── 1. Header: totals per currency ──
	fmt.Println()
	for _, c := range curOrder {
		s := byCur[c]
		label := "Totals"
		if len(curOrder) > 1 {
			label = c
		}
		feesPart := ""
		if s.fees > 0 {
			feesPart = fmt.Sprintf("   fees %s%s%s", Fmt.Yellow, formatBalancePlain(s.fees, c), Fmt.Reset)
		}
		netAfterFees := s.pos + s.neg - s.fees
		fmt.Printf("  %s%s%s  %s%d txs%s   in %s%s%s   out %s%s%s%s   net %s%s%s\n",
			Fmt.Bold, label, Fmt.Reset,
			Fmt.Dim, len(txs), Fmt.Reset,
			Fmt.Green, formatBalancePlain(s.pos, c), Fmt.Reset,
			Fmt.Red, formatBalancePlain(s.neg, c), Fmt.Reset,
			feesPart,
			Fmt.Bold, formatBalancePlain(netAfterFees, c), Fmt.Reset,
		)
	}

	// ── 2. Per-category gross breakdown ──
	// Sort categories within each currency by descending |gross| so the
	// biggest movers float to the top of the list.
	sort.SliceStable(catOrder, func(i, j int) bool {
		ai, bi := catBuckets[catOrder[i]], catBuckets[catOrder[j]]
		if ai.currency != bi.currency {
			// Preserve currency grouping by first-seen order.
			for _, c := range curOrder {
				if c == ai.currency {
					return true
				}
				if c == bi.currency {
					return false
				}
			}
		}
		return math.Abs(ai.gross) > math.Abs(bi.gross)
	})
	if len(catOrder) > 0 {
		fmt.Printf("\n  %sBy category%s\n", Fmt.Dim, Fmt.Reset)
		maxNameW := 14
		for _, k := range catOrder {
			if w := displayWidth(catBuckets[k].category); w > maxNameW {
				maxNameW = w
			}
		}
		if maxNameW > 28 {
			maxNameW = 28
		}
		for _, k := range catOrder {
			b := catBuckets[k]
			name := Truncate(b.category, maxNameW)
			line := fmt.Sprintf("    %s  %4d  %s",
				padRight(name, maxNameW),
				b.n,
				padLeft(formatBalancePlain(b.gross, b.currency), 14),
			)
			if len(curOrder) > 1 {
				line += "  " + Fmt.Dim + b.currency + Fmt.Reset
			}
			fmt.Println(line)
		}
	}
	fmt.Println()

	// ── 3. The table itself ──
	showAccountColumn := filter.AccountSlug == ""
	cols := transactionTableColumns(showAccountColumn, false)
	headers := txColumnHeaders(cols)
	rightAlign := map[int]bool{}
	for i, col := range cols {
		if col.Kind == txColumnAmount {
			rightAlign[i] = true
		}
	}
	rows := make([][]string, 0, len(txs))
	for _, tx := range txs {
		row := make([]string, len(cols))
		for i, col := range cols {
			cell := transactionCellValue(tx, col.Kind, nil, false)
			switch col.Kind {
			case txColumnCounterparty:
				cell = Truncate(cell, 28)
			case txColumnDescription:
				cell = Truncate(cell, 40)
			case txColumnCollective, txColumnCategory:
				cell = Truncate(cell, 14)
			case txColumnSource:
				cell = Truncate(cell, 14)
			case txColumnReconciled:
				cell = Truncate(cell, 20)
			}
			row[i] = cell
		}
		rows = append(rows, row)
	}
	totalRow := make([]string, len(cols))
	totalRow[0] = Pluralize(len(txs), "transaction", "")
	if amtIdx := txColumnIndex(cols, txColumnAmount); amtIdx >= 0 && len(curOrder) == 1 {
		s := byCur[curOrder[0]]
		totalRow[amtIdx] = formatBalancePlain(s.pos+s.neg-s.fees, curOrder[0])
	}
	renderTicketsTable(headers, rows, totalRow, rightAlign)
}

func TransactionsBrowser(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		printTransactionsBrowserHelp()
		return
	}

	filter, n, skip, err := parseTxListFlags(args)
	if err != nil {
		if JSONMode(args) {
			EmitJSONError(err)
			os.Exit(1)
		}
		fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
		os.Exit(1)
	}

	if JSONMode(args) {
		emitTransactionsJSON(filter, n, skip, HasFlag(args, "--with-pii"))
		return
	}

	if HasFlag(args, "--csv") {
		txs := applyOffsetLimit(loadFilteredTransactions(filter), skip, n)
		printTransactionsCSV(txs)
		return
	}

	interactive := HasFlag(args, "-i", "--interactive")
	if !interactive {
		txs := applyOffsetLimit(loadFilteredTransactions(filter), skip, n)
		printTransactionsTable(filter, txs)
		return
	}

	fmt.Printf("  Loading transactions...\n")
	txs := applyOffsetLimit(loadFilteredTransactions(filter), skip, n)
	showAccountColumn := filter.AccountSlug == ""
	columns := transactionTableColumns(showAccountColumn, true)

	for {
		selected := map[string]bool{}
		t := newStickerTableForColumns(txs, 0, 0, columns, selected)
		selectedCol := txColumnIndex(columns, txColumnDate)
		if selectedCol < 0 {
			selectedCol = 0
		}

		m := txBrowserModel{
			table:         t,
			columns:       columns,
			txs:           txs,
			currency:      filter.Currency,
			account:       filter.AccountSlug,
			selectedCol:   selectedCol,
			sortCol:       -1,
			selectedTxIDs: selected,
		}

		p := tea.NewProgram(m, tea.WithAltScreen())
		result, err := p.Run()
		if err != nil {
			fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
			return
		}
		fm := result.(txBrowserModel)
		txs = fm.txs

		switch fm.action {
		case browserQuit:
			return
		case browserCreateRule:
			createRuleFromBrowser(txs)
			txs = applyOffsetLimit(loadFilteredTransactions(filter), skip, n)
		}
	}
}

// parseTxListFlags reads the shared filter flags used by both the interactive
// browser and the JSON listing. A negative limit means "no limit". A bare
// positional currency code (EUR/EURE/EURB/CHT) is still accepted as a
// shorthand for --currency to keep prior muscle memory working.
func parseTxListFlags(args []string) (TxFilter, int, int, error) {
	f := TxFilter{
		AccountSlug: GetOption(args, "--account"),
		Currency:    strings.ToUpper(GetOption(args, "--currency")),
	}
	for _, spec := range GetOptions(args, "--tag") {
		tag, ok := parseTransactionTagSpec(spec)
		if !ok {
			return f, 0, 0, fmt.Errorf("invalid --tag value %q", spec)
		}
		f.Tags = append(f.Tags, tag)
	}
	for _, specs := range GetOptions(args, "--tags") {
		for _, spec := range strings.Split(specs, ",") {
			spec = strings.TrimSpace(spec)
			if spec == "" {
				continue
			}
			tag, ok := parseTransactionTagSpec(spec)
			if !ok {
				return f, 0, 0, fmt.Errorf("invalid --tags value %q", spec)
			}
			f.Tags = append(f.Tags, tag)
		}
	}
	// Single-value tag filters (AND across keys, exact match on value).
	for _, alias := range []struct {
		flag string
		key  string
	}{
		{"--event", "event"},
		{"--application", "application"},
		{"--payment-link", "paymentLink"},
	} {
		if value := GetOption(args, alias.flag); value != "" {
			addTransactionTag(&f.Tags, alias.key, value)
		}
	}
	// Multi-value tag filters (OR within the key — comma-separated values).
	// Example: --category membership,donation,ticket matches any tx whose
	// category is one of those. This makes the common "show me revenue txs
	// across a few related categories" case a one-liner.
	for _, alias := range []struct {
		flag string
		key  string
	}{
		{"--category", "category"},
		{"--collective", "collective"},
	} {
		raw := GetOption(args, alias.flag)
		if raw == "" {
			continue
		}
		values := []string{}
		for _, v := range strings.Split(raw, ",") {
			v = strings.TrimSpace(v)
			if v != "" {
				values = append(values, v)
			}
		}
		if len(values) == 1 {
			addTransactionTag(&f.Tags, alias.key, values[0])
		} else if len(values) > 1 {
			group := make([][]string, 0, len(values))
			for _, v := range values {
				group = append(group, []string{alias.key, v})
			}
			f.AnyTags = append(f.AnyTags, group)
		}
	}
	// --search: case-insensitive substring match against counterparty,
	// description, payment ref, and display-friendly variants. Repeatable;
	// terms within a single --search are AND'd (all substrings must appear),
	// repeated --search flags are OR'd (any matches). Keeps the common cases
	// — "show me everything mentioning openletter" and "show me txs
	// matching 'membership OR donation'" — both spellable.
	for _, term := range GetOptions(args, "--search") {
		if t := strings.TrimSpace(term); t != "" {
			f.SearchAny = append(f.SearchAny, strings.ToLower(t))
		}
	}
	// --description: narrower than --search. Matches case-insensitive
	// against just the description field, ignoring counterparty / tags /
	// payment-ref noise that --search also scans. Repeatable.
	for _, term := range GetOptions(args, "--description") {
		if t := strings.TrimSpace(term); t != "" {
			f.DescriptionAny = append(f.DescriptionAny, strings.ToLower(t))
		}
	}

	if f.Currency == "" {
		for _, a := range args {
			if strings.HasPrefix(a, "-") {
				continue
			}
			upper := strings.ToUpper(a)
			if upper == "EUR" || upper == "EURE" || upper == "EURB" || upper == "CHT" {
				f.Currency = upper
				break
			}
		}
	}

	if s := GetOption(args, "--amount"); s != "" {
		af, err := parseAmountFilter(s)
		if err != nil {
			return f, 0, 0, err
		}
		f.Amount = af
	}
	if s := strings.ToLower(strings.TrimSpace(GetOption(args, "--direction"))); s != "" {
		switch s {
		case "in", "incoming", "credit", "+":
			f.Direction = "in"
		case "out", "outgoing", "debit", "-":
			f.Direction = "out"
		default:
			return f, 0, 0, fmt.Errorf("invalid --direction value %q (expected: in, out)", s)
		}
	}
	f.NoCategory = HasFlag(args, "--no-category")
	f.NoCollective = HasFlag(args, "--no-collective")
	// --daterange sets Since/Until from any spec ParseDateRangeSpec accepts
	// (2025/Q1, 2026, 20260101-20260331, etc.). Applied before --since /
	// --until so those still win when given alongside, matching the usual
	// "broad flag first, narrow flag overrides" precedence.
	if s := GetOption(args, "--daterange"); s != "" {
		spec, ok := ParseDateRangeSpec(s)
		if !ok {
			return f, 0, 0, fmt.Errorf("invalid --daterange value %q (expected %s)", s, DateRangeFormatHelp)
		}
		f.Since = spec.Start
		f.Until = spec.End.Add(-time.Second)
	}
	if s := GetOption(args, "--since"); s != "" {
		t, ok := ParseSinceDate(s)
		if !ok {
			return f, 0, 0, fmt.Errorf("invalid --since value %q (expected %s)", s, DateFormatHelp)
		}
		f.Since = t
	}
	if s := GetOption(args, "--until"); s != "" {
		t, ok := ParseDateEndExclusive(s)
		if !ok {
			return f, 0, 0, fmt.Errorf("invalid --until value %q (expected %s)", s, DateFormatHelp)
		}
		// --until is inclusive: use the last second before the parsed date ends.
		f.Until = t.Add(-time.Second)
	}

	limit := GetNumber(args, []string{"-n", "--limit"}, -1)
	skip := GetNumber(args, []string{"--skip"}, 0)
	if skip < 0 {
		skip = 0
	}
	return f, limit, skip, nil
}

func applyOffsetLimit(txs []TransactionEntry, skip, limit int) []TransactionEntry {
	if skip > 0 {
		if skip >= len(txs) {
			return nil
		}
		txs = txs[skip:]
	}
	if limit >= 0 && limit < len(txs) {
		txs = txs[:limit]
	}
	return txs
}

func emitTransactionsJSON(f TxFilter, limit, skip int, includePII bool) {
	loader := loadPublicFilteredTransactions
	if includePII {
		loader = loadFilteredTransactions
	}
	txs := applyOffsetLimit(loader(f), skip, limit)

	// One TX per line (JSONL). Each line carries the TransactionEntry
	// fields plus a computed uniqueImportId so a downstream tool can
	// look up the matching Odoo bank.statement.line directly. The
	// uniqueImportId requires the AccountConfig of the tx's account,
	// which we look up by AccountSlug — building once per slug is much
	// cheaper than per-tx for large result sets.
	// Build the lookup index once. tx.AccountSlug carries different
	// shapes per provider in the public file: Stripe writes the
	// acct_… id, Etherscan / Monerium write the wallet address, KBC
	// writes the IBAN. Index every identifying token so the lookup
	// hits regardless of provider. Lowercased keys handle case
	// variations between providers.
	accCache := map[string]*AccountConfig{}
	allAccounts := LoadAccountConfigs()
	for i := range allAccounts {
		a := &allAccounts[i]
		for _, key := range []string{a.Slug, a.AccountID, a.Address, a.IBAN} {
			if k := strings.ToLower(strings.TrimSpace(key)); k != "" {
				accCache[k] = a
			}
		}
	}
	resolveAcc := func(slug string) *AccountConfig {
		if slug == "" {
			return nil
		}
		return accCache[strings.ToLower(strings.TrimSpace(slug))]
	}

	for _, tx := range txs {
		envelope := txJSONEnvelope(tx, resolveAcc(tx.AccountSlug))
		_ = EmitJSONL(envelope)
	}
}

// txJSONEnvelope marshals a TransactionEntry plus enriched fields
// useful to downstream tools (odoo-cli, jq pipelines) into a single
// JSON object. The TX struct embeds via json.RawMessage round-trip
// so its existing JSON tags + custom UnmarshalJSON logic stay
// authoritative.
func txJSONEnvelope(tx TransactionEntry, acc *AccountConfig) map[string]interface{} {
	raw, err := json.Marshal(tx)
	if err != nil {
		return map[string]interface{}{"id": tx.ID, "error": err.Error()}
	}
	out := map[string]interface{}{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]interface{}{"id": tx.ID, "error": err.Error()}
	}

	// Computed: uniqueImportId — the Odoo-side identifier this tx was
	// uploaded with (account.bank.statement.line.unique_import_id).
	// Downstream tools use this to locate the matching bank line in
	// Odoo without round-tripping through (date, amount, partner)
	// heuristics that can collide on duplicates.
	if acc != nil {
		if uid := buildUniqueImportID(acc, tx); uid != "" {
			out["uniqueImportId"] = uid
		}
	}

	// Computed: display.description — what `chb transactions`'
	// table would show in the Description column, after the
	// memo / counterparty fallback. Lets downstream tools render
	// matching UI without re-implementing the chain.
	if desc := txDisplayDescription(tx); desc != "" {
		out["displayDescription"] = desc
	}
	return out
}

func printTransactionsBrowserHelp() {
	f := Fmt
	fmt.Printf(`
%schb transactions%s [filters] — List transactions (table by default, TUI with -i)

%sUSAGE%s
  %schb transactions%s                                  Print all transactions as a table
  %schb transactions -i%s                               Open the interactive browser
  %schb transactions --account savings%s                Filter to one account
  %schb transactions --application luma%s                Filter to Luma transactions
  %schb transactions --event evt-2gc6B12TEyRNRqN%s       Filter to one event
  %schb transactions --tag '#color:red'%s                Filter by Nostr-style tag
  %schb transactions --currency EUR%s                   Filter to EUR-family transactions
  %schb transactions --daterange 2026/Q1%s              Filter to any range Parse­DateRangeSpec accepts
  %schb transactions --since 20260101 --until 20260131%s   Date range (inclusive)
  %schb transactions -n 50 --skip 100%s                 Paginate
  %schb transactions ... | odoo attach <ref>%s          Pipe matches into odoo-cli
  %schb transactions --json%s                           Force JSONL (auto when piped)
  %schb transactions --text%s                           Force pretty table (auto when TTY)
  %schb transactions stats%s                            Show transaction statistics

%sFILTERS%s
  %s--account <slug>%s     Limit to one account (e.g. savings, stripe-asbl)
  %s--currency <CODE>%s    EUR (matches the EUR family), CHT, etc.
  %s--category <a,b,…>%s   Match category (comma-separated → any-of)
  %s--collective <a,b,…>%s Match collective (comma-separated → any-of)
  %s--search <text>%s      Substring match against counterparty, description,
                       payment ref (repeatable → any-of)
  %s--description <text>%s Substring match against the description field only
                       (case-insensitive, repeatable → any-of)
  %s--amount <expr>%s     Filter by absolute gross magnitude. Supports
                       "100", ">10", "<1000.40", ">=50", "<=200"
                       (quote the operator to escape shell redirection)
  %s--direction <in|out>%s Match only incoming (positive) or outgoing
                       (negative) transactions
  %s--no-category%s       Match only transactions with no category set
  %s--no-collective%s     Match only transactions with no collective set
  %s--event <id>%s         Match tag ["event", id]
  %s--application <slug>%s Match tag ["application", slug]
  %s--payment-link <id>%s  Match tag ["paymentLink", id]
  %s--tag <spec>%s         Match #tag, #key:value, or #[key:long value]
  %s--tags <a,b>%s         Match several tag specs
  %s--daterange <spec>%s   Convenience for --since/--until: any spec parsed by
                       chb's date-range parser (2025, 2025/Q1, 2026/03,
                       20260101-20260331, 2025/H1, …). Overridden by
                       --since/--until when both are passed.
  %s--since <date>%s       Inclusive lower bound on transaction date
  %s--until <date>%s       Inclusive upper bound on transaction date
  %s-n N%s                 Limit to N transactions (most recent first)
  %s--skip N%s             Skip the first N matches before applying -n
  %s--json%s               Force JSONL (one TX per line) on stdout. Auto-enabled
                       when stdout isn't a TTY (i.e. piped or redirected). Each
                       record includes a computed uniqueImportId so downstream
                       tools (e.g. 'odoo attach') can find the matching Odoo
                       bank.statement.line without heuristics. Use --text to
                       force the table format despite being piped.
  %s--text%s               Force the pretty table even when stdout is piped.
                       Useful for 'chb transactions ... --text | less'.
  %s--with-pii%s           With --json, merge private enrichment into results

%sINTERACTIVE KEYS%s
  %s↑↓/jk%s       Navigate rows
  %sx/Space%s     Toggle row selection
  %sA%s           Toggle all rows in current view
  %s←→%s          Select column (for filter/sort)
  %sPgUp/PgDn%s   Scroll page
  %s/%s           Filter on selected column
  %ss%s           Sort by selected column
  %se%s           Edit category + collective
  %sc%s           Assign category
  %sC%s           Assign collective
  %sd%s           Set accounting date
  %sr%s           Create categorization rule
  %sEnter%s       Show transaction details
  %sEsc%s         Clear filter / Back
  %sq%s           Quit
`,
		f.Bold, f.Reset, // heading
		f.Bold, f.Reset, // USAGE
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset, // --daterange example
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset, // | odoo attach
		f.Cyan, f.Reset, // --json (new doc line)
		f.Cyan, f.Reset, // --text
		f.Bold, f.Reset, // FILTERS
		f.Yellow, f.Reset, // --account
		f.Yellow, f.Reset, // --currency
		f.Yellow, f.Reset, // --category
		f.Yellow, f.Reset, // --collective
		f.Yellow, f.Reset, // --search
		f.Yellow, f.Reset, // --description
		f.Yellow, f.Reset, // --amount
		f.Yellow, f.Reset, // --direction
		f.Yellow, f.Reset, // --no-category
		f.Yellow, f.Reset, // --no-collective
		f.Yellow, f.Reset, // --event
		f.Yellow, f.Reset, // --application
		f.Yellow, f.Reset, // --payment-link
		f.Yellow, f.Reset, // --tag
		f.Yellow, f.Reset, // --tags
		f.Yellow, f.Reset, // --daterange
		f.Yellow, f.Reset, // --since
		f.Yellow, f.Reset, // --until
		f.Yellow, f.Reset, // -n
		f.Yellow, f.Reset, // --skip
		f.Yellow, f.Reset, // --json
		f.Yellow, f.Reset, // --text
		f.Yellow, f.Reset, // --with-pii
		f.Bold, f.Reset, // INTERACTIVE KEYS
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
