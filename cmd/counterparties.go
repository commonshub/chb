package cmd

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const counterpartiesDefaultLimit = 30

// Vendors lists EUR/EUR* counterparties WE paid (outgoing txs), ordered
// by total volume DESC.
func Vendors(args []string) {
	runCounterpartyView(args, "vendors")
}

// Customers lists EUR/EUR* counterparties WHO paid US (incoming txs).
func Customers(args []string) {
	runCounterpartyView(args, "customers")
}

func runCounterpartyView(args []string, kind string) {
	if HasFlag(args, "--help", "-h", "help") {
		printCounterpartiesHelp(kind)
		return
	}
	csv := HasFlag(args, "--csv")
	interactive := HasFlag(args, "-i", "--interactive")
	all := HasFlag(args, "--all")
	noCategory := HasFlag(args, "--no-category")
	noCollective := HasFlag(args, "--no-collective")
	limit := GetNumber(args, []string{"-n", "--limit"}, counterpartiesDefaultLimit)
	posYear, posMonth, _ := ParseYearMonthArg(args)

	direction := "out"
	if kind == "customers" {
		direction = "in"
	}

	settings, _ := LoadSettings()
	cat := NewCategorizer(settings)

	txs := loadCounterpartyTxs(direction, posYear, posMonth, cat)
	aggregates := aggregateCounterparties(txs)
	if noCategory || noCollective {
		// Keep only partners whose tx population still lacks the picked
		// dimension(s). Uses the majority Category/Collective the
		// aggregator already computed — partners with even one categorized
		// tx fall out, which is what the operator wants when shopping for
		// "review-me" candidates.
		filtered := aggregates[:0]
		for _, a := range aggregates {
			if noCategory && a.Category != "" {
				continue
			}
			if noCollective && a.Collective != "" {
				continue
			}
			filtered = append(filtered, a)
		}
		aggregates = filtered
	}
	sort.Slice(aggregates, func(i, j int) bool {
		return aggregates[i].Volume > aggregates[j].Volume
	})

	if !all && !interactive && limit > 0 && len(aggregates) > limit {
		aggregates = aggregates[:limit]
	}

	switch {
	case csv:
		printCounterpartiesCSV(direction, aggregates)
	case interactive:
		runCounterpartiesTUI(kind, direction, counterpartiesScopeLabel(posYear, posMonth), aggregates)
	default:
		printCounterpartiesTable(kind, posYear, posMonth, aggregates, direction)
	}
}

func counterpartiesScopeLabel(posYear, posMonth string) string {
	if posYear == "" {
		return "all time"
	}
	if posMonth == "" {
		return posYear
	}
	return posYear + "/" + posMonth
}

type counterpartyAgg struct {
	URI          string  // canonical URI (iban:..., stripe:cus_..., ethereum:100:address:0x...)
	Name         string  // best-known display name (from PII enrichment)
	Identifier   string  // human display: IBAN, cus_..., 0x... — what the user'd put in chb rules add
	TxCount      int
	Volume       float64 // sum of |amount|
	Collective   string  // most-common assigned collective across this counterparty's txs
	Category     string  // most-common assigned category
	RecentTxs    []TransactionEntry // up to counterpartyRecentTxLimit, sorted timestamp DESC
}

// counterpartyRecentTxLimit caps how many recent txs we stash per
// counterparty for the TUI detail view. Aggregation passes never look
// at this list — it's purely for display when the user presses
// [enter] on a row.
const counterpartyRecentTxLimit = 20

func loadCounterpartyTxs(direction, posYear, posMonth string, cat *Categorizer) []TransactionEntry {
	dataDir := DataDir()
	var out []TransactionEntry

	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		if posYear != "" && yd.Name() != posYear {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			if posMonth != "" && md.Name() != posMonth {
				continue
			}

			f := LoadTransactionsWithPII(dataDir, yd.Name(), md.Name())
			if f == nil {
				continue
			}
			for _, tx := range f.Transactions {
				if !isEURCurrency(tx.Currency) {
					continue
				}
				if tx.Type == "INTERNAL" {
					continue
				}
				isIn := tx.IsIncoming()
				isOut := tx.IsOutgoing()
				if direction == "in" && !isIn {
					continue
				}
				if direction == "out" && !isOut {
					continue
				}
				cat.Apply(&tx)
				out = append(out, tx)
			}
		}
	}
	return out
}

// canonicalCounterpartyURI returns the most-stable identifier we have
// for a tx's counterparty. Preference:
//
//   - IBAN (from PII enrichment) — survives across providers
//   - Existing tx.CounterpartyID — stripe:cus_..., ethereum:CHAIN:address:0x...
//
// Token-contract addresses (Monerium's internal EURb/EURe contracts) are
// skipped — those are not the real counterparty.
func canonicalCounterpartyURI(tx TransactionEntry) string {
	if iban, ok := tx.Metadata["iban"].(string); ok {
		if normalized := normalizeIBAN(iban); normalized != "" {
			return "iban:" + normalized
		}
	}
	cp := tx.CounterpartyID
	if strings.HasPrefix(cp, "ethereum:") && strings.Contains(cp, ":token:") {
		return ""
	}
	return cp
}

func aggregateCounterparties(txs []TransactionEntry) []counterpartyAgg {
	byURI := map[string]*counterpartyAgg{}
	catCounts := map[string]map[string]int{}
	collCounts := map[string]map[string]int{}

	for _, tx := range txs {
		uri := canonicalCounterpartyURI(tx)
		if uri == "" {
			continue
		}
		a := byURI[uri]
		if a == nil {
			a = &counterpartyAgg{URI: uri, Identifier: displayCounterpartyIdentifier(uri)}
			byURI[uri] = a
			catCounts[uri] = map[string]int{}
			collCounts[uri] = map[string]int{}
		}
		if a.Name == "" && tx.Counterparty != "" && !strings.HasPrefix(tx.Counterparty, "0x") {
			a.Name = tx.Counterparty
		}
		a.TxCount++
		a.Volume += math.Abs(counterpartyTxAmount(tx))
		a.RecentTxs = append(a.RecentTxs, tx)
		if c := txDisplayCategory(tx); c != "" {
			catCounts[uri][c]++
		}
		if c := txDisplayCollective(tx); c != "" {
			collCounts[uri][c]++
		}
	}

	for uri, a := range byURI {
		a.Category = majorityValue(catCounts[uri])
		a.Collective = majorityValue(collCounts[uri])
		// Keep only the latest N txs, sorted timestamp DESC.
		sort.Slice(a.RecentTxs, func(i, j int) bool {
			return a.RecentTxs[i].Timestamp > a.RecentTxs[j].Timestamp
		})
		if len(a.RecentTxs) > counterpartyRecentTxLimit {
			a.RecentTxs = a.RecentTxs[:counterpartyRecentTxLimit]
		}
	}
	out := make([]counterpartyAgg, 0, len(byURI))
	for _, a := range byURI {
		out = append(out, *a)
	}
	return out
}

func counterpartyTxAmount(tx TransactionEntry) float64 {
	if tx.NormalizedAmount != 0 {
		return tx.NormalizedAmount
	}
	if tx.GrossAmount != 0 {
		return tx.GrossAmount
	}
	return tx.Amount
}

func majorityValue(counts map[string]int) string {
	best := ""
	bestN := 0
	for k, n := range counts {
		if n > bestN {
			best = k
			bestN = n
		}
	}
	return best
}

// displayCounterpartyIdentifier renders a URI as the friendly identifier
// the user would paste into `chb rules add` — machine form, no spaces,
// so it copy-pastes verbatim.
func displayCounterpartyIdentifier(uri string) string {
	switch {
	case strings.HasPrefix(uri, "iban:"):
		return strings.TrimPrefix(uri, "iban:")
	case strings.HasPrefix(uri, "stripe:"):
		return strings.TrimPrefix(uri, "stripe:")
	case strings.HasPrefix(uri, "ethereum:"):
		parts := strings.Split(uri, ":")
		return parts[len(parts)-1]
	}
	return uri
}

// titleASCII upper-cases the first byte of an ASCII string. We only use
// it on hardcoded "vendors" / "customers" so the byte-wise version is
// safe and avoids the deprecated strings.Title.
func titleASCII(s string) string {
	if s == "" {
		return s
	}
	first := s[0]
	if first >= 'a' && first <= 'z' {
		first -= 'a' - 'A'
	}
	return string(first) + s[1:]
}

func printCounterpartiesTable(kind, posYear, posMonth string, aggregates []counterpartyAgg, direction string) {
	icon := "💸"
	if kind == "customers" {
		icon = "💰"
	}
	scope := "all time"
	if posYear != "" {
		scope = posYear
		if posMonth != "" {
			scope += "/" + posMonth
		}
	}
	fmt.Printf("\n%s%s %s — %s%s\n\n", Fmt.Bold, icon, titleASCII(kind), scope, Fmt.Reset)

	if len(aggregates) == 0 {
		fmt.Printf("  %sNo %s with EUR-family transactions in %s.%s\n\n", Fmt.Dim, kind, scope, Fmt.Reset)
		return
	}

	headers := []string{"Name / identifier", "# txs", "Volume", "Collective", "Category"}
	rightAlign := map[int]bool{1: true, 2: true}

	rows := make([][]string, 0, len(aggregates))
	var totalTx int
	var totalVolume float64
	for _, a := range aggregates {
		label := a.Name
		if label == "" {
			label = a.Identifier
		}
		label = Truncate(label, 38)
		if a.Name != "" {
			label += "  " + Fmt.Dim + Truncate(a.Identifier, 30) + Fmt.Reset
		}
		rows = append(rows, []string{
			label,
			fmt.Sprintf("%d", a.TxCount),
			fmtEUR(a.Volume),
			a.Collective,
			a.Category,
		})
		totalTx += a.TxCount
		totalVolume += a.Volume
	}

	totalRow := []string{
		Pluralize(len(aggregates), kind[:len(kind)-1], "") + " — total",
		fmt.Sprintf("%d", totalTx),
		fmtEUR(totalVolume),
		"",
		"",
	}
	renderTicketsTable(headers, rows, totalRow, rightAlign)

	// Footer: command shape for adding a rule.
	dirFlag := ""
	exampleID := "BE31891874100655"
	if direction == "in" {
		dirFlag = " --direction=in"
		exampleID = "cus_QbkYjHh3CFevdN"
	} else {
		dirFlag = " --direction=out"
		exampleID = "0x6fdf0aae33e313d9c98d2aa19bcd8ef777912cbf"
	}
	fmt.Printf("%sTo assign a collective/category — adds a rule so future txs are tagged automatically:%s\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("  chb rules add <identifier> --collective=<slug> --category=<slug>%s\n", dirFlag)
	fmt.Printf("  %se.g. chb rules add %s --collective=commonshub --category=fridge%s%s\n\n",
		Fmt.Dim, exampleID, dirFlag, Fmt.Reset)
}

func printCounterpartiesCSV(direction string, aggregates []counterpartyAgg) {
	fmt.Println("name,identifier,uri,txs,volume,collective,category")
	for _, a := range aggregates {
		fmt.Printf("%s,%s,%s,%d,%.2f,%s,%s\n",
			csvCell(a.Name),
			csvCell(a.Identifier),
			csvCell(a.URI),
			a.TxCount,
			a.Volume,
			csvCell(a.Collective),
			csvCell(a.Category),
		)
	}
}

func printCounterpartiesHelp(kind string) {
	f := Fmt
	verb := "we paid (DEBIT side)"
	if kind == "customers" {
		verb = "who paid us (CREDIT side)"
	}
	fmt.Printf(`
%schb %s%s — EUR-family counterparties %s, sorted by volume DESC

%sUSAGE%s
  %schb %s%s              All time
  %schb %s%s 2025         For the year 2025
  %schb %s%s 2025/12      For December 2025

%sCOLUMNS%s
  Name / identifier  Display name (when known) + IBAN / cus_… / 0x…
  # txs              Number of EUR-family txs in scope
  Volume             Total absolute amount
  Collective         Majority-assigned collective (via chb rules)
  Category           Majority-assigned category (via chb rules)

%sOPTIONS%s
  %s-i%s, %s--interactive%s    Open a TUI: arrow keys to move, [space] to select,
                       [e] to add a categorization rule for the selection
  %s--no-category%s        Keep only counterparties whose txs have no category
  %s--no-collective%s      Keep only counterparties whose txs have no collective
  %s-n%s <N>, %s--limit%s <N>   Limit output rows (default %d, use --all to show all)
  %s--all%s                Show every counterparty
  %s--csv%s                Output CSV instead of a formatted table
  %s--help, -h%s           Show this help
`,
		f.Bold, kind, f.Reset, verb,
		f.Bold, f.Reset,
		f.Cyan, kind, f.Reset,
		f.Cyan, kind, f.Reset,
		f.Cyan, kind, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset, // -i, --interactive
		f.Yellow, f.Reset,                    // --no-category
		f.Yellow, f.Reset,                    // --no-collective
		f.Yellow, f.Reset, f.Yellow, f.Reset, counterpartiesDefaultLimit,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
