package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	nostrsource "github.com/CommonsHub/chb/providers/nostr"
)

// structuredPaymentRefPattern matches a Belgian OGM structured reference
// (`+++NNN/NNNN/NNNNN+++`) and similar digits-and-slashes-only strings
// that Odoo sometimes uses as the move title. These references are
// unique IDs but tell the human reader nothing about what the move was
// for — so we fall back to the first useful line-item title when the
// move title looks like one of these.
var structuredPaymentRefPattern = regexp.MustCompile(`^\+*[\d/.\s-]+\+*$`)

// moveDescription returns the most informative one-line description for
// a move: the title, unless it's an opaque payment reference, in which
// case the first non-empty line-item title (or product name) takes
// over. Empty string when neither yields anything readable.
func moveDescription(m OdooOutgoingInvoicePublic) string {
	title := strings.TrimSpace(m.Title)
	if title != "" && !structuredPaymentRefPattern.MatchString(title) {
		return title
	}
	for _, li := range m.LineItems {
		if t := strings.TrimSpace(li.Title); t != "" {
			return t
		}
		if p := strings.TrimSpace(li.ProductName); p != "" {
			return p
		}
	}
	return title
}

const invoicesDefaultLimit = 30

// InvoicesList is the `chb invoices [YYYY[/MM]]` entry point — lists
// outgoing invoices issued from Odoo. The interactive mode (-i) lets
// the user navigate, drill into a single invoice (line items + linked
// txs), and stamp category/collective directly on the record (and on
// every reconciled tx via a local Nostr annotation entry).
func InvoicesList(args []string) error {
	return runMoveList(args, moveKindInvoice)
}

// BillsList is the same but for vendor bills (incoming).
func BillsList(args []string) error {
	return runMoveList(args, moveKindBill)
}

func runMoveList(args []string, kind moveKind) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMoveListHelp(kind)
		return nil
	}
	csv := HasFlag(args, "--csv")
	interactive := HasFlag(args, "-i", "--interactive")
	all := HasFlag(args, "--all")
	unreconciled := HasFlag(args, "--unreconciled", "--open")
	limit := GetNumber(args, []string{"-n", "--limit"}, invoicesDefaultLimit)
	posYear, posMonth, _ := ParseYearMonthArg(args)

	rows, err := loadMoveRows(kind, posYear, posMonth)
	if err != nil {
		return err
	}

	if unreconciled {
		filtered := rows[:0]
		for _, r := range rows {
			if moveIsOpen(r.Move) {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}

	if len(rows) == 0 {
		noun := kind.labelPl
		if unreconciled {
			noun = "unreconciled " + noun
		}
		fmt.Printf("\n%sNo %s found%s\n\n", Fmt.Dim, noun, Fmt.Reset)
		return nil
	}

	// Newest first.
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Move.Date > rows[j].Move.Date })

	if !all && !interactive && limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}

	switch {
	case csv:
		printMoveListCSV(kind, rows)
	case interactive:
		runMovesTUI(kind, counterpartiesScopeLabel(posYear, posMonth), rows)
	default:
		printMoveListTable(kind, posYear, posMonth, rows)
	}
	return nil
}

// moveIsOpen reports whether an invoice / bill is still awaiting a
// payment — i.e. fair game for `--unreconciled` filtering and for the
// invoice-side reconcile flow. PaymentState "paid" is excluded; an
// already-attached ReconciledTransaction is excluded; everything else
// (not_paid / in_payment / partial / blank) stays open.
func moveIsOpen(m OdooOutgoingInvoicePublic) bool {
	if m.ReconciledTransaction != nil && m.ReconciledTransaction.ID != "" {
		return false
	}
	if strings.EqualFold(m.PaymentState, "paid") {
		return false
	}
	return true
}

// moveRow wraps an OdooOutgoingInvoicePublic with location info (year,
// month) needed to re-save it, plus the counterpart display name pulled
// from the private companion file.
type moveRow struct {
	Year, Month string
	Move        OdooOutgoingInvoicePublic
	Partner     string // customer (invoices) or vendor (bills) display name
}

// loadMoveRows walks every month directory and collects matching moves.
// Year/month filters narrow the scope. The private file is consulted
// for the partner display name — it stays in memory, never written to
// the public table.
//
// Rules from rules.json with target="invoice" / "bill" are applied at
// the row layer (not in loadMoves) so saveMoveRowAnnotation, which
// round-trips through loadMoves, doesn't accidentally persist
// rule-derived defaults onto every row in a month on every edit. The
// JSON stays a faithful reflection of what Odoo + manual annotations
// produced; the display + reconcile flow see the rule-filled values.
func loadMoveRows(kind moveKind, posYear, posMonth string) ([]moveRow, error) {
	dataDir := DataDir()
	rules, _ := LoadRules() // best-effort; missing / malformed rules.json is a no-op
	var out []moveRow
	err := walkMoveMonths(dataDir, kind, func(year, month string) error {
		if posYear != "" && year != posYear {
			return nil
		}
		if posMonth != "" && month != posMonth {
			return nil
		}
		moves, err := loadMoves(dataDir, year, month, kind)
		if err != nil {
			Warnf("  %s⚠ %s/%s: %v%s", Fmt.Yellow, year, month, err, Fmt.Reset)
			return nil
		}
		partners := loadMovePartners(dataDir, year, month, kind)
		for _, m := range moves {
			partner := partners[m.ID]
			ApplyMoveRules(&m, partner, kind, rules)
			out = append(out, moveRow{
				Year:    year,
				Month:   month,
				Move:    m,
				Partner: partner,
			})
		}
		return nil
	})
	return out, err
}

func moveListTitle(kind moveKind, scope string) string {
	icon := "🧾"
	if kind.isBill {
		icon = "🧮"
	}
	if scope == "" {
		scope = "all time"
	}
	return fmt.Sprintf("%s %s — %s", icon, titleASCII(kind.labelPl), scope)
}

func printMoveListTable(kind moveKind, posYear, posMonth string, rows []moveRow) {
	scope := counterpartiesScopeLabel(posYear, posMonth)
	fmt.Printf("\n%s%s%s\n\n", Fmt.Bold, moveListTitle(kind, scope), Fmt.Reset)

	headers := []string{"Date", partnerColumnLabel(kind), "Description", "Gross", "VAT", "Net", "Paid", "Collective", "Category"}
	rightAlign := map[int]bool{3: true, 4: true, 5: true, 6: true}

	cells := make([][]string, 0, len(rows))
	var totalGross, totalVAT, totalNet, totalPaid float64
	paidCount := 0
	for _, r := range rows {
		desc := moveDescription(r.Move)
		cur := r.Move.Currency
		cells = append(cells, []string{
			r.Move.Date,
			Truncate(r.Partner, 30),
			Truncate(desc, 40),
			fmtAmountCurrency(r.Move.TotalAmount, cur),
			fmtAmountCurrency(r.Move.VATAmount, cur),
			fmtAmountCurrency(r.Move.UntaxedAmount, cur),
			movePaidCell(r.Move),
			Truncate(r.Move.Collective, 14),
			Truncate(r.Move.Category, 14),
		})
		totalGross += r.Move.TotalAmount
		totalVAT += r.Move.VATAmount
		totalNet += r.Move.UntaxedAmount
		if !moveIsOpen(r.Move) {
			paidCount++
			totalPaid += r.Move.TotalAmount
		}
	}

	totalRow := []string{
		"",
		Pluralize(len(rows), kind.label, "") + " — total",
		"",
		fmtEUR(totalGross),
		fmtEUR(totalVAT),
		fmtEUR(totalNet),
		fmt.Sprintf("%d/%d", paidCount, len(rows)),
		"",
		"",
	}
	renderTicketsTable(headers, cells, totalRow, rightAlign)
}

// movePaidCell returns the table-cell string for the "Paid" column:
// "✓" when the move is reconciled / payment_state=paid, "—" otherwise.
// Plain runes so displayWidth (rune count) gives the right column width.
func movePaidCell(m OdooOutgoingInvoicePublic) string {
	if moveIsOpen(m) {
		return "—"
	}
	return "✓"
}

func partnerColumnLabel(kind moveKind) string {
	if kind.isBill {
		return "Vendor"
	}
	return "Customer"
}

func printMoveListCSV(kind moveKind, rows []moveRow) {
	partner := partnerColumnLabel(kind)
	fmt.Printf("date,%s,description,gross,vat,net,currency,paid,collective,category,state,payment_state\n", strings.ToLower(partner))
	for _, r := range rows {
		desc := moveDescription(r.Move)
		paid := "no"
		if !moveIsOpen(r.Move) {
			paid = "yes"
		}
		fmt.Printf("%s,%s,%s,%.2f,%.2f,%.2f,%s,%s,%s,%s,%s,%s\n",
			csvCell(r.Move.Date),
			csvCell(r.Partner),
			csvCell(desc),
			r.Move.TotalAmount, r.Move.VATAmount, r.Move.UntaxedAmount,
			csvCell(r.Move.Currency),
			paid,
			csvCell(r.Move.Collective),
			csvCell(r.Move.Category),
			csvCell(r.Move.State),
			csvCell(r.Move.PaymentState),
		)
	}
}

// fmtAmountCurrency formats a non-EUR amount with a currency suffix; EUR
// reuses the existing thousands-separated fmtEUR.
func fmtAmountCurrency(v float64, currency string) string {
	if currency == "" || strings.EqualFold(currency, "EUR") {
		return fmtEUR(v)
	}
	return fmt.Sprintf("%s %s", fmtEUR(v), strings.ToUpper(currency))
}

// saveMoveRowAnnotation persists a (category, collective) decision
// onto the move record AND propagates the same tag to every linked
// transaction via a local Nostr annotation entry. The next `chb
// generate transactions` run will pick those entries up and stamp the
// matching txs.
//
// Returns counts (movesUpdated, txsAnnotated, err).
func saveMoveRowAnnotation(row *moveRow, kind moveKind, category, collective string) (int, int, error) {
	dataDir := DataDir()

	// 1. Re-read the month file so we don't clobber concurrent edits.
	moves, err := loadMoves(dataDir, row.Year, row.Month, kind)
	if err != nil {
		return 0, 0, err
	}
	found := false
	for i := range moves {
		if moves[i].ID == row.Move.ID {
			if category != "" {
				moves[i].Category = category
			}
			if collective != "" {
				moves[i].Collective = collective
			}
			row.Move = moves[i] // reflect in caller-visible row
			found = true
			break
		}
	}
	if !found {
		return 0, 0, fmt.Errorf("move #%d not found in %s/%s", row.Move.ID, row.Year, row.Month)
	}
	if err := saveMoves(dataDir, row.Year, row.Month, kind, moves); err != nil {
		return 0, 0, err
	}

	// 2. For each linked tx, write a local Nostr annotation entry so
	//    the next regenerate picks it up.
	txCount := 0
	if rec := row.Move.ReconciledTransaction; rec != nil && rec.ID != "" {
		if err := writeMoveTxAnnotation(rec, category, collective); err == nil {
			txCount++
		}
	}

	// 3. Also write a Nostr annotation keyed by the move's own URI
	//    (odoo:<host>:<db>:account.move:<id>). Lets the next `chb
	//    nostr push` ship the (category, collective) classification
	//    so other chb instances pulling from the same relay learn it
	//    too. Best-effort — credential issues downgrade to a silent
	//    skip rather than failing the JSON write the user just
	//    confirmed.
	if creds, err := ResolveOdooCredentials(); err == nil {
		host := OdooHost(creds.URL)
		_ = writeMoveNostrAnnotation(*row, kind, host, creds.DB,
			row.Move.Category, row.Move.Collective)
	}

	return 1, txCount, nil
}

func writeMoveTxAnnotation(rec *OdooReconciledTransaction, category, collective string) error {
	if rec == nil || rec.ID == "" || rec.Date == "" {
		return fmt.Errorf("incomplete reconciled tx record")
	}
	return WriteNostrAnnotation(rec.ID, rec.Date, category, collective)
}

// WriteNostrAnnotation persists a (category, collective) annotation
// keyed by the tx / move URI into the month's annotation cache. The
// month is derived from `date` (YYYY-MM-DD prefix). Used by both the
// invoice TUI [e] flow and the income/expenses drill-view [e] flow.
// Empty category / collective leave the existing value untouched so
// the caller can stamp one field without clobbering the other.
func WriteNostrAnnotation(uri, date, category, collective string) error {
	if uri == "" {
		return fmt.Errorf("empty URI")
	}
	if len(date) < 10 {
		return fmt.Errorf("date %q too short", date)
	}
	t, err := time.Parse("2006-01-02", date[:10])
	if err != nil {
		return fmt.Errorf("parse date %q: %v", date, err)
	}
	year := fmt.Sprintf("%04d", t.Year())
	month := fmt.Sprintf("%02d", t.Month())

	dataDir := DataDir()
	path := nostrsource.Path(dataDir, year, month, nostrsource.AnnotationsFile)

	cache := NostrAnnotationCache{Annotations: map[string]*TxAnnotation{}}
	if data, readErr := os.ReadFile(path); readErr == nil {
		_ = json.Unmarshal(data, &cache)
	}
	if cache.Annotations == nil {
		cache.Annotations = map[string]*TxAnnotation{}
	}
	prev := cache.Annotations[uri]
	if prev == nil {
		prev = &TxAnnotation{URI: uri}
	}
	if category != "" {
		prev.Category = category
	}
	if collective != "" {
		prev.Collective = collective
	}
	prev.CreatedAt = time.Now().Unix()
	cache.Annotations[uri] = prev
	cache.FetchedAt = time.Now().UTC().Format(time.RFC3339)
	return nostrsource.WriteJSON(dataDir, year, month, cache, nostrsource.AnnotationsFile)
}

func printMoveListHelp(kind moveKind) {
	f := Fmt
	noun := "invoices we issued"
	if kind.isBill {
		noun = "bills we received"
	}
	fmt.Printf(`
%schb %s%s — List %s with totals, sorted newest first

%sUSAGE%s
  %schb %s%s                    All time
  %schb %s%s 2025               Year 2025
  %schb %s%s 2025/12            December 2025
  %schb %s --unreconciled -i%s   Interactive picker for open %s only
  %schb %s reconcile [yyyy/mm]%s Attach unreconciled %s to bank lines

%sCOLUMNS%s
  Date         Invoice / bill date
  %s    Display name from the private file (PII; in-memory only)
  Description  Title, or first line-item title when empty
  Gross        Total amount (VAT-inclusive)
  VAT          Total VAT
  Net          Untaxed amount
  Collective   Annotation on the move record
  Category     Annotation on the move record

%sOPTIONS%s
  %s-i%s, %s--interactive%s    Open a TUI: navigate, drill in, set collective/category,
                       and on rows with no payment, press [r] to pick from
                       candidate bank lines and reconcile in place.
  %s--unreconciled%s       Only %s whose payment_state ≠ paid AND that have no
                       attached payment yet. Pair with -i to triage open items.
  %s-n%s <N>, %s--limit%s <N>   Limit output rows (default %d, use --all to show all)
  %s--all%s                Show every row
  %s--csv%s                Output CSV instead of a formatted table
  %s--help, -h%s           Show this help

%sRECONCILE SUBCOMMAND%s
  %schb %s reconcile [YYYY[/MM]] [--yes] [-v]%s

  Scans unreconciled %s in scope, looks for a single matching unreconciled
  bank line (same amount, correct direction) across every linked journal
  cache, and (with --yes) attaches them via the same flow as
  %schb odoo journals <id> reconcile%s. Default is dry-run.
`,
		f.Bold, kind.labelPl, f.Reset, noun,
		f.Bold, f.Reset,
		f.Cyan, kind.labelPl, f.Reset,
		f.Cyan, kind.labelPl, f.Reset,
		f.Cyan, kind.labelPl, f.Reset,
		f.Cyan, kind.labelPl, f.Reset, kind.labelPl,
		f.Cyan, kind.labelPl, f.Reset, kind.labelPl,
		f.Bold, f.Reset,
		partnerColumnLabel(kind),
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset, kind.labelPl,
		f.Yellow, f.Reset, f.Yellow, f.Reset, invoicesDefaultLimit,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, kind.labelPl, f.Reset,
		kind.labelPl,
		f.Cyan, f.Reset,
	)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
