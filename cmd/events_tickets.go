package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

// EventsTickets renders ticket-sale tables. The granularity depends on
// the positional arg:
//
//	chb events tickets              → year summary (one row per year)
//	chb events tickets 2025         → month summary for the year (one row per month)
//	chb events tickets 2025/11      → per-event detail for the month
//
// Columns: # events, # txs, gross (already net of refunds), net after
// Stripe fees, VAT (gross × 21 / 121). Only EUR-denominated txs are
// counted — non-EUR (e.g. CHT) is ignored.
func EventsTickets(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintEventsTicketsHelp()
		return
	}
	csv := HasFlag(args, "--csv")
	posYear, posMonth, _ := ParseYearMonthArg(args)

	dataDir := DataDir()
	txsByEvent := loadTicketEventTxs(dataDir)
	if len(txsByEvent) == 0 {
		if csv {
			return
		}
		fmt.Printf("\n%sNo ticket-sale transactions found.%s Run `chb generate` first.\n\n", Fmt.Dim, Fmt.Reset)
		return
	}
	eventsByID := loadEventsByID(dataDir)

	aggregates := buildEventTicketAggregates(txsByEvent, eventsByID)
	if len(aggregates) == 0 {
		if csv {
			return
		}
		fmt.Printf("\n%sNo EUR ticket-sale events found.%s\n\n", Fmt.Dim, Fmt.Reset)
		return
	}

	filtered := filterEventTicketAggregates(aggregates, posYear, posMonth)
	if len(filtered) == 0 {
		if csv {
			return
		}
		scope := "any year"
		if posYear != "" {
			scope = posYear
			if posMonth != "" {
				scope += "/" + posMonth
			}
		}
		fmt.Printf("\n%sNo events with ticket sales found for %s.%s\n\n", Fmt.Dim, scope, Fmt.Reset)
		return
	}

	// Single-month: render per-event detail.
	if posMonth != "" {
		title := posYear + "/" + posMonth
		sort.SliceStable(filtered, func(i, j int) bool { return filtered[i].StartAt < filtered[j].StartAt })
		if csv {
			printEventTicketsDetailCSV(filtered)
			return
		}
		printEventTicketsTable(title, filtered)
		return
	}

	// Year breakdown: one row per month for the year. Otherwise (no
	// positional arg): one row per year.
	period := "year"
	periodTitle := "All years"
	if posYear != "" {
		period = "month"
		periodTitle = posYear
	}
	if csv {
		printEventTicketsSummaryCSV(period, filtered)
		return
	}
	printEventTicketsSummary(periodTitle, period, filtered)
}

type eventTicketAgg struct {
	ID           string
	Name         string
	StartAt      string
	StartTime    time.Time
	URL          string // fallback identifier (Luma URL from tx tags) when calendar match is missing
	FirstTxTime  time.Time
	HasEventDate bool // true when StartTime came from events.json, false when from first tx
	TxCount      int
	RefundCount  int
	Gross        float64 // customer payments net of refunds
	Fees         float64 // Stripe processing fees
}

// VAT is the 21% Belgian VAT extracted from a VAT-inclusive gross.
func (a eventTicketAgg) VAT() float64 { return a.Gross * vatRate / (1 + vatRate) }

// Net is what the business keeps: gross minus the Stripe fees and the
// VAT owed to the government.
func (a eventTicketAgg) Net() float64 { return a.Gross - a.Fees - a.VAT() }

// loadTicketEventTxs walks every month's transactions.json and groups
// the EUR ticket-sale txs (CREDIT/DEBIT) by canonical event id. Skips
// internal transfers, mints, burns — only customer-facing payments and
// refunds count toward ticket revenue.
func loadTicketEventTxs(dataDir string) map[string][]TransactionEntry {
	out := map[string][]TransactionEntry{}
	_ = forEachGeneratedMonth(dataDir, "transactions.json", func(path string) {
		data, err := os.ReadFile(path)
		if err != nil {
			return
		}
		var f TransactionsFile
		if json.Unmarshal(data, &f) != nil {
			return
		}
		for _, tx := range f.Transactions {
			if !strings.EqualFold(tx.Currency, "EUR") {
				continue
			}
			if tx.Type != "CREDIT" && tx.Type != "DEBIT" {
				continue
			}
			ids := transactionEventIDs(tx)
			if len(ids) == 0 {
				continue
			}
			seen := map[string]bool{}
			for _, id := range ids {
				key := bareEventID(id)
				if key == "" || seen[key] {
					continue
				}
				seen[key] = true
				out[key] = append(out[key], tx)
			}
		}
	})
	// The same tx can appear in multiple month files (e.g. latest/
	// mirrors month dirs). Dedup by tx.ID within each event.
	for id, txs := range out {
		seen := map[string]bool{}
		uniq := txs[:0]
		for _, tx := range txs {
			if tx.ID == "" || seen[tx.ID] {
				continue
			}
			seen[tx.ID] = true
			uniq = append(uniq, tx)
		}
		out[id] = uniq
	}
	return out
}

// loadEventsByID returns the richest FullEvent record per event id seen
// across all months, keyed by the *bare* Luma id (stripping the
// "@events.lu.ma" calendar-source suffix the ics sync attaches) so the
// keys match what Stripe transactions reference via tx.Event. "Richest"
// is by name length, since the Luma sync may write multiple events with
// the same id and we want the one that actually has the human-readable
// name.
func loadEventsByID(dataDir string) map[string]FullEvent {
	out := map[string]FullEvent{}
	_ = forEachGeneratedMonth(dataDir, "events.json", func(path string) {
		data, err := os.ReadFile(path)
		if err != nil {
			return
		}
		var f FullEventsFile
		if json.Unmarshal(data, &f) != nil {
			return
		}
		for _, ev := range f.Events {
			key := bareEventID(ev.ID)
			if key == "" {
				continue
			}
			prev, ok := out[key]
			if !ok || len(ev.Name) > len(prev.Name) {
				out[key] = ev
			}
		}
	})
	return out
}

// bareEventID strips the optional calendar-source suffix
// (e.g. "evt-XXX@events.lu.ma" → "evt-XXX") so event ids from
// events.json match the bare ids referenced by tx.Event.
func bareEventID(id string) string {
	if i := strings.Index(id, "@"); i > 0 {
		return id[:i]
	}
	return id
}

func buildEventTicketAggregates(txsByEvent map[string][]TransactionEntry, eventsByID map[string]FullEvent) []eventTicketAgg {
	var out []eventTicketAgg
	for eventID, txs := range txsByEvent {
		ev := eventsByID[bareEventID(eventID)]
		agg := eventTicketAgg{
			ID:      eventID,
			Name:    ev.Name,
			StartAt: ev.StartAt,
			URL:     ev.URL,
		}
		if t, ok := parseEventStart(ev.StartAt); ok && !t.IsZero() {
			agg.StartTime = t
			agg.HasEventDate = true
		}
		for _, tx := range txs {
			amt := math.Abs(tx.GrossAmount)
			if amt == 0 {
				amt = math.Abs(tx.NormalizedAmount)
			}
			if amt == 0 {
				amt = math.Abs(tx.Amount)
			}
			switch tx.Type {
			case "CREDIT":
				agg.Gross += amt
				agg.TxCount++
			case "DEBIT":
				agg.Gross -= amt
				agg.RefundCount++
				agg.TxCount++
			}
			agg.Fees += math.Abs(tx.Fee)

			txTime := time.Unix(tx.Timestamp, 0).UTC()
			if agg.FirstTxTime.IsZero() || txTime.Before(agg.FirstTxTime) {
				agg.FirstTxTime = txTime
			}
			if agg.URL == "" {
				agg.URL = txTagValue(tx, "eventUrl")
			}
		}
		if agg.TxCount == 0 {
			continue
		}
		// Calendar match missing — group by the first tx date instead so
		// the row still lands in the right month/year bucket. The Date
		// column will mark it with "~" to make the inference visible.
		if !agg.HasEventDate && !agg.FirstTxTime.IsZero() {
			agg.StartTime = agg.FirstTxTime
		}
		if agg.Name == "" {
			if agg.URL != "" {
				agg.Name = agg.URL
			} else {
				agg.Name = eventID
			}
		}
		out = append(out, agg)
	}
	return out
}

func txTagValue(tx TransactionEntry, key string) string {
	for _, raw := range tx.Tags {
		if len(raw) >= 2 && raw[0] == key {
			return raw[1]
		}
	}
	return ""
}

func filterEventTicketAggregates(aggregates []eventTicketAgg, posYear, posMonth string) []eventTicketAgg {
	if posYear == "" {
		return aggregates
	}
	var out []eventTicketAgg
	for _, a := range aggregates {
		if a.StartTime.IsZero() {
			continue
		}
		if fmt.Sprintf("%04d", a.StartTime.Year()) != posYear {
			continue
		}
		if posMonth != "" && fmt.Sprintf("%02d", int(a.StartTime.Month())) != posMonth {
			continue
		}
		out = append(out, a)
	}
	return out
}

// printEventTicketsSummary collapses aggregates into one row per period
// (year or month) instead of per event. Used by the no-arg and
// chb-events-tickets-YYYY modes to give an at-a-glance overview with a
// grand-total row.
func printEventTicketsSummary(title, period string, aggregates []eventTicketAgg) {
	buckets := bucketAggregates(period, aggregates)

	fmt.Printf("\n%s🎟  Ticket sales — %s%s\n\n", Fmt.Bold, title, Fmt.Reset)

	headerLabel := "Year"
	if period == "month" {
		headerLabel = "Month"
	}
	headers := []string{headerLabel, "# events", "# txs", "Gross", "Fees", "VAT", "Net"}
	rightAlign := map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true}

	rows := make([][]string, 0, len(buckets))
	var totalEvents, totalTx int
	var totalGross, totalFees, totalVAT, totalNet float64
	for _, b := range buckets {
		txCell := fmt.Sprintf("%d", b.TxCount)
		if b.RefundCount > 0 {
			txCell = fmt.Sprintf("%d (incl. %d refund)", b.TxCount, b.RefundCount)
		}
		rows = append(rows, []string{
			b.Label,
			fmt.Sprintf("%d", b.Events),
			txCell,
			fmtEUR(b.Gross),
			fmtEUR(b.Fees),
			fmtEUR(b.VAT),
			fmtEUR(b.Net),
		})
		totalEvents += b.Events
		totalTx += b.TxCount
		totalGross += b.Gross
		totalFees += b.Fees
		totalVAT += b.VAT
		totalNet += b.Net
	}

	totalRow := []string{
		"Total",
		fmt.Sprintf("%d", totalEvents),
		fmt.Sprintf("%d", totalTx),
		fmtEUR(totalGross),
		fmtEUR(totalFees),
		fmtEUR(totalVAT),
		fmtEUR(totalNet),
	}

	renderTicketsTable(headers, rows, totalRow, rightAlign)
}

type eventTicketBucket struct {
	Key, Label                    string
	Events, TxCount, RefundCount  int
	Gross, Fees, VAT, Net         float64
}

func bucketAggregates(period string, aggregates []eventTicketAgg) []eventTicketBucket {
	keyOf := func(a eventTicketAgg) (string, string) {
		if period == "month" {
			if a.StartTime.IsZero() {
				return "----", "—"
			}
			return fmt.Sprintf("%04d/%02d", a.StartTime.Year(), int(a.StartTime.Month())),
				a.StartTime.Format("Jan 2006")
		}
		if a.StartTime.IsZero() {
			return "----", "—"
		}
		return fmt.Sprintf("%04d", a.StartTime.Year()), fmt.Sprintf("%d", a.StartTime.Year())
	}

	byKey := map[string]*eventTicketBucket{}
	for _, a := range aggregates {
		k, label := keyOf(a)
		b := byKey[k]
		if b == nil {
			b = &eventTicketBucket{Key: k, Label: label}
			byKey[k] = b
		}
		b.Events++
		b.TxCount += a.TxCount
		b.RefundCount += a.RefundCount
		b.Gross += a.Gross
		b.Fees += a.Fees
		b.VAT += a.VAT()
		b.Net += a.Net()
	}
	out := make([]eventTicketBucket, 0, len(byKey))
	for _, b := range byKey {
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// renderTicketsTable draws header → body → separator → total. Splits out
// the rendering so printEventTicketsTable and printEventTicketsSummary
// share the alignment logic.
func renderTicketsTable(headers []string, rows [][]string, totalRow []string, rightAlign map[int]bool) {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = displayWidth(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if w := displayWidth(c); w > widths[i] {
				widths[i] = w
			}
		}
	}
	for i, c := range totalRow {
		if w := displayWidth(c); w > widths[i] {
			widths[i] = w
		}
	}

	printRow := func(r []string, dim bool) {
		fmt.Print("  ")
		for i := range headers {
			cell := ""
			if i < len(r) {
				cell = r[i]
			}
			if i > 0 {
				fmt.Print("  ")
			}
			out := cell
			if rightAlign[i] {
				out = padLeft(cell, widths[i])
			} else {
				out = padRight(cell, widths[i])
			}
			if dim {
				out = Fmt.Dim + out + Fmt.Reset
			}
			fmt.Print(out)
		}
		fmt.Println()
	}
	separator := func() {
		fmt.Printf("  %s%s%s\n", Fmt.Dim, strings.Repeat("─", tableWidth(widths, len(headers))), Fmt.Reset)
	}

	printRow(headers, true)
	separator()
	for _, r := range rows {
		printRow(r, false)
	}
	separator()
	printRow(totalRow, false)
	fmt.Println()
}

func printEventTicketsTable(title string, list []eventTicketAgg) {
	fmt.Printf("\n%s🎟  Ticket sales — %s%s\n\n", Fmt.Bold, title, Fmt.Reset)

	headers := []string{"Date", "Event", "# txs", "Gross", "Fees", "VAT", "Net"}
	rightAlign := map[int]bool{2: true, 3: true, 4: true, 5: true, 6: true}

	rows := make([][]string, 0, len(list))
	var totalTx int
	var totalGross, totalFees, totalVAT, totalNet float64
	for _, a := range list {
		date := "—"
		if !a.StartTime.IsZero() {
			date = a.StartTime.Format("2006-01-02")
			if !a.HasEventDate {
				// First-tx fallback; flag so the reader knows it's not the event date.
				date = "~" + date
			}
		}
		row := []string{
			date,
			Truncate(a.Name, 45),
			fmt.Sprintf("%d", a.TxCount),
			fmtEUR(a.Gross),
			fmtEUR(a.Fees),
			fmtEUR(a.VAT()),
			fmtEUR(a.Net()),
		}
		if a.RefundCount > 0 {
			row[2] = fmt.Sprintf("%d (incl. %d refund)", a.TxCount-a.RefundCount, a.RefundCount)
		}
		rows = append(rows, row)
		totalTx += a.TxCount
		totalGross += a.Gross
		totalFees += a.Fees
		totalVAT += a.VAT()
		totalNet += a.Net()
	}

	totalRow := []string{
		"",
		Pluralize(len(list), "event", "") + " — total",
		fmt.Sprintf("%d", totalTx),
		fmtEUR(totalGross),
		fmtEUR(totalFees),
		fmtEUR(totalVAT),
		fmtEUR(totalNet),
	}

	renderTicketsTable(headers, rows, totalRow, rightAlign)
}

// printEventTicketsSummaryCSV emits the period-summary table as CSV on
// stdout. Mirrors the table columns but without ANSI styling, currency
// formatting (raw float, 2 decimals) or the refund decoration on the
// tx-count cell.
func printEventTicketsSummaryCSV(period string, aggregates []eventTicketAgg) {
	buckets := bucketAggregates(period, aggregates)
	header := "year"
	if period == "month" {
		header = "month"
	}
	fmt.Printf("%s,events,txs,refunds,gross,fees,vat,net\n", header)
	for _, b := range buckets {
		fmt.Printf("%s,%d,%d,%d,%.2f,%.2f,%.2f,%.2f\n",
			csvCell(b.Key), b.Events, b.TxCount, b.RefundCount, b.Gross, b.Fees, b.VAT, b.Net)
	}
}

// printEventTicketsDetailCSV emits the per-event detail rows for a
// single month as CSV on stdout.
func printEventTicketsDetailCSV(list []eventTicketAgg) {
	fmt.Println("date,name,event_id,url,txs,refunds,gross,fees,vat,net,date_is_inferred")
	for _, a := range list {
		date := ""
		if !a.StartTime.IsZero() {
			date = a.StartTime.Format("2006-01-02")
		}
		fmt.Printf("%s,%s,%s,%s,%d,%d,%.2f,%.2f,%.2f,%.2f,%t\n",
			date,
			csvCell(a.Name),
			csvCell(a.ID),
			csvCell(a.URL),
			a.TxCount, a.RefundCount,
			a.Gross, a.Fees, a.VAT(), a.Net(),
			!a.HasEventDate,
		)
	}
}

// csvCell quotes a cell value if it contains a comma, quote or newline,
// per RFC 4180. Doubles internal quotes per the same spec.
func csvCell(s string) string {
	if !strings.ContainsAny(s, ",\"\n\r") {
		return s
	}
	return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
}

func PrintEventsTicketsHelp() {
	f := Fmt
	fmt.Printf(`
%schb events tickets%s — Ticket-sale summary by year, month, or per event

%sUSAGE%s
  %schb events tickets%s              Year summary (one row per year)
  %schb events tickets%s 2025         Month summary for 2025 (one row per month)
  %schb events tickets%s 2025/11      Per-event detail for November 2025

%sCOLUMNS%s
  # events    Distinct events with at least one ticket transaction
  # txs       Stripe charges + refunds attributed to those events
  Gross       Customer payments net of refunds (EUR only)
  Fees        Stripe processing fees
  VAT         Gross × 21 / 121 (Belgian standard rate, VAT-inclusive)
  Net         Gross − Fees − VAT (what the business keeps)

%sOPTIONS%s
  %s--csv%s                Output CSV instead of a formatted table
  %s--help, -h%s           Show this help
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
