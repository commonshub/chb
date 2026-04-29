package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// enrichEventsWithTicketSales attaches the transactions tagged with each
// event UID to the events.json that describes the event. Tickets for an
// event in April may have been sold in February — so we build a global
// event → []tx index by walking every month's generated/transactions.json,
// then rewrite every events.json to attach the matching tx summaries.
//
// Silent when there are no events or no tagged transactions. Safe to run
// repeatedly — replaces any prior TicketSales field rather than appending.
func enrichEventsWithTicketSales(dataDir string) {
	// Step 1 — walk every month's transactions.json and index by canonical
	// event ids from tx.Event and event tags.
	eventTxs := map[string][]TransactionEntry{}
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
			for _, eventID := range transactionEventIDs(tx) {
				eventTxs[eventID] = append(eventTxs[eventID], tx)
			}
		}
	})

	if len(eventTxs) == 0 {
		return
	}

	// Sort each event's txs by timestamp for stable output.
	for id, list := range eventTxs {
		sort.Slice(list, func(i, j int) bool {
			return list[i].Timestamp < list[j].Timestamp
		})
		eventTxs[id] = list
	}

	// Step 2 — walk every month's events.json and rewrite with ticket sales.
	touched := 0
	_ = forEachGeneratedMonth(dataDir, "events.json", func(path string) {
		data, err := os.ReadFile(path)
		if err != nil {
			return
		}
		var f FullEventsFile
		if json.Unmarshal(data, &f) != nil {
			return
		}
		changed := false
		for i, ev := range f.Events {
			txs := eventTxs[ev.ID]
			summary := summariseEventTx(txs)
			if !eventTicketSalesEqual(ev.TicketSales, summary) {
				f.Events[i].TicketSales = summary
				changed = true
			}
		}
		if !changed {
			return
		}
		f.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
		out, err := json.MarshalIndent(f, "", "  ")
		if err != nil {
			return
		}
		// Route through writeDataFile so enforcePIIPolicy scrubs name fields
		// and warns on email-regex matches — events.json is a public path.
		if err := writeDataFile(path, out); err != nil {
			Warnf("  %s⚠ enrich events: %s: %v%s", Fmt.Yellow, path, err, Fmt.Reset)
			return
		}
		touched++
	})
	if touched > 0 {
		fmt.Printf("  ✓ attached ticket sales to events in %d file(s)\n", touched)
	}
}

func transactionEventIDs(tx TransactionEntry) []string {
	seen := map[string]bool{}
	var ids []string
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		ids = append(ids, id)
	}
	add(tx.Event)
	for _, raw := range tx.Tags {
		tag, ok := normalizeTransactionTag(raw)
		if ok && len(tag) >= 2 && tag[0] == "event" {
			add(tag[1])
		}
	}
	return ids
}

// summariseEventTx builds the TicketSales summary for a single event. Returns
// nil when there are no transactions to attach — callers can then skip
// serialising the field entirely.
func summariseEventTx(txs []TransactionEntry) *EventTicketSales {
	if len(txs) == 0 {
		return nil
	}
	s := &EventTicketSales{
		Gross: map[string]float64{},
		Net:   map[string]float64{},
	}
	for _, tx := range txs {
		amt := eventTicketTransactionAmount(tx)
		currency := tx.Currency
		if currency == "" {
			currency = "EUR"
		}
		s.TxCount++
		if tx.Type == "DEBIT" {
			s.RefundCount++
		} else {
			s.Gross[currency] += amt
		}
		s.Net[currency] += amt

		date := ""
		if tx.Timestamp > 0 {
			date = time.Unix(tx.Timestamp, 0).UTC().Format("2006-01-02")
		}
		if s.FirstTx == "" || (date != "" && date < s.FirstTx) {
			s.FirstTx = date
		}
		if date != "" && date > s.LastTx {
			s.LastTx = date
		}
		s.Transactions = append(s.Transactions, EventTicketTx{
			ID:       tx.ID,
			Provider: tx.Provider,
			Date:     date,
			Amount:   amt,
			Currency: currency,
			Type:     tx.Type,
		})
	}
	// Round to 2 decimals to keep the numbers readable.
	for c, v := range s.Gross {
		s.Gross[c] = round2(v)
	}
	for c, v := range s.Net {
		s.Net[c] = round2(v)
	}
	return s
}

func eventTicketTransactionAmount(tx TransactionEntry) float64 {
	if tx.Provider == "stripe" && tx.GrossAmount != 0 {
		amt := math.Abs(tx.GrossAmount)
		if tx.Type == "DEBIT" {
			return -amt
		}
		return amt
	}
	if tx.NormalizedAmount != 0 {
		return tx.NormalizedAmount
	}
	return tx.Amount
}

// eventTicketSalesEqual reports whether two TicketSales summaries would
// serialise identically. Skips updating the file when nothing changed, which
// matters because `chb generate` can run on schedule and we don't want
// spurious writes.
func eventTicketSalesEqual(a, b *EventTicketSales) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.TxCount != b.TxCount || a.RefundCount != b.RefundCount ||
		a.FirstTx != b.FirstTx || a.LastTx != b.LastTx {
		return false
	}
	if !floatMapEqual(a.Gross, b.Gross) || !floatMapEqual(a.Net, b.Net) {
		return false
	}
	if len(a.Transactions) != len(b.Transactions) {
		return false
	}
	for i := range a.Transactions {
		if a.Transactions[i] != b.Transactions[i] {
			return false
		}
	}
	return true
}

func floatMapEqual(a, b map[string]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5*sign(v))) / 100
}

func sign(v float64) float64 {
	if v < 0 {
		return -1
	}
	return 1
}

// forEachGeneratedMonth walks <dataDir>/<YYYY>/<MM>/generated/<name> for
// every present month, plus latest/generated/<name> when it exists, and
// invokes fn for each existing file path. Returns nil on success — walk
// errors bubble up.
func forEachGeneratedMonth(dataDir, name string, fn func(path string)) error {
	yearEntries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	for _, ye := range yearEntries {
		if !ye.IsDir() {
			continue
		}
		switch {
		case len(ye.Name()) == 4:
			// YYYY/MM/generated/<name>
			monthEntries, _ := os.ReadDir(filepath.Join(dataDir, ye.Name()))
			for _, me := range monthEntries {
				if !me.IsDir() || len(me.Name()) != 2 {
					continue
				}
				p := filepath.Join(dataDir, ye.Name(), me.Name(), "generated", name)
				if _, err := os.Stat(p); err == nil {
					fn(p)
				}
			}
		case ye.Name() == "latest":
			p := filepath.Join(dataDir, "latest", "generated", name)
			if _, err := os.Stat(p); err == nil {
				fn(p)
			}
		}
	}
	return nil
}
