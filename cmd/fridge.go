package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"time"
)

// fridgeFeesStartDate is the cutoff before which fridge txs are assumed
// fee-free (customer paid exactly what arrived on-chain). From this date
// on we charge a per-payment fee and the gross is inferred by rounding
// each tx up to the next 0.50 EUR.
var fridgeFeesStartDate = time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)

// fridgeMaxSaleAmount caps what we treat as a customer purchase. Real
// fridge items cost a few euros; large incoming amounts are wallet
// top-ups (Monerium fiat-to-EURb mints) and would otherwise inflate the
// "sales" totals. Tunable if pricing ever changes.
const fridgeMaxSaleAmount = 50.0

// Fridge prints a sales-style breakdown of the EURb fridge wallet.
//
// Etherscan reports POST-fee amounts (the merchant fee is taken before
// the on-chain transfer reaches the wallet). Customer prices are always
// round multiples of 0.50 EUR, so the gross is recovered by rounding
// each tx amount up to the next 0.50 — unless the amount is already a
// 0.50 multiple, in which case no fee was applied.
//
//	chb fridge              → year summary
//	chb fridge 2025         → month summary for 2025
//	chb fridge 2025/10      → day summary for October 2025
func Fridge(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintFridgeHelp()
		return
	}
	csv := HasFlag(args, "--csv")
	posYear, posMonth, _ := ParseYearMonthArg(args)

	txs := loadFridgeIncomingTxs(DataDir())
	if len(txs) == 0 {
		if csv {
			return
		}
		fmt.Printf("\n%sNo fridge transactions found.%s Run `chb generate` first.\n\n", Fmt.Dim, Fmt.Reset)
		return
	}

	filtered := filterFridgeTxsByYearMonth(txs, posYear, posMonth)
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
		fmt.Printf("\n%sNo fridge txs in %s.%s\n\n", Fmt.Dim, scope, Fmt.Reset)
		return
	}

	period := "year"
	title := "All years"
	switch {
	case posMonth != "":
		period = "day"
		title = posYear + "/" + posMonth
	case posYear != "":
		period = "month"
		title = posYear
	}

	buckets := bucketFridgeTxs(period, filtered)
	if csv {
		printFridgeCSV(period, buckets)
		return
	}
	printFridgeTable(title, period, buckets)
}

// loadFridgeIncomingTxs walks every month's transactions.json and
// returns the deduped incoming txs on the fridge wallet (type MINT —
// fee-bearing direct mints to the wallet — or CREDIT — direct
// wallet-to-wallet transfers that bypass the payment processor).
// INTERNAL transfers (savings ↔ fridge) and outgoing BURN/DEBIT are
// excluded since they aren't customer sales.
func loadFridgeIncomingTxs(dataDir string) []TransactionEntry {
	seen := map[string]bool{}
	var out []TransactionEntry
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
			if tx.AccountSlug != "fridge" {
				continue
			}
			if tx.Type != "MINT" && tx.Type != "CREDIT" {
				continue
			}
			if tx.ID == "" || seen[tx.ID] {
				continue
			}
			actual := math.Abs(tx.Amount)
			if actual == 0 {
				actual = math.Abs(tx.GrossAmount)
			}
			if actual > fridgeMaxSaleAmount {
				continue
			}
			seen[tx.ID] = true
			out = append(out, tx)
		}
	})
	return out
}

func filterFridgeTxsByYearMonth(txs []TransactionEntry, posYear, posMonth string) []TransactionEntry {
	if posYear == "" {
		return txs
	}
	var out []TransactionEntry
	for _, tx := range txs {
		t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
		if fmt.Sprintf("%04d", t.Year()) != posYear {
			continue
		}
		if posMonth != "" && fmt.Sprintf("%02d", int(t.Month())) != posMonth {
			continue
		}
		out = append(out, tx)
	}
	return out
}

// fridgeGrossFromActual recovers the customer-facing gross from an
// on-chain amount. Before the fee-era cutoff, gross == actual (no fee
// model existed). After: ceil to next 0.50 EUR with a tiny epsilon so
// amounts that are *already* multiples of 0.50 don't round up.
func fridgeGrossFromActual(actual float64, ts int64) float64 {
	if time.Unix(ts, 0).Before(fridgeFeesStartDate) {
		return actual
	}
	return math.Ceil(actual*2-1e-9) / 2
}

type fridgeBucket struct {
	Key, Label            string
	TxCount               int
	Gross, Fees, VAT, Net float64
}

func bucketFridgeTxs(period string, txs []TransactionEntry) []fridgeBucket {
	keyOf := func(t time.Time) (string, string) {
		switch period {
		case "day":
			return t.Format("2006-01-02"), t.Format("Mon Jan 2")
		case "month":
			return fmt.Sprintf("%04d/%02d", t.Year(), int(t.Month())), t.Format("Jan 2006")
		default:
			return fmt.Sprintf("%04d", t.Year()), fmt.Sprintf("%d", t.Year())
		}
	}

	byKey := map[string]*fridgeBucket{}
	for _, tx := range txs {
		when := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
		k, label := keyOf(when)
		b := byKey[k]
		if b == nil {
			b = &fridgeBucket{Key: k, Label: label}
			byKey[k] = b
		}
		actual := math.Abs(tx.Amount)
		if actual == 0 {
			actual = math.Abs(tx.GrossAmount)
		}
		gross := fridgeGrossFromActual(actual, tx.Timestamp)
		fees := gross - actual
		if fees < 0 {
			fees = 0
		}
		vat := gross * vatRate / (1 + vatRate)
		net := gross - fees - vat

		b.TxCount++
		b.Gross += gross
		b.Fees += fees
		b.VAT += vat
		b.Net += net
	}

	out := make([]fridgeBucket, 0, len(byKey))
	for _, b := range byKey {
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func printFridgeTable(title, period string, buckets []fridgeBucket) {
	fmt.Printf("\n%s🥶  Fridge sales — %s%s\n\n", Fmt.Bold, title, Fmt.Reset)

	headerLabel := "Year"
	switch period {
	case "month":
		headerLabel = "Month"
	case "day":
		headerLabel = "Day"
	}
	headers := []string{headerLabel, "# txs", "Gross", "Fees", "VAT", "Net"}
	rightAlign := map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true}

	rows := make([][]string, 0, len(buckets))
	var totalTx int
	var totalGross, totalFees, totalVAT, totalNet float64
	for _, b := range buckets {
		rows = append(rows, []string{
			b.Label,
			fmt.Sprintf("%d", b.TxCount),
			fmtEUR(b.Gross),
			fmtEUR(b.Fees),
			fmtEUR(b.VAT),
			fmtEUR(b.Net),
		})
		totalTx += b.TxCount
		totalGross += b.Gross
		totalFees += b.Fees
		totalVAT += b.VAT
		totalNet += b.Net
	}

	totalRow := []string{
		"Total",
		fmt.Sprintf("%d", totalTx),
		fmtEUR(totalGross),
		fmtEUR(totalFees),
		fmtEUR(totalVAT),
		fmtEUR(totalNet),
	}

	renderTicketsTable(headers, rows, totalRow, rightAlign)
}

func printFridgeCSV(period string, buckets []fridgeBucket) {
	headerLabel := "year"
	switch period {
	case "month":
		headerLabel = "month"
	case "day":
		headerLabel = "day"
	}
	fmt.Printf("%s,txs,gross,fees,vat,net\n", headerLabel)
	for _, b := range buckets {
		fmt.Printf("%s,%d,%.2f,%.2f,%.2f,%.2f\n",
			csvCell(b.Key), b.TxCount, b.Gross, b.Fees, b.VAT, b.Net)
	}
}

func PrintFridgeHelp() {
	f := Fmt
	fmt.Printf(`
%schb fridge%s — Fridge (EURb) sales summary by year, month, or day

%sUSAGE%s
  %schb fridge%s              Year summary (one row per year)
  %schb fridge%s 2025         Month summary for 2025 (one row per month)
  %schb fridge%s 2025/10      Day summary for October 2025

%sCOLUMNS%s
  # txs    Incoming MINT + CREDIT txs on the fridge wallet
  Gross    Customer payment — amount received rounded UP to the next 0.50 EUR
           (etherscan reports post-fee amounts; fees apply since 2025-03)
  Fees     Inferred merchant fee = Gross − amount received
  VAT      Gross × 21 / 121 (Belgian standard, VAT-inclusive)
  Net      Gross − Fees − VAT

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
