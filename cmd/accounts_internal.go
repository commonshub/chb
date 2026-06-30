package cmd

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// AccountsInternal audits every internal-transfer leg across all local accounts
// and reports whether they net to zero. They should: every transfer between two
// of our own accounts books two opposite-sign legs (tagged INTERNAL /
// category=internal_transfer), so the sum over all accounts must be 0.
//
// It pairs legs in two passes:
//  1. on-chain legs that share a tx hash and cancel within that hash (a real
//     wallet→wallet transfer captured on both sides);
//  2. the remainder, paired by equal magnitude + opposite sign across accounts
//     (cross-provider transfers — Stripe/KBC ↔ Monerium — that have no shared
//     hash).
//
// What survives both passes is an orphan: a leg tagged internal with no
// counterpart in our data. Those are exactly the legs to investigate — either a
// missing counter-leg (a transfer whose other side wasn't fetched / wasn't
// tagged) or a misclassification (money that actually left to an external
// address, e.g. the hack, and isn't an internal transfer at all).
//
//	chb accounts internal            Summary + per-account orphan breakdown
//	chb accounts internal --verbose  Also list every orphan leg
//	chb accounts internal --csv      Orphan legs as CSV
func AccountsInternal(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printAccountsInternalHelp()
		return nil
	}
	verbose := HasFlag(args, "--verbose", "-v")
	csv := HasFlag(args, "--csv")

	// --since lets the audit ignore transactions before a cutoff (e.g. the
	// 2025-01-01 accounting cutoff, before which accounts are represented by an
	// opening balance in Odoo rather than per-transaction history).
	var since time.Time
	if s := GetOption(args, "--since"); s != "" {
		t, ok := ParseSinceDate(s)
		if !ok {
			return fmt.Errorf("invalid --since %q (expected %s)", s, DateFormatHelp)
		}
		since = t
	}

	legs := loadInternalLegs(since)
	if len(legs) == 0 {
		fmt.Printf("\n  No internal-transfer legs found locally. Run %schb generate%s first.\n\n", Fmt.Cyan, Fmt.Reset)
		return nil
	}

	var net float64
	for _, l := range legs {
		net += l.amount
	}

	orphans, hashPairs, amountPairs := pairInternalLegs(legs)

	if csv {
		fmt.Println("date,account,provider,amount,currency,counterparty,description,id")
		for _, l := range orphans {
			fmt.Printf("%s,%s,%s,%.2f,%s,%s,%s,%s\n",
				internalLegDate(l.tx.Timestamp), csvCell(l.slug), csvCell(l.tx.Provider),
				l.amount, csvCell(l.tx.Currency), csvCell(internalLegCounterpartyRef(l.tx)),
				csvCell(internalLegDesc(l.tx)), csvCell(l.tx.ID))
		}
		return nil
	}

	fmt.Printf("\n%s🔁 Internal transfers — local reconciliation%s\n", Fmt.Bold, Fmt.Reset)
	if !since.IsZero() {
		fmt.Printf("  %ssince %s (earlier transactions excluded — covered by opening balances)%s\n", Fmt.Dim, since.Format("2006-01-02"), Fmt.Reset)
	}
	fmt.Printf("\n  %d internal legs · %d cancelled on-chain (by hash) · %d cancelled by amount · %s%d orphan%s\n",
		len(legs), hashPairs*2, amountPairs*2, Fmt.Bold, len(orphans), Fmt.Reset)

	// Per-account orphan breakdown, largest |net| last (most-negative first).
	type acctAgg struct {
		slug  string
		net   float64
		count int
	}
	byAcct := map[string]*acctAgg{}
	for _, l := range orphans {
		a := byAcct[l.slug]
		if a == nil {
			a = &acctAgg{slug: l.slug}
			byAcct[l.slug] = a
		}
		a.net += l.amount
		a.count++
	}
	aggs := make([]*acctAgg, 0, len(byAcct))
	for _, a := range byAcct {
		aggs = append(aggs, a)
	}
	sort.Slice(aggs, func(i, j int) bool { return aggs[i].net < aggs[j].net })

	if len(aggs) > 0 {
		fmt.Printf("\n  %sUnmatched by account:%s\n", Fmt.Bold, Fmt.Reset)
		for _, a := range aggs {
			fmt.Printf("    %-28s %s%12s%s  %s(%d)%s\n",
				a.slug, internalAmtColor(a.net), signPrefix(a.net)+fmtNumber(math.Abs(a.net)), Fmt.Reset,
				Fmt.Dim, a.count, Fmt.Reset)
		}
	}

	if verbose && len(orphans) > 0 {
		fmt.Printf("\n  %sOrphan legs:%s\n", Fmt.Bold, Fmt.Reset)
		sort.Slice(orphans, func(i, j int) bool { return orphans[i].tx.Timestamp < orphans[j].tx.Timestamp })
		for _, l := range orphans {
			fmt.Printf("    %s  %-22s %s%12s%s  %s%-22s%s  %s\n",
				internalLegDate(l.tx.Timestamp), Truncate(l.slug, 22),
				internalAmtColor(l.amount), signPrefix(l.amount)+fmtNumber(math.Abs(l.amount)), Fmt.Reset,
				Fmt.Dim, Truncate(internalLegCounterpartyRef(l.tx), 22), Fmt.Reset,
				Truncate(internalLegDesc(l.tx), 34))
		}
	}

	fmt.Println()
	if math.Abs(net) < 0.005 {
		fmt.Printf("  %s✓ Internal transfers net to zero.%s\n\n", Fmt.Green, Fmt.Reset)
	} else {
		fmt.Printf("  %s⚠ Internal transfers net to %s%s EUR%s — should be zero.%s\n",
			Fmt.Yellow, signPrefix(net), fmtNumber(math.Abs(net)), Fmt.Reset, "")
		fmt.Printf("  %sThe orphan legs above carry the full imbalance. Use --verbose to list them.%s\n\n", Fmt.Dim, Fmt.Reset)
	}
	return nil
}

// internalLeg is one INTERNAL / internal_transfer transaction leg, with its
// signed balance impact pulled into `amount` for pairing.
type internalLeg struct {
	slug   string
	amount float64
	tx     TransactionEntry
}

// loadInternalLegs reads every generated transactions.json (with PII so tx
// hashes are present) and returns the internal-transfer legs, deduped by
// (id, account, sign) so a leg listed twice doesn't distort the net.
func loadInternalLegs(since time.Time) []internalLeg {
	dataDir := DataDir()
	var legs []internalLeg
	seen := map[string]bool{}
	sinceUnix := int64(0)
	if !since.IsZero() {
		sinceUnix = since.Unix()
	}

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
			txFile := loadTransactionsFile(dataDir, yd.Name(), md.Name(), true)
			if txFile == nil {
				continue
			}
			for _, tx := range txFile.Transactions {
				if tx.Type != "INTERNAL" && tx.Category != "internal_transfer" {
					continue
				}
				if sinceUnix > 0 && tx.Timestamp < sinceUnix {
					continue
				}
				amt := tx.NormalizedAmount
				if amt == 0 {
					amt = tx.Amount
				}
				sign := "+"
				if amt < 0 {
					sign = "-"
				}
				key := tx.ID + "|" + tx.AccountSlug + "|" + sign + "|" + fmt.Sprintf("%.2f", math.Abs(amt))
				if seen[key] {
					continue
				}
				seen[key] = true
				slug := tx.AccountSlug
				if slug == "" {
					slug = tx.Provider
				}
				legs = append(legs, internalLeg{slug: slug, amount: roundCents(amt), tx: tx})
			}
		}
	}
	return legs
}

// pairInternalLegs cancels legs in two passes (on-chain by hash, then by equal
// magnitude across accounts) and returns the unpaired remainder plus the count
// of pairs removed in each pass. The remainder always sums to the same net as
// the input — only cancelling pairs are removed.
func pairInternalLegs(legs []internalLeg) (orphans []internalLeg, hashPairs, amountPairs int) {
	// Pass 1: group on-chain legs by tx hash; a hash whose legs net to zero is a
	// fully-captured wallet→wallet transfer.
	byHash := map[string][]int{}
	remaining := make([]bool, len(legs))
	for i, l := range legs {
		remaining[i] = true
		if h := strings.ToLower(l.tx.TxHash); h != "" {
			byHash[h] = append(byHash[h], i)
		}
	}
	for _, idxs := range byHash {
		if len(idxs) < 2 {
			continue
		}
		var sum float64
		for _, i := range idxs {
			sum += legs[i].amount
		}
		if math.Abs(sum) < 0.005 {
			for _, i := range idxs {
				remaining[i] = false
			}
			hashPairs += len(idxs) / 2
		}
	}

	// Pass 2: link the rest as cross-provider transfers — opposite sign, equal
	// magnitude, on two *different* accounts, close in time (a transfer can't go
	// from an account to itself, and the two legs settle within days). Greedy,
	// nearest-date-first, so the attribution of what's left is meaningful per
	// account rather than an arbitrary global cancellation.
	const pairWindow = 31 * 24 * 60 * 60 // seconds
	posByAmt := map[string][]int{}
	var negIdx []int
	for i, keep := range remaining {
		if !keep {
			continue
		}
		l := legs[i]
		key := fmt.Sprintf("%.2f", math.Abs(l.amount))
		if l.amount > 0 {
			posByAmt[key] = append(posByAmt[key], i)
		} else if l.amount < 0 {
			negIdx = append(negIdx, i)
		}
	}
	// Match the earliest debits first for stable, deterministic pairing.
	sort.Slice(negIdx, func(a, b int) bool { return legs[negIdx[a]].tx.Timestamp < legs[negIdx[b]].tx.Timestamp })
	for _, ni := range negIdx {
		neg := legs[ni]
		key := fmt.Sprintf("%.2f", math.Abs(neg.amount))
		best, bestPos := int64(-1), -1
		for _, pi := range posByAmt[key] {
			if !remaining[pi] {
				continue
			}
			pos := legs[pi]
			if pos.slug == neg.slug {
				continue // a transfer has two distinct accounts
			}
			d := neg.tx.Timestamp - pos.tx.Timestamp
			if d < 0 {
				d = -d
			}
			if d > pairWindow {
				continue
			}
			if best < 0 || d < best {
				best, bestPos = d, pi
			}
		}
		if bestPos >= 0 {
			remaining[ni] = false
			remaining[bestPos] = false
			amountPairs++
		}
	}

	// Pass 2b: cancel any leftover equal-opposite pair on the *same* account —
	// these are reversals/corrections (e.g. a Stripe payout and its
	// payout-reversal), which net to zero and aren't part of the imbalance.
	sameAcct := map[string]*struct{ pos, neg []int }{}
	for i, keep := range remaining {
		if !keep || legs[i].amount == 0 {
			continue
		}
		key := legs[i].slug + "|" + fmt.Sprintf("%.2f", math.Abs(legs[i].amount))
		b := sameAcct[key]
		if b == nil {
			b = &struct{ pos, neg []int }{}
			sameAcct[key] = b
		}
		if legs[i].amount > 0 {
			b.pos = append(b.pos, i)
		} else {
			b.neg = append(b.neg, i)
		}
	}
	for _, b := range sameAcct {
		n := len(b.pos)
		if len(b.neg) < n {
			n = len(b.neg)
		}
		amountPairs += n
		for k := 0; k < n; k++ {
			remaining[b.pos[k]] = false
			remaining[b.neg[k]] = false
		}
	}

	for i, keep := range remaining {
		if keep {
			orphans = append(orphans, legs[i])
		}
	}
	return orphans, hashPairs, amountPairs
}

// internalLegDate formats a unix timestamp as an ISO date in Brussels time.
func internalLegDate(ts int64) string {
	return time.Unix(ts, 0).In(BrusselsTZ()).Format("2006-01-02")
}

// internalAmtColor greens credits and reds debits for the amount column.
func internalAmtColor(v float64) string {
	if v < 0 {
		return Fmt.Red
	}
	return Fmt.Green
}

// internalLegCounterpartyRef returns a stable identifier for the other side of
// an internal leg: the counterpart IBAN when known (fiat/Monerium, loaded from
// PII enrichment), otherwise a shortened 0x address for on-chain transfers.
func internalLegCounterpartyRef(tx TransactionEntry) string {
	if tx.Metadata != nil {
		if iban, ok := tx.Metadata["iban"].(string); ok && iban != "" {
			return iban
		}
	}
	addr := ""
	if id := tx.CounterpartyID; strings.Contains(id, "0x") {
		if i := strings.LastIndex(id, "0x"); i >= 0 {
			addr = id[i:]
		}
	}
	if addr == "" && strings.HasPrefix(tx.Counterparty, "0x") {
		addr = tx.Counterparty
	}
	if len(addr) >= 12 {
		return addr[:6] + "…" + addr[len(addr)-4:]
	}
	return addr
}

// internalLegDesc picks the most useful human label for an internal leg.
func internalLegDesc(tx TransactionEntry) string {
	if tx.Counterparty != "" {
		return tx.Counterparty
	}
	if tx.Metadata != nil {
		if d, ok := tx.Metadata["description"].(string); ok && d != "" {
			return d
		}
	}
	return tx.AccountName
}

func printAccountsInternalHelp() {
	f := Fmt
	fmt.Printf(`
%schb accounts internal%s — Audit internal-transfer legs across all local accounts.

Every transfer between two of our own accounts books two opposite-sign legs
tagged INTERNAL. Summed over all accounts they must net to zero; what doesn't
cancel is an orphan — a missing counter-leg or a misclassified external payment.

%sUSAGE%s
  %schb accounts internal%s              Summary + per-account orphan breakdown
  %schb accounts internal --since 2025%s  Ignore pre-cutoff legs (opening balances)
  %schb accounts internal --verbose%s    Also list every orphan leg (with counterparty)
  %schb accounts internal --csv%s        Orphan legs as CSV

%sNOTES%s
  Reads generated/transactions.json (run %schb generate%s first). Read-only.
  %s--verbose%s/%s--csv%s show the counterpart IBAN (fiat) or short 0x (on-chain).
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
	)
}
