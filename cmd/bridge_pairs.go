package cmd

import (
	"math"
	"sort"
	"strings"
)

// Bridge pairing: EURe moved between two of our own accounts on different
// chains (Monerium "redeem on Polygon, issue on Gnosis") shows up as a BURN on
// one account and a MINT of the same amount on another, a few seconds to a few
// days apart, both carrying the SEPA memo of the original payment. Neither leg
// has an own-address counterparty (both sides are the zero address), so the
// own-account checks never see them as internal, and the memo often makes a
// rule categorise the MINT as income ("Financial contribution …" → loan).
//
// pairBridgeTransfers runs after the rules and tags both legs
// internal_transfer, overriding the rules: the money was already counted when
// it first arrived. It never touches a leg whose category came from a trusted
// Nostr annotation (skip), and it only pairs within the month being generated.

// bridgePairWindowSeconds is how far apart the two legs may be.
const bridgePairWindowSeconds = 3 * 24 * 3600

// bridgePairDescPrefix is how many leading characters of the description
// both legs must share (case-insensitive). Monerium copies the memo onto
// both legs, sometimes truncated, so compare a prefix.
const bridgePairDescPrefix = 20

func bridgeLegDescription(tx TransactionEntry) string {
	d := strings.TrimSpace(stringMetadata(tx.Metadata, "description"))
	if d == "" {
		d = strings.TrimSpace(stringMetadata(tx.Metadata, "memo"))
	}
	d = strings.ToLower(d)
	if r := []rune(d); len(r) > bridgePairDescPrefix {
		d = string(r[:bridgePairDescPrefix])
	}
	return d
}

func isBridgeCandidate(tx TransactionEntry) bool {
	return tx.Provider != "stripe" && (tx.Type == "BURN" || tx.Type == "MINT")
}

// pairBridgeTransfers pairs each BURN with the earliest unused MINT of the
// opposite amount, in the same currency, on another account, within the
// window and with the same (non-empty) description prefix. It returns the
// number of pairs tagged. skip holds indexes of transactions whose category a
// trusted annotation set; they are neither tagged nor used as a counterpart.
func pairBridgeTransfers(txs []TransactionEntry, skip map[int]bool) int {
	var burns, mints []int
	for i := range txs {
		if skip[i] || !isBridgeCandidate(txs[i]) || bridgeLegDescription(txs[i]) == "" {
			continue
		}
		if txs[i].Type == "BURN" {
			burns = append(burns, i)
		} else {
			mints = append(mints, i)
		}
	}
	byTime := func(idx []int) {
		sort.SliceStable(idx, func(a, b int) bool { return txs[idx[a]].Timestamp < txs[idx[b]].Timestamp })
	}
	byTime(burns)
	byTime(mints)

	used := map[int]bool{}
	pairs := 0
	for _, b := range burns {
		burn := txs[b]
		desc := bridgeLegDescription(burn)
		for _, m := range mints {
			mint := txs[m]
			if used[m] || strings.EqualFold(mint.AccountSlug, burn.AccountSlug) ||
				!strings.EqualFold(mint.Currency, burn.Currency) {
				continue
			}
			if math.Abs(mint.Amount+burn.Amount) >= 0.005 {
				continue
			}
			dt := mint.Timestamp - burn.Timestamp
			if dt < 0 {
				dt = -dt
			}
			if dt >= bridgePairWindowSeconds || bridgeLegDescription(mint) != desc {
				continue
			}
			used[m] = true
			tagBridgeLeg(&txs[b], mint)
			tagBridgeLeg(&txs[m], burn)
			pairs++
			break
		}
	}
	return pairs
}

func tagBridgeLeg(tx *TransactionEntry, other TransactionEntry) {
	tx.Category = "internal_transfer"
	tx.Type = "INTERNAL"
	setMetadata(tx, "bridgePair", other.ID)
	stampVAT(tx)
}
