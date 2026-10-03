package cmd

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
)

// excludedOnchainTx is one entry in settings/excluded-transactions.json — an
// on-chain transfer that must be dropped from generated accounting data and
// from local balance reconciliation because counting it would double-count.
// The canonical case is the EURe V1->V2 migration mint, which re-issues a
// wallet's legacy-contract balance on the new contract (the legacy balance is
// already recorded via the legacy-contract transfers).
type excludedOnchainTx struct {
	Chain  string  `json:"chain"`
	Hash   string  `json:"hash"`
	To     string  `json:"to"`
	Amount float64 `json:"amount,omitempty"`
	Reason string  `json:"reason,omitempty"`
	// Keep: the transaction stays in transactions.json, marked
	// metadata.excluded = reason, and is left out of every total (test
	// mints, for example). Without it the transfer is dropped. The hash
	// alone identifies it (chain and to are optional).
	Keep bool `json:"keep,omitempty"`
}

type excludedTransactionsFile struct {
	Description  string              `json:"description,omitempty"`
	Transactions []excludedOnchainTx `json:"transactions"`
}

var (
	excludedOnchainOnce sync.Once
	excludedOnchainSet  map[string]bool
)

// excludedOnchainKey is the lookup key for an excluded transfer: chain, tx
// hash and recipient, lower-cased. The recipient is included so a single batch
// migration tx (one hash, several recipients) can exclude exactly the legs that
// hit our wallets.
func excludedOnchainKey(chain, hash, to string) string {
	return strings.ToLower(chain) + "|" + strings.ToLower(hash) + "|" + strings.ToLower(to)
}

// loadExcludedOnchainTxs returns the set of excluded transfers keyed by
// excludedOnchainKey. Loaded once; missing/invalid file yields an empty set.
func loadExcludedOnchainTxs() map[string]bool {
	excludedOnchainOnce.Do(func() {
		excludedOnchainSet = map[string]bool{}
		data, err := os.ReadFile(settingsFilePath("excluded-transactions.json"))
		if err != nil {
			return
		}
		var f excludedTransactionsFile
		if json.Unmarshal(data, &f) != nil {
			return
		}
		for _, t := range f.Transactions {
			if !t.Keep {
				excludedOnchainSet[excludedOnchainKey(t.Chain, t.Hash, t.To)] = true
			}
		}
	})
	return excludedOnchainSet
}

// isExcludedOnchainTx reports whether the (chain, hash, to) transfer is on the
// exclusion list.
func isExcludedOnchainTx(chain, hash, to string) bool {
	return loadExcludedOnchainTxs()[excludedOnchainKey(chain, hash, to)]
}

// Excluded-but-kept transactions: one registry for every total.
//
// Two sources mark a transaction excluded: excluded-transactions.json
// entries with "keep": true, and trusted Nostr annotations carrying an
// ["exclude", "<reason>"] tag (docs/annotations.md). An excluded
// transaction stays in transactions.json with metadata.excluded = reason
// and is skipped by summary.json, contributors.json token totals, the
// token report and coverage.

type txExclusions struct {
	byURI  map[string]string // canonical URI → reason
	byHash map[string]string // lower-case tx hash → reason (on-chain)
}

var (
	txExclusionsMu    sync.Mutex
	txExclusionsCache = map[string]*txExclusions{}
)

// resetTxExclusions forgets the cached registry (after a Nostr pull, in
// tests).
func resetTxExclusions() {
	txExclusionsMu.Lock()
	txExclusionsCache = map[string]*txExclusions{}
	txExclusionsMu.Unlock()
}

func loadTxExclusions(dataDir string) *txExclusions {
	txExclusionsMu.Lock()
	defer txExclusionsMu.Unlock()
	if e, ok := txExclusionsCache[dataDir]; ok {
		return e
	}
	e := &txExclusions{byURI: map[string]string{}, byHash: map[string]string{}}
	if data, err := os.ReadFile(settingsFilePath("excluded-transactions.json")); err == nil {
		var f excludedTransactionsFile
		if json.Unmarshal(data, &f) == nil {
			for _, t := range f.Transactions {
				if t.Keep && t.Hash != "" {
					e.byHash[strings.ToLower(t.Hash)] = exclusionReason(t.Reason)
				}
			}
		}
	}
	for uri, a := range loadTransactionAnnotations(dataDir) {
		if a.Exclude == "" {
			continue
		}
		e.byURI[uri] = a.Exclude
		if i := strings.LastIndex(uri, ":tx:"); i >= 0 {
			e.byHash[strings.ToLower(uri[i+4:])] = a.Exclude
		}
	}
	txExclusionsCache[dataDir] = e
	return e
}

func exclusionReason(r string) string {
	if r = strings.TrimSpace(r); r == "" {
		return "excluded"
	}
	return r
}

// hash returns the reason an on-chain transfer is excluded, or "".
func (e *txExclusions) hash(h string) string {
	if e == nil || h == "" {
		return ""
	}
	return e.byHash[strings.ToLower(h)]
}

// reasonFor returns why a transaction is excluded, or "".
func (e *txExclusions) reasonFor(tx TransactionEntry) string {
	if e == nil {
		return ""
	}
	if r := e.byURI[tx.ID]; r != "" {
		return r
	}
	for _, h := range []string{tx.TxHash, tx.ProviderID} {
		if strings.HasPrefix(strings.ToLower(h), "0x") {
			if r := e.hash(h); r != "" {
				return r
			}
		}
	}
	return ""
}

// isExcludedTx: a transaction marked metadata.excluded (by generate).
func isExcludedTx(tx TransactionEntry) bool {
	if tx.Metadata == nil {
		return false
	}
	r, _ := tx.Metadata["excluded"].(string)
	return r != ""
}
