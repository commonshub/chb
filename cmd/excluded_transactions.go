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
			excludedOnchainSet[excludedOnchainKey(t.Chain, t.Hash, t.To)] = true
		}
	})
	return excludedOnchainSet
}

// isExcludedOnchainTx reports whether the (chain, hash, to) transfer is on the
// exclusion list.
func isExcludedOnchainTx(chain, hash, to string) bool {
	return loadExcludedOnchainTxs()[excludedOnchainKey(chain, hash, to)]
}
