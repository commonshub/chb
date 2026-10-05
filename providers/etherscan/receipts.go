package etherscan

// Log indexes from transaction receipts. A transaction with several
// transfers touching one account (a burn and a mint, two identical mints)
// cannot be identified by its position in the explorer's list: the order
// is not stable across fetches. The receipt's Transfer logs give each
// transfer its real log index.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"
)

const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// ReceiptTransfer is one ERC-20 Transfer log of a receipt.
type ReceiptTransfer struct {
	LogIndex int
	Token    string
	From     string
	To       string
	Value    *big.Int
}

var receiptHTTP = &http.Client{Timeout: 30 * time.Second}

// FetchReceiptTransfers reads a transaction receipt over JSON-RPC.
var FetchReceiptTransfers = func(rpcURL, hash string) ([]ReceiptTransfer, error) {
	body, _ := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionReceipt", "params": []string{hash}})
	resp, err := receiptHTTP.Post(rpcURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Result *struct {
			Logs []struct {
				Address  string   `json:"address"`
				Topics   []string `json:"topics"`
				Data     string   `json:"data"`
				LogIndex string   `json:"logIndex"`
			} `json:"logs"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Error != nil {
		return nil, fmt.Errorf("rpc: %s", out.Error.Message)
	}
	if out.Result == nil {
		return nil, fmt.Errorf("no receipt for %s", hash)
	}
	var transfers []ReceiptTransfer
	for _, l := range out.Result.Logs {
		if len(l.Topics) < 3 || !strings.EqualFold(l.Topics[0], transferTopic) {
			continue
		}
		v, ok := new(big.Int).SetString(strings.TrimPrefix(l.Data, "0x"), 16)
		if !ok {
			v = new(big.Int)
		}
		idx, ok := new(big.Int).SetString(strings.TrimPrefix(l.LogIndex, "0x"), 16)
		if !ok {
			continue
		}
		transfers = append(transfers, ReceiptTransfer{
			LogIndex: int(idx.Int64()),
			Token:    strings.ToLower(l.Address),
			From:     "0x" + strings.ToLower(l.Topics[1][len(l.Topics[1])-40:]),
			To:       "0x" + strings.ToLower(l.Topics[2][len(l.Topics[2])-40:]),
			Value:    v,
		})
	}
	return transfers, nil
}

// MultiTransferHashes returns the hashes that appear more than once in txs.
func MultiTransferHashes(txs []TokenTransfer) map[string]int {
	n := map[string]int{}
	for _, t := range txs {
		n[strings.ToLower(t.Hash)]++
	}
	for h, c := range n {
		if c < 2 {
			delete(n, h)
		}
	}
	return n
}

// AssignLogIndexes matches transfers of one transaction to its receipt's
// Transfer logs (same from, to and value, in log order) and sets LogIndex.
// Returns how many were assigned; transfers without a match keep nil.
func AssignLogIndexes(txs []*TokenTransfer, receipt []ReceiptTransfer) int {
	used := map[int]bool{}
	assigned := 0
	for _, t := range txs {
		if t.LogIndex != nil {
			used[*t.LogIndex] = true
		}
	}
	for _, t := range txs {
		if t.LogIndex != nil {
			continue
		}
		v, ok := new(big.Int).SetString(t.Value, 10)
		if !ok {
			continue
		}
		for _, r := range receipt {
			if used[r.LogIndex] || !strings.EqualFold(r.From, t.From) || !strings.EqualFold(r.To, t.To) || r.Value.Cmp(v) != 0 {
				continue
			}
			idx := r.LogIndex
			t.LogIndex = &idx
			used[idx] = true
			assigned++
			break
		}
	}
	return assigned
}

// EnrichLogIndexes fills LogIndex for every multi-transfer transaction in
// txs that lacks one, fetching each receipt once. Returns the number of
// transfers assigned and the first error (the rest are still tried).
func EnrichLogIndexes(txs []TokenTransfer, rpcURL string) (int, error) {
	if rpcURL == "" {
		return 0, nil
	}
	byHash := map[string][]*TokenTransfer{}
	for i := range txs {
		h := strings.ToLower(txs[i].Hash)
		byHash[h] = append(byHash[h], &txs[i])
	}
	assigned := 0
	var firstErr error
	for h := range MultiTransferHashes(txs) {
		group := byHash[h]
		missing := false
		for _, t := range group {
			if t.LogIndex == nil {
				missing = true
			}
		}
		if !missing {
			continue
		}
		receipt, err := FetchReceiptTransfers(rpcURL, group[0].Hash)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		assigned += AssignLogIndexes(group, receipt)
	}
	return assigned, firstErr
}
