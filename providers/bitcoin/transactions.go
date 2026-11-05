package bitcoin

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const defaultAPIURL = "https://mempool.space/api"

// APIURL returns the Esplora base URL (MEMPOOL_API_URL overrides).
func APIURL() string {
	if v := strings.TrimRight(strings.TrimSpace(os.Getenv("MEMPOOL_API_URL")), "/"); v != "" {
		return v
	}
	return defaultAPIURL
}

// Tx is the subset of an Esplora transaction that generate reads.
type Tx struct {
	TxID string `json:"txid"`
	Fee  int64  `json:"fee"`
	Vin  []struct {
		Prevout *struct {
			Address string `json:"scriptpubkey_address"`
			Value   int64  `json:"value"`
		} `json:"prevout"`
		IsCoinbase bool `json:"is_coinbase"`
	} `json:"vin"`
	Vout []struct {
		Address string `json:"scriptpubkey_address"`
		Value   int64  `json:"value"`
	} `json:"vout"`
	Status struct {
		Confirmed   bool   `json:"confirmed"`
		BlockHeight int64  `json:"block_height"`
		BlockHash   string `json:"block_hash"`
		BlockTime   int64  `json:"block_time"`
	} `json:"status"`
}

// Flow is a transaction seen from one address.
type Flow struct {
	Received     int64    // sats paid to the address
	Spent        int64    // sats spent from the address (inputs)
	Inputs       []string // other input addresses (senders)
	Outputs      []string // other output addresses (recipients)
	FundedInputs bool     // every input belongs to the address (so the fee is ours)
}

// Net is the signed balance change in sats.
func (f Flow) Net() int64 { return f.Received - f.Spent }

// FlowFor computes how tx moves funds for address.
func (tx Tx) FlowFor(address string) Flow {
	var f Flow
	f.FundedInputs = len(tx.Vin) > 0
	for _, in := range tx.Vin {
		if in.Prevout == nil {
			f.FundedInputs = false
			continue
		}
		if strings.EqualFold(in.Prevout.Address, address) {
			f.Spent += in.Prevout.Value
			continue
		}
		f.FundedInputs = false
		if in.Prevout.Address != "" {
			f.Inputs = appendUnique(f.Inputs, in.Prevout.Address)
		}
	}
	for _, out := range tx.Vout {
		if strings.EqualFold(out.Address, address) {
			f.Received += out.Value
			continue
		}
		if out.Address != "" {
			f.Outputs = appendUnique(f.Outputs, out.Address)
		}
	}
	return f
}

func appendUnique(list []string, v string) []string {
	for _, x := range list {
		if x == v {
			return list
		}
	}
	return append(list, v)
}

// FetchAddressTransactions returns every confirmed transaction touching
// address, newest first, as raw JSON. Unconfirmed (mempool) transactions are
// skipped: they have no block time and may still be replaced.
func FetchAddressTransactions(address string) ([]json.RawMessage, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	base := APIURL() + "/address/" + address + "/txs/chain"
	var out []json.RawMessage
	lastSeen := ""
	for page := 0; page < 1000; page++ {
		url := base
		if lastSeen != "" {
			url += "/" + lastSeen
		}
		resp, err := client.Get(url)
		if err != nil {
			return nil, fmt.Errorf("fetch %s: %w", url, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("fetch %s: HTTP %d: %s", url, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		var batch []json.RawMessage
		if err := json.Unmarshal(body, &batch); err != nil {
			return nil, fmt.Errorf("decode %s: %w", url, err)
		}
		if len(batch) == 0 {
			break
		}
		for _, raw := range batch {
			var tx Tx
			if json.Unmarshal(raw, &tx) != nil || tx.TxID == "" {
				continue
			}
			lastSeen = tx.TxID
			if tx.Status.Confirmed {
				out = append(out, raw)
			}
		}
		// Esplora pages confirmed history 25 at a time.
		if len(batch) < 25 {
			break
		}
	}
	return out, nil
}

// FetchBalance returns the confirmed balance of address in sats.
func FetchBalance(address string) (int64, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(APIURL() + "/address/" + address)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var info struct {
		ChainStats struct {
			Funded int64 `json:"funded_txo_sum"`
			Spent  int64 `json:"spent_txo_sum"`
		} `json:"chain_stats"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return 0, err
	}
	return info.ChainStats.Funded - info.ChainStats.Spent, nil
}

// TxURL is the public explorer link for a transaction.
func TxURL(txid string) string { return "https://mempool.space/tx/" + txid }

// AddressURL is the public explorer link for an address.
func AddressURL(address string) string { return "https://mempool.space/address/" + address }
