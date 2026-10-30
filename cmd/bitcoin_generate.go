package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"

	bitcoinsource "github.com/CommonsHub/chb/providers/bitcoin"
)

const satsPerBTC = 100_000_000

// satsToBTC converts sats to BTC, rounded to 8 decimals.
func satsToBTC(sats int64) float64 {
	return math.Round(float64(sats)) / satsPerBTC
}

// BuildBitcoinTxURI is the NIP-73 id of a Bitcoin transaction.
func BuildBitcoinTxURI(txid string) string {
	return "bitcoin:tx:" + strings.ToLower(strings.TrimSpace(txid))
}

// BuildBitcoinAddressURI is the NIP-73 id of a Bitcoin address.
func BuildBitcoinAddressURI(address string) string {
	return "bitcoin:address:" + strings.TrimSpace(address)
}

// bitcoinAccounts returns the configured bitcoin accounts.
func bitcoinAccounts() []AccountConfig {
	var out []AccountConfig
	for _, acc := range LoadAccountConfigs() {
		if strings.EqualFold(acc.Provider, bitcoinsource.Source) && acc.Address != "" {
			out = append(out, acc)
		}
	}
	return out
}

// bitcoinTransactionsForMonth reads providers/bitcoin/<slug>.json for every
// configured bitcoin account and maps each raw transaction to a
// TransactionEntry seen from that account's address. Amount is the signed
// balance change in BTC (fee included for spends we funded).
func bitcoinTransactionsForMonth(dataDir, year, month string) []TransactionEntry {
	if year == "latest" {
		return nil
	}
	var out []TransactionEntry
	for _, acc := range bitcoinAccounts() {
		path := bitcoinsource.Path(dataDir, year, month, bitcoinsource.FileName(acc.Slug))
		if _, err := os.Stat(path); err != nil {
			continue
		}
		cache, ok := bitcoinsource.LoadCache(path)
		if !ok {
			continue
		}
		for _, raw := range cache.Transactions {
			var tx bitcoinsource.Tx
			if json.Unmarshal(raw, &tx) != nil || tx.TxID == "" {
				continue
			}
			if entry, ok := bitcoinTxToEntry(acc, tx); ok {
				out = append(out, entry)
			}
		}
	}
	return out
}

func bitcoinTxToEntry(acc AccountConfig, tx bitcoinsource.Tx) (TransactionEntry, bool) {
	flow := tx.FlowFor(acc.Address)
	net := flow.Net()
	if net == 0 && flow.Received == 0 && flow.Spent == 0 {
		return TransactionEntry{}, false
	}
	txType := "CREDIT"
	counterparty := ""
	if net < 0 {
		txType = "DEBIT"
		if len(flow.Outputs) > 0 {
			counterparty = flow.Outputs[0]
		}
	} else if len(flow.Inputs) > 0 {
		counterparty = flow.Inputs[0]
	}
	var fee int64
	if net < 0 && flow.FundedInputs {
		fee = tx.Fee
	}

	signed := satsToBTC(net)
	metadata := map[string]interface{}{
		"sats":        net,
		"blockHeight": tx.Status.BlockHeight,
		"url":         bitcoinsource.TxURL(tx.TxID),
	}
	if fee > 0 {
		metadata["feeSats"] = fee
	}
	counterpartyURI := ""
	if counterparty != "" {
		counterpartyURI = BuildBitcoinAddressURI(counterparty)
	}
	chain := "bitcoin"
	currency := strings.ToUpper(acc.Currency)
	if currency == "" {
		currency = "BTC"
	}
	return TransactionEntry{
		ID:               BuildBitcoinTxURI(tx.TxID),
		ProviderID:       tx.TxID,
		AccountID:        BuildBitcoinAddressURI(acc.Address),
		CounterpartyID:   counterpartyURI,
		Chain:            &chain,
		TxHash:           tx.TxID,
		Provider:         bitcoinsource.Source,
		Account:          acc.Address,
		AccountSlug:      acc.Slug,
		AccountName:      acc.Name,
		Currency:         currency,
		Value:            fmt.Sprintf("%.8f", signed),
		Amount:           signed,
		NetAmount:        signed,
		GrossAmount:      math.Abs(signed),
		NormalizedAmount: signed,
		Fee:              satsToBTC(fee),
		Type:             txType,
		Counterparty:     counterparty,
		Timestamp:        tx.Status.BlockTime,
		Metadata:         metadata,
	}, true
}
