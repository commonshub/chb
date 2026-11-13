package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"

	coinossource "github.com/CommonsHub/chb/providers/coinos"
)

// BuildCoinosPaymentURI is the id of a coinos payment.
func BuildCoinosPaymentURI(id string) string {
	return "coinos:payment:" + strings.TrimSpace(id)
}

// BuildCoinosUserURI is the id of a coinos wallet (username).
func BuildCoinosUserURI(username string) string {
	return "coinos:user:" + strings.ToLower(strings.TrimSpace(username))
}

func coinosAccounts() []AccountConfig {
	var out []AccountConfig
	for _, acc := range LoadAccountConfigs() {
		if strings.EqualFold(acc.Provider, coinossource.Source) {
			out = append(out, acc)
		}
	}
	return out
}

// coinosTransactionsForMonth reads providers/coinos/<slug>.json for every
// configured coinos account and maps each settled payment to a
// TransactionEntry. Amount is the signed balance change in BTC: outgoing
// payments include the network fee and the coinos fee.
func coinosTransactionsForMonth(dataDir, year, month string) []TransactionEntry {
	if year == "latest" {
		return nil
	}
	var out []TransactionEntry
	for _, acc := range coinosAccounts() {
		path := coinossource.Path(dataDir, year, month, coinossource.FileName(acc.Slug))
		if _, err := os.Stat(path); err != nil {
			continue
		}
		cache, ok := coinossource.LoadCache(path)
		if !ok {
			continue
		}
		for _, raw := range cache.Payments {
			var p coinossource.Payment
			if json.Unmarshal(raw, &p) != nil || p.ID == "" || p.Amount == 0 {
				continue
			}
			if p.Confirmed != nil && !*p.Confirmed {
				continue // pending on-chain receive
			}
			out = append(out, coinosPaymentToEntry(acc, p))
		}
	}
	return out
}

func coinosPaymentToEntry(acc AccountConfig, p coinossource.Payment) TransactionEntry {
	net := p.Amount
	var fee int64
	txType := "CREDIT"
	if p.Amount < 0 {
		txType = "DEBIT"
		fee = p.Fee + p.OurFee
		net = p.Amount - fee
	}
	signed := satsToBTC(net)

	metadata := map[string]interface{}{
		"sats": net,
		"type": p.Type,
	}
	if memo := strings.TrimSpace(p.Memo); memo != "" {
		metadata["description"] = memo
	}
	if fee > 0 {
		metadata["feeSats"] = fee
	}
	if p.Hash != "" {
		metadata["hash"] = p.Hash
	}
	if p.Rate > 0 && p.Currency != "" {
		// Fiat value at payment time, as coinos recorded it.
		metadata["fiatCurrency"] = strings.ToUpper(p.Currency)
		metadata["fiatAmount"] = roundCents(float64(net) / satsPerBTC * p.Rate)
		metadata["rate"] = p.Rate
	}

	counterparty := ""
	counterpartyURI := ""
	if p.With != nil && p.With.Username != "" {
		counterparty = p.With.Username
		counterpartyURI = BuildCoinosUserURI(p.With.Username)
	}
	currency := strings.ToUpper(acc.Currency)
	if currency == "" {
		currency = "BTC"
	}
	return TransactionEntry{
		ID:               BuildCoinosPaymentURI(p.ID),
		ProviderID:       p.ID,
		AccountID:        BuildCoinosUserURI(acc.AccountID),
		CounterpartyID:   counterpartyURI,
		TxHash:           p.ID,
		Provider:         coinossource.Source,
		Account:          acc.AccountID,
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
		Timestamp:        p.Created / 1000,
		Metadata:         metadata,
	}
}

// markBitcoinInternalTransfers flags transfers between our coinos wallet and
// our own bitcoin address (a coinos on-chain payment whose hash is a txid of
// a tracked address) as INTERNAL on both sides, keeping the direction.
func markBitcoinInternalTransfers(txs []TransactionEntry) {
	onchain := map[string]bool{}
	for _, tx := range txs {
		if tx.Provider == "bitcoin" {
			onchain[strings.ToLower(tx.TxHash)] = true
		}
	}
	linked := map[string]bool{}
	for i := range txs {
		tx := &txs[i]
		if tx.Provider != coinossource.Source {
			continue
		}
		hash := strings.ToLower(stringMetadata(tx.Metadata, "hash"))
		if hash != "" && onchain[hash] {
			linked[hash] = true
			markInternal(tx)
		}
	}
	for i := range txs {
		if txs[i].Provider == "bitcoin" && linked[strings.ToLower(txs[i].TxHash)] {
			markInternal(&txs[i])
		}
	}
}

func markInternal(tx *TransactionEntry) {
	if tx.Type == "INTERNAL" {
		return
	}
	if tx.Metadata == nil {
		tx.Metadata = map[string]interface{}{}
	}
	tx.Metadata["direction"] = tx.Type
	tx.Type = "INTERNAL"
}
