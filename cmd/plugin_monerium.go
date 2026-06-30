package cmd

import (
	"os"
	"strings"

	moneriumprocessor "github.com/CommonsHub/chb/processors/monerium"
	moneriumsource "github.com/CommonsHub/chb/providers/monerium"
)

type moneriumProcessor struct {
	ordersByTxHash map[string]moneriumOrderInfo
	// ownIBANs holds the normalized IBANs of accounts we control (e.g. our KBC
	// bank account). A Monerium mint/redeem whose counterpart IBAN is one of
	// ours is money moving between two accounts we own — an internal transfer,
	// not a payment to/from a third party.
	ownIBANs map[string]bool
}

type moneriumOrderInfo struct {
	Counterparty string
	IBAN         string
	Memo         string
	State        string
	Kind         string
}

func newMoneriumProcessor() *moneriumProcessor {
	return &moneriumProcessor{}
}

func (p *moneriumProcessor) Name() string {
	return moneriumprocessor.Name
}

func (p *moneriumProcessor) EnvVars() []ProcessorEnvVar {
	return []ProcessorEnvVar{
		{Name: "MONERIUM_CLIENT_ID", Description: "Monerium OAuth client ID.", Required: false},
		{Name: "MONERIUM_CLIENT_SECRET", Description: "Monerium OAuth client secret.", Required: false},
	}
}

func (p *moneriumProcessor) WarmUp(ctx *ProcessorContext) error {
	p.ordersByTxHash = map[string]moneriumOrderInfo{}

	// Build the set of IBANs we control so redeems/mints into our own bank
	// account are recognised as internal transfers (see ProcessTransaction).
	p.ownIBANs = map[string]bool{}
	for _, acc := range LoadAccountConfigs() {
		if iban := normalizeIBAN(acc.IBAN); iban != "" {
			p.ownIBANs[iban] = true
		}
	}

	entries, err := os.ReadDir(moneriumsource.Path(ctx.DataDir, ctx.Year, ctx.Month))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		cache, ok := moneriumsource.LoadCache(moneriumsource.Path(ctx.DataDir, ctx.Year, ctx.Month, e.Name()))
		if !ok {
			continue
		}
		for _, order := range cache.Orders {
			info := moneriumOrderInfo{
				Counterparty: moneriumCounterpartyName(order),
				IBAN:         normalizeIBAN(order.Counterpart.Identifier.IBAN),
				Memo:         order.Memo,
				State:        order.State,
				Kind:         order.Kind,
			}
			for _, hash := range order.Meta.TxHashes {
				if hash != "" {
					p.ordersByTxHash[strings.ToLower(hash)] = info
				}
			}
		}
	}
	return nil
}

func (p *moneriumProcessor) ProcessTransaction(ctx *ProcessorContext, tx *TransactionEntry) error {
	if tx.Provider != "etherscan" || tx.TxHash == "" {
		return nil
	}
	info, ok := p.ordersByTxHash[strings.ToLower(tx.TxHash)]
	if !ok {
		return nil
	}

	if info.Counterparty != "" && strings.HasPrefix(tx.Counterparty, "0x") {
		tx.Counterparty = info.Counterparty
	}
	if tx.Metadata == nil {
		tx.Metadata = map[string]interface{}{}
	}
	if info.IBAN != "" {
		// IBAN is PII; the public/private split moves the "iban" metadata
		// key into private/enrichment.json so it never reaches the public file.
		tx.Metadata["iban"] = info.IBAN

		// A redeem into (or mint from) one of our own bank accounts is an
		// internal transfer — the EURe leaving the wallet lands in our KBC
		// account, not with a third party. Mark it INTERNAL so push routes it
		// to the internal-transfer account (580000) and never tries to match
		// it against a bill/invoice. Preserve the original on-chain direction
		// for downstream labels (Amount already carries the sign).
		if p.ownIBANs[info.IBAN] && !strings.EqualFold(tx.Type, "INTERNAL") {
			if tx.Type != "" {
				tx.Metadata["direction"] = tx.Type
			}
			tx.Type = "INTERNAL"
		}
	}
	if info.Memo != "" {
		tx.Metadata["memo"] = info.Memo
		if tx.Metadata["description"] == nil || tx.Metadata["description"] == "" {
			tx.Metadata["description"] = info.Memo
		}
	}
	if info.State != "" {
		tx.Metadata["moneriumState"] = info.State
		addTransactionTag(&tx.Tags, "status", info.State)
	}
	if info.Kind != "" {
		tx.Metadata["moneriumKind"] = info.Kind
	}
	addTransactionTag(&tx.Tags, "source", "monerium")
	tx.Tags = normalizeTransactionTags(tx.Tags)
	return nil
}

func (p *moneriumProcessor) ProcessEvent(ctx *ProcessorContext, ev *FullEvent) error {
	return nil
}

func (p *moneriumProcessor) Flush(ctx *ProcessorContext) error {
	return nil
}

// normalizeIBAN strips whitespace and upper-cases an IBAN so we store the
// machine-form (ISO 13616) regardless of how the source formatted it.
func normalizeIBAN(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case ' ', '\t', '\n', '\r':
			continue
		}
		b.WriteRune(r)
	}
	return strings.ToUpper(b.String())
}

func moneriumCounterpartyName(order moneriumsource.Order) string {
	name := order.Counterpart.Details.CompanyName
	if name == "" {
		name = order.Counterpart.Details.Name
	}
	if name == "" && order.Counterpart.Details.FirstName != "" {
		name = strings.TrimSpace(order.Counterpart.Details.FirstName + " " + order.Counterpart.Details.LastName)
	}
	return name
}
