package cmd

import (
	"os"
	"strings"

	moneriumprocessor "github.com/CommonsHub/chb/processors/monerium"
	moneriumsource "github.com/CommonsHub/chb/sources/monerium"
)

type moneriumProcessor struct {
	ordersByTxHash map[string]moneriumOrderInfo
}

type moneriumOrderInfo struct {
	Counterparty string
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
