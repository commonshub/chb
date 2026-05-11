package stripe

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

type Payout struct {
	ID                  string `json:"id"`
	Amount              int64  `json:"amount"`
	ArrivalDate         int64  `json:"arrival_date"`
	Created             int64  `json:"created"`
	Currency            string `json:"currency"`
	Status              string `json:"status"`
	Description         string `json:"description,omitempty"`
	StatementDescriptor string `json:"statement_descriptor,omitempty"`
	Automatic           bool   `json:"automatic,omitempty"`
	TxCount             int    `json:"txCount,omitempty"`
	BankLast4           string `json:"bankLast4,omitempty"`
	BankName            string `json:"bankName,omitempty"`
}

func (p Payout) StatementName(loc *time.Location) string {
	if loc == nil {
		loc = time.Local
	}
	date := time.Unix(p.ArrivalDate, 0).In(loc).Format("2006-01-02")
	amount := float64(p.Amount) / 100
	if p.BankLast4 != "" {
		return fmt.Sprintf("%s Stripe → ****%s (%.2f %s)", date, p.BankLast4, amount, strings.ToUpper(p.Currency))
	}
	return fmt.Sprintf("%s Stripe payout (%.2f %s)", date, amount, strings.ToUpper(p.Currency))
}

type PayoutsCache struct {
	FetchedAt string   `json:"fetchedAt"`
	Payouts   []Payout `json:"payouts"`
}

func PayoutsCachePath(dataDir string) string {
	return Path(dataDir, "latest", "", PayoutsFile)
}

func LoadPayoutsCache(dataDir string) *PayoutsCache {
	data, err := os.ReadFile(PayoutsCachePath(dataDir))
	if err != nil {
		return nil
	}
	var cache PayoutsCache
	if json.Unmarshal(data, &cache) != nil {
		return nil
	}
	return &cache
}

func SavePayoutsCache(dataDir string, cache *PayoutsCache) error {
	return WriteJSON(dataDir, "latest", "", cache, PayoutsFile)
}

func RebuildPayoutsCacheFromTransactions(dataDir string) ([]Payout, error) {
	bts, err := LoadTransactions(dataDir, "")
	if err != nil {
		return nil, err
	}

	var merged []Payout
	for _, bt := range bts {
		if bt.Type != "payout" {
			continue
		}
		id := firstNonEmpty(bt.PayoutID, bt.ID)
		arrival := bt.PayoutArrivalDate
		if arrival == 0 {
			arrival = bt.Created
		}
		merged = append(merged, Payout{
			ID:                  id,
			Amount:              -bt.Net,
			ArrivalDate:         arrival,
			Created:             bt.Created,
			Currency:            bt.Currency,
			Status:              "paid",
			Description:         bt.Description,
			StatementDescriptor: bt.PayoutStatementDescriptor,
			Automatic:           bt.PayoutAutomatic,
			BankLast4:           bt.PayoutBankLast4,
		})
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].ArrivalDate > merged[j].ArrivalDate
	})

	if err := SavePayoutsCache(dataDir, &PayoutsCache{
		FetchedAt: time.Now().UTC().Format(time.RFC3339),
		Payouts:   merged,
	}); err != nil {
		return nil, err
	}
	return merged, nil
}

func FilterPayoutsByMonths(payouts []Payout, monthsLimit int, now time.Time) []Payout {
	if monthsLimit <= 0 {
		return payouts
	}
	if now.IsZero() {
		now = time.Now()
	}
	cutoff := now.AddDate(0, -monthsLimit, 0).Unix()
	var filtered []Payout
	for _, po := range payouts {
		if po.ArrivalDate >= cutoff {
			filtered = append(filtered, po)
		}
	}
	return filtered
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
