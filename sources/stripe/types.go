package stripe

import "encoding/json"

const (
	Source                  = "stripe"
	BalanceTransactionsFile = "balance-transactions.json"
	ChargesFile             = "charges.json"
	CustomersFile           = "customers.json"
	SubscriptionsFile       = "subscriptions.json"
	PayoutsFile             = "payouts.json"
)

// Transaction represents a Stripe balance transaction archived by the source.
type Transaction struct {
	ID                string                 `json:"id"`
	Created           int64                  `json:"created"`
	Amount            int64                  `json:"amount"`
	Fee               int64                  `json:"fee"`
	Net               int64                  `json:"net"`
	Currency          string                 `json:"currency"`
	Type              string                 `json:"type"`
	Description       string                 `json:"description,omitempty"`
	Source            json.RawMessage        `json:"source,omitempty"`
	ReportingCategory string                 `json:"reporting_category"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
	CustomerName      string                 `json:"customerName,omitempty"`
	CustomerEmail     string                 `json:"customerEmail,omitempty"`
	ChargeID          string                 `json:"chargeId,omitempty"`

	PayoutID                  string `json:"payoutId,omitempty"`
	PayoutAutomatic           bool   `json:"payoutAutomatic,omitempty"`
	PayoutStatementDescriptor string `json:"payoutStatementDescriptor,omitempty"`
	PayoutBankLast4           string `json:"payoutBankLast4,omitempty"`
	PayoutArrivalDate         int64  `json:"payoutArrivalDate,omitempty"`
}

// ListResponse is the response from /v1/balance_transactions.
type ListResponse struct {
	Data    []Transaction `json:"data"`
	HasMore bool          `json:"has_more"`
}

// CacheFile is the monthly source archive saved to disk.
type CacheFile struct {
	Transactions []Transaction `json:"transactions"`
	CachedAt     string        `json:"cachedAt"`
	AccountID    string        `json:"accountId,omitempty"`
	Currency     string        `json:"currency"`
}

// CustomerData holds PII extracted from Stripe charges.
type CustomerData struct {
	FetchedAt string                  `json:"fetchedAt"`
	Customers map[string]*CustomerPII `json:"customers"`
}

type CustomerPII struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email,omitempty"`
}
