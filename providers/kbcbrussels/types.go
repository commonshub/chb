package kbcbrussels

const (
	Source = "kbcbrussels"
)

// Transaction is one parsed row from a KBC Brussels CSV export.
// The CSV is a manual download — there is no upstream API.
type Transaction struct {
	// AccountIBAN is the account this transaction belongs to, e.g. BE46…
	AccountIBAN string `json:"accountIban"`
	// AccountName is the account holder ("CITIZEN SPRING VZW", …).
	AccountName string `json:"accountName,omitempty"`
	Currency    string `json:"currency"`

	// StatementNumber is KBC's per-year sequential statement label,
	// e.g. "02026030" → year 2026, statement 30.
	StatementNumber string `json:"statementNumber"`
	// Date is the booking date in ISO YYYY-MM-DD format.
	Date string `json:"date"`
	// ValueDate is the bank's value date (used for interest), ISO format.
	ValueDate string `json:"valueDate,omitempty"`
	// Timestamp is the booking date as Unix seconds at Brussels noon.
	// Same-day rows tie and break by Hash on sort.
	Timestamp int64 `json:"timestamp"`

	Description string  `json:"description"`
	Amount      float64 `json:"amount"`
	// Balance is the running balance reported *after* this transaction.
	Balance float64 `json:"balance"`

	CounterpartyIBAN    string `json:"counterpartyIban,omitempty"`
	CounterpartyBIC     string `json:"counterpartyBic,omitempty"`
	CounterpartyName    string `json:"counterpartyName,omitempty"`
	CounterpartyAddress string `json:"counterpartyAddress,omitempty"`

	StandardReference string `json:"standardReference,omitempty"`
	FreeReference     string `json:"freeReference,omitempty"`

	// Hash is the deterministic short hash of this row's stable content,
	// used as the per-tx identifier in URIs and Odoo's unique_import_id.
	Hash string `json:"hash"`
}
