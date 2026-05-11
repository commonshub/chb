package monerium

const (
	Source = "monerium"
)

// Order represents a single Monerium order.
type Order struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Profile     string `json:"profile"`
	Address     string `json:"address"`
	Chain       string `json:"chain"`
	Currency    string `json:"currency"`
	Amount      string `json:"amount"`
	Counterpart struct {
		Identifier struct {
			Standard string `json:"standard"`
			IBAN     string `json:"iban,omitempty"`
		} `json:"identifier"`
		Details struct {
			Name        string `json:"name,omitempty"`
			CompanyName string `json:"companyName,omitempty"`
			FirstName   string `json:"firstName,omitempty"`
			LastName    string `json:"lastName,omitempty"`
			Country     string `json:"country,omitempty"`
		} `json:"details"`
	} `json:"counterpart"`
	Memo  string `json:"memo,omitempty"`
	State string `json:"state"`
	Meta  struct {
		PlacedAt    string   `json:"placedAt"`
		ProcessedAt string   `json:"processedAt,omitempty"`
		TxHashes    []string `json:"txHashes,omitempty"`
	} `json:"meta"`
}

type CacheFile struct {
	Orders   []Order `json:"orders"`
	CachedAt string  `json:"cachedAt"`
	Address  string  `json:"address"`
}
