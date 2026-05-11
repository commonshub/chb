package etherscan

const (
	Source = "etherscan"
)

// Account describes one ERC20 transfer sync scope for Etherscan V2.
type Account struct {
	Slug          string
	Chain         string
	ChainID       int
	Address       string
	TokenAddress  string
	TokenSymbol   string
	TokenDecimals int
}

// TokenTransfer represents a single ERC20 token transfer from Etherscan V2.
type TokenTransfer struct {
	BlockNumber  string `json:"blockNumber"`
	TimeStamp    string `json:"timeStamp"`
	Hash         string `json:"hash"`
	From         string `json:"from"`
	To           string `json:"to"`
	Value        string `json:"value"`
	TokenName    string `json:"tokenName"`
	TokenSymbol  string `json:"tokenSymbol"`
	TokenDecimal string `json:"tokenDecimal"`
}

// CacheFile is the monthly source archive saved to disk.
type CacheFile struct {
	Transactions []TokenTransfer `json:"transactions"`
	CachedAt     string          `json:"cachedAt"`
	Account      string          `json:"account"`
	Chain        string          `json:"chain"`
	Token        string          `json:"token"`
}
