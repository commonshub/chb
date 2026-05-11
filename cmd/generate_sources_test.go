package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateMonthContributorsGoUsesDiscordSourceMessages(t *testing.T) {
	dataDir := t.TempDir()
	writeDiscordSourceMessagesFixture(t, dataDir, "2026", "04", "chan-1", `{
	  "messages": [{
	    "id": "msg-1",
	    "author": {"id": "user-1", "username": "alice", "global_name": "Alice", "avatar": "avatar-1"},
	    "content": "hello <@user-2>",
	    "timestamp": "2026-04-13T12:00:00.000000+00:00",
	    "mentions": [{"id": "user-2", "username": "bob", "global_name": "Bob"}]
	  }]
	}`)
	writeDiscordSourceMessagesFixture(t, dataDir, "2026", "04", "chan-2", `{
	  "messages": [{
	    "id": "msg-2",
	    "author": {"id": "user-2", "username": "bob", "global_name": "Bob"},
	    "content": "reply",
	    "timestamp": "2026-04-14T12:00:00.000000+00:00"
	  }]
	}`)

	settings := contributorTestSettings()
	runCache := contributorTestRunCache(settings, []string{"user-1", "user-2"})
	if n := generateMonthContributorsGo(dataDir, "2026", "04", settings, runCache, time.Time{}); n != 2 {
		t.Fatalf("generateMonthContributorsGo() = %d, want 2", n)
	}

	data, err := os.ReadFile(filepath.Join(dataDir, "2026", "04", "generated", "contributors.json"))
	if err != nil {
		t.Fatalf("read contributors.json: %v", err)
	}
	var out MonthlyContributorsFile
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal contributors.json: %v", err)
	}
	if out.Summary.TotalContributors != 2 || out.Summary.TotalMessages != 2 {
		t.Fatalf("unexpected summary: %#v", out.Summary)
	}
	byID := map[string]ContributorEntry{}
	for _, c := range out.Contributors {
		byID[c.ID] = c
	}
	if byID["user-1"].Discord.Messages != 1 {
		t.Fatalf("user-1 messages = %d, want 1", byID["user-1"].Discord.Messages)
	}
	if byID["user-2"].Discord.Messages != 1 || byID["user-2"].Discord.Mentions != 1 {
		t.Fatalf("user-2 discord stats = %#v, want 1 message and 1 mention", byID["user-2"].Discord)
	}
}

func TestGenerateTransactionsGoUsesStripeEtherscanAndMoneriumSources(t *testing.T) {
	dataDir := t.TempDir()
	hash := "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "stripe", "balance-transactions.json"), `{
	  "accountId": "acct_test",
	  "currency": "eur",
	  "transactions": [{
	    "id": "txn_stripe",
	    "amount": 1000,
	    "net": 950,
	    "fee": 50,
	    "currency": "eur",
	    "description": "Stripe payment",
	    "created": 1776427200,
	    "reporting_category": "charge",
	    "type": "charge",
	    "source": {"id": "ch_stripe"}
	  }]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "etherscan", "gnosis", "treasury.EURe.json"), `{
	  "cachedAt": "2026-04-01T00:00:00Z",
	  "account": "0xabc0000000000000000000000000000000000000",
	  "chain": "gnosis",
	  "token": "EURe",
	  "transactions": [{
	    "hash": "`+hash+`",
	    "from": "0xdead000000000000000000000000000000000000",
	    "to": "0xabc0000000000000000000000000000000000000",
	    "value": "1000000000000000000",
	    "timeStamp": "1776427300",
	    "tokenDecimal": "18",
	    "tokenSymbol": "EURe"
	  }]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "monerium", "treasury.json"), `{
	  "cachedAt": "2026-04-01T00:00:00Z",
	  "address": "0xabc0000000000000000000000000000000000000",
	  "orders": [{
	    "id": "ord_1",
	    "kind": "issue",
	    "memo": "SEPA topup",
	    "state": "processed",
	    "counterpart": {"details": {"companyName": "Bank Sender"}},
	    "meta": {"placedAt": "2026-04-13T12:00:00Z", "txHashes": ["`+hash+`"]}
	  }]
	}`)

	settings := &Settings{}
	settings.Finance.Accounts = []FinanceAccount{
		{
			Name:     "Treasury",
			Slug:     "treasury",
			Provider: "etherscan",
			Chain:    "gnosis",
			ChainID:  100,
			Address:  "0xabc0000000000000000000000000000000000000",
			Token: &struct {
				Address  string `json:"address"`
				Name     string `json:"name"`
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			}{Address: "0xtoken", Name: "EURe", Symbol: "EURe", Decimals: 18},
		},
	}

	if n := generateTransactionsGo(dataDir, "2026", "04", settings); n != 2 {
		t.Fatalf("generateTransactionsGo() = %d, want 2", n)
	}

	data, err := os.ReadFile(filepath.Join(dataDir, "2026", "04", "generated", "transactions.json"))
	if err != nil {
		t.Fatalf("read transactions.json: %v", err)
	}
	var out TransactionsFile
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal transactions.json: %v", err)
	}
	byProvider := map[string]TransactionEntry{}
	for _, tx := range out.Transactions {
		byProvider[tx.Provider] = tx
	}
	if byProvider["stripe"].ID != "stripe:txn_stripe" {
		t.Fatalf("stripe transaction missing: %#v", out.Transactions)
	}
	chainTx := byProvider["etherscan"]
	if chainTx.TxHash != hash {
		t.Fatalf("etherscan transaction missing: %#v", out.Transactions)
	}
	if chainTx.Counterparty != "Bank Sender" {
		t.Fatalf("monerium counterparty = %q, want Bank Sender", chainTx.Counterparty)
	}
	if got := stringMetadata(chainTx.Metadata, "memo"); got != "SEPA topup" {
		t.Fatalf("monerium memo = %q, want SEPA topup", got)
	}
	if !transactionHasTag(chainTx, []string{"source", "monerium"}) || !transactionHasTag(chainTx, []string{"status", "processed"}) {
		t.Fatalf("missing monerium tags: %#v", chainTx.Tags)
	}
}

func TestGenerateTransactionsGoKeepsBothSidesOfInternalAccountTransfer(t *testing.T) {
	dataDir := t.TempDir()
	hash := "0xinternal1234567890abcdef1234567890abcdef1234567890abcdef123456"
	savings := "0xaaa0000000000000000000000000000000000000"
	checking := "0xbbb0000000000000000000000000000000000000"
	txJSON := `{
	  "hash": "` + hash + `",
	  "from": "` + savings + `",
	  "to": "` + checking + `",
	  "value": "10000000000000000000000",
	  "timeStamp": "1776427300",
	  "tokenDecimal": "18",
	  "tokenSymbol": "EURe"
	}`
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "etherscan", "gnosis", "savings.EURe.json"), `{
	  "cachedAt": "2026-04-01T00:00:00Z",
	  "account": "`+savings+`",
	  "chain": "gnosis",
	  "token": "EURe",
	  "transactions": [`+txJSON+`]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "etherscan", "gnosis", "checking.EURe.json"), `{
	  "cachedAt": "2026-04-01T00:00:00Z",
	  "account": "`+checking+`",
	  "chain": "gnosis",
	  "token": "EURe",
	  "transactions": [`+txJSON+`]
	}`)

	settings := &Settings{}
	settings.Finance.Accounts = []FinanceAccount{
		{Slug: "savings", Provider: "etherscan", Chain: "gnosis", Address: savings},
		{Slug: "checking", Provider: "etherscan", Chain: "gnosis", Address: checking},
	}

	if n := generateTransactionsGo(dataDir, "2026", "04", settings); n != 2 {
		t.Fatalf("generateTransactionsGo() = %d, want 2", n)
	}

	data, err := os.ReadFile(filepath.Join(dataDir, "2026", "04", "generated", "transactions.json"))
	if err != nil {
		t.Fatalf("read transactions.json: %v", err)
	}
	var out TransactionsFile
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal transactions.json: %v", err)
	}
	bySlug := map[string]TransactionEntry{}
	for _, tx := range out.Transactions {
		bySlug[tx.AccountSlug] = tx
	}
	if bySlug["savings"].Type != "INTERNAL" || stringMetadata(bySlug["savings"].Metadata, "direction") != "DEBIT" {
		t.Fatalf("savings side = %#v, want INTERNAL DEBIT", bySlug["savings"])
	}
	if bySlug["checking"].Type != "INTERNAL" || stringMetadata(bySlug["checking"].Metadata, "direction") != "CREDIT" {
		t.Fatalf("checking side = %#v, want INTERNAL CREDIT", bySlug["checking"])
	}
}

func TestGenerateTransactionsGoDetectsInternalAccountsFromAccountsConfig(t *testing.T) {
	dataDir := t.TempDir()
	appDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	t.Setenv("APP_DATA_DIR", appDir)

	hash := "0xinternal2234567890abcdef1234567890abcdef1234567890abcdef123456"
	savings := "0xaaa0000000000000000000000000000000000000"
	checking := "0xbbb0000000000000000000000000000000000000"
	txJSON := `{
	  "hash": "` + hash + `",
	  "from": "` + savings + `",
	  "to": "` + checking + `",
	  "value": "10000000000000000000000",
	  "timeStamp": "1776427300",
	  "tokenDecimal": "18",
	  "tokenSymbol": "EURe"
	}`
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "etherscan", "gnosis", "savings.EURe.json"), `{
	  "cachedAt": "2026-04-01T00:00:00Z",
	  "account": "`+savings+`",
	  "chain": "gnosis",
	  "token": "EURe",
	  "transactions": [`+txJSON+`]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "etherscan", "gnosis", "checking.EURe.json"), `{
	  "cachedAt": "2026-04-01T00:00:00Z",
	  "account": "`+checking+`",
	  "chain": "gnosis",
	  "token": "EURe",
	  "transactions": [`+txJSON+`]
	}`)
	if err := SaveAccountConfigs([]AccountConfig{
		{Slug: "savings", Provider: "etherscan", Chain: "gnosis", Address: savings},
		{Slug: "checking", Provider: "etherscan", Chain: "gnosis", Address: checking},
	}); err != nil {
		t.Fatalf("SaveAccountConfigs: %v", err)
	}

	if n := generateTransactionsGo(dataDir, "2026", "04", &Settings{}); n != 2 {
		t.Fatalf("generateTransactionsGo() = %d, want 2", n)
	}
	data, err := os.ReadFile(filepath.Join(dataDir, "2026", "04", "generated", "transactions.json"))
	if err != nil {
		t.Fatalf("read transactions.json: %v", err)
	}
	var out TransactionsFile
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal transactions.json: %v", err)
	}
	for _, tx := range out.Transactions {
		if tx.Type != "INTERNAL" {
			t.Fatalf("tx %s type = %s, want INTERNAL", tx.AccountSlug, tx.Type)
		}
	}
}

func TestGenerateMonthlyReportGoSummarizesGeneratedFilesAndSources(t *testing.T) {
	dataDir := t.TempDir()
	hash := "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "contributors.json"), `{
	  "summary": {"totalContributors": 2},
	  "contributors": [{ "id": "user-1" }, { "id": "user-2" }]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "images.json"), `{
	  "count": 1,
	  "images": [{ "id": "img-1" }]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "transactions.json"), `{
	  "transactions": [
	    {"id":"stripe:txn_1","provider":"stripe","account":"stripe","accountSlug":"acct_test","accountName":"Stripe","currency":"EUR","netAmount":9.5,"grossAmount":10,"fee":0.5,"type":"CREDIT"},
	    {"id":"stripe:txn_2","provider":"stripe","account":"stripe","accountSlug":"acct_test","accountName":"Stripe","currency":"EUR","netAmount":-4,"grossAmount":4,"type":"DEBIT"},
	    {"id":"gnosis:abc","provider":"etherscan","chain":"gnosis","account":"0xabc","accountSlug":"treasury","accountName":"Treasury","currency":"EURe","amount":1,"type":"CREDIT"}
	  ]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "events.json"), `{
	  "events": [
	    {"id":"event-1","name":"Public event","calendarSource":"ostrom"},
	    {"id":"event-2","name":"Other event","calendarSource":"commons"}
	  ]
	}`)

	writeDiscordSourceMessagesFixture(t, dataDir, "2026", "04", "chan-1", `{
	  "messages": [{
	    "id": "msg-1",
	    "attachments": [
	      {"id":"att-1","url":"https://example.com/a.jpg","content_type":"image/jpeg"},
	      {"id":"att-2","url":"https://example.com/a.txt","content_type":"text/plain"}
	    ]
	  }]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "stripe", "balance-transactions.json"), `{
	  "transactions": [{"id":"txn_1"}]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "etherscan", "gnosis", "treasury.EURe.json"), `{
	  "transactions": [{"hash":"`+hash+`"}]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "monerium", "treasury.json"), `{
	  "orders": [{"id":"ord_1","state":"processed"}]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "odoo", "invoices.json"), `{
	  "invoices": [{"id": 1}]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "odoo", "private", "invoices.json"), `{
	  "invoices": [{"id": 1, "attachments": [{"id": 7, "name": "invoice.pdf"}]}]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "ics", "ostrom.ics"), `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:booking-1
SUMMARY:Booking 1
DTSTART:20260410T100000Z
DTEND:20260410T110000Z
END:VEVENT
BEGIN:VEVENT
UID:booking-2
SUMMARY:Booking 2
DTSTART:20260411T100000Z
DTEND:20260411T110000Z
END:VEVENT
END:VCALENDAR`)

	if !generateMonthlyReportGo(dataDir, "2026", "04", &Settings{}) {
		t.Fatalf("generateMonthlyReportGo() = false")
	}

	data, err := os.ReadFile(filepath.Join(dataDir, "2026", "04", "generated", "report.json"))
	if err != nil {
		t.Fatalf("read report.json: %v", err)
	}
	var report MonthlyReportFile
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal report.json: %v", err)
	}
	if report.Summary.Contributors != 2 || report.Summary.Images != 1 || report.Summary.Transactions != 3 || report.Summary.Events != 2 || report.Summary.Bookings != 2 {
		t.Fatalf("unexpected report summary: %#v", report.Summary)
	}
	if len(report.Currencies) != 2 {
		t.Fatalf("currencies = %d, want 2: %#v", len(report.Currencies), report.Currencies)
	}
	for _, cur := range report.Currencies {
		if cur.Currency == "EUR" && (cur.Transactions != 2 || cur.In != 10 || cur.Out != 4 || cur.Net != 5.5) {
			t.Fatalf("EUR summary = %#v, want 2 txs, in 10, out 4, net 5.5", cur)
		}
	}
	if len(report.Accounts) != 2 {
		t.Fatalf("accounts = %d, want 2: %#v", len(report.Accounts), report.Accounts)
	}
	sources := map[string]MonthlyReportSource{}
	for _, src := range report.Sources {
		sources[src.Source] = src
	}
	if sources["discord"].Records != 1 || sources["discord"].Attachments != 2 {
		t.Fatalf("discord source summary = %#v", sources["discord"])
	}
	if sources["odoo"].Records != 1 || sources["odoo"].Attachments != 1 {
		t.Fatalf("odoo source summary = %#v", sources["odoo"])
	}
	if sources["monerium"].Records != 1 || sources["etherscan"].Records != 1 || sources["stripe"].Records != 1 {
		t.Fatalf("missing transaction source counts: %#v", sources)
	}
	if sources["ics"].Records != 2 {
		t.Fatalf("ics source summary = %#v", sources["ics"])
	}
}

func TestGenerateMonthlyReportGoSummarizesMintableTokens(t *testing.T) {
	dataDir := t.TempDir()
	zero := "0x0000000000000000000000000000000000000000"
	addr1 := "0x1111111111111111111111111111111111111111"
	addr2 := "0x2222222222222222222222222222222222222222"
	addr3 := "0x3333333333333333333333333333333333333333"

	writeJSONFixture(t, filepath.Join(dataDir, "2026", "03", "sources", "etherscan", "celo", "cht.CHT.json"), `{
	  "cachedAt": "2026-03-01T00:00:00Z",
	  "account": "",
	  "chain": "celo",
	  "token": "CHT",
	  "transactions": [{
	    "hash": "0xmintmarch",
	    "from": "`+zero+`",
	    "to": "`+addr1+`",
	    "value": "100000000000000000000",
	    "timeStamp": "1772400000",
	    "tokenDecimal": "18",
	    "tokenSymbol": "CHT"
	  }]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "etherscan", "celo", "cht.CHT.json"), `{
	  "cachedAt": "2026-04-01T00:00:00Z",
	  "account": "",
	  "chain": "celo",
	  "token": "CHT",
	  "transactions": [{
	    "hash": "0xtransferapril",
	    "from": "`+addr1+`",
	    "to": "`+addr2+`",
	    "value": "10000000000000000000",
	    "timeStamp": "1775078400",
	    "tokenDecimal": "18",
	    "tokenSymbol": "CHT"
	  }, {
	    "hash": "0xburnapril",
	    "from": "`+addr2+`",
	    "to": "`+zero+`",
	    "value": "2000000000000000000",
	    "timeStamp": "1775164800",
	    "tokenDecimal": "18",
	    "tokenSymbol": "CHT"
	  }, {
	    "hash": "0xmintapril",
	    "from": "`+zero+`",
	    "to": "`+addr3+`",
	    "value": "5000000000000000000",
	    "timeStamp": "1775251200",
	    "tokenDecimal": "18",
	    "tokenSymbol": "CHT"
	  }]
	}`)

	settings := &Settings{Tokens: []TokenConfig{{
		Name:     "Commons Hub Token",
		Slug:     "cht",
		Provider: "etherscan",
		Chain:    "celo",
		ChainID:  42220,
		Address:  "0xcht0000000000000000000000000000000000000",
		Symbol:   "CHT",
		Decimals: 18,
		Mintable: true,
		Burnable: true,
	}}}

	if !generateMonthlyReportGo(dataDir, "2026", "04", settings) {
		t.Fatalf("generateMonthlyReportGo() = false")
	}

	data, err := os.ReadFile(filepath.Join(dataDir, "2026", "04", "generated", "report.json"))
	if err != nil {
		t.Fatalf("read report.json: %v", err)
	}
	var report MonthlyReportFile
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal report.json: %v", err)
	}
	if len(report.Tokens) != 1 {
		t.Fatalf("tokens = %d, want 1: %#v", len(report.Tokens), report.Tokens)
	}
	token := report.Tokens[0]
	if token.Mints != 1 || token.Burns != 1 || token.Transfers != 1 || token.Transactions != 3 {
		t.Fatalf("unexpected token counts: %#v", token)
	}
	if token.Minted != 5 || token.Burnt != 2 || token.TotalSupply != 103 {
		t.Fatalf("unexpected token amounts: %#v", token)
	}
	if token.TokenHolders != 3 || token.ActiveTokenHolders != 3 {
		t.Fatalf("unexpected holder counts: %#v", token)
	}
}

func contributorTestSettings() *Settings {
	settings := &Settings{}
	settings.ContributionToken = &ContributionTokenSettings{
		Chain:         "celo",
		ChainID:       42220,
		Address:       "0xcht0000000000000000000000000000000000000",
		Name:          "Commons Hub Token",
		Symbol:        "CHT",
		Decimals:      6,
		WalletManager: "citizenwallet",
	}
	return settings
}

func contributorTestRunCache(settings *Settings, discordIDs []string) *contributorsRunCache {
	cache := &walletResolutionCache{Version: walletCacheVersion, Entries: map[string]walletResolutionEntry{}}
	scope := contributionWalletResolverScope(settings)
	now := time.Now().UTC().Format(time.RFC3339)
	for _, id := range discordIDs {
		cache.Entries[scope.cacheKey(id)] = walletResolutionEntry{CheckedAt: now}
	}
	return &contributorsRunCache{wallets: cache}
}

func writeJSONFixture(t *testing.T, path, payload string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(payload), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
