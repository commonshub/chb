package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseBalanceTargetDefaultsChainAndSplitsTokenAndWallet(t *testing.T) {
	target, err := parseBalanceTarget("EURe:0x1111111111111111111111111111111111111111")
	if err != nil {
		t.Fatalf("parseBalanceTarget: %v", err)
	}
	if target.Chain != "ethereum" {
		t.Fatalf("default chain = %q, want ethereum", target.Chain)
	}
	if target.Token != "EURe" {
		t.Fatalf("token = %q", target.Token)
	}
	if target.Address != "0x1111111111111111111111111111111111111111" {
		t.Fatalf("address = %q", target.Address)
	}
}

func TestResolveBalanceTokenUsesConfiguredSymbolForChain(t *testing.T) {
	tokens := []TokenConfig{
		{Chain: "gnosis", ChainID: 100, Address: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Symbol: "EURe", Decimals: 18},
		{Chain: "ethereum", ChainID: 1, Address: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Symbol: "EURe", Decimals: 6},
	}
	resolved, err := resolveBalanceToken("gnosis", "eure", tokens)
	if err != nil {
		t.Fatalf("resolveBalanceToken: %v", err)
	}
	if resolved.ChainID != 100 || resolved.Address != "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || resolved.Decimals != 18 || resolved.Symbol != "EURe" {
		t.Fatalf("resolved = %+v", resolved)
	}
}

func TestBalanceToolHelpIsRoutedFromToolCommand(t *testing.T) {
	out := captureStdout(t, func() {
		if err := Tools([]string{"balance", "--help"}); err != nil {
			t.Fatalf("Tools balance --help: %v", err)
		}
	})
	if !strings.Contains(out, "chb tool balance") || !strings.Contains(out, "ETHERSCAN_API_KEY") {
		t.Fatalf("balance help output unexpected:\n%s", out)
	}
}

func TestBalanceDateTimestampAcceptsDateFormats(t *testing.T) {
	compact, err := balanceDateTimestamp("20260523")
	if err != nil {
		t.Fatalf("balanceDateTimestamp compact: %v", err)
	}
	dashed, err := balanceDateTimestamp("2026-05-23")
	if err != nil {
		t.Fatalf("balanceDateTimestamp dashed: %v", err)
	}
	if compact != dashed {
		t.Fatalf("compact timestamp = %d, dashed = %d", compact, dashed)
	}
	monthEnd, err := balanceDateTimestamp("2026/05")
	if err != nil {
		t.Fatalf("balanceDateTimestamp month: %v", err)
	}
	dayEnd, err := balanceDateTimestamp("2026-05-31")
	if err != nil {
		t.Fatalf("balanceDateTimestamp day end: %v", err)
	}
	if monthEnd != dayEnd {
		t.Fatalf("month timestamp = %d, day end = %d", monthEnd, dayEnd)
	}
}

func TestBuiltinBalanceTokenIncludesPolygonEURe(t *testing.T) {
	token, ok := builtinBalanceToken("polygon", "EURE")
	if !ok {
		t.Fatalf("polygon EURe builtin not found")
	}
	if token.ChainID != 137 || token.Address != "0x18ec0A6E18E5bc3784fDd3a3634b31245ab704F6" || token.Decimals != 18 || token.Symbol != "EURe" {
		t.Fatalf("polygon EURe = %+v", token)
	}
}

func TestBalanceToolFetchesHistoricalBalanceAtBlockForDate(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		seen = append(seen, q.Encode())
		switch q.Get("action") {
		case "getblocknobytime":
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "1", "message": "OK", "result": "12345"})
		case "tokenbalancehistory":
			if q.Get("blockno") != "12345" {
				t.Fatalf("tokenbalancehistory blockno = %q, want 12345", q.Get("blockno"))
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "1", "message": "OK", "result": "1234567"})
		default:
			t.Fatalf("unexpected action %q", q.Get("action"))
		}
	}))
	defer server.Close()

	oldBaseURL := etherscanAPIBaseURL
	etherscanAPIBaseURL = server.URL + "/api"
	defer func() { etherscanAPIBaseURL = oldBaseURL }()

	out, err := fetchBalanceToolResult(balanceToolRequest{
		Chain:        "gnosis",
		ChainID:      100,
		TokenAddress: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TokenSymbol:  "EURe",
		Decimals:     6,
		Wallet:       "0x1111111111111111111111111111111111111111",
		Date:         "2024-01-31",
		APIKey:       "test-key",
	})
	if err != nil {
		t.Fatalf("fetchBalanceToolResult: %v", err)
	}
	if out.Block != "12345" || out.Raw != "1234567" || out.Balance != "1.234567" {
		t.Fatalf("out = %+v", out)
	}
	joined := strings.Join(seen, "\n")
	for _, want := range []string{"chainid=100", "module=block", "action=getblocknobytime", "closest=before", "module=account", "action=tokenbalancehistory", "blockno=12345", "apikey=test-key"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("request missing %q in:\n%s", want, joined)
		}
	}
}
