package etherscan

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func withTestExplorers(t *testing.T, etherscan, blockscout string) {
	t.Helper()
	origE, origB, origU := etherscanV2Base, blockscoutBases, etherscanUnsupported
	etherscanV2Base = etherscan
	blockscoutBases = map[int]string{100: blockscout, 42220: blockscout}
	etherscanUnsupported = map[int]bool{100: true}
	t.Cleanup(func() {
		etherscanV2Base, blockscoutBases, etherscanUnsupported = origE, origB, origU
		OnFallback = nil
	})
}

func transfersJSON(hashes ...string) string {
	var out []TokenTransfer
	for i, h := range hashes {
		out = append(out, TokenTransfer{Hash: h, BlockNumber: "100", TimeStamp: "1700000000", Value: "1", From: "a", To: "b", TokenSymbol: "EURe", TokenDecimal: "18"})
		_ = i
	}
	b, _ := json.Marshal(map[string]interface{}{"status": "1", "message": "OK", "result": out})
	return string(b)
}

func TestGnosisSkipsEtherscanAndSendsNoKey(t *testing.T) {
	var etherscanHits int
	var gotURL string
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { etherscanHits++ }))
	bs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.Write([]byte(transfersJSON("0xaaa")))
	}))
	defer es.Close()
	defer bs.Close()
	withTestExplorers(t, es.URL, bs.URL)

	got, err := FetchTokenTransfersSince(Account{ChainID: 100, TokenAddress: "0xtoken", Address: "0xme"}, "SECRET", 42)
	if err != nil {
		t.Fatal(err)
	}
	if etherscanHits != 0 {
		t.Error("chain 100 must never be sent to Etherscan (its free plan refuses it)")
	}
	if strings.Contains(gotURL, "apikey") || strings.Contains(gotURL, "SECRET") {
		t.Errorf("Blockscout query must not carry the Etherscan key: %s", gotURL)
	}
	if !strings.Contains(gotURL, "startblock=42") || !strings.Contains(gotURL, "address=0xme") {
		t.Errorf("query lost startblock/address: %s", gotURL)
	}
	if len(got) != 1 || got[0].Hash != "0xaaa" {
		t.Errorf("got %+v", got)
	}
}

func TestBlockscoutEmptySentinelIsNotAnError(t *testing.T) {
	bs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"0","message":"No token transfers found","result":[]}`))
	}))
	defer bs.Close()
	withTestExplorers(t, "http://127.0.0.1:1", bs.URL)

	got, err := FetchTokenTransfersSince(Account{ChainID: 100, TokenAddress: "0xtoken"}, "", 0)
	if err != nil {
		t.Fatalf("Blockscout's empty message must read as 'nothing new', got error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d transfers, want 0", len(got))
	}
}

func TestQuotaRefusalFallsBackToBlockscout(t *testing.T) {
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"0","message":"NOTOK","result":"Community Free API Limit reached. Resets 2026-09-15 00:00:00 UTC. For higher, uninterrupted limits, please upgrade to API Pro: https://etherscan.io/apis"}`))
	}))
	bsHits := 0
	bs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bsHits++
		w.Write([]byte(transfersJSON("0xcelo")))
	}))
	defer es.Close()
	defer bs.Close()
	withTestExplorers(t, es.URL, bs.URL)

	var note string
	OnFallback = func(chainID int, from, to, reason string) { note = from + "→" + to + ": " + reason }

	got, err := FetchTokenTransfersSince(Account{ChainID: 42220, TokenAddress: "0xcht"}, "KEY", 0)
	if err != nil {
		t.Fatal(err)
	}
	if bsHits != 1 || len(got) != 1 || got[0].Hash != "0xcelo" {
		t.Errorf("expected exactly one Blockscout fetch serving the data, hits=%d got=%+v", bsHits, got)
	}
	if !strings.HasPrefix(note, "etherscan→blockscout") || !strings.Contains(note, "Free API Limit") {
		t.Errorf("fallback note = %q", note)
	}
}

func TestGenuineAPIErrorDoesNotFallBack(t *testing.T) {
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"0","message":"NOTOK","result":"Invalid API Key"}`))
	}))
	bsHits := 0
	bs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { bsHits++ }))
	defer es.Close()
	defer bs.Close()
	withTestExplorers(t, es.URL, bs.URL)

	if _, err := FetchTokenTransfersSince(Account{ChainID: 42220, TokenAddress: "0xcht"}, "BAD", 0); err == nil {
		t.Fatal("an invalid key is a real error, not a quota refusal")
	}
	if bsHits != 0 {
		t.Error("a non-quota error must not be retried on Blockscout")
	}
}

func TestMergeTokenTransfers(t *testing.T) {
	tx := func(hash, block, ts string) TokenTransfer {
		return TokenTransfer{Hash: hash, BlockNumber: block, TimeStamp: ts, Value: "1", TokenSymbol: "EURe", TokenDecimal: "18"}
	}
	existing := []TokenTransfer{tx("0xb", "200", "2000"), tx("0xa", "100", "1000")}
	fetched := []TokenTransfer{tx("0xc", "300", "3000"), tx("0xb", "200", "2000")} // 0xb overlaps (inclusive startblock)
	out := MergeTokenTransfers(existing, fetched)
	if len(out) != 3 {
		t.Fatalf("len = %d, want 3 (overlap deduped)", len(out))
	}
	if out[0].Hash != "0xc" || out[1].Hash != "0xb" || out[2].Hash != "0xa" {
		t.Errorf("order = %s %s %s, want newest-first", out[0].Hash, out[1].Hash, out[2].Hash)
	}
	if NewestBlock(out) != 300 {
		t.Errorf("NewestBlock = %d", NewestBlock(out))
	}
}

func TestLatestCachedBlockGlobal(t *testing.T) {
	dir := t.TempDir()
	write := func(year, month, block string) {
		p := filepath.Join(dir, year, month, RelPath("gnosis", FileName("checking", "0xD578e7cd845e1ecD979b04784e77068D5eBd8716", "EURe")))
		os.MkdirAll(filepath.Dir(p), 0o755)
		b, _ := json.Marshal(CacheFile{Transactions: []TokenTransfer{{Hash: "0x" + block, BlockNumber: block}}})
		os.WriteFile(p, b, 0o644)
	}
	write("2026", "07", "41000000")
	write("2026", "08", "41500000")
	write("2026", "06", "40000000")
	if got := LatestCachedBlockGlobal(dir, "gnosis", "checking", "0xD578e7cd845e1ecD979b04784e77068D5eBd8716", "EURe"); got != 41500000 {
		t.Errorf("LatestCachedBlockGlobal = %d, want 41500000", got)
	}
	if got := LatestCachedBlockGlobal(dir, "gnosis", "nobody", "0x0", "EURe"); got != 0 {
		t.Errorf("unknown scope = %d, want 0", got)
	}
}
