package cmd

import (
	"path/filepath"
	"testing"
)

func TestTxExclusions(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	writeFile(t, filepath.Join(tmp, "app", "settings", "excluded-transactions.json"),
		`{"transactions":[{"chain":"celo","hash":"0xAAA","reason":"test mint","keep":true},{"chain":"gnosis","hash":"0xbbb","to":"0x1"}]}`)
	resetTxExclusions()
	defer resetTxExclusions()
	ex := loadTxExclusions(DataDir())
	if r := ex.reasonFor(TransactionEntry{ID: "ethereum:42220:tx:0xaaa", TxHash: "0xaaa"}); r != "test mint" {
		t.Errorf("kept exclusion: %q", r)
	}
	if r := ex.reasonFor(TransactionEntry{TxHash: "0xbbb"}); r != "" {
		t.Errorf("a dropped (non-keep) entry is not a kept exclusion: %q", r)
	}
	if !isExcludedTx(TransactionEntry{Metadata: map[string]interface{}{"excluded": "x"}}) || isExcludedTx(TransactionEntry{}) {
		t.Error("isExcludedTx")
	}
}

func TestExcludeAnnotationTag(t *testing.T) {
	a := parseAnnotation("ethereum:42220:tx:0xaaa", NostrEvent{Tags: [][]string{{"i", "ethereum:42220:tx:0xaaa"}, {"k", "ethereum:tx"}, {"exclude", "test mint"}}})
	if a.Exclude != "test mint" {
		t.Fatalf("exclude = %q", a.Exclude)
	}
	for _, tg := range a.Tags {
		if tg[0] == "exclude" {
			t.Error("exclude must not also become a free tag")
		}
	}
}
