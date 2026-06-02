package etherscan

import (
	"os"
	"path/filepath"
	"testing"
)

func TestShortAddr(t *testing.T) {
	cases := map[string]string{
		"0x6fDF0AaE33E313d9C98D2Aa19Bcd8EF777912CBf": "0x6fdf-2cbf",
		"0xABCD000000000000000000000000000000007890": "0xabcd-7890",
		"":      "",
		"0x123": "0x123", // too short to abbreviate
		"  0xABCD000000000000000000000000000000007890 ": "0xabcd-7890",
	}
	for in, want := range cases {
		if got := ShortAddr(in); got != want {
			t.Errorf("ShortAddr(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFileName(t *testing.T) {
	if got := FileName("Savings", "0x6fDF0AaE33E313d9C98D2Aa19Bcd8EF777912CBf", "EURe"); got != "savings.0x6fdf-2cbf.EURe.json" {
		t.Errorf("FileName = %q", got)
	}
	// No address → legacy name so the result is still matchable.
	if got := FileName("Savings", "", "EURe"); got != "savings.EURe.json" {
		t.Errorf("FileName(no addr) = %q", got)
	}
}

func TestFindFileMatchesNewAndLegacy(t *testing.T) {
	dir := t.TempDir()
	chainDir := Path(dir, "2024", "01", "gnosis")
	if err := os.MkdirAll(chainDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Legacy file is found when only it exists.
	legacy := filepath.Join(chainDir, "coffee.EURb.json")
	if err := os.WriteFile(legacy, []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}
	if got, ok := FindFile(dir, "2024", "01", "gnosis", "coffee", "EURb"); !ok || got != legacy {
		t.Fatalf("FindFile(legacy) = %q, %v", got, ok)
	}

	// Address-qualified file is preferred once it exists.
	qualified := filepath.Join(chainDir, "savings.0x6fdf-2cbf.EURe.json")
	if err := os.WriteFile(qualified, []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}
	if got, ok := FindFile(dir, "2024", "01", "gnosis", "savings", "EURe"); !ok || got != qualified {
		t.Fatalf("FindFile(qualified) = %q, %v", got, ok)
	}

	if _, ok := FindFile(dir, "2024", "01", "gnosis", "missing", "EURe"); ok {
		t.Fatal("FindFile(missing) returned ok")
	}
}

func TestResultString(t *testing.T) {
	if got := resultString([]byte(`"Invalid API Key"`)); got != "Invalid API Key" {
		t.Errorf("resultString(string) = %q", got)
	}
	if got := resultString([]byte(`[{"hash":"0x1"}]`)); got != `[{"hash":"0x1"}]` {
		t.Errorf("resultString(array) = %q", got)
	}
	if got := resultString(nil); got != "" {
		t.Errorf("resultString(nil) = %q", got)
	}
}

func TestRedactAPIKey(t *testing.T) {
	url := "https://api.etherscan.io/v2/api?apikey=SECRET123"
	if got := redactAPIKey(url, "SECRET123"); got != "https://api.etherscan.io/v2/api?apikey=***" {
		t.Errorf("redactAPIKey = %q", got)
	}
	if got := redactAPIKey(url, ""); got != url {
		t.Errorf("redactAPIKey(empty) should be unchanged, got %q", got)
	}
}
