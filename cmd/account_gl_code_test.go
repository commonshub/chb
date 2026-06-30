package cmd

import (
	"encoding/json"
	"testing"
)

// TestAccountConfigGlCodeRoundTrips locks in that the odooGlAccountCode field
// unmarshals from accounts.json into AccountConfig.
func TestAccountConfigGlCodeRoundTrips(t *testing.T) {
	const blob = `[{"slug":"checking","provider":"etherscan","odooGlAccountCode":"550010"}]`
	var accs []AccountConfig
	if err := json.Unmarshal([]byte(blob), &accs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(accs) != 1 || accs[0].OdooGlAccountCode != "550010" {
		t.Fatalf("OdooGlAccountCode = %q, want 550010", accs[0].OdooGlAccountCode)
	}
}

// TestEmbeddedDefaultsCarryGlCodes verifies the committed defaults/accounts.json
// shipped in the binary carries GL account codes for the linked accounts, so a
// fresh install (which force-overwrites accounts.json from these defaults) still
// knows each journal's GL account.
func TestEmbeddedDefaultsCarryGlCodes(t *testing.T) {
	data, err := defaultSettingsFS.ReadFile("defaults/accounts.json")
	if err != nil {
		t.Fatalf("read embedded defaults: %v", err)
	}
	var accs []AccountConfig
	if err := json.Unmarshal(data, &accs); err != nil {
		t.Fatalf("unmarshal defaults: %v", err)
	}
	want := map[string]string{
		"checking": "550010",
		"kbc":      "550003",
		"stripe":   "550014",
		"savings":  "550020",
	}
	got := map[string]string{}
	for _, a := range accs {
		got[a.Slug] = a.OdooGlAccountCode
	}
	for slug, code := range want {
		if got[slug] != code {
			t.Errorf("defaults[%s].odooGlAccountCode = %q, want %q", slug, got[slug], code)
		}
	}
}
