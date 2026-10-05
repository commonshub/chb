package cmd

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestIssuedReasonAndKind(t *testing.T) {
	if got := issuedReason("Issued 3 CHT to leen8610 for a 3h shift on 02/10/2026 at 08:30"); got != "3h shift on 02/10/2026 at 08:30" {
		t.Errorf("reason: %q", got)
	}
	if got := issuedReason("park cleaning"); got != "park cleaning" {
		t.Errorf("plain reason: %q", got)
	}
	if got := issuedKind(map[string]string{"t": "shift"}); got != "shift" {
		t.Errorf("kind t: %q", got)
	}
	if got := issuedKind(map[string]string{"role": "Token steward"}); got != "role" {
		t.Errorf("kind role: %q", got)
	}
}

func TestTokensIssuedForAudience(t *testing.T) {
	f := TokensIssuedFile{Token: "CHT", Issued: []TokenIssued{{ID: "ethereum:42220:tx:0xabc", Wallet: "0xdef", Amount: 3, Timestamp: "2026-10-02T08:30:00Z"}}}
	for _, a := range []Audience{AudiencePublic, AudienceMembers} {
		data, _ := json.Marshal(tokensIssuedForAudience(f, a))
		if strings.Contains(string(data), "0x") {
			t.Errorf("%s copy carries a tx id or wallet: %s", a, data)
		}
	}
	if got := tokensIssuedForAudience(f, AudienceStewards); got.Issued[0].Wallet != "0xdef" {
		t.Error("stewards keep the wallet")
	}
	if f.Issued[0].ID == "" {
		t.Error("projection must not mutate the input")
	}
}
