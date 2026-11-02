package providers

import "testing"

func TestEmailHint(t *testing.T) {
	for in, want := range map[string]string{
		"Xavier@Gmail.com": "x…@gmail.com",
		" x@example.org ":  "…@example.org",
		"élise@ex.be":      "é…@ex.be",
		"":                 "",
		"no-at-sign":       "",
		"@domain.com":      "",
		"user@":            "",
	} {
		if got := EmailHint(in); got != want {
			t.Errorf("EmailHint(%q) = %q, want %q", in, got, want)
		}
	}
}
