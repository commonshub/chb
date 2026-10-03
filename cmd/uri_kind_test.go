package cmd

import "testing"

func TestURIKind(t *testing.T) {
	cases := map[string]string{
		"ethereum:100:tx:0xabc":             "ethereum:tx",
		"stripe:txn_123":                    "stripe:txn",
		"iban:be46734072238636:tx:23435":    "iban:tx",
		"odoo:h.odoo.com:db:account.move:1": "odoo:account.move",
		"odoo:h.odoo.com:db:hr.expense:2":   "odoo:hr.expense",
	}
	for uri, want := range cases {
		if got := uriKind(uri); got != want {
			t.Errorf("uriKind(%q) = %q, want %q", uri, got, want)
		}
	}
}
