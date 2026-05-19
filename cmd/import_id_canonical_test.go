package cmd

import "testing"

func TestCanonicalizeImportID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "etherscan broken URI form",
			in:   "gnosis:0xd578e7cd845e1ecd979b04784e77068d5ebd8716:ethereum:100:tx:0xabc123:0",
			want: "gnosis:0xd578e7cd845e1ecd979b04784e77068d5ebd8716:0xabc123:0",
		},
		{
			name: "etherscan broken URI form with logIndex > 0",
			in:   "gnosis:0xd578:ethereum:100:tx:0xabc:2",
			want: "gnosis:0xd578:0xabc:2",
		},
		{
			name: "stripe broken double-prefix form",
			in:   "stripe:acct_1Nn0FaFAhaWeDyow:stripe:txn_abc:0",
			want: "stripe:acct_1Nn0FaFAhaWeDyow:txn_abc",
		},
		{
			name: "stripe old indexed form",
			in:   "stripe:acct_xyz:txn_abc:0",
			want: "stripe:acct_xyz:txn_abc",
		},
		{
			name: "already clean etherscan form returns empty",
			in:   "gnosis:0xd578:0xabc:0",
			want: "",
		},
		{
			name: "already clean stripe form returns empty",
			in:   "stripe:acct_xyz:txn_abc",
			want: "",
		},
		{
			name: "empty input",
			in:   "",
			want: "",
		},
		{
			name: "unrecognized form",
			in:   "some:random:thing",
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CanonicalizeImportID(tc.in); got != tc.want {
				t.Fatalf("CanonicalizeImportID(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestTxHashFromURI(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"ethereum:100:tx:0xabc123", "0xabc123"},
		{"stripe:txn_abc", "txn_abc"},
		{"stripe:acct_xyz", "acct_xyz"},
		{"", ""},
		{"unknown:thing", ""},
		{"ethereum:100:address:0xabc", ""},
	}
	for _, tc := range cases {
		if got := TxHashFromURI(tc.in); got != tc.want {
			t.Errorf("TxHashFromURI(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
