package odoo

import (
	"testing"
	"time"
)

// TestRPCHTTPClientHasTimeout guards against regressing to http.DefaultClient
// (no timeout), which would let a stalled Odoo response hang the whole
// `chb sync` cron indefinitely.
func TestRPCHTTPClientHasTimeout(t *testing.T) {
	if rpcHTTPClient.Timeout <= 0 {
		t.Fatalf("rpcHTTPClient must have a positive timeout, got %v", rpcHTTPClient.Timeout)
	}
}

func TestResolveRPCHTTPTimeout(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"unset uses default", "", defaultRPCHTTPTimeout},
		{"valid override", "45", 45 * time.Second},
		{"zero falls back", "0", defaultRPCHTTPTimeout},
		{"negative falls back", "-5", defaultRPCHTTPTimeout},
		{"garbage falls back", "soon", defaultRPCHTTPTimeout},
		{"whitespace trimmed", "  30  ", 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ODOO_RPC_TIMEOUT_SECONDS", tc.env)
			if got := resolveRPCHTTPTimeout(); got != tc.want {
				t.Fatalf("resolveRPCHTTPTimeout(%q) = %v, want %v", tc.env, got, tc.want)
			}
		})
	}
}
