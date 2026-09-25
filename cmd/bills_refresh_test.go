package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// refreshOpenBills finds old bills whose payment state moved (or that are
// open and unknown locally) with one light query and fetches only those in
// full.
func TestRefreshOpenBillsFetchesOnlyChangedOnes(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	dataDir := filepath.Join(tmp, "data")
	t.Setenv("DATA_DIR", dataDir)

	b := testBills()
	b[0].WriteDate = "2026-05-01 10:00:00" // 101: open locally, now paid in Odoo
	b[1].WriteDate = "2026-05-01 10:00:00" // 102: open, unchanged
	seedBills(t, dataDir, "2025", "04", b[:2])

	var liteDomain []interface{}
	var fullIDs []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Params struct {
				Args []json.RawMessage `json:"args"`
			} `json:"params"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		var method string
		if len(req.Params.Args) > 4 {
			json.Unmarshal(req.Params.Args[4], &method)
		}
		switch method {
		case "fields_get":
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"id":{},"state":{},"payment_state":{},"write_date":{},"name":{},"move_type":{},"invoice_date":{},"amount_total":{},"amount_residual":{}}}`))
		case "search_read":
			var domainWrap []json.RawMessage
			json.Unmarshal(req.Params.Args[5], &domainWrap)
			var kw struct {
				Fields []string `json:"fields"`
				Offset int      `json:"offset"`
			}
			json.Unmarshal(req.Params.Args[6], &kw)
			if kw.Offset > 0 {
				w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":[]}`))
				return
			}
			if len(kw.Fields) == 4 { // the light candidate query
				json.Unmarshal(domainWrap[0], &liteDomain)
				w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":[
					{"id":101,"state":"posted","payment_state":"paid","write_date":"2026-06-01 09:00:00"},
					{"id":102,"state":"posted","payment_state":"partial","write_date":"2026-05-01 10:00:00"},
					{"id":201,"state":"posted","payment_state":"not_paid","write_date":"2026-06-02 09:00:00"}]}`))
				return
			}
			var dom [][]interface{}
			json.Unmarshal(domainWrap[0], &dom)
			for _, v := range dom[0][2].([]interface{}) {
				fullIDs = append(fullIDs, int(v.(float64)))
			}
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":[{"id":101},{"id":201}]}`))
		default:
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":[]}`))
		}
	}))
	defer server.Close()

	creds := &OdooCredentials{URL: server.URL, DB: "testdb", Login: "x", Password: "y"}
	got, err := refreshOpenBills(creds, 2, dataDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	sort.Ints(fullIDs)
	if len(fullIDs) != 2 || fullIDs[0] != 101 || fullIDs[1] != 201 || len(got) != 2 {
		t.Errorf("fetched in full %v (got %d records), want [101 201]: 101 changed, 201 unknown, 102 unchanged", fullIDs, len(got))
	}
	// The candidate query covers open bills in Odoo OR bills open in our cache.
	raw, _ := json.Marshal(liteDomain)
	if !json.Valid(raw) || !strings.Contains(string(raw), `"|"`) || !strings.Contains(string(raw), `"id","in"`) {
		t.Errorf("candidate domain = %s", raw)
	}

	// Bills the date-window fetch already has are not fetched twice.
	fullIDs = nil
	if _, err := refreshOpenBills(creds, 2, dataDir, []map[string]interface{}{{"id": float64(201)}}); err != nil {
		t.Fatal(err)
	}
	if len(fullIDs) != 1 || fullIDs[0] != 101 {
		t.Errorf("with 201 already fetched, full fetch = %v, want [101]", fullIDs)
	}
}
