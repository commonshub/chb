package cmd

import (
	"path/filepath"
	"testing"
)

func TestRebuildCommissionsBuildsTenPercentTransfer(t *testing.T) {
	dataDir := t.TempDir()

	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "transactions.json"), `{
	  "year": "2026", "month": "04",
	  "transactions": [
	    {"id":"stripe:c1","provider":"stripe","currency":"EUR","amount":1210,"grossAmount":1210,"type":"CREDIT","collective":"openletter","category":"ticket","timestamp":1712000000},
	    {"id":"stripe:c2","provider":"stripe","currency":"EUR","amount":500,"grossAmount":500,"type":"CREDIT","collective":"openletter","category":"donation","timestamp":1712000100},
	    {"id":"stripe:c3","provider":"stripe","currency":"EUR","amount":100,"grossAmount":100,"type":"DEBIT","collective":"openletter","category":"supplies","timestamp":1712000200},
	    {"id":"stripe:c4","provider":"stripe","currency":"EUR","amount":300,"grossAmount":300,"type":"CREDIT","collective":"commonshub","category":"ticket","timestamp":1712000300}
	  ]
	}`)

	if err := rebuildCommissions(dataDir); err != nil {
		t.Fatalf("rebuildCommissions: %v", err)
	}

	items := LoadCommissions(dataDir, "2026", "04")
	if len(items) != 1 {
		t.Fatalf("expected 1 commission row, got %d: %+v", len(items), items)
	}
	c := items[0]
	if c.Collective != "openletter" {
		t.Errorf("commission collective: got %q, want openletter", c.Collective)
	}
	// gross income for openletter = 1210 + 500 = 1710, 10% = 171.00
	if c.Amount != "171.00" {
		t.Errorf("commission amount: got %q, want 171.00", c.Amount)
	}
	if c.Base != "1710.00" {
		t.Errorf("commission base: got %q, want 1710.00", c.Base)
	}
}

func TestRebuildCommissionsClearsOrphans(t *testing.T) {
	dataDir := t.TempDir()
	// Pretend a previous rebuild left a stale file with no source data.
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "05", "generated", "commissions.json"),
		`{"items":[{"collective":"openletter","currency":"EUR","amount":"50.00"}]}`)
	if err := rebuildCommissions(dataDir); err != nil {
		t.Fatalf("rebuildCommissions: %v", err)
	}
	if got := LoadCommissions(dataDir, "2026", "05"); len(got) != 0 {
		t.Errorf("orphan commissions should be removed, got %d rows", len(got))
	}
}

func TestBuildMonthlyReportFoldsCommissionsIntoCollectives(t *testing.T) {
	dataDir := t.TempDir()

	writeJSONFixture(t, filepath.Join(dataDir, "2026", "06", "generated", "transactions.json"), `{
	  "year": "2026", "month": "06",
	  "transactions": [
	    {"id":"stripe:x1","provider":"stripe","currency":"EUR","amount":1210,"grossAmount":1210,"type":"CREDIT","collective":"openletter","category":"ticket","timestamp":1717200000,"metadata":{"vatAmount":210}}
	  ]
	}`)

	if err := rebuildCommissions(dataDir); err != nil {
		t.Fatalf("rebuildCommissions: %v", err)
	}

	collectives, _ := buildMonthlyReportTaggedFlows(dataDir, "2026", "06")
	openletter := findFlow(collectives, "openletter")
	if openletter == nil {
		t.Fatalf("expected openletter row, got %+v", collectives)
	}
	// in = 1210; out = vat(210) + commission(121) = 331; net = 879
	if openletter.In != 1210 {
		t.Errorf("openletter.In: got %v, want 1210", openletter.In)
	}
	if openletter.VAT != 210 {
		t.Errorf("openletter.VAT: got %v, want 210", openletter.VAT)
	}
	if openletter.Out != 331 {
		t.Errorf("openletter.Out: got %v, want 331 (vat 210 + commission 121)", openletter.Out)
	}
	if openletter.Net != 879 {
		t.Errorf("openletter.Net: got %v, want 879", openletter.Net)
	}

	commonshub := findFlow(collectives, "commonshub")
	if commonshub == nil {
		t.Fatalf("expected commonshub row from commission credit, got %+v", collectives)
	}
	if commonshub.In != 121 {
		t.Errorf("commonshub.In: got %v, want 121 (commission credit)", commonshub.In)
	}
	if commonshub.Net != 121 {
		t.Errorf("commonshub.Net: got %v, want 121", commonshub.Net)
	}
}
