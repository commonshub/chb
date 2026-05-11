package cmd

import (
	"path/filepath"
	"testing"
)

func TestRebuildInboundSpreadsAndAccrualAggregation(t *testing.T) {
	dataDir := t.TempDir()

	// Natural-month tx in 2025-12 with a spread targeting 2026-01..2026-03.
	// (€900 paid in December, recognized as €300/month in Jan/Feb/Mar.)
	writeJSONFixture(t, filepath.Join(dataDir, "2025", "12", "generated", "transactions.json"), `{
	  "year": "2025", "month": "12",
	  "transactions": [{
	    "id": "stripe:ch_aaa",
	    "txHash": "",
	    "provider": "stripe",
	    "stripeChargeId": "ch_aaa",
	    "currency": "EUR",
	    "amount": 900,
	    "grossAmount": 900,
	    "type": "DEBIT",
	    "counterparty": "Acme Annual Insurance",
	    "category": "insurance",
	    "collective": "commonshub",
	    "spread": [
	      {"month": "2026-01", "amount": "-300.00"},
	      {"month": "2026-02", "amount": "-300.00"},
	      {"month": "2026-03", "amount": "-300.00"}
	    ],
	    "timestamp": 1735680000
	  }]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2025", "12", "sources", "nostr", "transaction-annotations.json"), `{
	  "annotations": {
	    "stripe:txn:ch_aaa": {
	      "uri": "stripe:txn:ch_aaa",
	      "category": "insurance",
	      "collective": "commonshub",
	      "spread": [
	        {"month": "2026-01", "amount": "-300.00"},
	        {"month": "2026-02", "amount": "-300.00"},
	        {"month": "2026-03", "amount": "-300.00"}
	      ]
	    }
	  }
	}`)

	// 2026-01 has a regular tx + the inbound projection should add €300.
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "01", "generated", "transactions.json"), `{
	  "year": "2026", "month": "01",
	  "transactions": [{
	    "id": "stripe:ch_bbb", "stripeChargeId": "ch_bbb",
	    "provider": "stripe", "currency": "EUR",
	    "amount": 50, "grossAmount": 50,
	    "type": "CREDIT", "counterparty": "Donor",
	    "collective": "commonshub", "category": "donation",
	    "timestamp": 1735776000
	  }]
	}`)

	if err := rebuildInboundSpreads(dataDir); err != nil {
		t.Fatalf("rebuildInboundSpreads: %v", err)
	}

	// 2026-01 should have an inbound entry; 2026-02 and 2026-03 too.
	for _, ym := range []string{"2026-01", "2026-02", "2026-03"} {
		got := LoadInboundSpreads(dataDir, ym[:4], ym[5:])
		if len(got) != 1 {
			t.Fatalf("%s inbound: got %d rows, want 1: %#v", ym, len(got), got)
		}
		if got[0].NaturalYM != "2025-12" {
			t.Errorf("%s NaturalYM: got %q, want 2025-12", ym, got[0].NaturalYM)
		}
		if got[0].Amount != "-300.00" {
			t.Errorf("%s Amount: got %q, want -300.00", ym, got[0].Amount)
		}
		if got[0].Collective != "commonshub" || got[0].Category != "insurance" {
			t.Errorf("%s tags: got %+v", ym, got[0])
		}
	}

	// 2025-12 (natural) should NOT have an inbound file.
	if got := LoadInboundSpreads(dataDir, "2025", "12"); len(got) != 0 {
		t.Errorf("2025-12 should not have inbound (natural month not in spread), got %d rows", len(got))
	}

	// Accrual: 2026-01 collectives/categories sum the natural tx + inbound.
	collectives, categories := buildMonthlyReportTaggedFlows(dataDir, "2026", "01")
	commonshub := findFlow(collectives, "commonshub")
	if commonshub == nil {
		t.Fatalf("expected commonshub row in collectives: %+v", collectives)
	}
	if commonshub.In != 50 {
		t.Errorf("commonshub.In: got %v, want 50 (donor)", commonshub.In)
	}
	if commonshub.Out != 300 {
		t.Errorf("commonshub.Out: got %v, want 300 (inbound spread allocation)", commonshub.Out)
	}
	insurance := findFlow(categories, "insurance")
	if insurance == nil || insurance.Out != 300 {
		t.Errorf("insurance.Out: got %+v, want 300", insurance)
	}

	// 2025-12: natural tx has full spread elsewhere → contributes 0 to Dec.
	collectives, categories = buildMonthlyReportTaggedFlows(dataDir, "2025", "12")
	if commonshub := findFlow(collectives, "commonshub"); commonshub != nil {
		t.Errorf("2025-12 commonshub should be absent (full amount deferred), got %+v", commonshub)
	}
	if insurance := findFlow(categories, "insurance"); insurance != nil {
		t.Errorf("2025-12 insurance should be absent (full amount deferred), got %+v", insurance)
	}
}

func TestRebuildInboundSpreadsClearsOrphans(t *testing.T) {
	dataDir := t.TempDir()
	// Pretend a previous rebuild left an orphan.
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "06", "generated", "inbound_spreads.json"), `{"inbound":[{"uri":"stripe:txn:old","naturalYM":"2024-01","amount":"-100"}]}`)
	// No annotations at all → rebuild should remove the orphan.
	if err := rebuildInboundSpreads(dataDir); err != nil {
		t.Fatalf("rebuildInboundSpreads: %v", err)
	}
	if got := LoadInboundSpreads(dataDir, "2026", "06"); len(got) != 0 {
		t.Errorf("orphan inbound should have been removed, got %d rows", len(got))
	}
}

func findFlow(rows []MonthlyReportTaggedFlow, tag string) *MonthlyReportTaggedFlow {
	for i := range rows {
		if rows[i].Tag == tag {
			return &rows[i]
		}
	}
	return nil
}
