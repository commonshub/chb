package cmd

import "testing"

func TestParseOdooSyncStagesDefault(t *testing.T) {
	stages := parseOdooSyncStages(nil)
	if !stages.Transactions || !stages.Partners || !stages.Accounts || !stages.Metadata || !stages.Reconcile || stages.Explicit {
		t.Fatalf("default stages = %+v, want all enabled and not explicit", stages)
	}
}

func TestParseOdooSyncStagesExplicit(t *testing.T) {
	stages := parseOdooSyncStages([]string{"--transactions", "--accounts"})
	if !stages.Explicit {
		t.Fatalf("explicit stages = %+v, want Explicit=true", stages)
	}
	if !stages.Transactions || stages.Partners || !stages.Accounts || stages.Metadata || stages.Reconcile {
		t.Fatalf("explicit stages = %+v, want only transactions+accounts", stages)
	}
}

func TestParseOdooSyncStagesMetadata(t *testing.T) {
	stages := parseOdooSyncStages([]string{"--metadata"})
	if !stages.Explicit {
		t.Fatalf("metadata stages = %+v, want Explicit=true", stages)
	}
	if stages.Transactions || stages.Partners || stages.Accounts || !stages.Metadata || stages.Reconcile {
		t.Fatalf("metadata stages = %+v, want only metadata", stages)
	}
}
