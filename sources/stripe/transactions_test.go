package stripe

import "testing"

func TestMergeTransactionsDedupesIncomingAndSortsNewestFirst(t *testing.T) {
	existing := []Transaction{
		{ID: "txn_old", Created: 100, Net: 100},
		{ID: "txn_dup", Created: 200, Net: 200},
	}
	incoming := []Transaction{
		{ID: "txn_new", Created: 300, Net: 300},
		{ID: "txn_dup", Created: 250, Net: 250},
	}

	got := MergeTransactions(existing, incoming)
	if len(got) != 3 {
		t.Fatalf("len(MergeTransactions()) = %d, want 3", len(got))
	}
	wantIDs := []string{"txn_new", "txn_dup", "txn_old"}
	wantNets := []int64{300, 250, 100}
	for i := range wantIDs {
		if got[i].ID != wantIDs[i] || got[i].Net != wantNets[i] {
			t.Fatalf("merged[%d] = (%s, %d), want (%s, %d)", i, got[i].ID, got[i].Net, wantIDs[i], wantNets[i])
		}
	}
}
