package cmd

import (
	"fmt"
	"testing"
)

// Members sharing a creation date must come out in the same order every
// run, or the members/ and public/ hashes in hashes.json move for nothing.
func TestMergeProviderSnapshotsIsDeterministic(t *testing.T) {
	var subs []providerSubscription
	for i := 0; i < 30; i++ {
		subs = append(subs, providerSubscription{ID: fmt.Sprintf("sub_%02d", 29-i), EmailHash: fmt.Sprintf("h%02d", i), CreatedAt: "2026-02-02"})
	}
	snap := []providerSnapshot{{Provider: "stripe", Subscriptions: subs}}
	first := mergeProviderSnapshots(snap)
	for run := 0; run < 20; run++ {
		got := mergeProviderSnapshots(snap)
		for i := range got {
			if got[i].ID != first[i].ID {
				t.Fatalf("run %d: position %d is %s, was %s", run, i, got[i].ID, first[i].ID)
			}
		}
	}
	if first[0].ID != "sub_00" {
		t.Errorf("ties break on id: first = %s", first[0].ID)
	}
}
