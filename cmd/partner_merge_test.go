package cmd

import (
	"path/filepath"
	"testing"
)

// withTempMergeStore points the merge store at a temp data dir for the test.
func withTempMergeStore(t *testing.T) {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))
}

func TestPartnerMergeRecordAndLoad(t *testing.T) {
	withTempMergeStore(t)
	if len(loadPartnerMerges()) != 0 {
		t.Fatal("expected empty store")
	}
	if err := recordPartnerMerge(1445, "In Progress", []int{1446, 1447}, []string{"Aldo", "Cammara"}); err != nil {
		t.Fatal(err)
	}
	got := loadPartnerMerges()
	if len(got) != 1 || got[0].SurvivorID != 1445 || len(got[0].MergedIDs) != 2 {
		t.Fatalf("unexpected store: %+v", got)
	}
	if len(pendingPartnerMerges()) != 1 {
		t.Fatal("merge should be pending until applied")
	}
}

func TestPartnerCanonicalTransitive(t *testing.T) {
	withTempMergeStore(t)
	// 1446 → 1445, then 1445 → 1000 ⇒ 1446 resolves to 1000.
	_ = recordPartnerMerge(1445, "B", []int{1446}, nil)
	_ = recordPartnerMerge(1000, "A", []int{1445}, nil)
	canon := partnerCanonical()
	if canon[1446] != 1000 {
		t.Errorf("1446 should resolve to 1000, got %d", canon[1446])
	}
	if canon[1445] != 1000 {
		t.Errorf("1445 should resolve to 1000, got %d", canon[1445])
	}
}

func TestContactsContextGrouping(t *testing.T) {
	withTempMergeStore(t)
	_ = recordPartnerMerge(1000, "Survivor", []int{1445, 1446}, nil)
	c := &contactsContext{canon: partnerCanonical(), members: map[int][]int{}}
	for v, s := range c.canon {
		c.members[s] = append(c.members[s], v)
	}
	if c.survivorOf(1445) != 1000 || c.survivorOf(1446) != 1000 {
		t.Error("victims should resolve to survivor 1000")
	}
	g := c.groupIDs(1446)
	for _, id := range []int{1000, 1445, 1446} {
		if !g[id] {
			t.Errorf("group should contain %d: %v", id, g)
		}
	}
}
