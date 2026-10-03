package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func signedKind(t *testing.T, sk string, kind int, created int64, tags nostr.Tags) NostrEvent {
	t.Helper()
	e := nostr.Event{Kind: kind, CreatedAt: nostr.Timestamp(created), Tags: tags}
	if err := e.Sign(sk); err != nil {
		t.Fatal(err)
	}
	ev := NostrEvent{ID: e.ID, PubKey: e.PubKey, CreatedAt: int64(e.CreatedAt), Kind: e.Kind, Sig: e.Sig}
	for _, tg := range e.Tags {
		ev.Tags = append(ev.Tags, []string(tg))
	}
	return ev
}

func TestTrustThroughFollows(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	seedSK, friendSK, strangerSK, outsiderSK := nostr.GeneratePrivateKey(), nostr.GeneratePrivateKey(), nostr.GeneratePrivateKey(), nostr.GeneratePrivateKey()
	seed, _ := nostr.GetPublicKey(seedSK)
	friend, _ := nostr.GetPublicKey(friendSK)
	stranger, _ := nostr.GetPublicKey(strangerSK)
	writeFile(t, filepath.Join(tmp, "app", "settings", "settings.json"), `{"nostr":{"trustedAuthors":["`+seed+`"]}}`)
	seeds := nostrTrustSeeds()

	follows := signedKind(t, seedSK, 3, 100, nostr.Tags{{"p", friend}})
	byOutsider := signedKind(t, outsiderSK, 3, 100, nostr.Tags{{"p", stranger}}) // not a seed
	forged := signedKind(t, strangerSK, 3, 200, nostr.Tags{{"p", stranger}})
	forged.PubKey = seed // claims to be the seed

	got := followsFromContactLists(map[string]NostrEvent{"a": follows, "b": byOutsider, "c": forged}, seeds)
	if len(got) != 1 || len(got[friend]) != 1 || got[friend][0] != seed {
		t.Fatalf("follows = %v (only the seed's signed list counts)", got)
	}

	// Persist as the pull would, then the trusted set includes the friend.
	writeFile(t, nostrTrustFilePath(DataDir()), `{"seeds":["`+seed+`"],"follows":{"`+friend+`":["`+seed+`"]}}`)
	if tr := nostrTrustedPubkeys(); !tr[friend] || !tr[seed] || tr[stranger] {
		t.Errorf("trusted = %v", tr)
	}

	// Unfollowing: the newer contact list replaces the older one.
	unfollow := signedKind(t, seedSK, 3, 300, nostr.Tags{})
	if got := followsFromContactLists(map[string]NostrEvent{"a": follows, "u": unfollow}, seeds); len(got) != 0 {
		t.Errorf("after unfollow: %v", got)
	}

	// trustFollows:false turns it off.
	writeFile(t, filepath.Join(tmp, "app", "settings", "settings.json"), `{"nostr":{"trustedAuthors":["`+seed+`"],"trustFollows":false}}`)
	if nostrTrustedPubkeys()[friend] {
		t.Error("follows must not count with trustFollows:false")
	}
	os.Remove(nostrTrustFilePath(DataDir()))
}

func TestMoveNeedsPublish(t *testing.T) {
	m := OdooOutgoingInvoicePublic{Category: "drinks", Collective: "commonshub"}
	ann := &TxAnnotation{Category: "drinks", Collective: "commonshub", NostrEventID: "e1", Author: "x"}
	if !moveNeedsPublish(m, nil, "") {
		t.Error("Odoo categorised, nothing on Nostr: publish")
	}
	if moveNeedsPublish(m, ann, "e1") {
		t.Error("Odoo and the applied annotation agree: nothing to publish")
	}
	m2 := m
	m2.Category = "catering" // the accountant changed it after consolidation
	if !moveNeedsPublish(m2, ann, "e1") {
		t.Error("an Odoo correction after consolidation must be published")
	}
	if moveNeedsPublish(m2, ann, "") {
		t.Error("a trusted annotation not yet applied to Odoo must not be overridden by stale Odoo data")
	}
	if moveNeedsPublish(OdooOutgoingInvoicePublic{}, nil, "") {
		t.Error("an uncategorised document publishes nothing")
	}
}

// A trusted annotation on a KBC transaction (iban:<iban>:tx:<statement line
// id>) drives the line's analytic distribution in Odoo once; after it is in
// the ledger, Odoo is the truth again.
func TestKBCCategorizeAppliesAnnotationOnce(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))
	acc := &AccountConfig{Slug: "kbc", Provider: "kbcbrussels", IBAN: "BE46 7340 7223 8636"}
	plans := &OdooAnalyticPlansFile{Categories: []OdooAnalyticAccountID{{Slug: "drinks", AccountID: 41}}}
	lines := []odooCategorizeLine{{StatementLineID: 39976, MoveLineID: 1, ImportID: "no-csv-row"}}
	uri := BuildIBANTxURI(acc.IBAN, "39976")
	anns := map[string]*TxAnnotation{uri: {URI: uri, Category: "drinks", NostrEventID: "ev1"}}

	plan := buildCategorizeJournalPlanWithAnnotations(lines, nil, acc, kbcMergeContext{}, plans, anns, map[string]string{})
	if len(plan.ToUpdate) != 1 || plan.ToUpdate[0].DesiredDistribution[41] != 100 || plan.ToUpdate[0].AnnotationEventID != "ev1" {
		t.Fatalf("plan = %+v (a line without CSV row is still categorised from its annotation)", plan)
	}
	plan = buildCategorizeJournalPlanWithAnnotations(lines, nil, acc, kbcMergeContext{}, plans, anns, map[string]string{uri: "ev1"})
	if len(plan.ToUpdate) != 0 || plan.Missing != 1 {
		t.Errorf("an applied annotation must not be pushed again: %+v", plan)
	}
}
