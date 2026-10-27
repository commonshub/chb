package cmd

import (
	"encoding/json"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip44"
)

func gm(id, name string, member bool) GuildMember {
	return GuildMember{ID: id, Username: name, DisplayName: name, MemberRole: member}
}

func TestBuildDiscordPartnersPlan(t *testing.T) {
	salt := "s"
	partners := []OdooPartner{
		{ID: 1, Name: "Ann", Email: "ann@x.org", Active: true, DiscordID: "100000000000000001"}, // already linked
		{ID: 2, Name: "Bob B", Email: "bob@x.org", Active: true},                                // via email hash
		{ID: 3, Name: "Cleo", Active: true},                                                     // via hand map, no email
		{ID: 4, Name: "Dan", Active: true},                                                      // same name as a Discord member
		{ID: 5, Name: "Eve", Active: true, CategoryIDs: []int{9}, DiscordID: "100000000000000005"},
		{ID: 6, Name: "Taken", Email: "taken@x.org", Active: true},
		{ID: 7, Name: "Old", Active: false, DiscordID: "100000000000000008"}, // archived: ignored
	}
	members := []GuildMember{
		gm("100000000000000001", "ann", true),
		gm("100000000000000002", "bobby", true),
		gm("100000000000000003", "cleo", true),
		gm("100000000000000004", "Dan", true),
		gm("100000000000000005", "eve", true),
		gm("100000000000000006", "fay", true),
		gm("100000000000000007", "gus", true),
		gm("100000000000000008", "hal", true),
		gm("100000000000000009", "notamember", false),
	}
	plan := buildDiscordPartnersPlan(discordPartnerInputs{
		Members: members, Partners: partners, TagID: 9, Salt: salt,
		Hand:      map[string]string{"odoo:3": "100000000000000003"},
		HashLinks: map[string]string{hashEmail("bob@x.org", salt): "100000000000000002"},
		Emails: map[string]PartnerEmailSubmission{
			"100000000000000005": {DiscordID: "100000000000000005", Email: "eve@x.org"},   // set on a Discord-tagged partner
			"100000000000000006": {DiscordID: "100000000000000006", Email: "fay@x.org"},   // new partner with email
			"100000000000000007": {DiscordID: "100000000000000007", Email: "taken@x.org"}, // duplicate
		},
	})
	if plan.Members != 8 || plan.Linked != 2 {
		t.Fatalf("members %d linked %d", plan.Members, plan.Linked)
	}
	got := map[string]string{}
	for _, a := range plan.Actions {
		got[a.DiscordID] = a.Op
	}
	want := map[string]string{
		"100000000000000002": "link", "100000000000000003": "link",
		"100000000000000005": "set_email", "100000000000000006": "create", "100000000000000008": "create",
	}
	if len(got) != len(want) {
		t.Errorf("actions %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: got %q want %q (all %v)", k, got[k], v, got)
		}
	}
	reviewed := map[string]bool{}
	for _, r := range plan.Review {
		reviewed[r.DiscordID] = true
	}
	if !reviewed["100000000000000004"] || !reviewed["100000000000000007"] || len(plan.Review) != 2 {
		t.Errorf("review %+v", plan.Review)
	}
	// Cleo (linked by hand, no email) and hal (created, no email) need one.
	if len(plan.NeedsEmail) != 2 || plan.NeedsEmail[0] != "100000000000000003" || plan.NeedsEmail[1] != "100000000000000008" {
		t.Errorf("needsEmail %v", plan.NeedsEmail)
	}
}

func TestDiscordPartnersEmailOnlyOnTaggedPartners(t *testing.T) {
	plan := buildDiscordPartnersPlan(discordPartnerInputs{
		Members:  []GuildMember{gm("100000000000000001", "ann", true)},
		Partners: []OdooPartner{{ID: 1, Name: "Ann", Active: true, DiscordID: "100000000000000001"}},
		TagID:    9,
		Emails:   map[string]PartnerEmailSubmission{"100000000000000001": {Email: "ann@x.org"}},
	})
	if len(plan.Actions) != 0 || len(plan.Review) != 1 {
		t.Errorf("an older partner's email must be reviewed, not set: %+v", plan)
	}
}

func TestDecryptPartnerEmail(t *testing.T) {
	siteSK := nostr.GeneratePrivateKey()
	sitePK, _ := nostr.GetPublicKey(siteSK)
	chbSK := nostr.GeneratePrivateKey()
	chbPK, _ := nostr.GetPublicKey(chbSK)
	ephSK := nostr.GeneratePrivateKey()
	ephPK, _ := nostr.GetPublicKey(ephSK)
	ck, _ := nip44.GenerateConversationKey(chbPK, ephSK)
	body, _ := json.Marshal(map[string]string{"discordId": "100000000000000001", "email": "ann@x.org", "verifiedAt": "2026-10-28T10:00:00Z"})
	ct, err := nip44.Encrypt(string(body), ck)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(sk string) NostrEvent {
		ev := nostr.Event{Kind: partnerEmailEventKind, CreatedAt: 1, Content: ct,
			Tags: nostr.Tags{{"d", "partner-email:x"}, {"t", partnerEmailTag}, {"p", chbPK}, {"epk", ephPK}}}
		_ = ev.Sign(sk)
		tags := make([][]string, len(ev.Tags))
		for i, t := range ev.Tags {
			tags[i] = t
		}
		return NostrEvent{ID: ev.ID, PubKey: ev.PubKey, CreatedAt: int64(ev.CreatedAt), Kind: ev.Kind, Tags: tags, Content: ev.Content, Sig: ev.Sig}
	}
	k := &partnerEmailKey{PrivHex: chbSK, PubHex: chbPK}

	// Signed by someone other than the website: rejected.
	if _, err := decryptPartnerEmail(mk(siteSK), k); err == nil {
		t.Fatal("accepted an event not signed by the website key")
	}
	old := websiteNostrPubkeyForTest(sitePK)
	defer websiteNostrPubkeyForTest(old)
	sub, err := decryptPartnerEmail(mk(siteSK), k)
	if err != nil || sub.Email != "ann@x.org" || sub.DiscordID != "100000000000000001" {
		t.Fatalf("decrypt: %+v %v", sub, err)
	}
}

func websiteNostrPubkeyForTest(pk string) string {
	old := websiteNostrPubkey
	websiteNostrPubkey = pk
	return old
}
