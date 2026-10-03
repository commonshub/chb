package cmd

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func signedAttestation(t *testing.T, sk string, created int64, content string, tags nostr.Tags) NostrEvent {
	t.Helper()
	e := nostr.Event{Kind: 31926, CreatedAt: nostr.Timestamp(created), Tags: tags, Content: content}
	if err := e.Sign(sk); err != nil {
		t.Fatal(err)
	}
	ev := NostrEvent{ID: e.ID, PubKey: e.PubKey, CreatedAt: int64(e.CreatedAt), Kind: e.Kind, Sig: e.Sig, Content: e.Content}
	for _, tg := range e.Tags {
		ev.Tags = append(ev.Tags, []string(tg))
	}
	return ev
}

func TestTrustThroughAttestations(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	seedSK, outsiderSK := nostr.GeneratePrivateKey(), nostr.GeneratePrivateKey()
	seed, _ := nostr.GetPublicKey(seedSK)
	key := func() string { k, _ := nostr.GetPublicKey(nostr.GeneratePrivateKey()); return k }
	member, steward, visitor, otherGuild, stranger := key(), key(), key(), key(), key()
	writeFile(t, filepath.Join(tmp, "app", "settings", "settings.json"),
		`{"discord":{"guildId":"g1"},"nostr":{"trustedAuthors":["`+seed+`"]}}`)
	seeds := nostrTrustSeeds()

	events := map[string]NostrEvent{
		"m": signedAttestation(t, seedSK, 100, `{"name":"M"}`, nostr.Tags{{"d", "discord:1"}, {"p", member}, {"role", "member"}, {"i", "discord:g1"}}),
		"s": signedAttestation(t, seedSK, 100, `{"name":"S","roles":["steward"]}`, nostr.Tags{{"d", "discord:2"}, {"p", steward}, {"i", "discord:g1"}}),
		"v": signedAttestation(t, seedSK, 100, `{"name":"V"}`, nostr.Tags{{"d", "discord:3"}, {"p", visitor}, {"i", "discord:g1"}}),
		"g": signedAttestation(t, seedSK, 100, `{}`, nostr.Tags{{"d", "discord:4"}, {"p", otherGuild}, {"role", "member"}, {"i", "discord:g2"}}),
		"o": signedAttestation(t, outsiderSK, 100, `{}`, nostr.Tags{{"d", "discord:5"}, {"p", stranger}, {"role", "member"}}),
	}
	forged := signedAttestation(t, outsiderSK, 100, `{}`, nostr.Tags{{"d", "discord:6"}, {"p", stranger}, {"role", "member"}})
	forged.PubKey = seed
	events["f"] = forged

	att := attestationsFromEvents(events, seeds, "g1")
	if _, ok := att[otherGuild]; ok {
		t.Error("an attestation for another Discord guild must not count")
	}
	if _, ok := att[stranger]; ok {
		t.Error("only a seed's signed attestations count")
	}
	data, _ := json.Marshal(NostrTrustFile{Seeds: []string{seed}, Attested: att})
	writeFile(t, nostrTrustFilePath(DataDir()), string(data))
	tr := nostrTrustedPubkeys()
	if !tr[member] || !tr[steward] {
		t.Errorf("member and steward keys must be trusted: %v", tr)
	}
	if tr[visitor] {
		t.Error("an attestation without an allowed role must not count")
	}

	// Revocation: a newer attestation for the same d without the p tag.
	events["m2"] = signedAttestation(t, seedSK, 200, `{}`, nostr.Tags{{"d", "discord:1"}, {"role", "member"}, {"i", "discord:g1"}})
	if _, ok := attestationsFromEvents(events, seeds, "g1")[member]; ok {
		t.Error("a newer attestation without the key revokes it")
	}

	// trustAttestations:false turns it off.
	writeFile(t, filepath.Join(tmp, "app", "settings", "settings.json"),
		`{"nostr":{"trustedAuthors":["`+seed+`"],"trustAttestations":false}}`)
	if nostrTrustedPubkeys()[member] {
		t.Error("attestations must not count with trustAttestations:false")
	}
}
