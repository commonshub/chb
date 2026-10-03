package cmd

// Nostr annotations: which relays, whose annotations count, and what an
// annotation is.
//
// Anyone may annotate a transaction or an Odoo document on Nostr, but the
// published dataset only applies annotations signed by a trusted author
// (settings.json `nostr.trustedAuthors`, plus this instance's own key).
// The newest trusted snapshot per URI wins; everything else is ignored.
//
// An annotation snapshot is a kind 1111 event tagged with lowercase `i`
// (the URI) and `k` (its kind) only, plus `category`, `collective`,
// `event`, `spread`, and its content as the description. An event with an
// uppercase `I` is a NIP-22 comment (a discussion, shown as such by the
// website) and is never applied, even when it also carries `i`.

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
)

// defaultNostrRelays is where annotations are read and published unless
// settings.json `nostr.relays` says otherwise.
var defaultNostrRelays = []string{"wss://relay.commonshub.brussels"}

// defaultTrustedAuthors seeds the trust list: the commonshub.brussels
// website (https://commonshub.brussels/api/nostr/identity) and chb's own
// publishing key.
var defaultTrustedAuthors = []string{
	"npub1wfaa749vdzd8tnu823rd6fpqj8twhlqen02clnsuv68cgpgcxjfqcyrgut", // commonshub.brussels website (727bdf54…)
	"npub1d8z6e7rkscejnym7m3v5036fh04s94755anqsvzxg3xp3jn4l6gsvza3qc", // chb
}

// NostrSettings is settings.json `nostr`.
type NostrSettings struct {
	Relays         []string `json:"relays,omitempty"`
	TrustedAuthors []string `json:"trustedAuthors,omitempty"` // npub… or hex
}

// loadNostrSettings reads settings.json `nostr` directly: no settings
// reconciliation side effects, cheap enough to call per lookup.
func loadNostrSettings() NostrSettings {
	data, err := os.ReadFile(settingsFilePath("settings.json"))
	if err != nil {
		return NostrSettings{}
	}
	var s struct {
		Nostr NostrSettings `json:"nostr"`
	}
	if json.Unmarshal(data, &s) != nil {
		return NostrSettings{}
	}
	return s.Nostr
}

// nostrRelayList is the relays chb reads from and publishes to.
func nostrRelayList() []string {
	if r := loadNostrSettings().Relays; len(r) > 0 {
		return r
	}
	return defaultNostrRelays
}

// nostrTrustedPubkeys returns the trusted authors as lowercase hex keys.
func nostrTrustedPubkeys() map[string]bool {
	list := loadNostrSettings().TrustedAuthors
	if len(list) == 0 {
		list = defaultTrustedAuthors
	}
	out := map[string]bool{}
	for _, a := range list {
		if hex := nostrPubkeyHex(a); hex != "" {
			out[hex] = true
		}
	}
	if keys := LoadNostrKeys(); keys != nil && keys.PubHex != "" {
		out[strings.ToLower(keys.PubHex)] = true // our own annotations
	}
	return out
}

// nostrPubkeyHex accepts an npub or a 64-char hex key.
func nostrPubkeyHex(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "npub1") {
		if prefix, v, err := nip19.Decode(s); err == nil && prefix == "npub" {
			if hex, ok := v.(string); ok {
				return strings.ToLower(hex)
			}
		}
		return ""
	}
	if len(s) == 64 && strings.Trim(strings.ToLower(s), "0123456789abcdef") == "" {
		return strings.ToLower(s)
	}
	return ""
}

// isNostrComment: an uppercase `I` marks a NIP-22 comment, not an annotation.
func isNostrComment(ev NostrEvent) bool {
	for _, t := range ev.Tags {
		if len(t) > 0 && t[0] == "I" {
			return true
		}
	}
	return false
}

// nostrEventSignatureValid checks the event id and signature, so a relay
// (or anyone) cannot put a trusted pubkey on someone else's event.
func nostrEventSignatureValid(ev NostrEvent) bool {
	tags := make(nostr.Tags, len(ev.Tags))
	for i, t := range ev.Tags {
		tags[i] = nostr.Tag(t)
	}
	e := nostr.Event{
		ID: ev.ID, PubKey: ev.PubKey, CreatedAt: nostr.Timestamp(ev.CreatedAt),
		Kind: ev.Kind, Tags: tags, Content: ev.Content, Sig: ev.Sig,
	}
	if e.GetID() != ev.ID {
		return false
	}
	ok, err := e.CheckSignature()
	return err == nil && ok
}

// acceptAnnotationEvent: a kind 1111 snapshot, not a comment, signed by a
// trusted author.
func acceptAnnotationEvent(ev NostrEvent, trusted map[string]bool) bool {
	if ev.Kind != 1111 || isNostrComment(ev) {
		return false
	}
	if !trusted[strings.ToLower(ev.PubKey)] {
		return false
	}
	return nostrEventSignatureValid(ev)
}

// annotationTags builds the tags of an annotation snapshot: lowercase
// i/k only. Never I/K, which would turn it into a comment.
func annotationTags(uri string) nostr.Tags {
	return nostr.Tags{{"i", uri}, {"k", uriKind(uri)}}
}

// annotationsFromEvents keeps, per URI, the newest accepted snapshot.
func annotationsFromEvents(events map[string]NostrEvent, trusted map[string]bool) map[string]*TxAnnotation {
	out := map[string]*TxAnnotation{}
	for _, ev := range events {
		if !acceptAnnotationEvent(ev, trusted) {
			continue
		}
		for _, tag := range ev.Tags {
			if len(tag) < 2 || tag[0] != "i" {
				continue
			}
			uri := tag[1]
			if existing, ok := out[uri]; !ok || ev.CreatedAt > existing.CreatedAt ||
				(ev.CreatedAt == existing.CreatedAt && ev.ID > existing.NostrEventID) {
				out[uri] = parseAnnotation(uri, ev)
			}
		}
	}
	return out
}

// annotationTrusted is the generate-time check on cached annotations
// (caches written before v3.18 may hold untrusted ones).
func annotationTrusted(a *TxAnnotation, trusted map[string]bool) bool {
	return a != nil && trusted[strings.ToLower(a.Author)]
}

// trustedNostrMetadata drops chain transaction and address annotations not
// signed by a trusted author (caches written before v3.18).
func trustedNostrMetadata(c NostrMetadataCache) NostrMetadataCache {
	trusted := nostrTrustedPubkeys()
	out := c
	out.Transactions = map[string]*TxMetadata{}
	for k, v := range c.Transactions {
		if v != nil && trusted[strings.ToLower(v.Author)] {
			out.Transactions[k] = v
		}
	}
	out.Addresses = map[string]*AddressMetadata{}
	for k, v := range c.Addresses {
		if v != nil && trusted[strings.ToLower(v.Author)] {
			out.Addresses[k] = v
		}
	}
	return out
}
