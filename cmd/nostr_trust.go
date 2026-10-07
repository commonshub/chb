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
	"path/filepath"
	"sort"
	"strings"

	nostrsource "github.com/CommonsHub/chb/providers/nostr"
	odoosource "github.com/CommonsHub/chb/providers/odoo"

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
	// token-bot (3a8e2823…): a seed since 2026-10-07 (Xavier), so the
	// steward keys it attests (Discord role containing "steward") may
	// change categories.
	"npub1828zsgu6xv0j0y0axr4agq5uf0yh8djxtd89guqgg8vuymqgc8es3ge93h",
}

// NostrSettings is settings.json `nostr`.
type NostrSettings struct {
	Relays         []string `json:"relays,omitempty"`
	TrustedAuthors []string `json:"trustedAuthors,omitempty"` // npub… or hex: the trust seeds
	// TrustFollows (default true): an author followed (kind 3 contact
	// list) by a seed is trusted too. One level only: follows of followed
	// authors are not.
	TrustFollows *bool `json:"trustFollows,omitempty"`
	// TrustAttestations (default true): a key a seed attests (kind 31926,
	// `p` tag, e.g. the website linking a member's browser key to their
	// Discord account) is trusted when the attestation carries one of
	// AttestationRoles (default member, steward) and names our Discord
	// guild. One level only, like follows.
	TrustAttestations *bool    `json:"trustAttestations,omitempty"`
	AttestationRoles  []string `json:"attestationRoles,omitempty"`
	// CategoryRoles (default steward): attested keys whose annotations may
	// set category, collective, spread and exclude. Seeds and the keys
	// they follow (bots, e.g. token-bot) always may. Other trusted
	// authors (members) only add notes, events and tags.
	CategoryRoles []string `json:"categoryRoles,omitempty"`
}

// defaultAttestationRoles: roles an attestation must carry for its key to
// be trusted.
var defaultAttestationRoles = []string{"member", "steward"}

func nostrTrustAttestations() bool {
	if v := loadNostrSettings().TrustAttestations; v != nil {
		return *v
	}
	return true
}

func nostrAttestationRoles() map[string]bool {
	list := loadNostrSettings().AttestationRoles
	if len(list) == 0 {
		list = defaultAttestationRoles
	}
	out := map[string]bool{}
	for _, r := range list {
		out[strings.ToLower(strings.TrimSpace(r))] = true
	}
	return out
}

// discordGuildIDSetting reads settings.json discord.guildId directly.
func discordGuildIDSetting() string {
	data, err := os.ReadFile(settingsFilePath("settings.json"))
	if err != nil {
		return ""
	}
	var s struct {
		Discord struct {
			GuildID string `json:"guildId"`
		} `json:"discord"`
	}
	_ = json.Unmarshal(data, &s)
	return strings.TrimSpace(s.Discord.GuildID)
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

// nostrTrustSeeds: settings.json trustedAuthors (or the defaults) plus this
// instance's own key, as lowercase hex.
func nostrTrustSeeds() map[string]bool {
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

func nostrTrustFollows() bool {
	if v := loadNostrSettings().TrustFollows; v != nil {
		return *v
	}
	return true
}

// nostrTrustedPubkeys returns every trusted author as lowercase hex keys:
// the seeds, the authors they follow, and the keys they attest with an
// allowed role, as recorded by the last `chb nostr pull` in trust.json.
// Offline: generate calls it.
func nostrTrustedPubkeys() map[string]bool {
	seeds := nostrTrustSeeds()
	out := map[string]bool{}
	for k := range seeds {
		out[k] = true
	}
	t := loadNostrTrustFile(DataDir())
	if nostrTrustFollows() {
		for hex, by := range t.Follows {
			for _, seed := range by {
				if seeds[seed] { // only while the follower is still a seed
					out[hex] = true
					break
				}
			}
		}
	}
	if nostrTrustAttestations() {
		roles := nostrAttestationRoles()
		for hex, att := range t.Attested {
			if !seeds[att.By] {
				continue // only while the attester is still a seed
			}
			for _, r := range att.Roles {
				if roles[r] {
					out[hex] = true
					break
				}
			}
		}
	}
	return out
}

// NostrTrustFile is latest/providers/nostr/trust.json: who is trusted
// because a seed follows them, and which seeds do.
type NostrTrustFile struct {
	UpdatedAt string              `json:"updatedAt"`
	Seeds     []string            `json:"seeds"`
	Follows   map[string][]string `json:"follows"` // followed pubkey → seeds following it
	// Attested: pubkey → the newest attestation of it by a seed (kind 31926).
	Attested map[string]NostrAttestation `json:"attested,omitempty"`
}

// NostrAttestation is a seed's kind 31926 attestation of a key.
type NostrAttestation struct {
	By      string   `json:"by"`      // attesting seed (hex)
	Subject string   `json:"subject"` // the attestation's d tag, e.g. discord:<user id>
	Roles   []string `json:"roles,omitempty"`
	EventID string   `json:"eventId"`
}

// attestationsFromEvents keeps, per (seed, d), the newest signed kind 31926
// by a seed (parameterised replaceable: a newer one revokes what an older
// one said), restricted to our Discord guild when one is configured, and
// maps each p-tagged key to it. Roles come from `role` tags and the
// content's "roles".
func attestationsFromEvents(events map[string]NostrEvent, seeds map[string]bool, guildID string) map[string]NostrAttestation {
	newest := map[string]NostrEvent{}
	for _, ev := range events {
		pk := strings.ToLower(ev.PubKey)
		if ev.Kind != 31926 || !seeds[pk] || !nostrEventSignatureValid(ev) {
			continue
		}
		key := pk + "|" + firstTagValue(ev.Tags, "d")
		if cur, ok := newest[key]; !ok || ev.CreatedAt > cur.CreatedAt {
			newest[key] = ev
		}
	}
	out := map[string]NostrAttestation{}
	keys := make([]string, 0, len(newest))
	for k := range newest {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ev := newest[k]
		if guildID != "" {
			if g := firstTagValue(ev.Tags, "i"); g != "" && g != "discord:"+guildID {
				continue
			}
		}
		roles := map[string]bool{}
		for _, t := range ev.Tags {
			if len(t) >= 2 && t[0] == "role" {
				roles[strings.ToLower(strings.TrimSpace(t[1]))] = true
			}
		}
		var content struct {
			Roles []string `json:"roles"`
		}
		if json.Unmarshal([]byte(ev.Content), &content) == nil {
			for _, r := range content.Roles {
				roles[strings.ToLower(strings.TrimSpace(r))] = true
			}
		}
		var rs []string
		for r := range roles {
			if r != "" {
				rs = append(rs, r)
			}
		}
		sort.Strings(rs)
		for _, t := range ev.Tags {
			if len(t) < 2 || t[0] != "p" {
				continue
			}
			hex := nostrPubkeyHex(t[1])
			if hex == "" || seeds[hex] {
				continue
			}
			out[hex] = NostrAttestation{By: strings.ToLower(ev.PubKey), Subject: firstTagValue(ev.Tags, "d"), Roles: rs, EventID: ev.ID}
		}
	}
	return out
}

func firstTagValue(tags [][]string, name string) string {
	for _, t := range tags {
		if len(t) >= 2 && t[0] == name {
			return t[1]
		}
	}
	return ""
}

func nostrTrustFilePath(dataDir string) string {
	return filepath.Join(dataDir, "latest", "providers", "nostr", "trust.json")
}

func loadNostrTrustFile(dataDir string) NostrTrustFile {
	var t NostrTrustFile
	if data, err := os.ReadFile(nostrTrustFilePath(dataDir)); err == nil {
		_ = json.Unmarshal(data, &t)
	}
	return t
}

// followsFromContactLists turns the seeds' contact lists (kind 3, the
// newest signed one per seed) into followed pubkey → seeds.
func followsFromContactLists(events map[string]NostrEvent, seeds map[string]bool) map[string][]string {
	newest := map[string]NostrEvent{}
	for _, ev := range events {
		pk := strings.ToLower(ev.PubKey)
		if ev.Kind != 3 || !seeds[pk] || !nostrEventSignatureValid(ev) {
			continue
		}
		if cur, ok := newest[pk]; !ok || ev.CreatedAt > cur.CreatedAt {
			newest[pk] = ev
		}
	}
	out := map[string][]string{}
	for seed, ev := range newest {
		for _, t := range ev.Tags {
			if len(t) < 2 || t[0] != "p" {
				continue
			}
			hex := nostrPubkeyHex(t[1])
			if hex == "" || seeds[hex] {
				continue
			}
			if !containsString(out[hex], seed) {
				out[hex] = append(out[hex], seed)
			}
		}
	}
	for k := range out {
		sort.Strings(out[k])
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
// nostrCategoryAuthors: who may classify money (category, collective,
// spread, exclude) — Xavier: only stewards. The seeds and our own key, the
// keys they follow (the bots), and keys a seed attests with a category
// role (default steward). Offline, like nostrTrustedPubkeys.
func nostrCategoryAuthors() map[string]bool {
	seeds := nostrTrustSeeds()
	out := map[string]bool{}
	for k := range seeds {
		out[k] = true
	}
	t := loadNostrTrustFile(DataDir())
	if nostrTrustFollows() {
		for hex, by := range t.Follows {
			for _, seed := range by {
				if seeds[seed] {
					out[hex] = true
					break
				}
			}
		}
	}
	roles := map[string]bool{}
	list := loadNostrSettings().CategoryRoles
	if len(list) == 0 {
		list = []string{"steward"}
	}
	for _, r := range list {
		roles[strings.ToLower(strings.TrimSpace(r))] = true
	}
	if nostrTrustAttestations() {
		for hex, att := range t.Attested {
			if !seeds[att.By] {
				continue
			}
			for _, r := range att.Roles {
				if roles[strings.ToLower(r)] {
					out[hex] = true
					break
				}
			}
		}
	}
	return out
}

// restrictAnnotation drops the money classification (category,
// collective, spread, exclude) of an annotation whose author is trusted
// but not a category author: a member's note still shows, the category
// falls back to rules and Odoo.
//
// It also normalises the category: "uncategorized" (and the legacy
// "none") means no category — the rules and Odoo decide, and the hourly
// check still flags it. "other" is a deliberate choice and stays.
func restrictAnnotation(a *TxAnnotation, categoryAuthors map[string]bool) *TxAnnotation {
	if a != nil && isNoCategory(a.Category) {
		cp := *a
		cp.Category = ""
		var tags [][]string
		for _, t := range a.Tags {
			if len(t) > 0 && t[0] == "category" {
				continue
			}
			tags = append(tags, t)
		}
		cp.Tags = tags
		a = &cp
	}
	if a == nil || categoryAuthors[strings.ToLower(a.Author)] {
		return a
	}
	if a.Category == "" && a.Collective == "" && len(a.Spread) == 0 && a.Exclude == "" {
		return a
	}
	cp := *a
	cp.Category, cp.Collective, cp.Spread, cp.Exclude = "", "", nil, ""
	var tags [][]string
	for _, t := range a.Tags {
		if len(t) > 0 && (t[0] == "category" || t[0] == "collective") {
			continue
		}
		tags = append(tags, t)
	}
	cp.Tags = tags
	return &cp
}

// isNoCategory: the category values that mean "not categorised".
func isNoCategory(c string) bool {
	switch strings.ToLower(strings.TrimSpace(c)) {
	case "uncategorized", "uncategorised", "none":
		return true
	}
	return false
}

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

// ── Consolidation ledger ─────────────────────────────────────────────────

// appliedAnnotationsPath records, per URI, the Nostr event id last
// consolidated into Odoo by `chb bills|invoices push`.
func appliedAnnotationsPath(dataDir string) string {
	return filepath.Join(dataDir, "latest", odoosource.RelPath("annotations-applied.json"))
}

func loadAppliedAnnotations(dataDir string) map[string]string {
	out := map[string]string{}
	if data, err := os.ReadFile(appliedAnnotationsPath(dataDir)); err == nil {
		_ = json.Unmarshal(data, &out)
	}
	return out
}

func saveAppliedAnnotations(dataDir string, ledger map[string]string) error {
	data, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		return err
	}
	return writeDataFile(appliedAnnotationsPath(dataDir), data)
}

func shortEventID(id string) string {
	if len(id) > 10 {
		return id[:10] + "…"
	}
	return id
}

// loadTransactionAnnotations reads every month's transaction-annotations.json,
// trusted authors only, newest per URI.
func loadTransactionAnnotations(dataDir string) map[string]*TxAnnotation {
	out := map[string]*TxAnnotation{}
	trusted := nostrTrustedPubkeys()
	classifiers := nostrCategoryAuthors()
	for _, ym := range dataMonthRange(dataDir) {
		data, err := os.ReadFile(nostrsource.Path(dataDir, ym[:4], ym[5:], nostrsource.AnnotationsFile))
		if err != nil {
			continue
		}
		var cache NostrAnnotationCache
		if json.Unmarshal(data, &cache) != nil {
			continue
		}
		for uri, a := range cache.Annotations {
			if annotationTrusted(a, trusted) {
				if cur, ok := out[uri]; !ok || a.CreatedAt > cur.CreatedAt {
					out[uri] = restrictAnnotation(a, classifiers)
				}
			}
		}
	}
	return out
}
