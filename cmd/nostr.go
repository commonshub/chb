package cmd

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// NostrEvent represents a Nostr event (kind 1111 for txinfo)
type NostrEvent struct {
	ID        string     `json:"id"`
	PubKey    string     `json:"pubkey"`
	CreatedAt int64      `json:"created_at"`
	Kind      int        `json:"kind"`
	Tags      [][]string `json:"tags"`
	Content   string     `json:"content"`
	Sig       string     `json:"sig"`
}

// TxMetadata holds enrichment data for a blockchain transaction
type TxMetadata struct {
	TxHash       string            `json:"txHash"`
	Description  string            `json:"description"`
	Tags         map[string]string `json:"tags"` // project, category, etc.
	TagList      [][]string        `json:"tagList,omitempty"`
	NostrEventID string            `json:"nostrEventId"`
	Author       string            `json:"author"`
	CreatedAt    int64             `json:"createdAt"`
}

// AddressMetadata holds enrichment data for a blockchain address
type AddressMetadata struct {
	Address      string            `json:"address"`
	Name         string            `json:"name"`
	About        string            `json:"about"`
	Picture      string            `json:"picture,omitempty"`
	Tags         map[string]string `json:"tags"`
	NostrEventID string            `json:"nostrEventId"`
	Author       string            `json:"author"`
	CreatedAt    int64             `json:"createdAt"`
}

// NostrMetadataCache is the structure saved to disk per chain
type NostrMetadataCache struct {
	FetchedAt    string                      `json:"fetchedAt"`
	ChainID      int                         `json:"chainId"`
	Transactions map[string]*TxMetadata      `json:"transactions"` // keyed by txHash (lowercase)
	Addresses    map[string]*AddressMetadata `json:"addresses"`    // keyed by address (lowercase)
}

var nostrRelays = []string{
	"wss://nostr.commonshub.brussels",
	"wss://nostr-pub.wellorder.net",
	"wss://nostr.swiss-enigma.ch",
	"wss://relay.nostr.band",
	"wss://relay.damus.io",
}

const (
	nostrConnectTimeout = 5 * time.Second
	nostrDataTimeout    = 6 * time.Second
	nostrBatchSize      = 50
)

// FetchNostrTxMetadata fetches NIP-73 / txinfo annotations (kind 1111) for the
// given tx hashes, optionally filtered by `since`. Tx annotations are append-only
// so an incremental sync is safe.
func FetchNostrTxMetadata(chainID int, txHashes []string, since *time.Time) (map[string]*TxMetadata, error) {
	if len(txHashes) == 0 {
		return map[string]*TxMetadata{}, nil
	}
	uris := make([]string, 0, len(txHashes))
	for _, hash := range txHashes {
		uris = append(uris, fmt.Sprintf("ethereum:%d:tx:%s", chainID, strings.ToLower(hash)))
	}
	events := fetchKind1111ByURIs(uris, since)
	out := map[string]*TxMetadata{}
	for _, ev := range events {
		for _, tag := range ev.Tags {
			if len(tag) < 2 || tag[0] != "i" {
				continue
			}
			if !isTxURI(tag[1], chainID) {
				continue
			}
			hash := strings.ToLower(extractURIPart(tag[1], "tx"))
			if hash == "" {
				continue
			}
			if existing, ok := out[hash]; !ok || ev.CreatedAt > existing.CreatedAt {
				out[hash] = parseTxMetadata(hash, ev)
			}
		}
	}
	return out, nil
}

// FetchNostrAddressMetadata fetches kind 1111 annotations for the given Ethereum
// addresses. No `since` filter — address profiles mutate over time and we always
// want the latest version.
func FetchNostrAddressMetadata(chainID int, addresses []string) (map[string]*AddressMetadata, error) {
	if len(addresses) == 0 {
		return map[string]*AddressMetadata{}, nil
	}
	uris := make([]string, 0, len(addresses))
	for _, addr := range addresses {
		uris = append(uris, fmt.Sprintf("ethereum:%d:address:%s", chainID, strings.ToLower(addr)))
	}
	events := fetchKind1111ByURIs(uris, nil)
	out := map[string]*AddressMetadata{}
	for _, ev := range events {
		for _, tag := range ev.Tags {
			if len(tag) < 2 || tag[0] != "i" {
				continue
			}
			if !isAddressURI(tag[1], chainID) {
				continue
			}
			addr := strings.ToLower(extractURIPart(tag[1], "address"))
			if addr == "" {
				continue
			}
			if existing, ok := out[addr]; !ok || ev.CreatedAt > existing.CreatedAt {
				out[addr] = parseAddressMetadata(addr, ev)
			}
		}
	}
	return out, nil
}

// fetchKind1111ByURIs queries every configured Nostr relay in parallel for kind
// 1111 events whose `i` tag matches one of the URIs. Returns the deduplicated
// (latest createdAt wins) set keyed by event ID.
func fetchKind1111ByURIs(uris []string, since *time.Time) map[string]NostrEvent {
	if len(uris) == 0 {
		return map[string]NostrEvent{}
	}

	var batches [][]string
	for i := 0; i < len(uris); i += nostrBatchSize {
		end := i + nostrBatchSize
		if end > len(uris) {
			end = len(uris)
		}
		batches = append(batches, uris[i:end])
	}

	eventsMu := sync.Mutex{}
	allEvents := map[string]NostrEvent{}

	var wg sync.WaitGroup
	for _, relay := range nostrRelays {
		wg.Add(1)
		go func(relayURL string) {
			defer wg.Done()
			events, err := fetchFromRelay(relayURL, batches, since)
			if err != nil {
				return
			}
			eventsMu.Lock()
			defer eventsMu.Unlock()
			for id, ev := range events {
				if existing, ok := allEvents[id]; !ok || ev.CreatedAt > existing.CreatedAt {
					allEvents[id] = ev
				}
			}
		}(relay)
	}
	wg.Wait()
	return allEvents
}

// fetchFromRelay connects to a single relay and fetches events for all batches.
func fetchFromRelay(relayURL string, batches [][]string, since *time.Time) (map[string]NostrEvent, error) {
	dialer := websocket.Dialer{
		HandshakeTimeout: nostrConnectTimeout,
	}
	conn, _, err := dialer.Dial(relayURL, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(nostrDataTimeout))

	events := map[string]NostrEvent{}

	for _, batch := range batches {
		subID := fmt.Sprintf("chb-%d", rand.Int63())

		filter := map[string]interface{}{
			"kinds": []int{1111},
			"#i":    batch,
		}
		if since != nil && !since.IsZero() {
			filter["since"] = since.UTC().Unix()
		}
		req, _ := json.Marshal([]interface{}{"REQ", subID, filter})
		if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
			return events, err
		}

		// Read until EOSE or timeout
		for {
			conn.SetReadDeadline(time.Now().Add(nostrDataTimeout))
			_, msg, err := conn.ReadMessage()
			if err != nil {
				// Timeout or connection closed — stop reading this batch
				break
			}

			var raw []json.RawMessage
			if err := json.Unmarshal(msg, &raw); err != nil || len(raw) < 2 {
				continue
			}

			var msgType string
			if err := json.Unmarshal(raw[0], &msgType); err != nil {
				continue
			}

			switch msgType {
			case "EVENT":
				if len(raw) < 3 {
					continue
				}
				var ev NostrEvent
				if err := json.Unmarshal(raw[2], &ev); err != nil {
					continue
				}
				if ev.Kind == 1111 {
					events[ev.ID] = ev
				}
			case "EOSE":
				// End of stored events — send CLOSE and move to next batch
				close_, _ := json.Marshal([]interface{}{"CLOSE", subID})
				conn.WriteMessage(websocket.TextMessage, close_)
				goto nextBatch
			}
		}
	nextBatch:
	}

	return events, nil
}

// isTxURI checks if a URI matches ethereum:<chainID>:tx:...
func isTxURI(uri string, chainID int) bool {
	prefix := fmt.Sprintf("ethereum:%d:tx:", chainID)
	return strings.HasPrefix(strings.ToLower(uri), strings.ToLower(prefix))
}

// isAddressURI checks if a URI matches ethereum:<chainID>:address:...
func isAddressURI(uri string, chainID int) bool {
	prefix := fmt.Sprintf("ethereum:%d:address:", chainID)
	return strings.HasPrefix(strings.ToLower(uri), strings.ToLower(prefix))
}

// extractURIPart extracts the hash/address after the kind segment.
// uri format: ethereum:<chainId>:<kind>:<value>
func extractURIPart(uri string, kind string) string {
	parts := strings.SplitN(uri, ":", 4)
	if len(parts) != 4 {
		return ""
	}
	if !strings.EqualFold(parts[2], kind) {
		return ""
	}
	return parts[3]
}

// parseTxMetadata builds a TxMetadata from a Nostr event.
func parseTxMetadata(txHash string, ev NostrEvent) *TxMetadata {
	m := &TxMetadata{
		TxHash:       txHash,
		Description:  ev.Content,
		Tags:         map[string]string{},
		NostrEventID: ev.ID,
		Author:       ev.PubKey,
		CreatedAt:    ev.CreatedAt,
	}
	skipTags := map[string]bool{"i": true, "k": true, "e": true, "p": true}
	for _, tag := range ev.Tags {
		if len(tag) < 2 || skipTags[strings.ToLower(tag[0])] {
			continue
		}
		m.Tags[tag[0]] = tag[1]
		if normalized, ok := normalizeTransactionTag(tag); ok {
			m.TagList = append(m.TagList, normalized)
		}
	}
	m.TagList = normalizeTransactionTags(m.TagList)
	return m
}

// ── Annotation structures and fetch ──────────────────────────────────────────

// TxAnnotation holds accounting annotations for a transaction from Nostr.
type TxAnnotation struct {
	URI          string        `json:"uri"`
	Category     string        `json:"category,omitempty"`
	Collective   string        `json:"collective,omitempty"`
	Event        string        `json:"event,omitempty"`
	Tags         [][]string    `json:"tags,omitempty"`
	Description  string        `json:"description,omitempty"`
	Spread       []SpreadEntry `json:"spread,omitempty"`
	NostrEventID string        `json:"nostrEventId"`
	Author       string        `json:"author"`
	CreatedAt    int64         `json:"createdAt"`
}

// SpreadEntry represents a monthly amount allocation.
type SpreadEntry struct {
	Month  string `json:"month"`
	Amount string `json:"amount"`
}

// NostrAnnotationCache is saved per source per month.
type NostrAnnotationCache struct {
	FetchedAt   string                   `json:"fetchedAt"`
	Annotations map[string]*TxAnnotation `json:"annotations"` // keyed by URI
}

// FetchNostrAnnotations fetches kind 1111 annotations for a set of URIs.
// Works with any URI scheme (ethereum:..., stripe:txn:..., etc.)
func FetchNostrAnnotations(uris []string, since *time.Time) (map[string]*TxAnnotation, error) {
	if len(uris) == 0 {
		return map[string]*TxAnnotation{}, nil
	}

	// Use custom relay list if user has configured one
	relays := nostrRelays
	if keys := LoadNostrKeys(); keys != nil && len(keys.Relays) > 0 {
		relays = keys.Relays
	}

	// Batch URIs
	var batches [][]string
	for i := 0; i < len(uris); i += nostrBatchSize {
		end := i + nostrBatchSize
		if end > len(uris) {
			end = len(uris)
		}
		batches = append(batches, uris[i:end])
	}

	// Collect events from all relays in parallel
	eventsMu := sync.Mutex{}
	allEvents := map[string]NostrEvent{}

	var wg sync.WaitGroup
	for _, relay := range relays {
		wg.Add(1)
		go func(relayURL string) {
			defer wg.Done()
			events, err := fetchFromRelay(relayURL, batches, since)
			if err != nil {
				return
			}
			eventsMu.Lock()
			defer eventsMu.Unlock()
			for id, ev := range events {
				if existing, ok := allEvents[id]; !ok || ev.CreatedAt > existing.CreatedAt {
					allEvents[id] = ev
				}
			}
		}(relay)
	}
	wg.Wait()

	// Parse annotations
	annotations := map[string]*TxAnnotation{}
	for _, ev := range allEvents {
		for _, tag := range ev.Tags {
			if len(tag) < 2 || (tag[0] != "i" && tag[0] != "I") {
				continue
			}
			uri := tag[1]
			existing, ok := annotations[uri]
			if !ok || ev.CreatedAt > existing.CreatedAt {
				annotations[uri] = parseAnnotation(uri, ev)
			}
		}
	}

	return annotations, nil
}

// parseAnnotation extracts accounting tags from a Nostr event.
func parseAnnotation(uri string, ev NostrEvent) *TxAnnotation {
	a := &TxAnnotation{
		URI:          uri,
		Description:  ev.Content,
		NostrEventID: ev.ID,
		Author:       ev.PubKey,
		CreatedAt:    ev.CreatedAt,
	}

	for _, tag := range ev.Tags {
		if len(tag) < 2 {
			continue
		}
		switch tag[0] {
		case "category":
			a.Category = tag[1]
		case "collective":
			a.Collective = tag[1]
		case "event":
			a.Event = tag[1]
		case "spread":
			if len(tag) >= 3 {
				a.Spread = append(a.Spread, SpreadEntry{Month: tag[1], Amount: tag[2]})
			}
		}
		if tag[0] == "i" || tag[0] == "I" || tag[0] == "k" || tag[0] == "K" || tag[0] == "e" || tag[0] == "p" {
			continue
		}
		if normalized, ok := normalizeTransactionTag(tag); ok {
			a.Tags = append(a.Tags, normalized)
		}
	}
	a.Tags = normalizeTransactionTags(a.Tags)

	return a
}

// BuildStripeURI returns a NIP-73-style URI for a Stripe object. The Stripe
// id itself carries the type prefix (txn_…, cus_…, acct_…, ch_…), so we
// don't repeat it as a URI segment.
func BuildStripeURI(stripeID string) string {
	v := strings.TrimSpace(stripeID)
	if v == "" {
		return ""
	}
	return "stripe:" + v
}

// BuildBlockchainURI creates a NIP-73 URI for a blockchain transaction.
func BuildBlockchainURI(chainID int, txHash string) string {
	return fmt.Sprintf("ethereum:%d:tx:%s", chainID, strings.ToLower(txHash))
}

// TxHashFromURI extracts the raw transaction hash / id from a NIP-73
// transaction URI. Returns "" for unrecognized forms. This is the
// inverse of BuildBlockchainURI / BuildStripeURI and is used at load
// time to backfill TransactionEntry.TxHash, which the public-projection
// step in generate.go strips before saving.
func TxHashFromURI(uri string) string {
	if uri == "" {
		return ""
	}
	if strings.HasPrefix(uri, "ethereum:") {
		const sep = ":tx:"
		if i := strings.Index(uri, sep); i >= 0 {
			return uri[i+len(sep):]
		}
		return ""
	}
	if strings.HasPrefix(uri, "stripe:") {
		return strings.TrimPrefix(uri, "stripe:")
	}
	return ""
}

// CanonicalizeImportID rewrites a broken URI-form unique_import_id back
// to the clean form expected by buildUniqueImportID. The bug surfaced
// while generate.go's public projection stripped TxHash, so
// buildUniqueImportID fell back to using tx.ID (the NIP-73 URI) as the
// hash segment.
//
//	Broken etherscan: <chain>:<address>:ethereum:<chainId>:tx:<hash>:<n>
//	Clean etherscan:  <chain>:<address>:<hash>:<n>
//
//	Broken stripe:    stripe:<account>:stripe:<txId>:<n>
//	Clean stripe:     stripe:<account>:<txId>
//
// Returns "" for an already-clean or unrecognized form, so callers can
// distinguish "repair available" from "true orphan" cleanly.
func CanonicalizeImportID(brokenID string) string {
	parts := strings.Split(brokenID, ":")
	if len(parts) == 7 && parts[2] == "ethereum" && parts[4] == "tx" {
		return fmt.Sprintf("%s:%s:%s:%s", parts[0], parts[1], parts[5], parts[6])
	}
	if len(parts) == 5 && parts[0] == "stripe" && parts[2] == "stripe" {
		return fmt.Sprintf("%s:%s:%s", parts[0], parts[1], parts[3])
	}
	if len(parts) == 4 && parts[0] == "stripe" && parts[3] == "0" {
		return fmt.Sprintf("%s:%s:%s", parts[0], parts[1], parts[2])
	}
	return ""
}

// BuildBlockchainAddressURI creates a NIP-73 URI for a blockchain address.
func BuildBlockchainAddressURI(chainID int, chain, address string) string {
	addr := strings.ToLower(strings.TrimSpace(address))
	if addr == "" {
		return ""
	}
	if chainID > 0 {
		return fmt.Sprintf("ethereum:%d:address:%s", chainID, addr)
	}
	if chain != "" {
		return fmt.Sprintf("%s:address:%s", strings.ToLower(chain), addr)
	}
	return "address:" + addr
}

// BuildStripeCustomerURI returns the URI for a Stripe customer (cus_…).
// Same scheme as BuildStripeURI — the Stripe id prefix is self-describing.
func BuildStripeCustomerURI(customerID string) string {
	return BuildStripeURI(customerID)
}

// BuildStripeAccountURI returns the URI for a Stripe connected account
// (acct_…). Same scheme as BuildStripeURI.
func BuildStripeAccountURI(accountID string) string {
	return BuildStripeURI(accountID)
}

// BuildBlockchainTokenURI creates an `<chain>:<chainId>:token:<contract>` URI
// for a token contract. Used as the counterparty when one side of a transfer
// is the zero address (mint/burn) — receiving from 0x0 EURb vs 0x0 CHT are
// semantically different and should resolve to the issuing contract.
func BuildBlockchainTokenURI(chainID int, chain, contract string) string {
	c := strings.ToLower(strings.TrimSpace(contract))
	if c == "" {
		return ""
	}
	if chainID > 0 {
		return fmt.Sprintf("ethereum:%d:token:%s", chainID, c)
	}
	if chain != "" {
		return fmt.Sprintf("%s:token:%s", strings.ToLower(chain), c)
	}
	return "token:" + c
}

// parseAddressMetadata builds an AddressMetadata from a Nostr event. Tags
// take precedence (kind 1111 / NIP-73 style); when content holds a kind-0
// style JSON profile, we fill in any fields the tags didn't set.
func parseAddressMetadata(addr string, ev NostrEvent) *AddressMetadata {
	m := &AddressMetadata{
		Address:      addr,
		Tags:         map[string]string{},
		NostrEventID: ev.ID,
		Author:       ev.PubKey,
		CreatedAt:    ev.CreatedAt,
	}
	skipTags := map[string]bool{"i": true, "k": true, "e": true, "p": true}
	for _, tag := range ev.Tags {
		if len(tag) < 2 || skipTags[tag[0]] {
			continue
		}
		switch tag[0] {
		case "name":
			m.Name = tag[1]
		case "about":
			m.About = tag[1]
		case "picture":
			m.Picture = tag[1]
		default:
			m.Tags[tag[0]] = tag[1]
		}
	}
	applyKind0Content(m, ev.Content)
	return m
}

// applyKind0Content fills empty fields on m from a kind-0-style JSON profile
// (`{"name":"…","display_name":"…","about":"…","picture":"…","website":"…","nip05":"…"}`).
// When the content isn't JSON, it's used as an About fallback only when no
// explicit about exists and it isn't just a copy of the name (so a publisher
// that put the name in `content` doesn't end up duplicating it into `about`).
func applyKind0Content(m *AddressMetadata, content string) {
	content = strings.TrimSpace(content)
	if content == "" {
		return
	}
	if strings.HasPrefix(content, "{") {
		var profile struct {
			Name        string `json:"name"`
			DisplayName string `json:"display_name"`
			About       string `json:"about"`
			Picture     string `json:"picture"`
			Website     string `json:"website"`
			Nip05       string `json:"nip05"`
		}
		if err := json.Unmarshal([]byte(content), &profile); err == nil {
			if m.Name == "" {
				if profile.Name != "" {
					m.Name = profile.Name
				} else if profile.DisplayName != "" {
					m.Name = profile.DisplayName
				}
			}
			if m.About == "" {
				m.About = profile.About
			}
			if m.Picture == "" {
				m.Picture = profile.Picture
			}
			if profile.Website != "" && m.Tags["website"] == "" {
				m.Tags["website"] = profile.Website
			}
			if profile.Nip05 != "" && m.Tags["nip05"] == "" {
				m.Tags["nip05"] = profile.Nip05
			}
			return
		}
	}
	if m.About == "" && content != m.Name {
		m.About = content
	}
}

// LoadNostrMetadataCache reads a metadata.json file. Returns an empty (but
// initialized) cache if the file is missing or unreadable.
func LoadNostrMetadataCache(path string) NostrMetadataCache {
	cache := NostrMetadataCache{
		Transactions: map[string]*TxMetadata{},
		Addresses:    map[string]*AddressMetadata{},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cache
	}
	_ = json.Unmarshal(data, &cache)
	if cache.Transactions == nil {
		cache.Transactions = map[string]*TxMetadata{}
	}
	if cache.Addresses == nil {
		cache.Addresses = map[string]*AddressMetadata{}
	}
	// Sanitize on load so cache files written by an older code path (which
	// dumped ev.Content into About, sometimes a kind-0 JSON profile and
	// sometimes a copy of the name) are normalized in memory.
	for _, md := range cache.Addresses {
		sanitizeAddressMetadata(md)
	}
	return cache
}

// sanitizeAddressMetadata cleans up cached metadata: promotes JSON-content
// `about` into Name/About/Picture, then drops About when it duplicates Name.
func sanitizeAddressMetadata(m *AddressMetadata) {
	if m == nil {
		return
	}
	if strings.HasPrefix(strings.TrimSpace(m.About), "{") {
		var profile struct {
			Name        string `json:"name"`
			DisplayName string `json:"display_name"`
			About       string `json:"about"`
			Picture     string `json:"picture"`
			Website     string `json:"website"`
			Nip05       string `json:"nip05"`
		}
		if err := json.Unmarshal([]byte(m.About), &profile); err == nil {
			if m.Name == "" {
				if profile.Name != "" {
					m.Name = profile.Name
				} else if profile.DisplayName != "" {
					m.Name = profile.DisplayName
				}
			}
			m.About = profile.About
			if m.Picture == "" {
				m.Picture = profile.Picture
			}
			if m.Tags == nil {
				m.Tags = map[string]string{}
			}
			if profile.Website != "" && m.Tags["website"] == "" {
				m.Tags["website"] = profile.Website
			}
			if profile.Nip05 != "" && m.Tags["nip05"] == "" {
				m.Tags["nip05"] = profile.Nip05
			}
		}
	}
	if m.About == m.Name {
		m.About = ""
	}
	// Legacy: when only `content` was set (no `name` tag), the old parser
	// dumped it into About. Publishers typically meant that as the label —
	// promote it to Name when Name is otherwise empty. We deliberately don't
	// also keep it in About: a future re-sync (`chb nostr sync`) will
	// populate both fields correctly from the actual tags.
	if m.Name == "" && m.About != "" {
		m.Name = m.About
		m.About = ""
	}
}

// MergeNostrMetadata merges incoming entries into base, keeping the entry with
// the higher CreatedAt when both contain the same key. The returned cache
// carries `incoming`'s FetchedAt and ChainID (falling back to base's when zero).
func MergeNostrMetadata(base, incoming NostrMetadataCache) NostrMetadataCache {
	out := NostrMetadataCache{
		FetchedAt:    incoming.FetchedAt,
		ChainID:      incoming.ChainID,
		Transactions: map[string]*TxMetadata{},
		Addresses:    map[string]*AddressMetadata{},
	}
	if out.FetchedAt == "" {
		out.FetchedAt = base.FetchedAt
	}
	if out.ChainID == 0 {
		out.ChainID = base.ChainID
	}
	for k, v := range base.Transactions {
		out.Transactions[k] = v
	}
	for k, v := range incoming.Transactions {
		if existing, ok := out.Transactions[k]; !ok || v.CreatedAt > existing.CreatedAt {
			out.Transactions[k] = v
		}
	}
	for k, v := range base.Addresses {
		out.Addresses[k] = v
	}
	for k, v := range incoming.Addresses {
		if existing, ok := out.Addresses[k]; !ok || v.CreatedAt > existing.CreatedAt {
			out.Addresses[k] = v
		}
	}
	return out
}

// WriteNostrMetadataCache writes a metadata cache to disk directly. Unlike
// writeMonthFile, this does NOT mirror to `latest/`; the caller decides where
// to write each layer (per-month vs latest).
func WriteNostrMetadataCache(path string, cache NostrMetadataCache) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
