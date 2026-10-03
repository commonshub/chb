package cmd

// `chb nostr pull` (and so the hourly `chb pull`): read the trusted
// annotations and file them next to the records they annotate.
//
// One query per relay: every kind 1111 event by a trusted author since the
// last pull (everything on the first pull, or when the trust list or the
// relays change). Each accepted snapshot is matched by URI to a known
// transaction (transaction-annotations.json of its month) or Odoo document
// (odoo-annotations.json of its month); `chb generate` applies them.
// Read-only: no keys needed, nothing is published.

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	nostrsource "github.com/CommonsHub/chb/providers/nostr"
	"github.com/gorilla/websocket"
)

type nostrAnnotationsState struct {
	LastPullAt int64    `json:"lastPullAt"`
	Relays     []string `json:"relays"`
	Trusted    []string `json:"trusted"`
}

func nostrAnnotationsStatePath(dataDir string) string {
	return filepath.Join(dataDir, "latest", "providers", "nostr", "annotations-state.json")
}

// annotationIndex maps a URI to the month ("2026-08") of its record.
type annotationIndex struct {
	tx   map[string]string
	odoo map[string]string
}

func buildAnnotationIndex(dataDir string) annotationIndex {
	idx := annotationIndex{tx: map[string]string{}, odoo: map[string]string{}}
	for _, ym := range dataMonthRange(dataDir) {
		data, err := os.ReadFile(audiencePath(dataDir, ym[:4], ym[5:], AudienceStewards, "transactions.json"))
		if err != nil {
			continue
		}
		var f TransactionsFile
		if json.Unmarshal(data, &f) != nil {
			continue
		}
		for _, tx := range f.Transactions {
			if u := txURI(tx); u != "" {
				idx.tx[u] = ym
			}
			if tx.ID != "" {
				idx.tx[tx.ID] = ym
			}
		}
	}
	for _, inv := range loadAllCachedBills(dataDir) {
		if ym := invoiceYearMonth(inv); ym != "" {
			idx.odoo[odooDocURI("account.move", inv.ID, inv.InvoiceURL)] = ym
		}
	}
	for _, inv := range loadAllCachedInvoices(dataDir) {
		if ym := invoiceYearMonth(inv); ym != "" {
			idx.odoo[odooDocURI("account.move", inv.ID, inv.InvoiceURL)] = ym
		}
	}
	for _, c := range loadAllOdooExpenses(dataDir) {
		if len(c.Date) >= 7 {
			idx.odoo[odooDocURI("hr.expense", c.ID, "")] = c.Date[:7]
		}
	}
	return idx
}

// pullNostrAnnotations is the read-only annotation pull. Returns a summary.
func pullNostrAnnotations(dataDir string, force bool) (string, error) {
	relays := nostrRelayList()
	trusted := nostrTrustedPubkeys()
	authors := make([]string, 0, len(trusted))
	for a := range trusted {
		authors = append(authors, a)
	}
	sort.Strings(authors)

	var state nostrAnnotationsState
	if data, err := os.ReadFile(nostrAnnotationsStatePath(dataDir)); err == nil {
		_ = json.Unmarshal(data, &state)
	}
	full := force || state.LastPullAt == 0 || !reflect.DeepEqual(state.Relays, relays) || !reflect.DeepEqual(state.Trusted, authors)
	filter := map[string]interface{}{"kinds": []int{1111}, "authors": authors}
	if !full {
		filter["since"] = state.LastPullAt - 3600 // overlap: relays are not instant
	}
	startedAt := time.Now().Unix()

	events, okRelays := fetchEventsFromRelays(relays, filter)
	if okRelays == 0 {
		return "", fmt.Errorf("no relay answered (%s)", strings.Join(relays, ", "))
	}
	annotations := annotationsFromEvents(events, trusted)
	idx := buildAnnotationIndex(dataDir)

	txByMonth := map[string]map[string]*TxAnnotation{}
	odooByMonth := map[string]map[string]*TxAnnotation{}
	unmatched := 0
	for uri, ann := range annotations {
		switch {
		case idx.odoo[uri] != "":
			ym := idx.odoo[uri]
			if odooByMonth[ym] == nil {
				odooByMonth[ym] = map[string]*TxAnnotation{}
			}
			odooByMonth[ym][uri] = ann
		case idx.tx[uri] != "":
			ym := idx.tx[uri]
			if txByMonth[ym] == nil {
				txByMonth[ym] = map[string]*TxAnnotation{}
			}
			txByMonth[ym][uri] = ann
		default:
			unmatched++
		}
	}

	months := dataMonthRange(dataDir)
	changed := 0
	for _, ym := range months {
		for _, f := range []struct {
			file string
			got  map[string]*TxAnnotation
			has  bool
		}{
			{nostrsource.AnnotationsFile, txByMonth[ym], monthHasURIs(idx.tx, ym)},
			{nostrsource.OdooAnnotationsFile, odooByMonth[ym], monthHasURIs(idx.odoo, ym)},
		} {
			if !f.has && len(f.got) == 0 {
				continue
			}
			ok, err := mergeAnnotationCache(dataDir, ym, f.file, f.got, full)
			if err != nil {
				return "", err
			}
			if ok {
				changed++
			}
		}
	}

	state = nostrAnnotationsState{LastPullAt: startedAt, Relays: relays, Trusted: authors}
	if data, err := json.MarshalIndent(state, "", "  "); err == nil {
		_ = writeDataFile(nostrAnnotationsStatePath(dataDir), data)
	}
	mode := "since last pull"
	if full {
		mode = "full"
	}
	summary := fmt.Sprintf("%s from %s (%s)", Pluralize(len(annotations), "trusted annotation", ""), Pluralize(len(trusted), "author", ""), mode)
	if unmatched > 0 {
		summary += fmt.Sprintf(", %d for records not held here", unmatched)
	}
	if changed > 0 {
		summary += fmt.Sprintf(", %s updated", Pluralize(changed, "month file", ""))
	}
	return summary, nil
}

func monthHasURIs(index map[string]string, ym string) bool {
	for _, m := range index {
		if m == ym {
			return true
		}
	}
	return false
}

// mergeAnnotationCache writes one month's cache. A full pull replaces it
// (dropping untrusted or comment entries cached by older versions); an
// incremental pull merges, newest per URI. Returns whether it changed.
func mergeAnnotationCache(dataDir, ym, file string, got map[string]*TxAnnotation, full bool) (bool, error) {
	year, month := ym[:4], ym[5:]
	path := nostrsource.Path(dataDir, year, month, file)
	var prev NostrAnnotationCache
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &prev)
	}
	next := map[string]*TxAnnotation{}
	if !full {
		for k, v := range prev.Annotations {
			next[k] = v
		}
	}
	for uri, ann := range got {
		if cur, ok := next[uri]; !ok || ann.CreatedAt >= cur.CreatedAt {
			next[uri] = ann
		}
	}
	if reflect.DeepEqual(next, prev.Annotations) || (len(next) == 0 && len(prev.Annotations) == 0 && fileExists(path)) {
		return false, nil
	}
	cache := NostrAnnotationCache{FetchedAt: time.Now().UTC().Format(time.RFC3339), Annotations: next}
	return true, nostrsource.WriteJSON(dataDir, year, month, cache, file)
}

// fetchEventsFromRelays runs one filter on every relay (in parallel),
// paging back with `until` when a relay caps its answer. Returns the
// events by id and how many relays answered.
func fetchEventsFromRelays(relays []string, filter map[string]interface{}) (map[string]NostrEvent, int) {
	var mu sync.Mutex
	all := map[string]NostrEvent{}
	ok := 0
	var wg sync.WaitGroup
	for _, relay := range relays {
		wg.Add(1)
		go func(relayURL string) {
			defer wg.Done()
			events, err := fetchEventsByFilter(relayURL, filter)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				Warnf("⚠ nostr relay %s: %v", relayURL, err)
				return
			}
			ok++
			for id, ev := range events {
				all[id] = ev
			}
		}(relay)
	}
	wg.Wait()
	return all, ok
}

const nostrPageSize = 500

func fetchEventsByFilter(relayURL string, base map[string]interface{}) (map[string]NostrEvent, error) {
	dialer := websocket.Dialer{HandshakeTimeout: nostrConnectTimeout}
	conn, _, err := dialer.Dial(relayURL, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	events := map[string]NostrEvent{}
	var until int64
	for page := 0; page < 200; page++ {
		filter := map[string]interface{}{"limit": nostrPageSize}
		for k, v := range base {
			filter[k] = v
		}
		if until > 0 {
			filter["until"] = until
		}
		subID := fmt.Sprintf("chb-%d", rand.Int63())
		req, _ := json.Marshal([]interface{}{"REQ", subID, filter})
		if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
			return events, err
		}
		got := 0
		oldest := int64(0)
	read:
		for {
			conn.SetReadDeadline(time.Now().Add(nostrDataTimeout))
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return events, err
			}
			var raw []json.RawMessage
			if json.Unmarshal(msg, &raw) != nil || len(raw) < 2 {
				continue
			}
			var typ string
			_ = json.Unmarshal(raw[0], &typ)
			switch typ {
			case "EVENT":
				if len(raw) < 3 {
					continue
				}
				var ev NostrEvent
				if json.Unmarshal(raw[2], &ev) != nil {
					continue
				}
				got++
				if _, seen := events[ev.ID]; !seen {
					events[ev.ID] = ev
				}
				if oldest == 0 || ev.CreatedAt < oldest {
					oldest = ev.CreatedAt
				}
			case "EOSE":
				closeMsg, _ := json.Marshal([]interface{}{"CLOSE", subID})
				_ = conn.WriteMessage(websocket.TextMessage, closeMsg)
				break read
			case "CLOSED", "NOTICE":
				if typ == "CLOSED" {
					break read
				}
			}
		}
		if got < nostrPageSize || oldest == 0 {
			break
		}
		until = oldest // events at the same second are re-sent and deduplicated
		if until-1 <= 0 {
			break
		}
	}
	return events, nil
}
