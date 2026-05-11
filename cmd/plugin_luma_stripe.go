package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	lumastripeplugin "github.com/CommonsHub/chb/processors/luma-stripe"
)

type lumaStripeProcessor struct {
	baseURL          string
	client           *http.Client
	eventURLs        map[string]string
	eventByID        map[string]lumaStripeCalendarEventRef
	eventByURL       map[string]lumaStripeCalendarEventRef
	transactionHints []lumaStripeTransactionEventHint
	changed          bool
}

type lumaStripeCalendarEventRef struct {
	ID   string
	Name string
	URL  string
}

type lumaStripeEventURLCache struct {
	FetchedAt string            `json:"fetchedAt"`
	EventURLs map[string]string `json:"eventUrls"`
}

type lumaStripeTransactionEventHint struct {
	LumaEventID string
	EventID     string
	Name        string
	URL         string
	Collective  string
}

func newLumaStripeProcessor() *lumaStripeProcessor {
	return &lumaStripeProcessor{
		baseURL: "https://luma.com/event/",
	}
}

func (p *lumaStripeProcessor) Name() string {
	return lumastripeplugin.Name
}

func (p *lumaStripeProcessor) EnvVars() []ProcessorEnvVar {
	return []ProcessorEnvVar{
		{
			Name:        "LUMA_API_KEY",
			Description: "Optional for future rich Luma API enrichment; event URL redirect lookup does not require it.",
			Required:    false,
		},
	}
}

func (p *lumaStripeProcessor) WarmUp(ctx *ProcessorContext) error {
	p.eventURLs = map[string]string{}
	p.eventByID = map[string]lumaStripeCalendarEventRef{}
	p.eventByURL = map[string]lumaStripeCalendarEventRef{}
	p.transactionHints = nil
	p.changed = false
	p.client = ctx.HTTPClient
	if p.client == nil {
		p.client = &http.Client{Timeout: 15 * time.Second}
	}
	if p.baseURL == "" {
		p.baseURL = "https://luma.com/event/"
	}

	if err := p.loadEventURLCaches(ctx); err != nil {
		return err
	}
	if err := p.loadCalendarEventAliases(ctx); err != nil {
		return err
	}
	p.buildTransactionHints()
	return nil
}

func (p *lumaStripeProcessor) ProcessTransaction(ctx *ProcessorContext, tx *TransactionEntry) error {
	eventID := p.transactionLumaEventID(*tx)
	if eventID == "" {
		if hint := p.inferTransactionEvent(*tx); hint.LumaEventID != "" {
			eventID = hint.LumaEventID
			if tx.Event == "" {
				tx.Event = hint.EventID
				if tx.Event == "" {
					tx.Event = hint.LumaEventID
				}
			}
			if hint.URL != "" {
				addTransactionTag(&tx.Tags, "eventUrl", hint.URL)
			}
			p.applyLumaTransactionMetadata(tx, hint)
		}
	}
	eventURL := firstTransactionTagValue(*tx, "eventUrl")
	if eventURL == "" && eventID != "" {
		resolvedURL, err := p.resolveEventURL(eventID)
		if err != nil {
			return err
		}
		eventURL = resolvedURL
		addTransactionTag(&tx.Tags, "eventUrl", eventURL)
	}
	if eventID != "" {
		addTransactionTag(&tx.Tags, "lumaEvent", eventID)
		addTransactionTag(&tx.Tags, "eventId", eventID)
	}
	if eventURL != "" {
		addNIP73WebTags(&tx.Tags, eventURL)
	}
	if canonical := p.canonicalEvent(eventID, eventURL); canonical.ID != "" {
		tx.Event = canonical.ID
		addTransactionTag(&tx.Tags, "event", canonical.ID)
		if canonical.Name != "" {
			addTransactionTag(&tx.Tags, "eventName", canonical.Name)
		}
		p.applyLumaTransactionMetadata(tx, lumaStripeTransactionEventHint{
			LumaEventID: eventID,
			EventID:     canonical.ID,
			Name:        canonical.Name,
			URL:         firstNonEmptyStr(eventURL, canonical.URL),
			Collective:  lumaCollectiveSlugFromURL(firstNonEmptyStr(eventURL, canonical.URL)),
		})
	}
	if eventURL == "" && eventID == "" {
		return nil
	}
	if tx.Metadata == nil {
		tx.Metadata = map[string]interface{}{}
	}
	tx.Application = "Luma"
	tx.Metadata["application"] = "Luma"
	if eventID != "" {
		tx.Metadata["eventId"] = eventID
	}
	if eventURL != "" {
		tx.Metadata["eventUrl"] = eventURL
	}
	tx.Tags = normalizeTransactionTags(tx.Tags)
	return nil
}

func (p *lumaStripeProcessor) loadEventURLCaches(ctx *ProcessorContext) error {
	load := func(path string) error {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var cache lumaStripeEventURLCache
		if err := json.Unmarshal(data, &cache); err != nil {
			return err
		}
		for eventID, eventURL := range cache.EventURLs {
			if eventID != "" && eventURL != "" {
				p.eventURLs[eventID] = eventURL
			}
		}
		return nil
	}

	monthPath := lumastripeplugin.Path(ctx.DataDir, ctx.Year, ctx.Month, lumastripeplugin.EventURLsFile)
	if err := load(monthPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	latestPath := lumastripeplugin.Path(ctx.DataDir, "latest", "", lumastripeplugin.EventURLsFile)
	if err := load(latestPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (p *lumaStripeProcessor) ProcessEvent(ctx *ProcessorContext, ev *FullEvent) error {
	if ev.URL != "" || !isLumaEventID(ev.ID) {
		return nil
	}
	eventURL, err := p.resolveEventURL(ev.ID)
	if err != nil {
		return err
	}
	ev.URL = eventURL
	return nil
}

func (p *lumaStripeProcessor) buildTransactionHints() {
	seen := map[string]bool{}
	for lumaEventID, eventURL := range p.eventURLs {
		if !isLumaEventID(lumaEventID) || eventURL == "" {
			continue
		}
		ref := p.canonicalEvent(lumaEventID, eventURL)
		hint := lumaStripeTransactionEventHint{
			LumaEventID: lumaEventID,
			EventID:     firstNonEmptyStr(ref.ID, lumaEventID),
			Name:        ref.Name,
			URL:         firstNonEmptyStr(eventURL, ref.URL),
			Collective:  lumaCollectiveSlugFromURL(firstNonEmptyStr(eventURL, ref.URL)),
		}
		key := hint.LumaEventID + "\x00" + hint.EventID
		if seen[key] {
			continue
		}
		seen[key] = true
		p.transactionHints = append(p.transactionHints, hint)
	}
}

func (p *lumaStripeProcessor) inferTransactionEvent(tx TransactionEntry) lumaStripeTransactionEventHint {
	if tx.Provider != "stripe" || len(p.transactionHints) == 0 || !looksLikeLumaTicketTransaction(tx) {
		return lumaStripeTransactionEventHint{}
	}

	if eventURL := firstNonEmptyStr(firstTransactionTagValue(tx, "eventUrl"), stringMetadata(tx.Metadata, "eventUrl"), stringMetadata(tx.Metadata, "stripe_event_url")); eventURL != "" {
		alias := normalizeLumaEventURLAlias(eventURL)
		for _, hint := range p.transactionHints {
			if alias != "" && alias == normalizeLumaEventURLAlias(hint.URL) {
				return hint
			}
		}
	}

	txCollective := normalizeTransactionTagSlug(firstNonEmptyStr(tx.Collective, stringMetadata(tx.Metadata, "collective"), stringMetadata(tx.Metadata, "stripe_collective")))
	txText := normalizeLumaMatchText(firstNonEmptyStr(
		stringMetadata(tx.Metadata, "eventName"),
		stringMetadata(tx.Metadata, "stripe_event_name"),
		stringMetadata(tx.Metadata, "description"),
		tx.Counterparty,
	))

	var match lumaStripeTransactionEventHint
	for _, hint := range p.transactionHints {
		if hint.Name == "" || normalizeLumaMatchText(hint.Name) != txText {
			continue
		}
		hintCollective := normalizeTransactionTagSlug(hint.Collective)
		if txCollective != "" && hintCollective != "" && txCollective != hintCollective {
			continue
		}
		if match.LumaEventID != "" {
			return lumaStripeTransactionEventHint{}
		}
		match = hint
	}
	return match
}

func (p *lumaStripeProcessor) applyLumaTransactionMetadata(tx *TransactionEntry, hint lumaStripeTransactionEventHint) {
	if tx.Metadata == nil {
		tx.Metadata = map[string]interface{}{}
	}
	tx.Application = "Luma"
	tx.Metadata["application"] = "Luma"
	if hint.LumaEventID != "" {
		tx.Metadata["eventId"] = hint.LumaEventID
	}
	if hint.URL != "" {
		tx.Metadata["eventUrl"] = hint.URL
		addTransactionTag(&tx.Tags, "eventUrl", hint.URL)
	}
	if hint.Name != "" {
		tx.Metadata["eventName"] = hint.Name
		tx.Metadata["description"] = hint.Name
		addTransactionTag(&tx.Tags, "eventName", hint.Name)
	}
	if hint.Collective != "" && tx.Collective == "" {
		tx.Collective = hint.Collective
	}
	if tx.Collective != "" {
		tx.Metadata["collective"] = tx.Collective
		addTransactionTag(&tx.Tags, "collective", tx.Collective)
	}
	addTransactionTag(&tx.Tags, "application", "luma")
	if hint.LumaEventID != "" {
		addTransactionTag(&tx.Tags, "eventId", hint.LumaEventID)
	}
	if tx.Category != "" {
		addTransactionTag(&tx.Tags, "category", tx.Category)
	}
}

func (p *lumaStripeProcessor) loadCalendarEventAliases(ctx *ProcessorContext) error {
	visit := func(path string) {
		data, err := os.ReadFile(path)
		if err != nil {
			return
		}
		var f FullEventsFile
		if json.Unmarshal(data, &f) != nil {
			return
		}
		for _, ev := range f.Events {
			if ev.ID == "" {
				continue
			}
			ref := lumaStripeCalendarEventRef{
				ID:   ev.ID,
				Name: ev.Name,
				URL:  ev.URL,
			}
			p.eventByID[ev.ID] = ref
			if ev.URL != "" {
				if key := normalizeLumaEventURLAlias(ev.URL); key != "" {
					p.eventByURL[key] = ref
				}
			}
			for _, id := range lumaEventIDsFromEvent(ev) {
				p.eventByID[id] = ref
			}
		}
	}

	years, err := os.ReadDir(ctx.DataDir)
	if err != nil {
		return nil
	}
	for _, y := range years {
		if !y.IsDir() || len(y.Name()) != 4 {
			continue
		}
		months, err := os.ReadDir(filepath.Join(ctx.DataDir, y.Name()))
		if err != nil {
			continue
		}
		for _, m := range months {
			if !m.IsDir() || len(m.Name()) != 2 {
				continue
			}
			visit(filepath.Join(ctx.DataDir, y.Name(), m.Name(), "generated", "events.json"))
		}
	}
	visit(filepath.Join(ctx.DataDir, "latest", "generated", "events.json"))
	return nil
}

func (p *lumaStripeProcessor) canonicalEvent(eventID, eventURL string) lumaStripeCalendarEventRef {
	if eventID != "" {
		if canonical := p.eventByID[eventID]; canonical.ID != "" {
			return canonical
		}
	}
	if eventURL != "" {
		if canonical := p.eventByURL[normalizeLumaEventURLAlias(eventURL)]; canonical.ID != "" {
			return canonical
		}
	}
	return lumaStripeCalendarEventRef{}
}

func (p *lumaStripeProcessor) Flush(ctx *ProcessorContext) error {
	if !p.changed {
		return nil
	}
	cache := lumaStripeEventURLCache{
		FetchedAt: time.Now().UTC().Format(time.RFC3339),
		EventURLs: p.eventURLs,
	}
	return ctx.WritePublicJSON(p.Name(), lumastripeplugin.EventURLsFile, cache)
}

func (p *lumaStripeProcessor) transactionLumaEventID(tx TransactionEntry) string {
	if isLumaEventID(tx.Event) {
		return tx.Event
	}
	if tagValue := firstTransactionTagValue(tx, "event"); isLumaEventID(tagValue) {
		return tagValue
	}
	if tx.Metadata != nil {
		for _, key := range []string{"event", "eventId", "event_id", "stripe_event_api_id"} {
			if s, ok := tx.Metadata[key].(string); ok && isLumaEventID(s) {
				return s
			}
		}
	}
	return ""
}

func (p *lumaStripeProcessor) resolveEventURL(eventID string) (string, error) {
	if eventURL := p.eventURLs[eventID]; eventURL != "" {
		return eventURL, nil
	}
	eventURL, err := p.fetchEventURL(eventID)
	if err != nil {
		return "", err
	}
	p.eventURLs[eventID] = eventURL
	p.changed = true
	return eventURL, nil
}

func (p *lumaStripeProcessor) fetchEventURL(eventID string) (string, error) {
	client := *p.client
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	reqURL := p.baseURL + url.PathEscape(eventID)
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	location := resp.Header.Get("Location")
	if location == "" {
		return "", fmt.Errorf("luma redirect for %s returned no Location header", eventID)
	}
	resolved, err := resp.Request.URL.Parse(location)
	if err != nil {
		return "", err
	}
	return resolved.String(), nil
}

func isLumaEventID(s string) bool {
	return strings.HasPrefix(s, "evt-")
}

func normalizeLumaEventURLAlias(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return ""
	}
	host := strings.ToLower(u.Host)
	if host == "lu.ma" {
		host = "luma.com"
	}
	if host != "luma.com" && !strings.HasSuffix(host, ".luma.com") {
		return ""
	}
	path := strings.Trim(strings.TrimSuffix(u.EscapedPath(), "/"), "/")
	if path == "" {
		return ""
	}
	return host + "/" + path
}

func addNIP73WebTags(tags *[][]string, rawURL string) {
	id := normalizeNIP73WebIdentifier(rawURL)
	if id == "" {
		return
	}
	addTransactionTag(tags, "i", id, rawURL)
	addTransactionTag(tags, "k", "web")
}

func normalizeNIP73WebIdentifier(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	return u.String()
}

func lumaEventIDsFromEvent(ev FullEvent) []string {
	ids := map[string]bool{}
	if isLumaEventID(ev.ID) {
		ids[ev.ID] = true
	}
	collectLumaEventIDsFromRaw(ev.LumaData, ids)
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	return out
}

func collectLumaEventIDsFromRaw(raw json.RawMessage, ids map[string]bool) {
	if len(raw) == 0 {
		return
	}
	var v interface{}
	if json.Unmarshal(raw, &v) != nil {
		return
	}
	collectLumaEventIDs(v, ids)
}

func collectLumaEventIDs(v interface{}, ids map[string]bool) {
	switch t := v.(type) {
	case string:
		if isLumaEventID(t) {
			ids[t] = true
		}
	case []interface{}:
		for _, item := range t {
			collectLumaEventIDs(item, ids)
		}
	case map[string]interface{}:
		for _, item := range t {
			collectLumaEventIDs(item, ids)
		}
	}
}

func looksLikeLumaTicketTransaction(tx TransactionEntry) bool {
	reportingCategory := strings.ToLower(firstNonEmptyStr(stringMetadata(tx.Metadata, "category"), stringMetadata(tx.Metadata, "stripe_reporting_category")))
	description := strings.ToLower(firstNonEmptyStr(stringMetadata(tx.Metadata, "description"), tx.Counterparty))
	if reportingCategory == "fee" || reportingCategory == "tax" ||
		strings.Contains(description, "billing - usage fee") ||
		strings.Contains(description, "automatic taxes") {
		return false
	}
	if strings.EqualFold(tx.Category, "tickets") || transactionHasTag(tx, []string{"category", "tickets"}) {
		return true
	}
	if strings.EqualFold(tx.Application, "Luma") || strings.EqualFold(stringMetadata(tx.Metadata, "application"), "Luma") {
		return true
	}
	return false
}

func lumaCollectiveSlugFromURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return ""
	}
	host := strings.ToLower(u.Host)
	if host == "lu.ma" {
		host = "luma.com"
	}
	if host != "luma.com" && !strings.HasSuffix(host, ".luma.com") {
		return ""
	}
	path := strings.Trim(strings.TrimSuffix(u.EscapedPath(), "/"), "/")
	if path == "" {
		return ""
	}
	first := strings.Split(path, "/")[0]
	switch strings.ToLower(first) {
	case "event", "embed", "checkout", "manage":
		return ""
	default:
		return normalizeTransactionTagSlug(first)
	}
}

func normalizeLumaMatchText(s string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(s))), " ")
}

func stringMetadata(metadata map[string]interface{}, key string) string {
	if metadata == nil {
		return ""
	}
	if s, ok := metadata[key].(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}
