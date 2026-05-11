package cmd

import (
	"regexp"
	"sort"
	"strings"
)

var ibanLikePattern = regexp.MustCompile(`(?i)\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b`)

// parseTransactionTagSpec converts CLI/user tag syntax to the public
// Nostr-style array form:
//
//	#ticket-sale            -> ["t", "ticket-sale"]
//	#color:red              -> ["color", "red"]
//	#[url:https://example]   -> ["url", "https://example"]
func parseTransactionTagSpec(spec string) ([]string, bool) {
	spec = strings.TrimSpace(spec)
	spec = strings.TrimPrefix(spec, "#")
	if strings.HasPrefix(spec, "[") && strings.HasSuffix(spec, "]") {
		spec = strings.TrimSuffix(strings.TrimPrefix(spec, "["), "]")
	}
	if spec == "" {
		return nil, false
	}

	if key, value, ok := strings.Cut(spec, ":"); ok {
		return normalizeTransactionTag([]string{key, value})
	}
	return normalizeTransactionTag([]string{"t", spec})
}

func addTransactionTag(tags *[][]string, key string, values ...string) {
	raw := make([]string, 0, 1+len(values))
	raw = append(raw, key)
	raw = append(raw, values...)
	if tag, ok := normalizeTransactionTag(raw); ok {
		*tags = append(*tags, tag)
	}
}

func addTransactionTagFromValue(tags *[][]string, key string, v interface{}) {
	if s, ok := v.(string); ok {
		addTransactionTag(tags, key, s)
	}
}

func normalizeTransactionTag(raw []string) ([]string, bool) {
	if len(raw) < 2 {
		return nil, false
	}
	key := normalizeTransactionTagKey(raw[0])
	if key == "" {
		return nil, false
	}

	tag := make([]string, 0, len(raw))
	tag = append(tag, key)
	for _, v := range raw[1:] {
		v = strings.TrimSpace(v)
		if v == "" || transactionTagValueLooksPrivate(v) {
			continue
		}
		if key == "t" || key == "category" || key == "collective" || key == "application" || key == "source" || key == "status" || key == "color" {
			v = normalizeTransactionTagSlug(v)
		}
		tag = append(tag, v)
	}
	if len(tag) < 2 {
		return nil, false
	}
	return tag, true
}

func normalizeTransactionTagKey(key string) string {
	key = strings.TrimSpace(strings.TrimPrefix(key, "#"))
	lower := strings.ToLower(key)
	switch lower {
	case "", "tag":
		return "t"
	case "eventurl":
		return "eventUrl"
	case "eventname":
		return "eventName"
	case "lumaevent":
		return "lumaEvent"
	case "paymentlink":
		return "paymentLink"
	}
	return key
}

func normalizeTransactionTagSlug(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.ReplaceAll(value, "_", "-")
	value = strings.Join(strings.Fields(value), "-")
	return value
}

func transactionTagValueLooksPrivate(value string) bool {
	return emailPattern.MatchString(value) || ibanLikePattern.MatchString(value)
}

func normalizeTransactionTags(tags [][]string) [][]string {
	if len(tags) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([][]string, 0, len(tags))
	for _, raw := range tags {
		tag, ok := normalizeTransactionTag(raw)
		if !ok {
			continue
		}
		key := strings.Join(tag, "\x00")
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, tag)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.Join(out[i], "\x00") < strings.Join(out[j], "\x00")
	})
	if len(out) == 0 {
		return nil
	}
	return out
}

func transactionHasTag(tx TransactionEntry, query []string) bool {
	query, ok := normalizeTransactionTag(query)
	if !ok {
		return false
	}
	for _, raw := range tx.Tags {
		tag, ok := normalizeTransactionTag(raw)
		if !ok || len(tag) < len(query) {
			continue
		}
		matches := true
		for i := range query {
			if i == 0 {
				if tag[i] != query[i] {
					matches = false
					break
				}
				continue
			}
			if !strings.EqualFold(tag[i], query[i]) {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	if transactionMatchesLegacyTagFields(tx, query) {
		return true
	}
	return false
}

func transactionHasTagKey(tx TransactionEntry, key string) bool {
	return firstTransactionTagValue(tx, key) != ""
}

func firstTransactionTagValue(tx TransactionEntry, key string) string {
	key = normalizeTransactionTagKey(key)
	for _, raw := range tx.Tags {
		tag, ok := normalizeTransactionTag(raw)
		if !ok || len(tag) < 2 {
			continue
		}
		if tag[0] == key {
			return tag[1]
		}
	}
	return ""
}

func formatTransactionTag(tag []string) string {
	tag, ok := normalizeTransactionTag(tag)
	if !ok {
		return ""
	}
	if tag[0] == "t" {
		return "#" + tag[1]
	}
	return "#" + tag[0] + ":" + strings.Join(tag[1:], ":")
}

func transactionMatchesLegacyTagFields(tx TransactionEntry, query []string) bool {
	if len(query) < 2 {
		return false
	}
	key, value := query[0], query[1]
	switch key {
	case "category":
		return transactionTagValueEqual(key, tx.Category, value)
	case "collective":
		return transactionTagValueEqual(key, tx.Collective, value)
	case "event":
		return transactionTagValueEqual(key, tx.Event, value)
	case "source":
		return transactionTagValueEqual(key, tx.Provider, value)
	case "type":
		return transactionTagValueEqual(key, tx.Type, value)
	}
	if tx.Metadata == nil {
		return false
	}
	keys := []string{key}
	if key == "paymentLink" {
		keys = append(keys, "stripe_payment_link")
	}
	if key == "eventUrl" {
		keys = append(keys, "stripe_event_url")
	}
	if key == "eventName" {
		keys = append(keys, "stripe_event_name")
	}
	if key == "application" {
		keys = append(keys, "stripe_application")
	}
	if key == "event" {
		keys = append(keys, "eventId", "event_id", "stripe_event_api_id")
	}
	if key == "collective" {
		keys = append(keys, "stripe_collective")
	}
	for _, k := range keys {
		if s, ok := tx.Metadata[k].(string); ok && transactionTagValueEqual(key, s, value) {
			return true
		}
	}
	return false
}

func transactionTagValueEqual(key, a, b string) bool {
	switch key {
	case "t", "category", "collective", "application", "source", "status", "color", "type":
		return normalizeTransactionTagSlug(a) == normalizeTransactionTagSlug(b)
	default:
		return strings.EqualFold(a, b)
	}
}

func syncTransactionTags(tx *TransactionEntry) {
	// Drop any stale spread tags from the existing list — the canonical source
	// is tx.Spread, which we re-emit below.
	var tags [][]string
	for _, t := range tx.Tags {
		if len(t) > 0 && t[0] == "spread" {
			continue
		}
		tags = append(tags, t)
	}
	for _, s := range tx.Spread {
		if s.Month == "" {
			continue
		}
		tags = append(tags, []string{"spread", s.Month, s.Amount})
	}

	if tx.Category != "" {
		addTransactionTag(&tags, "category", tx.Category)
	}
	if tx.Collective != "" {
		addTransactionTag(&tags, "collective", tx.Collective)
	}
	if tx.Event != "" {
		addTransactionTag(&tags, "event", tx.Event)
	}
	if tx.Application != "" {
		addTransactionTag(&tags, "application", tx.Application)
	}
	if tx.Provider != "" {
		addTransactionTag(&tags, "source", tx.Provider)
	}
	if tx.Type != "" {
		addTransactionTag(&tags, "type", tx.Type)
	}

	if tx.Metadata != nil {
		addTransactionTagFromValue(&tags, "application", tx.Metadata["application"])
		addTransactionTagFromValue(&tags, "application", tx.Metadata["stripe_application"])
		addTransactionTagFromValue(&tags, "paymentLink", tx.Metadata["paymentLink"])
		addTransactionTagFromValue(&tags, "paymentLink", tx.Metadata["stripe_payment_link"])
		addTransactionTagFromValue(&tags, "event", tx.Metadata["event"])
		addTransactionTagFromValue(&tags, "event", tx.Metadata["eventId"])
		addTransactionTagFromValue(&tags, "event", tx.Metadata["stripe_event_api_id"])
		addTransactionTagFromValue(&tags, "eventUrl", tx.Metadata["eventUrl"])
		addTransactionTagFromValue(&tags, "eventUrl", tx.Metadata["stripe_event_url"])
		addTransactionTagFromValue(&tags, "eventName", tx.Metadata["eventName"])
		addTransactionTagFromValue(&tags, "eventName", tx.Metadata["stripe_event_name"])
		addTransactionTagFromValue(&tags, "collective", tx.Metadata["collective"])
		addTransactionTagFromValue(&tags, "collective", tx.Metadata["stripe_collective"])
		addTransactionTagFromValue(&tags, "category", tx.Metadata["category"])
		addTransactionTagFromValue(&tags, "t", tx.Metadata["product"])
		addTransactionTagFromValue(&tags, "t", tx.Metadata["stripe_product"])

		skipMetadataTags := map[string]bool{
			"accountSlug": true, "application": true, "category": true,
			"collective": true, "description": true, "email": true,
			"event": true, "eventId": true,
			"eventName": true, "eventUrl": true,
			"memo": true, "paymentLink": true,
			"paymentMethod": true, "product": true, "state": true,
			"stripe_application": true, "stripe_collective": true,
			"stripe_event_api_id": true, "stripe_event_name": true, "stripe_event_url": true,
			"stripe_payment_link": true,
			"stripe_product":      true,
		}
		for k, v := range tx.Metadata {
			if skipMetadataTags[k] || strings.HasPrefix(k, "stripe_") || strings.HasPrefix(k, "custom_") {
				continue
			}
			addTransactionTagFromValue(&tags, k, v)
		}
	}

	tx.Tags = normalizeTransactionTags(tags)
}
