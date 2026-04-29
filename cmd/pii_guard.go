package cmd

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// emailPattern roughly matches email addresses. Used to detect PII leaks in
// files written outside a /private/ subdirectory.
var emailPattern = regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)

// pathHasPrivateSegment reports whether the given slash- or os-separated path
// contains a "private" path segment.
func pathHasPrivateSegment(path string) bool {
	for _, part := range strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		if part == "private" {
			return true
		}
	}
	return false
}

// pathHasSourceDataSegment reports whether the path is under a monthly or
// latest source dump directory: <YYYY>/<MM>/data/<source>/... or
// latest/data/<source>/.... Source dumps are private backups by default; only
// generated files are expected to be public-safe.
func pathHasSourceDataSegment(path string) bool {
	parts := strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	for i, part := range parts {
		if part != "data" {
			continue
		}
		if i >= 2 && isYearSegment(parts[i-2]) && isMonthSegment(parts[i-1]) {
			return true
		}
		if i >= 1 && parts[i-1] == "latest" {
			return true
		}
	}
	return false
}

func isYearSegment(s string) bool {
	if len(s) != 4 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func isMonthSegment(s string) bool {
	if len(s) != 2 {
		return false
	}
	return s >= "01" && s <= "12"
}

// nameFieldKeys are the JSON fields that must never contain an "@".
var nameFieldKeys = map[string]struct{}{
	"firstname": {},
	"lastname":  {},
	"name":      {},
}

// piiSoftAllowlist suppresses the "email in <field>" soft warning for known
// non-PII identifiers: Luma event IDs and Google Calendar UIDs, which embed
// an "@" in their ID but aren't actually mailboxes. Keyed by the JSON leaf
// key (lowercased). Each pattern is matched against the whole value.
//
// The /hard/ check on name fields is unaffected — only these specific fields
// get the soft-warning exemption.
var piiSoftAllowlist = map[string][]*regexp.Regexp{
	"id": {
		regexp.MustCompile(`@events\.lu\.ma$`),
		regexp.MustCompile(`@google\.com$`),
	},
	"coverimagelocal": {
		regexp.MustCompile(`@events\.lu\.ma\.(jpg|jpeg|png|webp)$`),
		regexp.MustCompile(`@google\.com\.(jpg|jpeg|png|webp)$`),
	},
}

// softAllowlistMatch reports whether a given value in a given field is
// covered by the soft-warning allowlist.
func softAllowlistMatch(leafKey, value string) bool {
	patterns, ok := piiSoftAllowlist[strings.ToLower(leafKey)]
	if !ok {
		return false
	}
	for _, p := range patterns {
		if p.MatchString(value) {
			return true
		}
	}
	return false
}

// PIILeak describes a single PII leak detected in a JSON payload.
type PIILeak struct {
	Field string // JSON path of the offending value
	Kind  string // "name-has-at" | "email"
	Value string // redacted sample (email local-part masked)
}

func (l PIILeak) String() string {
	switch l.Kind {
	case "name-has-at":
		return fmt.Sprintf("%s contains '@': %s", l.Field, l.Value)
	default:
		return fmt.Sprintf("email in %s: %s", l.Field, l.Value)
	}
}

// validatePublicJSON scans a JSON payload for PII that must not appear outside
// a /private/ directory. It returns:
//   - a list of hard violations (must block the write): "@" in name fields
//   - a list of soft violations (should warn): email-looking strings elsewhere
//
// The payload is only inspected when it is valid JSON; non-JSON bytes are
// returned as (nil, nil).
func validatePublicJSON(data []byte) (hard, soft []PIILeak) {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, nil
	}
	scanPII(v, "", &hard, &soft)
	return hard, soft
}

func scanPII(v interface{}, path string, hard, soft *[]PIILeak) {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, sub := range t {
			childPath := k
			if path != "" {
				childPath = path + "." + k
			}
			scanPII(sub, childPath, hard, soft)
		}
	case []interface{}:
		for i, sub := range t {
			childPath := fmt.Sprintf("%s[%d]", path, i)
			scanPII(sub, childPath, hard, soft)
		}
	case string:
		leaf := path
		if idx := strings.LastIndexAny(leaf, ".["); idx >= 0 {
			leaf = strings.TrimPrefix(leaf[idx:], ".")
			leaf = strings.TrimSuffix(leaf, "]")
			if i := strings.LastIndex(leaf, "["); i >= 0 {
				leaf = leaf[:i]
			}
		}
		leafLower := strings.ToLower(leaf)
		if _, isName := nameFieldKeys[leafLower]; isName {
			if strings.ContainsRune(t, '@') {
				*hard = append(*hard, PIILeak{Field: path, Kind: "name-has-at", Value: redactEmail(t)})
				return
			}
		}
		if emailPattern.MatchString(t) {
			if softAllowlistMatch(leafLower, t) {
				return
			}
			*soft = append(*soft, PIILeak{Field: path, Kind: "email", Value: redactEmail(t)})
		}
	}
}

// redactEmail masks the local part of any email-looking substring so leaks can
// be diagnosed without reproducing the PII in logs.
func redactEmail(s string) string {
	return emailPattern.ReplaceAllStringFunc(s, func(match string) string {
		at := strings.IndexByte(match, '@')
		if at <= 0 {
			return "***"
		}
		local := match[:at]
		if len(local) <= 2 {
			return "***" + match[at:]
		}
		return local[:1] + "***" + match[at:]
	})
}

// scrubNameFields rewrites firstName/lastName/name values that contain "@" so
// the written file never leaks emails in those fields. Returns the rewritten
// JSON bytes and the list of fields that were scrubbed.
func scrubNameFields(data []byte) ([]byte, []PIILeak) {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return data, nil
	}
	var scrubbed []PIILeak
	rewritten := scrubNameValue(v, "", &scrubbed)
	if len(scrubbed) == 0 {
		return data, nil
	}
	out, err := json.MarshalIndent(rewritten, "", "  ")
	if err != nil {
		return data, scrubbed
	}
	return out, scrubbed
}

func scrubNameValue(v interface{}, key string, scrubbed *[]PIILeak) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, sub := range t {
			t[k] = scrubNameValue(sub, k, scrubbed)
		}
		return t
	case []interface{}:
		for i, sub := range t {
			t[i] = scrubNameValue(sub, key, scrubbed)
		}
		return t
	case string:
		if _, isName := nameFieldKeys[strings.ToLower(key)]; isName {
			if strings.ContainsRune(t, '@') {
				*scrubbed = append(*scrubbed, PIILeak{Field: key, Kind: "name-has-at", Value: redactEmail(t)})
				cleaned := sanitizePersonName(t)
				if cleaned == "" && strings.ToLower(key) == "firstname" {
					return "Member"
				}
				return cleaned
			}
		}
		return t
	}
	return v
}
