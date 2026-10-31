package providers

import (
	"crypto/sha256"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

var nameTokenSplit = regexp.MustCompile(`[^a-z0-9]+`)

// NameTokens fingerprints a person's (or organisation's) name so the
// website can match a Discord name to a membership without seeing names:
// NFD, drop combining marks, lowercase, split on [^a-z0-9]+, keep tokens of
// two characters or more, then hex(sha256("name:" + token + salt))[:16].
// Sorted and distinct; nil for an empty name or an empty salt. The website
// hashes Discord names the same way, with the same salt as emailHash.
func NameTokens(name, salt string) []string {
	if salt == "" {
		return nil
	}
	var b strings.Builder
	for _, r := range norm.NFD.String(name) {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		b.WriteRune(unicode.ToLower(r))
	}
	seen := map[string]bool{}
	var out []string
	for _, tok := range nameTokenSplit.Split(b.String(), -1) {
		if len(tok) < 2 {
			continue
		}
		h := sha256.Sum256([]byte("name:" + tok + salt))
		hx := fmt.Sprintf("%x", h)[:16]
		if !seen[hx] {
			seen[hx] = true
			out = append(out, hx)
		}
	}
	sort.Strings(out)
	return out
}
