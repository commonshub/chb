package cmd

// Audience tiers.
//
// Every processed file chb writes is produced for one of three audiences —
// three levels of trust — and lives in a directory named after it:
//
//	YYYY/MM/public/    anyone: the website's public pages, bots answering
//	                   strangers. No personal data at all.
//	YYYY/MM/members/   people with the Discord `member` role: member-only
//	                   pages, a members-only assistant. Names and identities
//	                   of people in the community, no contact or bank data.
//	YYYY/MM/stewards/  the stewards (admins) and chb itself: everything,
//	                   including emails, IBANs, provider ids.
//
// The raw provider archives (YYYY/MM/providers/) and processor intermediates
// are stewards-only by construction and never served.
//
// The guarantee this buys: a consumer that mounts one tier directory can be
// handed *only* that directory (bind mount, rsync, unix group) and cannot
// see anything above its level — enforced by directory modes on disk, and
// by a per-tier policy at write time that refuses to write personal data
// into a tier that must not carry it.
//
// See docs/audiences.md for the classification of every artifact.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type Audience string

const (
	AudiencePublic   Audience = "public"
	AudienceMembers  Audience = "members"
	AudienceStewards Audience = "stewards"
)

// Audiences lists the tiers from least to most trusted.
var Audiences = []Audience{AudiencePublic, AudienceMembers, AudienceStewards}

func (a Audience) Dir() string { return string(a) }

// DirMode is the directory mode of a tier root and everything below it.
// public is world-readable; members is group-readable (consumers that may see
// member data run in the tier's unix group); stewards is owner-only.
func (a Audience) DirMode() os.FileMode {
	switch a {
	case AudienceMembers:
		return 0o750
	case AudienceStewards:
		return 0o700
	default:
		return 0o755
	}
}

// FileMode is the mode of files inside a tier.
func (a Audience) FileMode() os.FileMode {
	switch a {
	case AudienceMembers:
		return 0o640
	case AudienceStewards:
		return 0o600
	default:
		return 0o644
	}
}

// Level orders tiers: a higher level may contain everything a lower one does.
func (a Audience) Level() int {
	for i, x := range Audiences {
		if x == a {
			return i
		}
	}
	return -1
}

func parseAudience(s string) (Audience, bool) {
	switch Audience(s) {
	case AudiencePublic, AudienceMembers, AudienceStewards:
		return Audience(s), true
	}
	return "", false
}

// audienceOfPath reports the tier a data path belongs to: the first path
// segment named after a tier directly under a month (YYYY/MM/<tier>), a year
// (YYYY/<tier>) or latest/ (latest/<tier>). Paths outside the tier tree
// (providers/, processors/, legacy generated/) report false.
func audienceOfPath(path string) (Audience, bool) {
	parts := strings.FieldsFunc(filepath.ToSlash(path), func(r rune) bool { return r == '/' })
	for i, part := range parts {
		a, ok := parseAudience(part)
		if !ok {
			continue
		}
		if i >= 2 && isYearSegment(parts[i-2]) && isMonthSegment(parts[i-1]) {
			return a, true
		}
		if i >= 1 && (isYearSegment(parts[i-1]) || parts[i-1] == "latest") {
			return a, true
		}
	}
	return "", false
}

// audiencePath is dataDir/YYYY/MM/<tier>/rel — or dataDir/latest/<tier>/rel
// when year is "latest" (month empty).
func audiencePath(dataDir, year, month string, a Audience, rel string) string {
	if year == "latest" || month == "" {
		return filepath.Join(dataDir, year, a.Dir(), rel)
	}
	return filepath.Join(dataDir, year, month, a.Dir(), rel)
}

// ErrAudiencePolicy is returned when data must not be written to a tier.
var ErrAudiencePolicy = errors.New("audience policy violation")

// writeAudienceFile writes rel into the tier directory of the month (and
// mirrors it to latest/<tier>/rel, like writeMonthFile), after running the
// tier's policy. A policy violation is an error, not a warning: the file is
// not written, because a consumer of that tier could otherwise see it.
func writeAudienceFile(dataDir, year, month string, a Audience, rel string, data []byte) error {
	cleaned, err := enforceAudiencePolicy(a, rel, data)
	if err != nil {
		return err
	}
	targets := []string{audiencePath(dataDir, year, month, a, rel)}
	if year != "latest" {
		targets = append(targets, audiencePath(dataDir, "latest", "", a, rel))
	}
	for _, target := range targets {
		if err := os.MkdirAll(filepath.Dir(target), a.DirMode()); err != nil {
			return err
		}
		if err := os.WriteFile(target, cleaned, a.FileMode()); err != nil {
			return err
		}
		if base, ok := dataBaseForPath(target); ok {
			_ = applyDataPathPolicy(base, target, false)
		}
	}
	return nil
}

// enforceAudiencePolicy applies the tier's rules to a JSON payload.
//
//	public:   name fields containing an email are scrubbed (as before); any
//	          remaining email or IBAN anywhere in the document refuses the write.
//	members:  any email or IBAN refuses the write; names are allowed.
//	stewards: anything goes.
//
// Non-JSON payloads only get the IBAN/email scan.
func enforceAudiencePolicy(a Audience, rel string, data []byte) ([]byte, error) {
	if a == AudienceStewards {
		return data, nil
	}
	cleaned := data
	if strings.HasSuffix(rel, ".json") && a == AudiencePublic {
		var leaks []PIILeak
		cleaned, leaks = scrubNameFields(data)
		for _, leak := range leaks {
			Warnf("⚠ audience policy: scrubbed %s in %s/%s (%s)", leak.Kind, a, rel, leak.String())
		}
	}
	var problems []string
	if strings.HasSuffix(rel, ".json") {
		hard, soft := validatePublicJSON(cleaned)
		for _, l := range append(hard, soft...) {
			problems = append(problems, fmt.Sprintf("email at %s (%s)", l.Field, l.String()))
		}
	} else if containsEmail(string(cleaned)) {
		problems = append(problems, "email")
	}
	for _, iban := range findIBANs(cleaned) {
		problems = append(problems, "IBAN "+redactIBAN(iban))
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%w: %s/%s must not carry %s", ErrAudiencePolicy, a, rel, strings.Join(problems, "; "))
	}
	return cleaned, nil
}

// ibanCandidate matches the shape of an IBAN (country, check digits, BBAN);
// findIBANs keeps only candidates whose mod-97 checksum verifies, so hashes,
// ids and product codes never trip the policy.
var ibanCandidate = regexp.MustCompile(`\b[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}\b`)

func findIBANs(data []byte) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range ibanCandidate.FindAllString(string(data), -1) {
		if !seen[m] && ibanChecksumValid(m) {
			seen[m] = true
			out = append(out, m)
		}
	}
	return out
}

// ibanChecksumValid implements ISO 13616 mod-97: move the first four
// characters to the end, map letters to 10..35, the remainder must be 1.
func ibanChecksumValid(iban string) bool {
	if len(iban) < 15 || len(iban) > 34 {
		return false
	}
	rearranged := iban[4:] + iban[:4]
	rem := 0
	for _, r := range rearranged {
		var v int
		switch {
		case r >= '0' && r <= '9':
			v = int(r - '0')
		case r >= 'A' && r <= 'Z':
			v = int(r-'A') + 10
		default:
			return false
		}
		if v >= 10 {
			rem = (rem*100 + v) % 97
		} else {
			rem = (rem*10 + v) % 97
		}
	}
	return rem == 1
}

func redactIBAN(iban string) string {
	if len(iban) <= 8 {
		return iban
	}
	return iban[:4] + "…" + iban[len(iban)-4:]
}
