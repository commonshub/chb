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
	"sync"
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
	if strings.HasSuffix(rel, ".json") {
		// Name-like fields (name, firstName, lastName, displayName) whose
		// value is an email are scrubbed in both lower tiers: a display
		// name that is a mailbox is a contact detail, not a name.
		var leaks []PIILeak
		cleaned, leaks = scrubNameFields(cleaned)
		for _, leak := range leaks {
			Warnf("⚠ audience policy: scrubbed %s in %s/%s (%s)", leak.Kind, a, rel, leak.String())
		}
	}
	// Emails inside free text (a message, a memo, a CSV cell) are masked
	// rather than the whole file withheld — the file stays useful and the
	// mailbox is gone. Opaque ids that merely look like emails (Luma /
	// Google Calendar UIDs) and role mailboxes (hello@, info@) are kept.
	cleaned = maskEmails(cleaned)

	var problems []string
	if strings.HasSuffix(rel, ".json") {
		hard, _ := validatePublicJSON(cleaned)
		for _, l := range hard {
			problems = append(problems, fmt.Sprintf("email at %s (%s)", l.Field, l.String()))
		}
	}
	// A bank account number of a third party never belongs below stewards.
	// Our own accounts' IBANs are on every invoice we send and stay.
	for _, iban := range findIBANs(cleaned) {
		if ownAccountIBANs()[iban] {
			continue
		}
		problems = append(problems, "IBAN "+redactIBAN(iban))
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%w: %s/%s must not carry %s", ErrAudiencePolicy, a, rel, strings.Join(problems, "; "))
	}
	return cleaned, nil
}

// emailMask replaces a masked mailbox in free text.
const emailMask = "[email removed]"

// calendarUIDPattern matches Google Calendar event UIDs, which are shaped
// like emails (26 lowercase alphanumerics @google.com) but identify events.
var calendarUIDPattern = regexp.MustCompile(`^[a-z0-9]{20,}@google\.com$`)

// roleMailboxLocalParts are generic organisational mailboxes, not people.
var roleMailboxLocalParts = map[string]bool{
	"hello": true, "info": true, "contact": true, "team": true, "admin": true,
	"support": true, "press": true, "bookings": true, "booking": true, "events": true,
	"noreply": true, "no-reply": true, "billing": true, "invoices": true, "finance": true,
}

func isRoleMailbox(addr string) bool {
	at := strings.IndexByte(addr, '@')
	if at <= 0 {
		return false
	}
	return roleMailboxLocalParts[strings.ToLower(addr[:at])]
}

// maskEmails replaces every personal email-shaped substring with emailMask.
// Emails contain no quotes or backslashes, so a textual replacement keeps
// JSON valid.
func maskEmails(data []byte) []byte {
	return emailPattern.ReplaceAllFunc(data, func(m []byte) []byte {
		s := string(m)
		if isNonMailboxIdentifier(s) || calendarUIDPattern.MatchString(s) || isRoleMailbox(s) {
			return m
		}
		return []byte(emailMask)
	})
}

// ownAccountIBANs is the set of IBANs of the org's own tracked accounts
// (accounts.json). Computed once per process; tests override.
var (
	ownIBANsOnce    sync.Once
	ownIBANsSet     map[string]bool
	ownIBANsForTest map[string]bool
)

func ownAccountIBANs() map[string]bool {
	if ownIBANsForTest != nil {
		return ownIBANsForTest
	}
	ownIBANsOnce.Do(func() {
		ownIBANsSet = map[string]bool{}
		for _, acc := range LoadAccountConfigs() {
			if iban := normalizeIBAN(acc.IBAN); iban != "" {
				ownIBANsSet[iban] = true
			}
		}
	})
	return ownIBANsSet
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

// ---- chb's own view, legacy output, and migration ----------------------

// stewardsDirName is the directory chb reads from and writes to: the full,
// unredacted dataset. Every internal path that used to say "generated" says
// this now; members/ and public/ are projections written next to it.
const stewardsDirName = "stewards"

// legacyGeneratedDirName is the pre-tier output directory. It keeps being
// written (with the same content as before, minus generated/private/) for
// consumers that have not moved to a tier yet — the website, until it reads
// public/ and members/. Set CHB_LEGACY_GENERATED=0 to stop writing it.
const legacyGeneratedDirName = "generated"

func legacyGeneratedEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CHB_LEGACY_GENERATED"))) {
	case "0", "false", "no", "off":
		return false
	}
	return true
}

// tierPayload carries one artifact's bytes per tier. A nil entry means the
// artifact has no representation in that tier (e.g. profiles never reach
// public/). Stewards is always written.
type tierPayload struct {
	Stewards []byte
	Members  []byte
	Public   []byte
	Legacy   []byte // what generated/<rel> used to contain; nil = don't write it
}

// writeTiers writes one artifact to every tier it belongs to (month +
// latest/ mirror, like writeMonthFile), and to the legacy generated/ tree
// while that is still enabled. A members/public policy violation is
// reported and that tier is skipped — the stewards copy is never withheld.
func writeTiers(dataDir, year, month, rel string, p tierPayload) {
	if p.Stewards != nil {
		if err := writeAudienceFile(dataDir, year, month, AudienceStewards, rel, p.Stewards); err != nil {
			Warnf("  %s⚠ %s%s", Fmt.Yellow, err, Fmt.Reset)
		}
	}
	if p.Members != nil {
		if err := writeAudienceFile(dataDir, year, month, AudienceMembers, rel, p.Members); err != nil {
			Warnf("  %s⚠ %s%s", Fmt.Yellow, err, Fmt.Reset)
		}
	}
	if p.Public != nil {
		if err := writeAudienceFile(dataDir, year, month, AudiencePublic, rel, p.Public); err != nil {
			Warnf("  %s⚠ %s%s", Fmt.Yellow, err, Fmt.Reset)
		}
	}
	if p.Legacy != nil && legacyGeneratedEnabled() {
		_ = writeMonthFile(dataDir, year, month, filepath.Join(legacyGeneratedDirName, rel), p.Legacy)
	}
}

// writeTiersSame is writeTiers for artifacts that carry no personal data at
// all (aggregates, calendars, markdown): identical bytes in every tier.
func writeTiersSame(dataDir, year, month, rel string, data []byte) {
	writeTiers(dataDir, year, month, rel, tierPayload{Stewards: data, Members: data, Public: data, Legacy: data})
}

// migrateGeneratedToStewards seeds stewards/ from a pre-tier generated/ tree
// so chb keeps working on months that have not been regenerated since the
// tier split: every file under generated/ that stewards/ lacks is copied
// over (generated/private/ included, so PII enrichment stays readable), and
// generated/private/ is then removed — that subtree was the one thing in
// the legacy tree that must never be reachable by a public consumer.
// Idempotent and cheap: one stat per file.
func migrateGeneratedToStewards(baseDir string) {
	roots := []string{filepath.Join(baseDir, "latest")}
	years, _ := os.ReadDir(baseDir)
	for _, y := range years {
		if !y.IsDir() || !isYearSegment(y.Name()) {
			continue
		}
		roots = append(roots, filepath.Join(baseDir, y.Name()))
		months, _ := os.ReadDir(filepath.Join(baseDir, y.Name()))
		for _, m := range months {
			if m.IsDir() && isMonthSegment(m.Name()) {
				roots = append(roots, filepath.Join(baseDir, y.Name(), m.Name()))
			}
		}
	}
	for _, root := range roots {
		src := filepath.Join(root, legacyGeneratedDirName)
		if info, err := os.Stat(src); err != nil || !info.IsDir() {
			continue
		}
		dst := filepath.Join(root, stewardsDirName)
		copied := 0
		_ = filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(src, path)
			if err != nil {
				return nil
			}
			target := filepath.Join(dst, rel)
			if _, err := os.Stat(target); err == nil {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			if err := os.MkdirAll(filepath.Dir(target), AudienceStewards.DirMode()); err != nil {
				return nil
			}
			if err := os.WriteFile(target, data, AudienceStewards.FileMode()); err == nil {
				copied++
			}
			return nil
		})
		privateDir := filepath.Join(src, "private")
		_, hadPrivate := os.Stat(privateDir)
		_ = os.RemoveAll(privateDir)
		if copied > 0 || hadPrivate == nil {
			note := ""
			if hadPrivate == nil {
				note = "; generated/private/ removed"
			}
			fmt.Fprintf(os.Stderr, "  %s✓%s %s: seeded stewards/ from generated/ (%d files)%s\n",
				Fmt.Green, Fmt.Reset, strings.TrimPrefix(root, baseDir+string(os.PathSeparator)), copied, note)
		}
	}
}

// tierPathScope reports the (year, month) a tier file path belongs to —
// ("2026","09") for 2026/09/<tier>/…, ("2026","") for 2026/<tier>/…,
// ("latest","") for latest/<tier>/….
func tierPathScope(dataDir, path string) (year, month string, ok bool) {
	rel, err := filepath.Rel(dataDir, path)
	if err != nil {
		return "", "", false
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	switch {
	case len(parts) >= 4 && isYearSegment(parts[0]) && isMonthSegment(parts[1]):
		if _, isTier := parseAudience(parts[2]); isTier {
			return parts[0], parts[1], true
		}
	case len(parts) >= 3 && isYearSegment(parts[0]):
		if _, isTier := parseAudience(parts[1]); isTier {
			return parts[0], "", true
		}
	case len(parts) >= 3 && parts[0] == "latest":
		if _, isTier := parseAudience(parts[1]); isTier {
			return "latest", "", true
		}
	}
	return "", "", false
}
