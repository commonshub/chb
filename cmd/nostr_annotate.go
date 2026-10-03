package cmd

// `chb nostr annotate <uri>` — publish an annotation snapshot for any
// record: a blockchain or bank transaction, a Stripe charge, an Odoo bill,
// credit note, invoice, expense claim or journal entry. See
// docs/annotations.md.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	nostrsource "github.com/CommonsHub/chb/providers/nostr"
	"github.com/nbd-wtf/go-nostr"
)

// NostrAnnotate publishes one annotation, after a preview.
func NostrAnnotate(args []string) error {
	if HasFlag(args, "--help", "-h") || len(args) == 0 || strings.HasPrefix(args[0], "-") {
		printNostrAnnotateHelp()
		return nil
	}
	uri := strings.TrimSpace(args[0])
	if !strings.Contains(uri, ":") {
		return fmt.Errorf("%q is not a URI (expected e.g. odoo:<host>:<db>:account.move:<id>, ethereum:100:tx:0x…, stripe:txn_…)", uri)
	}
	keys := LoadNostrKeys()
	if keys == nil {
		return fmt.Errorf("no Nostr identity configured. Run: chb setup nostr")
	}
	dataDir := DataDir()

	// A snapshot replaces the previous one entirely: start from the
	// current trusted annotation so only the given fields change.
	ann := &TxAnnotation{URI: uri}
	if !HasFlag(args, "--clear") {
		if cur := findCachedAnnotation(dataDir, uri); cur != nil {
			c := *cur
			ann = &c
		}
	}
	set := func(flag string, dst *string) {
		if v := GetOptions(args, flag); len(v) > 0 {
			*dst = strings.TrimSpace(v[len(v)-1]) // "" clears the field
		}
	}
	set("--category", &ann.Category)
	set("--collective", &ann.Collective)
	set("--event", &ann.Event)
	set("--note", &ann.Description)
	set("--exclude", &ann.Exclude) // --exclude "" includes it again
	if spreads := GetOptions(args, "--spread"); len(spreads) > 0 {
		ann.Spread = nil
		for _, sp := range spreads {
			month, amount, ok := strings.Cut(sp, ":")
			if !ok || len(month) != 7 {
				return fmt.Errorf("--spread %q: expected YYYY-MM:amount", sp)
			}
			ann.Spread = append(ann.Spread, SpreadEntry{Month: month, Amount: amount})
		}
	}
	if ann.Category == "" && ann.Collective == "" && ann.Event == "" && ann.Description == "" && len(ann.Spread) == 0 &&
		ann.Exclude == "" && len(GetOptions(args, "--exclude")) == 0 {
		return fmt.Errorf("nothing to annotate: pass --category, --collective, --event, --spread, --note or --exclude")
	}

	// Warnings, not errors: the dataset only knows its own slugs.
	if ann.Category != "" && !knownCategory(ann.Category) {
		Warnf("⚠ category %q is not in categories.json", ann.Category)
	}
	if ann.Collective != "" {
		if _, ok := LoadCollectives()[ann.Collective]; !ok {
			Warnf("⚠ collective %q is not in collectives", ann.Collective)
		}
	}
	idx := buildAnnotationIndex(dataDir)
	if idx.tx[uri] == "" && idx.odoo[uri] == "" {
		Warnf("⚠ %s is not a record this instance holds; the annotation is published anyway", uri)
	}

	tags := annotationTags(uri)
	for _, kv := range [][2]string{{"category", ann.Category}, {"collective", ann.Collective}, {"event", ann.Event}} {
		if kv[1] != "" {
			tags = append(tags, nostr.Tag{kv[0], kv[1]})
		}
	}
	for _, sp := range ann.Spread {
		tags = append(tags, nostr.Tag{"spread", sp.Month, sp.Amount})
	}
	if ann.Exclude != "" {
		tags = append(tags, nostr.Tag{"exclude", ann.Exclude})
	}
	ev := &nostr.Event{Kind: 1111, Tags: tags, Content: ann.Description}

	preview, _ := json.MarshalIndent(map[string]interface{}{"kind": ev.Kind, "tags": ev.Tags, "content": ev.Content}, "  ", "  ")
	fmt.Printf("\n  %sAnnotation for%s %s\n  %s\n\n  Relays: %s\n", Fmt.Bold, Fmt.Reset, uri, preview, strings.Join(nostrRelayList(), ", "))
	if !nostrTrustedPubkeys()[strings.ToLower(keys.PubHex)] {
		Warnf("⚠ your key is not trusted by this instance; the published dataset will ignore it until a trusted author follows you")
	}
	if HasFlag(args, "--dry-run") {
		fmt.Printf("\n  %s(dry-run — nothing published)%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	if !HasFlag(args, "--yes", "-y") {
		if !isInteractiveTTY() {
			return fmt.Errorf("refusing to publish on a non-interactive shell without --yes")
		}
		fmt.Printf("\n  Publish? [y/N] ")
		resp, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if r := strings.ToLower(strings.TrimSpace(resp)); r != "y" && r != "yes" {
			fmt.Println("  Aborted.")
			return nil
		}
	}
	accepted, err := publishNostrEventWithOutbox(keys, uri, ev)
	if err != nil {
		return fmt.Errorf("publish: %w (queued in the outbox; retry with `chb nostr push`)", err)
	}
	fmt.Printf("\n  ✓ Published %s to %s. It applies after the next `chb pull` + `chb generate` (hourly).\n\n", shortEventID(ev.ID), strings.Join(accepted, ", "))
	return nil
}

func knownCategory(slug string) bool {
	for _, c := range LoadCategories() {
		if strings.EqualFold(c.Slug, slug) {
			return true
		}
	}
	return false
}

// findCachedAnnotation returns the newest cached trusted annotation for a
// URI, from any month's transaction or Odoo annotation cache.
func findCachedAnnotation(dataDir, uri string) *TxAnnotation {
	trusted := nostrTrustedPubkeys()
	var best *TxAnnotation
	for _, ym := range dataMonthRange(dataDir) {
		for _, file := range []string{nostrsource.AnnotationsFile, nostrsource.OdooAnnotationsFile} {
			data, err := os.ReadFile(nostrsource.Path(dataDir, ym[:4], ym[5:], file))
			if err != nil {
				continue
			}
			var c NostrAnnotationCache
			if json.Unmarshal(data, &c) != nil {
				continue
			}
			if a := c.Annotations[uri]; annotationTrusted(a, trusted) && (best == nil || a.CreatedAt > best.CreatedAt) {
				best = a
			}
		}
	}
	return best
}

func printNostrAnnotateHelp() {
	fmt.Print(`
chb nostr annotate — publish an annotation for a transaction or an Odoo document

USAGE
  chb nostr annotate <uri> [--category <slug>] [--collective <slug>] [--event <id>]
                           [--spread YYYY-MM:amount …] [--note "text"]
                           [--exclude "reason"] [--clear] [--dry-run] [--yes]

URIS (the "id"/"uri" fields of the published files)
  odoo:<host>:<db>:account.move:<id>       bill, credit note, invoice, journal entry
  odoo:<host>:<db>:hr.expense:<id>         expense claim not booked yet
  ethereum:<chainId>:tx:<hash>             on-chain transaction (100 = Gnosis, 42220 = Celo)
  stripe:txn_…                             Stripe balance transaction
  iban:<iban>:tx:<id>                      bank transaction (KBC, …)

An annotation is a snapshot: the newest trusted one per URI wins. The command
starts from the current annotation and changes only the fields you pass
(--clear starts empty). --exclude "test mint" leaves the record out of
every total (it stays listed, marked excluded); --exclude "" includes it
again. It is a kind 1111 event with lowercase i/k tags only;
uppercase I/K would make it a comment. See docs/annotations.md.
`)
}
