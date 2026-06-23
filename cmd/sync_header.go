package cmd

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SyncHeaderItem is one line in the pull / push / sync header: a
// provider label, the configured account (or DB/guild/relays for
// non-account providers), and the last sync/push time for that row.
// Empty fields are skipped on render.
type SyncHeaderItem struct {
	Label    string    // "Stripe", "Etherscan", "Odoo journal", …
	Scope    string    // "acct_1Nn0…", "gnosis:EURe", "citizenspring-test2 — #28 stripe"
	LastSync time.Time // last successful sync/push for this row (UTC)
}

// syncHeaderPrinted tracks, per-process, which named headers we've
// already emitted. `chb sync` calls both runAllProviderSync (Sources)
// and PushAllTargets (Targets); the guard ensures each header is
// printed at most once even if a future aggregator re-enters either
// half.
var syncHeaderPrinted = map[string]bool{}

// renderSyncHeader prints the given items as a compact, aligned
// table on stdout. `title` is the first line ("Fetching latest data" /
// "Pushing changes") — pass "" to skip the title line. No-op when the
// same non-empty title was already printed this process. Skips items
// with empty Scope unless verbose=true.
func renderSyncHeader(title string, items []SyncHeaderItem, verbose bool) {
	if title != "" {
		if syncHeaderPrinted[title] {
			return
		}
		syncHeaderPrinted[title] = true
	}
	if len(items) == 0 {
		return
	}
	// Column widths
	labelW := 0
	scopeW := 0
	for _, it := range items {
		if !verbose && it.Scope == "" {
			continue
		}
		if n := len(it.Label); n > labelW {
			labelW = n
		}
		if n := displayWidth(it.Scope); n > scopeW {
			scopeW = n
		}
	}
	if labelW > 16 {
		labelW = 16
	}
	if scopeW > 60 {
		scopeW = 60
	}

	if title != "" {
		fmt.Printf("  %s%s%s\n", Fmt.Bold, title, Fmt.Reset)
	}
	for _, it := range items {
		if !verbose && it.Scope == "" {
			continue
		}
		last := formatRelativeAndAbsolute(it.LastSync)
		fmt.Printf("    %-*s  %-*s  %s%s%s\n",
			labelW, it.Label,
			scopeW, truncate(it.Scope, scopeW),
			Fmt.Dim, last, Fmt.Reset)
	}
	fmt.Println()
}

// formatRelativeAndAbsolute returns "2h ago (2026-05-21 10:32)" or
// "never" when the time is zero.
func formatRelativeAndAbsolute(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	local := t.In(BrusselsTZ())
	d := time.Since(t)
	var rel string
	switch {
	case d < time.Minute:
		rel = "just now"
	case d < time.Hour:
		rel = fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		rel = fmt.Sprintf("%dh ago", int(d.Hours()))
	case d < 30*24*time.Hour:
		rel = fmt.Sprintf("%dd ago", int(d.Hours()/24))
	default:
		rel = local.Format("2006-01-02")
	}
	return fmt.Sprintf("%s (%s)", rel, local.Format("2006-01-02 15:04"))
}

// pullProviderHeaderItems builds the items for `chb pull` / `chb sync`.
// Emits one row per (provider, configured account), sorted alphabetically
// by provider label then account. Per-account last sync time is read from
// the SyncCursor (provider-specific) when available, falling back to the
// per-account aggregate in SyncState.
func pullProviderHeaderItems() []SyncHeaderItem {
	state := LoadSyncState()
	configs := LoadAccountConfigs()
	settings, _ := LoadSettings()

	var items []SyncHeaderItem

	// Account-based providers — one row each.
	for _, acc := range configs {
		switch strings.ToLower(acc.Provider) {
		case "stripe":
			if acc.AccountID == "" {
				continue
			}
			t := cursorOrAccountTime(SyncCursorKeyForStripeAccount(acc.AccountID), acc.Slug, state)
			items = append(items, SyncHeaderItem{
				Label:    "Stripe",
				Scope:    acc.AccountID,
				LastSync: t,
			})
		case "etherscan":
			// Format: "chain:slug" — e.g. "gnosis:savings". Compact
			// mode collapses multiple etherscan accounts into a single
			// "Etherscan" row, so we want each scope token to be short
			// and unambiguous. Dropping the token-symbol (acc.Currency)
			// keeps the joined list readable when there are many
			// accounts on the same chain.
			scope := acc.Chain
			if acc.Slug != "" {
				scope += ":" + acc.Slug
			}
			items = append(items, SyncHeaderItem{
				Label:    "Etherscan",
				Scope:    scope,
				LastSync: accountTime(acc.Slug, state),
			})
		case "monerium":
			// Prefer the slug; fall back to a truncated address or a
			// "(configured)" marker so a Monerium account without a
			// human-friendly slug still shows up as a row instead of
			// being silently dropped.
			scope := acc.Slug
			if scope == "" {
				scope = shortMoneriumScope(acc.Address)
			}
			if scope == "" {
				scope = "(configured)"
			}
			t := cursorOrAccountTime(SyncCursorKeyForMonerium(acc.Slug), acc.Slug, state)
			items = append(items, SyncHeaderItem{
				Label:    "Monerium",
				Scope:    scope,
				LastSync: t,
			})
		}
	}

	// Discord — one row, scoped by server name (falling back to guild ID
	// when the name isn't cached yet).
	if guildID := settings.Discord.GuildID; guildID != "" {
		scope := DiscordGuildName(guildID)
		if scope == "" {
			scope = guildID
		}
		items = append(items, SyncHeaderItem{
			Label:    "Discord",
			Scope:    scope,
			LastSync: lastSync(state.Messages),
		})
	}

	// Odoo — one row, no scope (the DB is shown in the surrounding
	// "Fetching latest data — Odoo: <db>" banner). Last-sync time is
	// still useful to know how stale the local cache is.
	if creds, err := ResolveOdooCredentials(); err == nil && creds.DB != "" {
		items = append(items, SyncHeaderItem{
			Label:    "Odoo",
			Scope:    "",
			LastSync: lastSync(state.Invoices),
		})
	}

	// ICS — one row per configured calendar feed.
	for _, src := range icsCalendarSlugs(settings) {
		t := cursorTime(SyncCursorKeyForICSCalendar(src))
		if t.IsZero() {
			t = lastSync(state.Calendars)
		}
		items = append(items, SyncHeaderItem{
			Label:    "ICS",
			Scope:    src,
			LastSync: t,
		})
	}

	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Label != items[j].Label {
			return items[i].Label < items[j].Label
		}
		return items[i].Scope < items[j].Scope
	})
	return items
}

// pushTargetHeaderItems builds the items for `chb push` / `chb sync`.
// Emits one row per Odoo journal and one row for the Nostr outbox,
// sorted alphabetically. The Odoo DB is not repeated on each row — the
// containing step header surfaces it once.
func pushTargetHeaderItems() []SyncHeaderItem {
	state := LoadSyncState()
	configs := LoadAccountConfigs()

	var items []SyncHeaderItem
	for _, acc := range configs {
		if acc.OdooJournalID <= 0 {
			continue
		}
		scope := fmt.Sprintf("#%d %s", acc.OdooJournalID, acc.Slug)
		t := cursorTime(SyncCursorKeyForOdooJournal(acc.OdooJournalID))
		if t.IsZero() {
			if state.OdooJournals != nil {
				t = lastSync(state.OdooJournals[strconv.Itoa(acc.OdooJournalID)])
			}
		}
		items = append(items, SyncHeaderItem{
			Label:    "Odoo journal",
			Scope:    scope,
			LastSync: t,
		})
	}

	// Nostr — one row, scoped by relay count.
	if keys := LoadNostrKeys(); keys != nil {
		relays := keys.Relays
		if len(relays) == 0 {
			relays = nostrRelays
		}
		if len(relays) > 0 {
			items = append(items, SyncHeaderItem{
				Label:    "Nostr",
				Scope:    fmt.Sprintf("%d relay%s", len(relays), plural(len(relays))),
				LastSync: cursorTime("nostr.outbox"),
			})
		}
	}

	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Label != items[j].Label {
			return items[i].Label < items[j].Label
		}
		return items[i].Scope < items[j].Scope
	})
	return items
}

// shortMoneriumScope returns a compact rendering of a Monerium
// account's on-chain address — used as a fallback row label when the
// account config lacks a human-friendly slug.
func shortMoneriumScope(addr string) string {
	if addr == "" {
		return ""
	}
	return truncateAddr(addr)
}

// odooTargetHeaderSuffix returns a short target tag suitable for appending
// to the top banner of `chb pull` / `chb push`. Returns "" when no Odoo creds
// are configured. Keep URL + DB visible once so writes are unambiguous.
func odooTargetHeaderSuffix() string {
	creds, err := ResolveOdooCredentials()
	if err != nil || creds.DB == "" {
		return ""
	}
	return fmt.Sprintf("  %s— Odoo: %s (DB: %s)%s", Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)
}

// lastSync returns the last-sync time from a SyncSourceState.
// Empty state yields a zero time which renders as "never".
func lastSync(s *SyncSourceState) time.Time {
	if s == nil {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339, s.LastSync); err == nil {
		return t
	}
	return time.Time{}
}

// cursorTime returns the UpdatedAt time of the SyncCursor for the given
// key, or zero if missing. UpdatedAt is stamped on every successful save.
func cursorTime(key string) time.Time {
	return LoadSyncCursor(key).UpdatedAt
}

// accountTime returns the per-account last-sync time from SyncState.
func accountTime(slug string, state *SyncState) time.Time {
	if state == nil || state.Accounts == nil || slug == "" {
		return time.Time{}
	}
	return lastSync(state.Accounts[strings.ToLower(slug)])
}

// cursorOrAccountTime returns the cursor UpdatedAt when present,
// falling back to the per-account aggregate in SyncState.
func cursorOrAccountTime(cursorKey, slug string, state *SyncState) time.Time {
	if t := cursorTime(cursorKey); !t.IsZero() {
		return t
	}
	return accountTime(slug, state)
}

// icsCalendarSlugs returns the configured ICS calendar slugs, sorted.
func icsCalendarSlugs(s *Settings) []string {
	if s == nil {
		return nil
	}
	var labels []string
	if s.Calendars.Google != "" {
		labels = append(labels, "google")
	}
	if s.Calendars.Luma != "" {
		labels = append(labels, "luma")
	}
	for _, src := range s.Calendars.Sources {
		if src.Slug != "" {
			labels = append(labels, src.Slug)
		}
	}
	sort.Strings(labels)
	return dedupStrings(labels)
}

func dedupStrings(in []string) []string {
	if len(in) <= 1 {
		return in
	}
	out := in[:0]
	prev := ""
	for _, s := range in {
		if s == prev {
			continue
		}
		out = append(out, s)
		prev = s
	}
	return out
}
