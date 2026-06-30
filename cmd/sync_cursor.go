package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// SyncCursor is the per-target / per-provider state we track so a
// subsequent `chb sync` can quickly answer "is there anything new
// since last time?" without round-tripping to a remote.
//
// Each consumer uses whichever fields fit:
//
//   - Push targets (Odoo journals): LastImportID is the dedup key of
//     the most recently-pushed line; LastWriteDate is the highest
//     write_date we've seen on the journal.
//   - Pull providers that have a content-stable list (Monerium):
//     ContentHash short-circuits the post-pull pipeline when the
//     order list hasn't changed.
//   - Generate-month dirtying: MaxSourceMTime captures the latest
//     mtime among source files for the month; generate skips months
//     whose mtime hasn't advanced.
//
// Stored at $DATA_DIR/last_sync/<key>.json. Key shape (no path separators):
//   odoo.journal.47
//   monerium.savings
//   etherscan.gnosis.checking
//   stripe.acct_1Nn0FaFAhaWeDyow
//   generate.2026-04
//   nostr.outbox
type SyncCursor struct {
	Key            string    `json:"key"`
	UpdatedAt      time.Time `json:"updatedAt"`
	LastImportID   string    `json:"lastImportID,omitempty"`
	LastTimestamp  int64     `json:"lastTimestamp,omitempty"`
	LastWriteDate  string    `json:"lastWriteDate,omitempty"`
	ContentHash    string    `json:"contentHash,omitempty"`
	MaxSourceMTime time.Time `json:"maxSourceMTime,omitempty"`
	Count          int       `json:"count,omitempty"` // last-known item count, for sanity checks

	// DestCount / DestBalanceCents capture the destination's state
	// after the last successful push (e.g. Odoo journal line count +
	// summed amount in cents). Used by push short-circuits to detect
	// drift on the destination side — a cursor that says "local
	// unchanged since last push" is only safe to act on when the
	// destination also looks the same as we left it. Without this,
	// deletions or external edits on Odoo go unnoticed and the next
	// push silently skips everything.
	DestCount        int   `json:"destCount,omitempty"`
	DestBalanceCents int64 `json:"destBalanceCents,omitempty"`
}

func syncCursorDir() string {
	dir := filepath.Join(DataDir(), "last_sync")
	_ = os.MkdirAll(dir, 0755)
	return dir
}

// SyncCursorKeyForOdooJournal returns the canonical key for an Odoo
// journal's push cursor, scoped to the active database so the same journal id
// on a different Odoo database keeps a separate cursor. Keeps the naming
// consistent across callers.
func SyncCursorKeyForOdooJournal(journalID int) string {
	if ns := odoosource.PathNamespace(); ns != "" {
		return "odoo." + ns + ".journal." + intToString(journalID)
	}
	return "odoo.journal." + intToString(journalID)
}

// SyncCursorKeyForMonerium returns the cursor key for a Monerium
// account's pull cache.
func SyncCursorKeyForMonerium(accountSlug string) string {
	return "monerium." + safeCursorKeyPart(accountSlug)
}

// SyncCursorKeyForStripeAccount returns the cursor key for a Stripe
// account's push state (latest balance transaction we pushed).
func SyncCursorKeyForStripeAccount(stripeAccountID string) string {
	if stripeAccountID == "" {
		stripeAccountID = "default"
	}
	return "stripe." + safeCursorKeyPart(stripeAccountID)
}

// SyncCursorKeyForICSCalendar returns the cursor key for an ICS
// calendar URL's pull state. ContentHash holds the ETag, LastWriteDate
// holds the Last-Modified header — both echoed back as conditional
// request headers on the next pull.
func SyncCursorKeyForICSCalendar(slug string) string {
	return "ics." + safeCursorKeyPart(slug)
}

// SyncCursorKeyForGenerateMonth returns the cursor key for a single
// month's generate run.
func SyncCursorKeyForGenerateMonth(year, month string) string {
	return "generate." + year + "-" + month
}

// safeCursorKeyPart strips characters that would make the key
// path-unsafe — slashes, backslashes, dots get collapsed to underscore.
// Keeps the on-disk file name predictable.
func safeCursorKeyPart(s string) string {
	r := strings.NewReplacer("/", "_", "\\", "_", " ", "_", "..", "_")
	return r.Replace(strings.TrimSpace(s))
}

func cursorPath(key string) string {
	return filepath.Join(syncCursorDir(), key+".json")
}

// LoadSyncCursor reads a cursor by key. Returns a zero cursor (Key set,
// UpdatedAt zero) when the file is missing — callers can rely on the
// zero value rather than nil checking.
func LoadSyncCursor(key string) SyncCursor {
	cur := SyncCursor{Key: key}
	data, err := os.ReadFile(cursorPath(key))
	if err != nil {
		return cur
	}
	_ = json.Unmarshal(data, &cur)
	cur.Key = key // defensive: don't trust key from the file
	return cur
}

// SaveSyncCursor writes a cursor by key. Always stamps UpdatedAt to
// the current UTC time so the caller doesn't have to remember.
func SaveSyncCursor(cur SyncCursor) error {
	if cur.Key == "" {
		return nil
	}
	cur.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(cur, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cursorPath(cur.Key), data, 0644)
}

// intToString is a tiny convenience — avoid importing strconv into
// every caller for one-shot int-to-string formatting of cursor keys.
func intToString(n int) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 12)
	for n > 0 {
		buf = append([]byte{digits[n%10]}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}
