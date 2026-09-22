# Data model

The on-disk shape `chb` reads and writes. Schemas first, file layout after.

## TransactionEntry

The canonical transaction shape, defined in `cmd/generate.go`. One per money-movement event from one account's perspective — see [transactions.md](transactions.md) for the full schema (signed-amount convention, account-based vs graph-based design, why there's no `from`/`to`, per-provider quirks).

Short version:

- **Account-based.** Each entry has an `accountId` and the amount is signed from that perspective. Internal transfers produce two entries (one per account), sharing the same `id`.
- **No `from` / `to` / `sender` / `receiver` fields.** Use `accountId` (the perspective) and `counterpartyId` (the other side).
- **Signed `amount`:** positive ⇔ money INTO `accountId`. `grossAmount` is always positive; `netAmount` is signed after fees.
- **Not in JSON, but on the in-memory struct:** `AccountCode`, `PartnerID` (Odoo-specific; live in `providers/odoo/pending/`).

## FullEvent

Defined in `cmd/events_generate.go`. One per public event or room booking.

Key fields: `id`, `name`, `start` / `end` (RFC3339 with Brussels offset, never naïve), `allDay`, `room`, `visibility`, `host`, `attendees`, `ticketUrl`, `metadata`.

All-day events (`allDay=true`) must be rendered without a clock time — see [philosophy.md](philosophy.md).

## Message

Defined in `cmd/messages_sync.go`. One per Discord message in a tracked channel: `channelId`, `messageId`, `author`, `content`, `timestamp`, `attachments`.

## File layout

The on-disk root is `$DATA_DIR` (default `$APP_DATA_DIR/data` → `~/.chb/data`). Three top-level shapes:

```
$DATA_DIR/
├── YYYY/MM/
│   ├── providers/<provider>/...        # raw provider archives (one per source)
│   ├── processors/<processor>/...      # cross-provider enrichment outputs
│   ├── public/                          # processed data, per audience — see audiences.md
│   │   ├── transactions.json            #   same file names in every tier,
│   │   ├── events.json                  #   strictly less in each lower one
│   │   ├── events/images/               #   (binary assets live in the lowest tier only)
│   │   └── …
│   ├── members/                         # 0750, group chb-members
│   ├── stewards/                        # 0700 — chb's own working tree (full data, incl. PII)
│   └── generated/                       # legacy copy of the pre-tier public tree, for consumers
│                                        #   not yet on a tier; no private/ anymore; CHB_LEGACY_GENERATED=0 stops it
└── latest/
    ├── {public,members,stewards}/       # the most recent month per audience, mirrored
    │   └── stewards/cache/              # chb caches (wallet resolution, OG images)
    └── generated/                       # legacy mirror
```

Audiences: `public/`, `members/`, `stewards/` are three levels of trust ([website.md](website.md) is the consumer-facing reference);
[audiences.md](audiences.md) defines what each may contain, how the policy is
enforced at write time and on disk, and the migration from `generated/`.

### `providers/<provider>/`

Raw provider state, archived unchanged. Re-running `sync` against the same period must produce identical files (idempotency invariant).

Examples:
- `providers/stripe/transactions.json` — Stripe balance-transactions for the month.
- `providers/etherscan/gnosis/<slug>.<symbol>.json` — Etherscan-format transfers per (chain, account, token).
- `providers/ics/<slug>.ics` — raw ICS feed bytes.
- `providers/odoo/<entity>.json` — Odoo journal/move/partner caches.

### `providers/<target>/pending/`

Targets (Odoo, Nostr) have an extra `pending/` folder that holds the changes the next `push` would publish:

```
providers/odoo/pending/transactions.json
```

Schema: `{ generatedAt, entries: { <txUri>: { accountCode, partnerId, category, collective } } }`. Written by `generate`, read by every push path. Inspect with `git diff providers/odoo/pending/2026-05/transactions.json`.

Nostr's equivalent lives at `$APP_DATA_DIR/nostr/outbox/` for historical reasons (the outbox holds signed-but-unsent events). List with `chb nostr pending`. Both serve the same role — pending changes you can inspect before publishing.

### `stewards/`, `members/`, `public/`

Every file `chb generate` produces lives in the three tiers. `stewards/` is the full dataset chb itself reads (push paths load from here, then enrich with the target-specific `pending/` entries); `members/` and `public/` are projections of the same files with less in them. Vendor-agnostic — no Odoo IDs, no partner-IDs, no Stripe-internal handles beyond what's needed to round-trip.

### `latest/`

A mirror of the most recent month's tier files (`latest/<tier>/`), plus aggregated multi-month files (e.g. `latest/generated/events.json` covers everything upcoming, not just one month). Convenient for downstream consumers that want "current state" without computing month bounds.

## URIs (NIP-73)

Every entity has a URI used as its canonical handle:

| Entity | URI form |
|---|---|
| Blockchain tx | `ethereum:<chainId>:tx:<hash>` |
| Blockchain address | `ethereum:<chainId>:address:<addr>` |
| Token contract | `ethereum:<chainId>:token:<contract>` |
| Stripe object | `stripe:<id>` (the id carries its own type prefix `txn_…`, `cus_…`, `ch_…`) |
| Bank tx | `iban:<iban-lowercase>:tx:<row-hash>` |
| Bank account | `iban:<iban-lowercase>` |

These are also the `i` tags used by Nostr kind-1111 annotations, so the same key works on both sides.

## Settings vs data

Settings live in `$APP_DATA_DIR/settings/` (see [README.md](../README.md#settings)). Data lives in `$DATA_DIR`. The two are deliberately separate roots so settings can be checked into a private git repo while data stays out.

`$APP_DATA_DIR/nostr/` holds the Nostr outbox + sent events (Nostr's pending state) and signing keys.
