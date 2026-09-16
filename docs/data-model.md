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
│   ├── members/                         #   same file names, strictly less in each lower tier
│   ├── stewards/                        #   (0755 / 0750 / 0700)
│   └── generated/                       # legacy: today's public tree + private/ (being phased out)
│       ├── transactions.json
│       ├── events.json
│       ├── messages.json
│       ├── images.json
│       ├── counterparties.json
│       └── private/                     # PII layer, requires --with-pii
│           └── enrichment.json
└── latest/
    ├── {public,members,stewards}/       # the most recent month per audience, mirrored
    └── generated/                       # legacy mirror
```

Audiences: `public/`, `members/`, `stewards/` are three levels of trust;
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

### `generated/`

Every file `chb generate` produces lives here. Vendor-agnostic — no Odoo IDs, no partner-IDs, no Stripe-internal handles beyond what's needed to round-trip. Push paths *load* from here, then enrich with the target-specific `pending/` entries.

### `latest/`

A mirror of the most recent month's `generated/` files, plus aggregated multi-month files (e.g. `latest/generated/events.json` covers everything upcoming, not just one month). Convenient for downstream consumers that want "current state" without computing month bounds.

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
