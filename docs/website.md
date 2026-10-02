# The dataset, for the website (and any other consumer)

This is the durable reference for whoever builds on the data `chb` produces:
where files are, what each one contains, which audience may see it, and how
to publish it. Migration steps from the pre-tier layout live in
[website-migration.md](website-migration.md); the reasoning behind the tiers
in [audiences.md](audiences.md).

## 1. Layout

`$DATA_DIR` (bind-mounted as `/data` in the website container, read-only by
construction — the runtime user cannot write anywhere under it):

```
/data/
├── YYYY/MM/
│   ├── providers/…              raw archives — NEVER readable by the website (0700)
│   ├── public/                  anyone                       0755
│   ├── members/                 Discord `member` role        0750, group chb-members (gid 1001)
│   ├── stewards/                stewards / chb only          0700 — the website cannot open it
│   ├── hashes.json              content hashes of the month's raw archives (public, §4)   0644
│   └── generated/               legacy copy of the old public tree; disappears once the site reads tiers
├── YYYY/{public,members,stewards}/       yearly rollups
├── YYYY/vat.json                         that year's VAT declarations (public, §5)   0644
├── latest/{public,members,stewards}/     the current state: newest month + lifetime files
├── latest/hashes.json                    index: every completed month's hash
└── latest/vat.json                       every VAT declaration ever filed
```

**One rule:** a page picks the tier its audience is entitled to and reads the
same relative path in it. Public pages read `public/`; pages behind the
`member` role read `members/`; nothing on the website reads `stewards/`,
`providers/` or `generated/`. Never merge tiers: the members file already
contains everything the public one does.

## 2. Files and audiences

Same file names in every tier; each lower tier has strictly less.

| file | public (anyone) | members (Discord `member` role) | where |
|---|---|---|---|
| `transactions.json` | amounts, direction, category, collective, account ids, canonical tx ids, `description` only when we wrote it — no bank narration (KBC/Wise/CSV, incoming SEPA memos), no counterparty, no memo, no bank reference, no donor display names; `counterpartyId` only when it names nobody (blockchain address, our own accounts) | + counterparty names, `memo`, narration (`fullDescription`), `reference` — account numbers and BICs masked | month, `latest/` |
| `counterparties.json` | our own accounts only (entries with `slug`) | every counterparty by display name | month, `latest/` |
| `summary.json` | per-account / per-collective / per-category aggregates (identical in all tiers) | = | month (`latest/` holds the lifetime rollup) |
| `commissions.json`, `inbound_spreads.json`, `activitygrid.json` | aggregates (spreads: no counterparty) | = (+ counterparty on spreads) | month / year / `latest/` |
| `events.json` | events as published: name, times, place, host, cover, url | + ticket sales, attendance, income, notes | month, year, `latest/` (upcoming only) |
| `events.csv` | year sheet without attendance / sales / revenue / income / note columns | full sheet | year |
| `events/images/<id>.<ext>` | cover images — **only in public/**; every tier's `events.json` `coverImageLocal` points here | (read from public) | month |
| `calendars/public.ics` | room bookings feed (identical in all tiers) | = | month |
| `events.md`, `rooms.md`, `README.md` | markdown for bots and humans (identical) | = | `latest/` |
| `members.json` | `summary` only (`members: []`) | who is a member, plan, status, amount; no email hash, no Stripe urls | month, `latest/` |
| `contributors.json` | Discord display identity (id, username, displayName, avatar), token counts, message counts — no wallet address | same | month, year, `latest/` (top contributors) |
| `profiles/<username>.json` | **absent** | full (their own guild posts) | `latest/` |
| `images.json` | photo, author identity, reactions — `message` is empty | + message text | month, `latest/` |
| `door.json` | counts only (`openers`, `openDays`, `tokenOpens`, `totalOpens`) | who (identity), days, opens, via — no dates | month, `latest/` |
| `expenses.json` | every vendor bill, credit note and expense claim, **line by line** (what was bought): organisations and sole traders named; sole traders' and individuals' free text and event tags dropped (no person linked to an event); individuals typed only; payroll text dropped; account code + class | + individuals' names and texts, account names | month, year (§7) |
| `vendors.json` | one row per vendor: category, documents, total, paid, due; individuals merged per category | one row per vendor, all named | month, year (§7) |
| `customers.json` | one row per customer: income types, invoices, total, received, due; only organisations named, everyone else merged per income type | all named | month, year (§7) |
| `bookings.json` | room bookings (room, times, hours; title only for public events), room-rental invoice lines, per-room summary | + booking titles, rental descriptions, customer names | month, year (§7) |
| `pending-bills.json` | every bill still to pay — **the "help us pay" list** (§6) | same, with individuals named | `latest/` only |

Outside the tiers, because they are public and the same for everyone, each
file exists once instead of as three identical copies:

| file | what | where |
|---|---|---|
| `hashes.json` | per-provider counts + content hashes and the month hash — **meant to be published** (§4) | `YYYY/MM/`; `latest/` has the index of every completed month |
| `vat.json` | the organisation's periodic VAT declarations — **meant to be published** (§5) | `YYYY/` (that year), `latest/` (every period) |

Every page may read these, whatever tier it otherwise reads.

Treat Discord identity fields (`username`, `displayName`, `avatar`) as
optional in `public/`: whether display identity is public-by-consent is an
open community decision (audiences.md), and the public projection may drop
them later without notice.

What is **never** on the website: emails, IBANs/BICs, Stripe/Odoo/Monerium
ids, attendee lists, raw provider payloads, Odoo partner bank details, Monerium
orders, exact door-opening dates. Those exist only in `stewards/` and
`providers/`, which the container cannot open — if a page needs them, the
page belongs in steward tooling, not on the website.

## 3. How to publish

- **Read at request time, never at build time.** Every route that touches
  `/data` is dynamic (`force-dynamic`); nothing under `/data` is copied into
  the image. Cache responses briefly (a few minutes for content, ~10 s for
  empty results) — see the existing `data-route` helpers.
- **Serve files, don't proxy directories.** Anything that maps a URL onto a
  path must resolve inside a `public/` (or, for members, `members/`) tier
  directory, reject `..`, and for images only serve image extensions. The
  `/api/image-proxy` route is the reference case: it must never reach
  `stewards/`, `providers/` or `generated/`.
- **Member gating is a tier choice, not a filter.** When the session has the
  `member` role, read the same path from `members/`; otherwise from
  `public/`. There is no enrichment file to merge anymore.
- **Never write.** The website's only durable output is Nostr events
  (annotations, proposals) that `chb` picks up; caches go to the runtime
  temp dir. `tests/no-server-writes.test.ts` guards this.
- **Legacy paths are gone**: `generated/private/`, `YYYY/MM/finance/odoo`,
  `YYYY/MM/messages/discord`, `DATA_DIR/generated/profiles`,
  `DATA_DIR/contributors.json`, `process.cwd()/data`.

## 4. Integrity manifests — publish them

`YYYY/MM/hashes.json` describes a completed month without revealing it. It
sits once at the month root, not in each tier: a hash is the same for
everyone, and the file and the month directory are world-readable.

It holds three kinds of hash:

- **Raw data**: one entry per provider archive (Odoo per database
  namespace) with counts, size and a sha256 hash, and the month `hash` over
  those entries. Two `chb` instances holding the same raw data produce the
  same hashes, whatever `chb` version they run: JSON is canonicalised
  (sorted keys, fetch timestamps dropped) and the per-instance Odoo outbox
  is excluded.
- **Processed data**: `tiers.public` and `tiers.members` hash the month's
  `public/` and `members/` trees, canonicalised the same way. They match
  across instances only when the raw data, the `chb` version **and** the
  settings match (the categorisation rules include the private
  `rules.local.json`). `stewards/` is never hashed.
- **`chb`**: the version and commit of the binary that wrote the manifest.
  Compare tier hashes only between instances on the same version.

```json
{
  "month": "2026-08", "algorithm": "sha256/canonical-json-v1",
  "chb": { "version": "3.15.0", "commit": "0c1d2e3f…" },
  "providers": 7, "files": 41, "bytes": 14707712,
  "hash": "f2a14a51d689a940…",
  "entries": [
    { "provider": "stripe",  "summary": "115 transactions, 70 charges, 9 customers, 3 products",
      "stats": { "transactions": 115, "charges": 70, "customers": 9, "products": 3 },
      "files": 4, "bytes": 812345, "hash": "597d674a44c782a7…" },
    { "provider": "discord", "summary": "8 channels, 766 messages, 4 attachments", "…": "…" },
    { "provider": "odoo/commonshub", "summary": "8 journals, 6,936 lines, 13 invoices, 7 bills, 4,246 partners", "…": "…" }
  ],
  "tiers": {
    "public":  { "files": 10, "bytes": 263168, "hash": "66782f20a3e41700…" },
    "members": { "files": 10, "bytes": 282624, "hash": "6391c3825bacb48d…" }
  }
}
```

`latest/hashes.json` is the index: `chb`, one row per completed month
(`month`, `providers`, `files`, `bytes`, `hash`, `tiers` as tier → hash),
and a top-level `hash` over every month's raw-data hash. That single value
answers "do we hold the same dataset?". Any month changing changes it.

Suggested rendering (one line per month, expandable):

```
2026-08   7 providers   14,363 KB   f2a14a51…
   stripe            115 transactions, 70 charges, 9 customers, 3 products   597d674a…
   discord           8 channels, 766 messages, 4 attachments                110cdbd3…
   odoo/commonshub   8 journals, 6,936 lines, 13 invoices, 7 bills           7ec74325…
   public/           10 files, 257 KB                                       66782f20…
   members/          10 files, 276 KB                                       6391c382…
```

Another instance checks itself with `chb integrity 2026/08 --json` and
compares hashes provider by provider; only compare the provider entries both
instances track (a mirror of an extra Odoo test database adds an entry and
changes the month hash, but not the production entry). Manifests are
written by `chb generate` for every completed month that has none yet or
whose providers or tier files changed since, and by `chb integrity
[--force]`. A manifest is rewritten only when a hash or the `chb` version
changes, so its `generatedAt` is when the data last changed.

**Why the months are not chained.** Past months are not frozen: a bill paid
today rewrites the Odoo archive of the month it was issued, a late import
(a VAT declaration, a bank statement) or a new provider adds files to old
months. With each month's hash including the previous one, any such change
would change every later month and the chain would say nothing useful. The
index `hash` gives the one-value comparison without claiming immutability.
To prove *when* the data looked like this, publish the index `hash`
somewhere append-only and timestamped (for example a signed Nostr event
per day) rather than chaining the months themselves.

## 5. VAT declarations — publish them

`latest/vat.json` lists every quarterly VAT return filed with the Belgian
State through Intervat: per period, the amount of every grid of the official
form, the control totals (output VAT, input VAT, net paid or refunded), and
every filing including corrections. `YYYY/vat.json` holds one year's.
Declarations are imported by hand after each quarter's filing, so a missing
recent quarter means "not imported yet". Schema, grid meanings and suggested
views: [vat.md](vat.md).

## 6. Pending bills — the "help us pay" list

`latest/public/pending-bills.json` lists every vendor bill the Hub still
has to pay, with totals, so a page can publish what is owed and invite
people to cover a bill. Covering a bill goes through the Hub's own payment
flow with the bill `number` as reference, never to the vendor directly.
The list is only as accurate as Odoo's reconciliation: a bill paid by
direct debit stays pending until its bank line is reconciled. Schema, tier
differences and page guidance: [bills.md](bills.md).

## 7. Expenses, vendors, customers, bookings — publish them

Four files per month (`YYYY/MM/<tier>/`) and per year (`YYYY/<tier>/`),
never in `latest/`. Together they show who the Hub pays and for what, line
by line; who pays the Hub; and how the rooms are used and what renting
them brings in. In `public/`, organisations (and VAT-registered sole
traders, as vendors) are named; private individuals appear only by type,
merged into one row per category. Schemas, the naming rules and page
recipes: [accounting-data.md](accounting-data.md).

The month `bills.json` of v3.14 is gone: read `expenses.json`.

## 8. Checklist for a new page

1. Which audience? → which tier root. If the answer is "stewards", stop: not a website page.
2. Does the file exist in that tier for that scope (month / year / latest)? See the table.
3. Is every field you render allowed in that tier? If a field is missing in `public/`, that is the answer, not a reason to read a higher tier.
4. Route is `force-dynamic`, reads through the data-path helper, never writes.
5. Add the path to the guard tests (no `generated`, `private`, `stewards`, `providers` in any data path).
