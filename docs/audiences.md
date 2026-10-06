# Audiences — three levels of trust for processed data

Consumer reference (what to read, what to publish, for whom): [website.md](website.md).

`chb` produces data for three audiences. Each is a directory; each directory
is a complete, self-contained dataset for that audience, with the same file
names as the others but less in them. A consumer is handed one directory and
nothing else.

| tier | who | unix mode | may contain |
|---|---|---|---|
| `public/` | anyone — the website's public pages, bots answering strangers, OG images | `0755` / files `0644` | aggregates, events, canonical ids, categories, collectives, amounts. **No person**: no names, no free-text narration, no bank references, no contact data |
| `members/` | people with the Discord `member` role — member-only pages, a members-only assistant | `0750` / files `0640`, unix group `chb-members` | + names of people and organisations, memos and bank narration, who did what (contributions, door openings by day count) — **no way to contact or pay anyone**: no email, no phone, no IBAN/BIC, no Stripe customer ids, no Odoo partner/bank details, no Monerium orders |
| `stewards/` | the stewards (admins) and `chb` itself | `0700` / files `0600` | everything, including emails, IBANs, provider ids, exact presence dates |

Raw provider archives (`YYYY/MM/providers/`), processor intermediates and the
Odoo/Nostr outboxes are **stewards-only by construction** (mode `0700`) and are
never a served surface.

```
$DATA_DIR/
├── YYYY/MM/
│   ├── providers/<provider>/…   raw archives            (stewards, 0700, never served)
│   ├── processors/<name>/…      cross-provider intermediates (stewards)
│   ├── public/                  ← mount this for the website's public pages
│   ├── members/                 ← + this for member-only surfaces
│   ├── stewards/                ← chb, admin tooling
│   └── generated/               legacy (= today's public + private/), being phased out
├── YYYY/{public,members,stewards}/     yearly rollups
└── latest/{public,members,stewards}/   most recent month, mirrored (as latest/generated today)
```

## Why directories, not fields

Two things make a tier trustworthy, and both are checked by the machine, not
by reviewers:

1. **At write time** — `writeAudienceFile` runs `enforceAudiencePolicy` before
   any byte lands. `public/` refuses (does not write) any document containing an
   email or a checksum-valid IBAN and scrubs name fields; `members/` refuses
   emails and IBANs; `stewards/` is passthrough. A violation is an error the
   step reports, never a warning that scrolls by.
2. **On disk** — the tier directory's mode is the guarantee the consumer's
   process cannot exceed: the website container (uid 1001) is not in the
   `chb-members` group unless it serves member pages; a members-only bot gets a
   bind mount of `members/` and nothing else; `stewards/` is readable by the
   `chb` user only. `normalizeDataDir` re-applies the modes on every run.

The existing binary rule ("a path with a literal `private` segment is 0700 and
exempt from the guard; everything else is public and only scanned for `@`")
stays for the legacy tree until it is removed; it is subsumed by the tiers.

## Identifiers below stewards

Since v3.18 every Odoo document is identified, everywhere, by its URI
`odoo:<host>:<db>:<model>:<id>` (for example
`odoo:citizen-spring-vzw.odoo.com:citizen-spring-vzw:account.move:1234`;
`hr.expense` for expense claims). It names a document, not a person, so it
is **allowed in every tier**: public and members files carry it as `uri`
(expenses, pending bills, room rentals) and as `invoices[]` (customer rows,
including the anonymous merged ones). It is the key Nostr annotations and
the website use.

Never below stewards, refused by the write-time policy:

- contact data: emails (masked in free text), phone numbers, postal addresses;
- bank details: third-party IBANs (compact or spaced), BICs;
- Stripe customer ids (`cus_…`): they follow one buyer across purchases;
- Belgian national register numbers.

Not published either, by projection: Odoo partner ids and links, Monerium
orders, exact door-opening dates, attendee lists, raw provider payloads.
Partners (vendors, customers) keep their `p-…` ids; they are not annotated.

## Per-artifact classification

What each output becomes in each tier. "—" means the artifact does not exist
in that tier; "= members" means the same projection as the tier below.

| artifact | public | members | stewards |
|---|---|---|---|
| `transactions.json` | amounts, categories, collectives, canonical ids, plain `description`; `counterpartyId` only for blockchain addresses and our own accounts (bank ids embed the payer's name, Stripe customer ids follow a buyer); no counterparty, no `memo`/`fullDescription`/`reference`/`balance`/`custom_*`/`name` | + counterparty names, memo, bank narration, reference | + IBAN, email, BIC, Stripe customer/charge ids, tx hashes (replaces `generated/private/enrichment.json`) |
| `door.json` | counts only: openers, open days, token opens, total opens | who (id, username, name, avatar), days, opens, via | + exact dates |
| `expenses.json`, `vendors.json`, `customers.json`, `bookings.json` | organisations named; vendors that are VAT-registered sole traders named (products only, no free text, no event tag); individuals typed only and merged per category; no free text about individuals, no natural person linked to an event, no payroll text, account classes only ([accounting-data.md](accounting-data.md)) | + every name, texts, account names, booking titles | + contact details, bank accounts, payments, attachments, invoice lists, Odoo links |
| `annual-accounts.json` | filed fiscal years only: key figures, figures by NBB code, checks; the filed abbreviated balance sheet and profit and loss (`YYYY/public/annual-accounts/*.pdf`) | = public | + draft fiscal years, trial balances and internal balance sheets (account level: natural persons' current accounts, salary lines), with archive paths ([annual-accounts.md](annual-accounts.md)) |
| `contributions.json` | #contributions feed: who posted, who was mentioned (Discord id + display name), when, reactions, public photo paths; no text; only if the channel is public | = | = |
| `tokens-issued.json` | contribution-token mints: time, amount, recipient Discord identity, reason/kind from the trusted token bot, Discord link; no tx id, no wallet | = public | + transaction URI, wallet |
| `categories.json` | the category taxonomy (no personal data) | = | = |
| `accounts-chart.json`, `ledger-balances.json` | every account and its yearly balances; accounts named after a person (current accounts, a person's fees) get a neutral label and are merged per two-digit group; payroll (62) one row; account numbers in labels masked ([accounting-data.md](accounting-data.md#chart-of-accounts-and-ledger-balances)) | = public | + one row per account, real labels |
| `pending-bills.json` | amounts, dates, status, category; companies by name, VAT number, invoice number and line descriptions; VAT-registered sole traders by name, VAT number and products (no free text, no event tag); private individuals anonymous | + individuals' names, references and line descriptions | + vendor contact details, bank account, payments, attachments, Odoo links ([bills.md](bills.md)) |
| `events.json` | today's `LatestEvent` projection for every month: no `guests`, `lumaData`, `metadata.host`, `ticketSales` | + host, ticket sales, attendance, income metadata | + guest lists, raw `lumaData` |
| `events.csv` (yearly) | without Host | = full | = full |
| `calendars/public.ics` | booking titles only if they carry no person (room + activity) | = today's file | = today's file |
| `events.md`, `rooms.md` | as today (Elinor reads these over HTTP) | = | = |
| `summary.json`, `commissions.json`, `activitygrid.json`, `inbound_spreads.json` | aggregates as today, minus `counterparty` on spreads | + counterparty on spreads | = |
| `counterparties.json` | organisations only (entries with a `slug` or a company name); individuals dropped | + individuals' display names | + everything (pictures, Nostr author) |
| `contributors.json` (month, year, top) | see **Open decision 1** — either Discord identity (id, username, avatar) with token counts, or counts only | Discord identity, tokens in/out, message counts, wallet address? (**decision 2**) | = + wallet ↔ Discord map (`cache/discord-wallets.json`) |
| `members.json` | counts (active incl. grace, grace, lapsed, plans, MRR) + organisations in good standing by name | + members in good standing by first name, plan, status, graceEndsAt | + lapsed members, email hash, Odoo partner ids, subscription links, Odoo/Stripe mismatches |
| `profiles/<username>.json` | — | full (messages the person posted in the guild's public channels) | = |
| `images.json` | photos from **public channels only** (settings.json `discord.publicChannels`), author display identity (**decision 1**), no message text; `filePath` → `YYYY/MM/public/images/<id>.<ext>` | every photo + message text; `filePath` → the public copy, empty for non-public channels | `filePath` → `providers/` |
| Odoo `invoices.json` / `bills.json` | today's public projection (ids, amounts, VAT, lines, category, collective) | + partner display name, reference | + partner bank, payments, attachments, Odoo urls (today's `private/`) |
| Monerium `private/monerium/<addr>.json` | — | — | full |
| `providers/odoo/pending/`, Nostr outbox | — | — | full |

### Open decisions (need the community's answer, not code)

1. **Is a Discord identity public?** Today the website lists members and
   contributors (id, username, avatar) without login. Strictly that is
   personal data; pragmatically people published it themselves by joining the
   guild. Recommendation: treat *display identity* (username, display name,
   avatar) of members as public-by-consent, with an opt-out role (`private`)
   honoured by generate; everything beyond it (real name, amounts, presence,
   wallet) stops at members.
2. **Wallet ↔ Discord mapping** (`contributors.json.address`,
   `cache/discord-wallets.json`) de-anonymises on-chain history. Recommendation:
   stewards only; members see token counts without addresses.
3. **Per-person amounts** (membership fees in `members.json`, tokens in
   `contributors.json`): members tier, public shows totals. This is the Open
   Collective norm the community already follows for expenses.

## Consumers, and what they should mount

| consumer | today | target |
|---|---|---|
| website public pages (`/`, `/events`, `/community`, `/finance`, `/contributions`, `.md` endpoints) | `generated/` (public tree) | `public/` |
| website member pages (`/[year]/transactions` enrichments, quarterly reports with partner names) | `generated/private/enrichment.json`, `providers/odoo/…/private/` | `members/` (the container joins group `chb-members`) |
| website admin pages (Monerium orders per account) | `private/monerium/` gated by the Discord admin bit | **move out of the website** into `chb serve` / steward tooling, so the web container never mounts `stewards/` |
| Elinor (OpenClaw bot) | HTTP `events.md`, `rooms.md`, `about.md` | unchanged; a members-only Elinor would get a `members/` mount |
| `chb` itself, `push`, reports | `providers/`, `generated/` | `providers/`, `stewards/` |

**Found during this survey — fix independently of the refactor:** the
website's `/api/image-proxy?url=/data/<rel>` serves *any* file under
`DATA_DIR` unauthenticated, including `generated/private/enrichment.json`
(names, IBANs, emails). With tiers the container simply would not have the
file; until then the route needs an allowlist (`generated/` only, image
extensions only, no `private` segment).

## How `chb` works with the tiers

- **`stewards/` is chb's own working tree.** Every command that used to read
  or write `generated/` now uses `stewards/` (`stewardsDirName` in code):
  `generate`, `report`, `stats`, `doctor`, `transactions`, `accounts …
  push`, `odoo …`, `nostr publish`, `rules`, `events`, `members`, `tokens`,
  `calendars`, the Luma/Stripe processor, ticket-sales enrichment. The PII
  enrichment layer is no longer a separate file: `stewards/transactions.json`
  carries counterparty, email and IBAN on the entry itself.
  `LoadTransactionsWithPII` still merges a `stewards/private/enrichment.json`
  when one exists (seeded from a pre-tier tree, see below).
- **Every writer emits all three tiers** through `writeTiers` /
  `writeTiersSame` (`cmd/audiences.go`) with the projections in
  `cmd/audience_projections.go`. `writeTiers` also mirrors to
  `latest/<tier>/`, as `writeMonthFile` did for `generated/`.
- **Binary assets live in the lowest tier they belong to** and are referenced
  from every tier: event cover images are written to
  `public/events/images/` only and `coverImageLocal` points there in all
  three `events.json`. Discord image attachments stay in the provider archive.
- **Caches** (`cache/discord-wallets.json`, `cache/event-og-images.json`)
  live under `latest/stewards/cache/`.
- **Seeding from a pre-tier tree.** On every run the data-dir normaliser runs
  `migrateGeneratedToStewards`: for each month/year/`latest`, every file
  under `generated/` that `stewards/` lacks is copied over (including
  `generated/private/`), then **`generated/private/` is deleted** — that
  subtree was the one thing in the legacy tree a public consumer must never
  reach. Months regenerated since the split are untouched. So an upgraded
  `chb` works on old months immediately; `members/` and `public/` for those
  months appear on the next `generate` of that month (`chb generate
  --history --force` once to fill them all — the sources-unchanged skip
  otherwise leaves old months alone).
- **Legacy `generated/` keeps being written**, with the same content as
  before the split (transactions: the old public projection), for consumers
  that have not moved yet — the website. It is a courtesy, not a tier:
  nothing checks it. Set `CHB_LEGACY_GENERATED=0` once the website reads
  `public/` + `members/`; the tree can then be deleted.

## Migration status

1. ✅ Mechanism, tier modes, write-time policy.
2. ✅ Every writer converted: `transactions`, `counterparties`, `members`,
   `contributors` (month, year, top), `profiles` (never public), `images`,
   `events` (month, year, latest, csv), `calendars/public.ics`,
   `events.md`, `rooms.md`, `door`, `summary` (month + lifetime rollup),
   `commissions`, `inbound_spreads`, `activitygrid`, `README.md`. Every chb
   reader points at `stewards/`. Pre-tier trees are seeded automatically and
   lose `generated/private/`.
3. ⬜ Website: read `public/` and `members/` — see
   [website-migration.md](website-migration.md). Move the Monerium admin
   views out of the web container.
4. ⬜ `CHB_LEGACY_GENERATED=0` on prod, delete `generated/`.
5. ⬜ Prod: create group `chb-members` (gid 1001 — the gid of the `nodejs`
   group inside the website image), `usermod -aG chb-members chb`, and run
   chb with `CHB_MEMBERS_GROUP=chb-members` so every `members/` directory
   and file is chgrp'ed to it (`applyMembersGroup`). The website container
   gets the gid with `--group-add 1001` (Coolify custom docker options)
   until its Dockerfile adds `nextjs` to `nodejs`.
