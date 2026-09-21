# Website: reading the audience tiers

Hand this to whoever adapts `commonshub.brussels` (the Next.js site). It is
self-contained; the design is in [audiences.md](audiences.md).

## What changed in the dataset

`$DATA_DIR` (bind-mounted as `/data` in the container) now has, per month,
three processed trees instead of one:

```
/data/YYYY/MM/public/     0755  ← the website's public pages read HERE
/data/YYYY/MM/members/    0750  ← pages behind the Discord `member` role read HERE
/data/YYYY/MM/stewards/   0700  ← the container cannot read this. Ever.
/data/YYYY/MM/generated/  legacy copy, disappears once the site is migrated
/data/YYYY/{public,members,stewards}/     yearly rollups (contributors, events, events.csv, activitygrid)
/data/latest/{public,members,stewards}/   most recent month + lifetime files (events, contributors, summary, profiles, README)
```

**Same file names in every tier, strictly less in each lower one.** A page
never merges tiers: it picks the one tier root its audience is entitled to
and reads the same relative path it reads today. That is the whole point —
a public page cannot accidentally read PII, because the file it opens does
not contain any, and the directory that does is not readable by the process.

There is **no `private/` anymore**. `generated/private/enrichment.json`
(names, emails, IBANs) is gone; the equivalent data exists only in
`stewards/transactions.json`, which the container cannot open.

## What each tier contains (relevant to the site)

| file | public | members |
|---|---|---|
| `transactions.json` | amounts, categories, collectives, canonical ids, plain `description`; **no counterparty name, no memo/narration, no bank reference, no donor display names** | + counterparty names, `memo`, `fullDescription`, `reference` (what the member-only enrichment used to add — now inline, no second file) |
| `counterparties.json` | our own accounts only (entries with a `slug`) | every counterparty by name |
| `members.json` | `summary` only, `members: []` | members with `firstName`, `plan`, `status`, `amount`, `accounts.discord`; no `emailHash`, no Stripe urls |
| `contributors.json` (month / year / latest) | Discord identity (id, username, displayName, avatar), token counts, message counts; **no wallet `address`** | same (wallet stays with stewards) |
| `profiles/<username>.json` | **absent** | full |
| `images.json` | photo, author identity, reactions; **`message` is empty** | + message text |
| `events.json` (month / year) | event as published: name, times, place, host, cover, url; no guests, no raw Luma payload, no ticket sales, no attendance/income/note | + `ticketSales`, `metadata.attendance/*Income/ticketRevenue/note` |
| `events.csv` (year) | without Attendance / Tickets Sold / Ticket Revenue / Fridge Income / Rental Income / Note | full |
| `door.json` | counts only (`openers`, `openDays`, `tokenOpens`, `totalOpens`) | who (id, username, name, avatar), `days`, `opens`, `via`; no dates |
| `summary.json`, `commissions.json`, `inbound_spreads.json`, `activitygrid.json`, `calendars/public.ics`, `events.md`, `rooms.md`, `README.md` | identical in every tier (aggregates / already public) | identical |
| `events/images/<id>.<ext>` | **lives only here**; every tier's `events.json` `coverImageLocal` points to `YYYY/MM/public/events/images/…` | (read from public) |

Open decisions that may still change the public column: whether a Discord
display identity is public-by-consent (today it is on the site without
login). If the community decides otherwise, `contributors.json`,
`images.json` authors and `door.json` drop identity in `public/` — the site
should therefore treat those fields as optional.

## Concrete changes in the website

1. **`src/lib/data-paths.ts`** — add tier roots. Suggested shape:
   ```ts
   export type Tier = "public" | "members";
   export function tierDir(tier: Tier, year?: string, month?: string) {
     const base = year ? (month ? join(DATA_DIR, year, month) : join(DATA_DIR, year)) : join(DATA_DIR, "latest");
     return join(base, tier);
   }
   ```
   Every reader that builds `…/generated/<file>` builds `tierDir(tier, …)/<file>`
   instead. `latest/generated/…` → `latest/<tier>/…`, `YYYY/generated/…` → `YYYY/<tier>/…`.
2. **Tier by surface, not by file.** Public routes and pages use `"public"`
   unconditionally. The pages that today gate on `isMember` /
   `readMonthlyEnrichments()` (`/[year]/transactions`, `/[year]/[month]/transactions`,
   the quarterly reports with partner names) use `"members"` when the session
   has the role, `"public"` otherwise — and drop the enrichment merge: the
   members file already has the names inline.
3. **Delete the reads that no longer exist:** `generated/private/enrichment.json`
   (`src/lib/transactions.ts:156-171`), `providers/odoo/…/private/{invoices,bills}.json`
   (`odoo-quarter.ts`, `contribute-expenses.ts`) and `private/monerium/…`
   (`[year]/[month]/finance/…`). Odoo partner names for members will be
   provided as a members-tier projection of invoices/bills in a follow-up on
   the chb side (`members/invoices.json`, `members/bills.json`); until then
   those pages show the public projection. The Monerium admin views move out
   of the website (steward tooling) — the container must not need `stewards/`.
4. **Legacy layouts** still referenced (`process.cwd()/data`, `DATA_DIR/generated/profiles`,
   `YYYY/MM/finance/odoo`, `YYYY/MM/messages/discord`, `DATA_DIR/contributors.json`)
   should go at the same time; they have no equivalent in the tiers.
5. **`/api/image-proxy`** (`src/app/api/image-proxy/route.ts`): only serve
   local files under a `public/` tier directory, only image extensions, and
   fix the containment check to compare against `root + path.sep`. This is
   the route that currently serves `generated/private/enrichment.json` to
   anyone. Do this first.
6. **`/data/*` browser and `/mcp`**: point at the tier the caller is
   entitled to (`public` for Basic-auth-less requests, `members` for the
   MCP key if that is the intent), remove the `private` special-casing.
7. **Container / Coolify**: `members/` is group `chb-members` = **gid 1001**
   on the host (chb chgrps it, `CHB_MEMBERS_GROUP=chb-members`). The image
   already has a `nodejs` group with gid 1001 but `nextjs` is not in it:
   change the Dockerfile to `adduser … -G nodejs nextjs` (primary or
   supplementary gid 1001). Until that ships, Coolify's custom docker run
   options carry `--group-add 1001` for the app. Nothing else changes — the
   bind mount stays the whole `/data/commonshub/prod`, and `stewards/`
   (0700, owned by `chb`) is simply unreadable. Keep the existing "never
   chown /data" entrypoint behaviour.
8. **Tests to add** (mirroring `tests/no-server-writes.test.ts`): a guard that
   no source file joins a data path with `generated`, `private`, `stewards`
   or `providers`; and that `image-proxy` refuses `..`, `stewards`, `members`
   (unless the session is a member) and non-image extensions.

## Sequencing

1. Website PR: image-proxy fix (can ship alone, today).
2. Website PR: tier roots + delete private reads + legacy layouts.
3. Prod: `usermod -aG chb-members <website uid>`; deploy; verify member pages.
4. chb side: `CHB_LEGACY_GENERATED=0` in the cron line, then `rm -rf` every
   `generated/` — after that the site has no fallback, which is the goal.

## Quick check from inside the container

```
ls /data/latest/public/           # works
ls /data/latest/members/          # works only if uid 1001 ∈ chb-members
ls /data/latest/stewards/         # Permission denied — expected
cat /data/latest/public/transactions.json | grep -c '"iban"'   # 0
```
