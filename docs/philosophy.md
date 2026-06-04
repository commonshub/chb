# Architecture Philosophy

> See also [operating-principles.md](operating-principles.md) for the three
> outward-facing rules this architecture enforces: offline-first (sync before
> use), writes go to an outbox, and preview-then-confirm before committing.

CHB has two complementary phases. Keep them separate.

## `sync` — download raw provider data

`sync` fetches data from external providers (Luma, Stripe, Etherscan, Discord,
Odoo, Google Calendar, …) and archives it **unchanged** under
`DATA_DIR/YYYY/MM/providers/<provider>/`.

Rules:

- No transformation, no enrichment, no normalisation.
- No timezone conversion. Keep timestamps in whatever form the provider sent.
- No re-serialisation that loses information (e.g. don't decode and re-encode
  JSON if it loses ordering or strips fields).
- Idempotent: running `sync` twice on the same period should produce identical
  files. Re-running `sync` must never change generated outputs — only re-fetched
  raw provider data can change them.
- Failures here only block fetching; they never corrupt generated state.

If a sync command currently emits derived artifacts (e.g. `generated/events.json`
or `latest/generated/events.md`), that is a layering violation to be migrated
into `generate`, not extended.

## `generate` — transform provider archives into standardised outputs

`generate` reads `providers/` archives and produces every file under `generated/`
and `latest/generated/`. This is where all normalisation happens.

Rules:

- **All times are normalised to `Europe/Brussels`** in human-facing outputs
  (Markdown, CSV, summary JSON). RFC3339 timestamps in machine-facing JSON may
  carry an explicit `+01:00` / `+02:00` offset; never write naïve local times
  and never write UTC when the user-facing semantics are Brussels.
- All-day events (ICS `VALUE=DATE`) are flagged and rendered without a clock
  time — never as "00:00" or "02:00".
- All currency, locale, and date formatting is consistent across outputs.
- Deterministic: given the same `providers/` input, `generate` produces
  byte-identical output. No timestamps-of-now in payload bodies, only in
  explicit "last updated" fields.
- Safe to re-run. Re-running `generate` after a `sync` must not require
  manual cleanup.

## File layout: one sync file, one generate file per provider

For every provider under `cmd/`, keep sync and generate code in **separate
files**. The naming convention is:

- `<provider>_sync.go` — fetch raw data, archive under `providers/<provider>/`.
- `<provider>_generate.go` — read archives, produce everything under
  `generated/`.

Example: `cmd/events_sync.go` only fetches ICS feeds and writes per-month
bookings to `providers/ics/`. `cmd/events_generate.go` owns every derived
artifact (`events.json`, `public.ics`, yearly aggregates, `events.csv`,
`events.md`, `rooms.md`).

When the same orchestrator command needs both phases (e.g. `chb calendars
sync` fetches then immediately generates), the sync file's orchestrator calls
into the generate file via a single hand-off function (e.g.
`generateCalendarsForMonths(...)`). Generate code never imports HTTP clients
or provider SDKs. Sync code never writes to `generated/`.

Provider packages and monthly data archives both live under `providers/<provider>/`
within their respective roots.

This file split:

- Makes the philosophy reviewable at a glance (a sync file with a
  `writeDataFile("…/generated/…")` is an obvious smell).
- Keeps the OG/enrichment/markdown code reachable when we later expose a
  `chb generate` entry point that runs without re-fetching.
- Lets provider contributors learn one provider at a time.

## Where to put new logic

| You want to… | Put it in |
|---|---|
| Call a provider API and store the response | `providers/<provider>/` and `cmd/<provider>_sync.go` |
| Parse a raw provider format (ICS, CSV, …) | parser package (e.g. `ical/`) — called from `<provider>_generate.go`, not the sync file |
| Normalise a time, currency, or label | `cmd/<provider>_generate.go`, near where the output is written |
| Render Markdown / CSV / summary JSON | `cmd/<provider>_generate.go` |
| Enrich an existing generated record with cross-provider data | a processor (see [processors.md](processors.md)) |

## Why this matters

When a user reports "the times look wrong in `events.md`", the fix must be in
`generate` (or in the parser used by `generate`). Touching the `sync` layer to
fix presentation bugs makes the archives stop matching the upstream source and
breaks the "re-sync is a no-op for generated outputs" invariant.

## "Account" means bank/payment account, never feed

The word *account* is reserved for sources that produce a `TransactionEntry`:
KBC (IBAN), Stripe, Etherscan-backed wallets, Monerium. These live in
`$APP_DATA_DIR/settings/accounts.json` and are what `chb accounts` lists.

ICS calendar feeds produce `FullEvent` / booking records, not transactions.
They live in `$APP_DATA_DIR/settings/calendars.json` and are operated by
`chb calendars …`. Don't conflate the two — a calendar feed is not an
account, and putting it under `accounts.json` would break that contract.

Both kinds are *providers* (a generic umbrella for "external source we
pull from"), which is why `chb providers …` covers both.

## Sources vs targets

Providers split into two roles:

- **Sources** (pull-only): Stripe, KBC, Etherscan, Monerium, ICS, Discord.
  We read their state into `providers/<source>/`; we never push back.
- **Targets** (pull + push, with pending changes): Odoo, Nostr. We pull
  current state into `providers/<target>/`, accumulate changes to push as
  `providers/<target>/pending/<YYYY-MM>.json`, then `chb <target> push`
  publishes them and clears the pending entries.

Sources are read-only mirrors. Targets are reconciled — we own a piece of
their state, and pending tells you exactly what would change on the next push.

## Mirror mode (thin clients)

Setting `CHB_SYNC_SOURCE=user@host:/abs/.chb` (in
`$APP_DATA_DIR/settings/config.env`) flips a chb instance into thin-client
mode. Instead of running every provider locally — which requires API keys
per teammate — the binary rsyncs the trusted host's already-pulled and
already-generated state into its own `$APP_DATA_DIR`.

In mirror mode:

- `chb pull` rsyncs `data/`, the nostr `outbox/` + `sent/`, and `settings/`
  from the trusted host. Provider calls are skipped.
- `chb generate` is a no-op (the trusted host already generated).
- `chb push` only flushes the local Nostr outbox; Odoo writes refuse with
  a "run on the trusted host" hint.

The "sync vs generate" hard rule above is unaffected: mirror mode SKIPS
both phases locally, but the invariant still holds on the trusted host,
which is where the real pipeline runs. See [mirror-mode.md](mirror-mode.md)
for the full architecture and decision tree per directory.

## Command verbs: pull / generate / push

Each command has exactly one data direction. Don't overload "sync" to mean
both pull and push — that's the bug we already removed.

- **`pull`** (alias: `sync`, deprecated) — network read, remote → local. Never
  writes to remote systems. Examples: `chb pull`, `chb providers stripe pull`,
  `chb odoo pull`.
- **`generate`** — local-only transform, `providers/` → `generated/`. Never hits
  the network. Resolves rules.json + odoo_mapping.json so downstream pushes
  trust the local files.
- **`push`** (alias: `sync`, deprecated) — network write, local → remote.
  Examples: `chb odoo journals push`, `chb odoo journals 28 push`.

Editing rules → `chb generate` → `chb odoo journals push`. Each step is
independently inspectable.

## Two stages of resolution: rules → mapping → pending

The two settings files do different jobs:

- **`rules.json` — semantic rules.** Patterns over description / IBAN /
  amount / counterparty resolve to `category` + `collective`. This is the
  vendor-agnostic step; the same rule means the same thing whether the
  next system is Odoo, Wave, or a CSV export.
- **`odoo_mapping.json` — Odoo-specific lookup.** Maps a semantic tag
  (category / collective + direction) to an Odoo `account_code` +
  `partner_id`. This is the only file that knows about Odoo identifiers.

Both apply during `generate`. The resolved Odoo values are written to
`providers/odoo/pending/<YYYY-MM>.json` (one entry per tx URI), keeping
`transactions.json` target-agnostic. Push paths (Stripe / blockchain / KBC
merge) read pending files directly — they never call `LookupOdooMapping`
themselves.

This means:

- `transactions.json` is the canonical vendor-agnostic record.
- `providers/odoo/pending/` is the canonical "what Odoo should look like
  after the next push" record.
- Changing a rule or mapping → re-run `generate` → next push applies the
  new resolution. No silent post-sync re-apply pass.
- To force re-apply onto Odoo lines that already exist (e.g. you fixed a
  rule and want existing lines updated): `chb odoo journals <id> categorize`.

## CLI / UX conventions

These conventions are load-bearing for the operator experience. Don't break
them silently — a one-line `chb pull` that suddenly shows three pages of
output makes daily ops painful.

### Help is free and immediate

`--help` / `-h` / `help` short-circuits **before any I/O** — no Odoo auth, no
provider fetch, no file scan. Output:

- One-line summary of what the command does
- USAGE block with the exact invocation shape
- The subcommands and flags accepted (with one-liner descriptions)
- An EXAMPLES block with copy-pasteable common invocations

Every command (top-level, subcommands, provider commands) must obey this. If
you're tempted to short-circuit for `-h` only in *some* paths, that's a bug.

### Default output is compact; `--verbose` adds detail

By default a command produces the minimum useful output:

- One-line per step in multi-step commands (e.g. `chb pull` prints one line
  per provider: `✓ Stripe:   1.2s`).
- Tables for repeated rows (see below).
- A summary at the end with totals.

`--verbose` (alias `-v` or `--debug`) shows the detailed per-action output
that compact mode swallows. Sub-syncs propagate `--verbose` to their own
helpers.

### Always show what's happening

Silence during a long step is the worst-of-both: the operator can't tell if
the process is alive, hung, or about to finish. The compact mode is allowed to
*shorten* output, never to *hide* the current step. The contract for every
long-running step is:

1. **Print the label immediately when the step starts** — before any I/O,
   not after completion. The first thing the operator sees is `  ⟳ Stripe…`.
2. **Emit progress while running** — either a spinner if the step is opaque,
   or an in-place subtask line that gets rewritten as it advances:
   `  ⟳ Stripe: fetching balance txs (page 3/12)`. Use a single rewritable
   line (carriage return), don't scroll.
3. **On completion, rewrite the line with the result** — counts of new and/or
   updated items, then elapsed time:
   `  ✓ Stripe: 12 new, 3 updated     1.2s`. On error:
   `  ✗ Stripe: error: <one-line msg>  1.2s`.

If a step has nothing more granular to report, the label + spinner is enough.
But never go more than a second with no terminal output during a step that
might take 30s. A `chb pull` that hangs silently on Stripe for 40s is a bug
even if it eventually prints `✓ Stripe: 41s`.

### Consistent flag vocabulary

- **`--verbose` / `-v`** — more output
- **`--dry-run`** — read-only preview; no writes to local files (where
  meaningful) and never writes to remote systems
- **`--yes` / `-y`** — skip confirmation prompts
- **`--json`** — machine-readable JSON output (mutually exclusive with
  human-formatted output; redirect stdout)
- **`--force`** — re-do work that would otherwise be skipped by an
  incremental cursor or month-boundary check
- **`--since <date>` / `--until <date>`** — narrow the time window
- **`--help` / `-h` / `help`** — print help and return immediately

If a command needs a new flag that overlaps with one of these, prefer
reusing the existing name even if the semantics are slightly stretched —
operators don't want to memorise twelve verbs for "show me more."

### Help text on misuse

When required arguments are missing or wrong, surface a useful error with at
least one concrete example:

```
$ chb odoo journals foo push
Error: 'foo' is not a journal id or linked account slug.

Example:
  chb odoo journals 28 push
  chb odoo journals kbc push --dry-run
```

Do NOT print full help on every misuse — that's noise. Print the one-line
error, one or two examples, and `(run with --help for all options)`.

### Hint the next step

When a command finishes and there's a likely follow-up, dim-print a hint at
the end. Examples:

- After `chb pull`: `↪ To push to Odoo: chb odoo journals push`
- After `chb odoo journals 28 push --dry-run`: `↪ Re-run without --dry-run to apply.`
- After `chb odoo sync` (fetch-only): mention `chb generate` then
  `chb odoo journals push`.

This turns a sequence of independent commands into a guided pipeline without
hardcoding it. Keep hints short, dim, and skippable.

### Tables for repeated rows

When you print more than ~3 rows with the same shape, render a table (or at
least aligned columns). Headers in bold/dim, totals row separated by `─`.
The merge/categorize/sync previews are the reference implementations
(`renderTicketsTable` in `cmd/events_tickets.go`).

Don't print 50 rows of free text and call it a "preview." The operator's job
is to scan; help them.

### Deprecation: alias, don't break

When renaming a command, keep the old name working as an alias for at least
one release. Print a one-line dim notice pointing to the new name. We did
this for `sync` → `pull` / `push`. Drop the alias only after the next major.
