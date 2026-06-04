# CLAUDE.md — guidance for Claude working in this repo

Read [docs/philosophy.md](docs/philosophy.md) before doing any non-trivial work
on `sync`, `generate`, or anything that emits files under `providers/` or
`generated/`.

## Operating principles (always apply)

See [docs/operating-principles.md](docs/operating-principles.md). In short:

1. **Offline-first.** Work on the local mirror under `$DATA_DIR`; never fetch
   inline. If data is missing/stale, stop and run the relevant `pull`/`sync`
   first, then re-run. `generate` is local-only.
2. **Writes go to an outbox**, never straight to the remote — Odoo →
   `providers/odoo/pending/<YYYY-MM>.json`, Nostr → `outbox/`. `push` is the
   only verb that writes to a remote.
3. **Preview, then confirm.** Show the full change set (`--dry-run`, pending
   listings) and commit only after explicit confirmation.

## The hard rule: `sync` vs `generate`

- **`sync` only downloads raw provider data into `providers/`. No transformation,
  no normalisation, no timezone conversion. Re-running `sync` must never
  change generated outputs.**
- **`generate` reads `providers/` and produces every file under `generated/` and
  `latest/generated/`. All timezone conversion to `Europe/Brussels`,
  all formatting, all enrichment happens here.**

If a presentation bug appears in a generated file (e.g. wrong time in
`events.md`), the fix lives in `generate` or in the parser/formatter it calls
— never in `sync`.

## File layout: one sync file, one generate file per provider

For every provider: **`cmd/<provider>_sync.go`** holds only fetch + raw archive
logic; **`cmd/<provider>_generate.go`** holds parsing, enrichment, normalisation,
and every write under `generated/`. When `chb calendars sync` needs both, the
sync file calls into the generate file via a single hand-off function (e.g.
`generateCalendarsForMonths`). Don't add a `generated/` write to a `*_sync.go`
file — split it instead.

Current pair: `cmd/events_sync.go` ↔ `cmd/events_generate.go`. When you add
or refactor a provider, follow the same pattern.

## Time handling

- The canonical timezone for user-facing output is `Europe/Brussels`. Helpers:
  `cmd/format.go` exposes `BrusselsTZ()`, `FormatDateLong`, `FormatTimeBrussels`,
  `FmtDate`, `FmtTime`.
- Never write naïve local times to JSON. Use RFC3339 with an explicit offset.
- All-day events (`ical.Event.AllDay == true`, or `FullEvent.AllDay == true`)
  must be rendered without a clock time.
- The iCal parser (`ical/parser.go`) honours `TZID` parameters and flags
  `VALUE=DATE` entries as `AllDay`. Don't bypass it.

## Other repo conventions

- New providers live under `providers/<provider>/` — see [docs/providers.md](docs/providers.md).
- New cross-provider enrichers are processors — see [docs/processors.md](docs/processors.md).
- Settings, accounts, tokens, etc. live in `$APP_DATA_DIR/settings/`
  (default `~/.chb/settings/`). Generated data lives in `$APP_DATA_DIR/data/`
  (default `~/.chb/data/`).
- Don't commit changes unless explicitly asked.
