# Agent quickstart

This page is the fast on-ramp for AI agents and new contributors picking up `chb`. Read this and the linked anchors before making any non-trivial change.

## Read order

1. **[philosophy.md](philosophy.md)** — the load-bearing architecture rules. Don't skip.
2. **[data-model.md](data-model.md)** — what `TransactionEntry` / `FullEvent` actually carry, and where they live on disk.
3. **[providers.md](providers.md)** + **[processors.md](processors.md)** — how to extend with a new source or cross-provider enricher.
4. **[rules.md](rules.md)** — the two-stage `rules.json` → `odoo_mapping.json` flow.
5. **CLAUDE.md** (project root) — the hard rules summary for agents.

## Invariants — never break these

These come from real bugs. Don't relax them without explicit user buy-in.

1. **`pull` never writes to remote systems, `push` never reads from non-archived sources.** See [philosophy.md § Command verbs](philosophy.md#command-verbs-pull--generate--push).
2. **`sync` files never write under `stewards/`.** Use `<provider>_sync.go` for fetch, `<provider>_generate.go` for transforms.
3. **All user-facing times are normalised to `Europe/Brussels` in `generate`, never in `sync`.** Use `BrusselsTZ()` / `FormatTimeBrussels` / `FmtDate` from `cmd/format.go`.
4. **All-day events render without a clock time.** Don't bypass the iCal parser.
5. **`transactions.json` is target-agnostic.** Odoo-specific resolution lives in `providers/odoo/pending/<YYYY-MM>.json`. Don't put `accountCode` / `partnerId` back into the public tx schema.
6. **Mapping resolution happens at `generate` time, not at `push` time.** Editing `odoo_mapping.json` requires a `chb generate` before the next push picks it up. ([philosophy.md § Two stages of resolution](philosophy.md#two-stages-of-resolution-rules--mapping--pending))
7. **The `--help` short-circuit runs before any I/O.** No Odoo auth, no provider fetch, no file scan.
8. **Compact output is the default; every long step prints live progress.** Silence during a 30s step is a regression. ([philosophy.md § Always show what's happening](philosophy.md#always-show-whats-happening))
9. **Renames keep the old name working as an alias for at least one release.** Print a one-line dim deprecation notice on use.

## Verifying a change

Minimum checks before declaring a change done:

```bash
go build ./...                       # clean compile
go vet ./...                         # static checks
go test ./cmd/... -count 1 -short    # unit tests (some Odoo-integration tests need live creds — they fail loudly, that's expected without ODOO_URL set)
```

For changes that touch the CLI surface, also run the relevant smoke command:

```bash
./chb --help                                   # top-level help, must be instant
./chb pull --help                              # short-circuit BEFORE any I/O
./chb pull                                     # live progress per provider
./chb generate                                 # local-only, deterministic
./chb odoo journals 28 push --dry-run          # confirm pending → push is wired
./chb nostr pending                            # confirm the outbox listing works
```

For UI-affecting changes, run the actual command and observe — type-checking doesn't catch "the spinner stops updating after 5s" or "the table is misaligned." See [philosophy.md § Always show what's happening](philosophy.md#always-show-whats-happening).

## Common patterns

### Adding a sub-sync that needs progress reporting

Long-running provider sync code reports progress via the package-level `Progress(msg)` setter — it bubbles up to whichever outer `StatusLine` is active (compact `chb pull`) and is a no-op when called from a direct subcommand.

```go
Progress(fmt.Sprintf("fetching %s transfers (%s)", token.Symbol, acc.Name))
transfers, err := source.FetchTokenTransfers(...)
```

The existing per-sub-sync `statusLine.Update(...)` calls already forward to `Progress()` automatically, so any code using `newStatusLine()` keeps working under compact mode.

### Adding a new push target

Mirror Odoo's pattern: a `providers/<target>/pending/<YYYY-MM>.json` (or equivalent) emitted by `generate`, read by `push`. `transactions.json` stays target-agnostic. See `cmd/odoo_pending.go` for the canonical implementation.

### Adding a new source provider

Follow the file split: `cmd/<provider>_sync.go` for fetch, `cmd/<provider>_generate.go` for transforms (or hand off to a shared generator). See [providers.md](providers.md) and the existing `events_sync.go` / `events_generate.go` pair.

## When in doubt

- A `*_sync.go` file with a `writeMonthFile("…/stewards/…")` call is an obvious smell.
- A push path that calls `LookupOdooMapping(...)` instead of reading `providers/odoo/pending/` is an obvious smell.
- A compact command that goes silent for >1s is an obvious smell.

If you find yourself reaching for `--no-verify`, `--force`, or `git reset --hard` to "make the obstacle go away," stop and investigate the root cause instead.
