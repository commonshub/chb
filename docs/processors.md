# Data Processors

Data processors enrich generated records without mixing cross-source logic into
the core transaction or event builders. A processor can warm a cache once per
month, process every transaction and/or event, then flush public and private
cache files.

Provider-owned fetching belongs in `sources/<source>/`. Processors should read
archived source data and generated records, then add derived fields, tags, or
relationships.

Processor-specific code should live under `processors/<processor>/` when it has
shared path constants or helper types. The CLI adapter implements the
`DataProcessor` interface from `cmd/data_processors.go`; keep only thin command
wiring in `cmd/`.

## Lifecycle

For each generated month, CHB creates a `ProcessorContext` with:

- `DataDir`: root data directory.
- `Year`, `Month`: the month being generated.
- `HTTPClient`: shared HTTP client with a default timeout.

Then CHB calls:

1. `WarmUp(ctx)`: load caches or build indexes once.
2. `ProcessTransaction(ctx, tx)`: called for each generated transaction.
3. `ProcessEvent(ctx, event)`: called for each generated event.
4. `Flush(ctx)`: write updated caches or backup files.

If a processor fails during warm-up, that processor is skipped for the month. If
it fails for one record, CHB logs a warning and continues with the next record.

## Interface

```go
type DataProcessor interface {
	Name() string
	EnvVars() []ProcessorEnvVar
	WarmUp(*ProcessorContext) error
	ProcessTransaction(*ProcessorContext, *TransactionEntry) error
	ProcessEvent(*ProcessorContext, *FullEvent) error
	Flush(*ProcessorContext) error
}
```

## Storage

Use context helpers or processor package archive helpers instead of hand-building
paths:

```go
ctx.ReadPublicJSON(processorName, "cache.json", &cache)
ctx.WritePublicJSON(processorName, "cache.json", cache)
ctx.ReadPrivateJSON(processorName, "raw.json", &raw)
ctx.WritePrivateJSON(processorName, "raw.json", raw)
```

Public files are written to:

```text
DATA_DIR/YYYY/MM/processors/<processor>/<file>.json
DATA_DIR/latest/processors/<processor>/<file>.json
```

Private files are written to:

```text
DATA_DIR/YYYY/MM/processors/<processor>/private/<file>.json
DATA_DIR/latest/processors/<processor>/private/<file>.json
```

Public files must not contain PII. Private files may contain provider raw data,
counterparty names, emails, IBANs, billing details, or other sensitive fields.

## Source Boundaries

- Etherscan is a source. Celo, Gnosis, and Ethereum are chain scopes under
  `sources/etherscan/<chain>/`.
- Nostr is a source. Nostr tx/address metadata lives under `sources/nostr/`.
- Monerium is both a source and a processor: `sources/monerium` archives orders,
  while the Monerium processor matches those orders to generated on-chain EURe
  transactions.
- Luma Stripe is a processor because it enriches Stripe transactions with event
  relationships and URL tags.

## Transaction Tags

Processors should prefer public Nostr-style transaction tags for filterable
enrichment:

```json
["event", "calendar-event-uid"]
["eventName", "Event name from generated/events.json"]
["lumaEvent", "evt-2gc6B12TEyRNRqN"]
["eventUrl", "https://luma.com/example"]
["i", "https://luma.com/example", "https://luma.com/example"]
["k", "web"]
["source", "monerium"]
["status", "needs-review"]
```

Do not put PII in tags. Avoid storing names, emails, IBANs, card fragments, or
free-form bank remittance text in public tags.

## Registering A Processor

Create a file such as `cmd/plugin_monerium.go`, implement `DataProcessor`, then
add it to `registeredDataProcessors()` in `cmd/data_processors.go`.

## Guidelines

- Keep provider raw data out of generated public files.
- Use sources for provider APIs and archived upstream facts.
- Use `WarmUp` to load source archives and build lookup maps.
- Keep per-record hooks cheap; they should mostly do map lookups.
- Make processors idempotent: rerunning generation should not duplicate tags or
  rewrite unchanged cache files.
- Prefer canonical tags for filterable values and `metadata` only for small,
  non-sensitive details.
