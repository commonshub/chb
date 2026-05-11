# Sources

Sources own provider data. A source is responsible for downloading provider data,
splitting it into the monthly `DATA_DIR/YYYY/MM/sources/<source>/` layout, and
reading that archived data back without calling the provider again for past
months.

Each source should have a directory under `sources/<source>/` with:

- `source.go`: source identity, declared output files, and source-level storage
  helpers.
- One file per provider data family where practical, for example
  `transactions.go`, `charges.go`, `customers.go`, `subscriptions.go`,
  `payouts.go`, or `balance.go`.
- `types.go` only for shared provider data structures used by multiple files.

The shared source descriptor lives in `sources/source.go`:

```go
type Source interface {
	Name() string
	Files() []File
}
```

`cmd/` should orchestrate commands and wire source data into existing CLI flows.
Provider API calls, provider file paths, provider archive readers, and provider
object types should live in the source package.

Current sources:

- `sources/stripe`: Stripe balance transactions, charges, customers,
  subscriptions, and payouts.
- `sources/etherscan`: Etherscan V2 ERC20 transfer archives. Chain-specific
  data lives under `sources/etherscan/<chain>/`.
- `sources/monerium`: Monerium SEPA order archives.
- `sources/nostr`: Nostr annotations and chain tx/address metadata.
- `sources/discord`: Discord messages for monitored channels and downloaded
  Discord image attachments referenced by `generated/images.json`.
- `sources/odoo`: Odoo invoices, bills, subscriptions, analytic enrichment,
  and private attachment metadata/binaries.
- `sources/ics`: Monthly ICS calendar archives for room bookings and configured
  calendars.

Monthly generation writes `generated/report.json` after the other generated
files. Each source can contribute source-specific record, attachment, and
summary counts through the monthly report contributor hook in `cmd`.

Derived public calendar exports and event cover image downloads live under
`generated/` (for example `generated/calendars/public.ics` and
`generated/events/images/`) because they are generated/enriched artifacts, not
provider source archives.
