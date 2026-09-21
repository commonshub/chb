# Providers

Providers own upstream data. A provider is responsible for downloading provider
data, splitting it into the monthly `DATA_DIR/YYYY/MM/providers/<provider>/`
archive layout, and reading that archived data back without calling the provider
again for past months.

Each provider should have a directory under `providers/<provider>/` with:

- `source.go`: provider identity, declared output files, and provider storage
  helpers.
- One file per provider data family where practical, for example
  `transactions.go`, `charges.go`, `customers.go`, `subscriptions.go`,
  `payouts.go`, or `balance.go`.
- `types.go` only for shared provider data structures used by multiple files.

The shared provider descriptor lives in `providers/source.go`:

```go
type Provider interface {
	Name() string
	Files() []File
}
```

`cmd/` should orchestrate commands and wire provider data into existing CLI flows.
Provider API calls, provider file paths, provider archive readers, and provider
object types should live in the provider package.

Current providers:

- `providers/stripe`: Stripe balance transactions, charges, customers,
  subscriptions, and payouts.
- `providers/etherscan`: Etherscan V2 ERC20 transfer archives. Chain-specific
  data lives under `providers/etherscan/<chain>/`.
- `providers/monerium`: Monerium SEPA order archives.
- `providers/nostr`: Nostr annotations and chain tx/address metadata.
- `providers/discord`: Discord messages for monitored channels and downloaded
  Discord image attachments referenced by `stewards/images.json`.
- `providers/odoo`: Odoo invoices, bills, subscriptions, analytic enrichment,
  and private attachment metadata/binaries.
- `providers/ics`: Monthly ICS calendar archives for room bookings and configured
  calendars.

Monthly generation writes `stewards/summary.json` after the other generated
files. Each provider can contribute provider-specific record, attachment, and
summary counts through the monthly report contributor hook in `cmd`. A
cross-month rollup pass then fills in per-collective `startBalance` /
`endBalance` and writes the global aggregate to
`latest/stewards/summary.json`.

Derived public calendar exports and event cover image downloads live under
`stewards/` (for example `stewards/calendars/public.ics` and
`stewards/events/images/`) because they are stewards/enriched artifacts, not
provider archives.
