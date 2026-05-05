# Transaction Spread

Spread lets a single transaction be recognized across multiple months for
accounting purposes, while the underlying cash flow stays anchored to the date
the money actually moved.

A January annual subscription, a Q1 prepayment, an annual insurance premium,
a yearly accountant invoice — all are paid once but should accrue to the
periods they cover. Spread captures that without inventing fake transactions.

## Cash vs accrual

Every report consumer picks one of two semantics:

| Mode | Used by | Rule |
|------|---------|------|
| Cash | `Accounts`, `Tokens`, `Top customers/vendors`, balance verification, currency totals | Anchored to the natural month, full amount, ignores `Spread` entirely. |
| Accrual | `Collectives`, `Categories`, P&L-style aggregates | Replaces the natural-month full amount with the per-month allocation. |

The two views never disagree about *what happened*; they disagree about *which
month it counts in*. Cash answers "what moved through this account?"; accrual
answers "what does this month look like on the books?".

## Data model

```go
// On TransactionEntry (cmd/generate.go)
Spread []SpreadEntry `json:"spread,omitempty"`

// On TxAnnotation (cmd/nostr.go)
Spread []SpreadEntry `json:"spread,omitempty"`

type SpreadEntry struct {
    Month  string `json:"month"`  // "YYYY-MM"
    Amount string `json:"amount"` // signed, cent-precision string ("-300.00")
}
```

The annotation is the **source of truth**. `tx.Spread` is a projection: it is
re-populated on every `chb generate` from the matching annotation, so manually
clearing it on a tx does nothing — clear or republish the annotation instead.

Amounts are signed and stored as strings so they round-trip exactly through
JSON and Nostr tags. `BuildSpreadEntries` distributes a total at cent precision
and routes the rounding remainder to the last bucket so the entries always sum
to the original total.

## Nostr tag format

Spread is published on the kind:1111 accounting annotation as one tag per
month. Multiple `spread` tags on the same event compose into a list:

```
{
  "kind": 1111,
  "tags": [
    ["i",          "ethereum:100:tx:0xabc..."],
    ["k",          "ethereum:tx"],
    ["category",   "insurance"],
    ["collective", "commonshub"],
    ["amount",     "5373.91", "EURe"],
    ["spread",     "2024-01", "-223.91"],
    ["spread",     "2024-02", "-223.91"],
    ...
    ["spread",     "2025-12", "-223.91"]
  ]
}
```

`spread` is multi-letter, so it is **not reliably indexable** as a `#spread`
filter on most relays (NIP-12 only requires single-letter tag indexing). Don't
rely on relay-side filtering to discover spreads; scan the local annotation
cache instead.

## Input formats (TUI)

The transactions browser exposes spread editing through the date column of the
detail view. The editor accepts a single line and parses several shorthands:

| Form | Example | Expansion |
|------|---------|-----------|
| Single month | `2025-04`, `202504` | one month |
| Full year | `2025` | 12 months |
| Year range | `2024-2025` | 24 months |
| Compact month range | `202501-202506` | 6 months |
| Long-form month range | `2025-01-2025-06` | 6 months |
| Comma list | `202501,202503,202507` or `2025-01,2025-03` | exact months |
| Mixed | `2025,2026-01` | combine any of the above |

Parsing is implemented in `ParseSpreadInput`. After parsing, `BuildSpreadEntries`
distributes the transaction's signed total across the months at cent precision
(remainder absorbed by the last entry).

The editor shows a live preview under the input:

```
12 month(s) × -€83.33 = -€1,000.00
2025-01-2025-12
```

Invalid input shows `⚠ <error>` instead of the preview; pressing `Enter` with
an invalid input is a no-op (existing spread unchanged).

## Storage

Spread lives in three places, in order of authority:

1. **Nostr relays.** kind:1111 events with `spread` tags. Multiple events per
   URI are kept by relays — the latest `created_at` wins on read. (Audit trail
   is recoverable but not yet exposed locally; the cache currently keeps only
   the latest.)
2. **Local annotation cache.** `DATA_DIR/YYYY/MM/sources/nostr/transaction-annotations.json`
   under the natural month, keyed by transaction URI:
   ```json
   {
     "annotations": {
       "stripe:txn:ch_aaa": {
         "uri": "stripe:txn:ch_aaa",
         "category": "insurance",
         "collective": "commonshub",
         "spread": [
           {"month": "2025-01", "amount": "-300.00"},
           {"month": "2025-02", "amount": "-300.00"},
           {"month": "2025-03", "amount": "-300.00"}
         ],
         "createdAt": 1706000000
       }
     }
   }
   ```
3. **Generated transactions.** `DATA_DIR/YYYY/MM/generated/transactions.json` —
   the `Spread` field on the transaction entry, copied from the annotation by
   `chb generate`.

## Inbound index

To answer "what allocates to month X?" without scanning every other month at
read time, `chb generate` produces a per-month inbound index:

```text
DATA_DIR/YYYY/MM/generated/inbound_spreads.json
```

```json
{
  "year": "2024",
  "month": "01",
  "updatedAt": "2026-05-04T22:52:37Z",
  "inbound": [
    {
      "uri":          "ethereum:100:tx:0x2922...",
      "naturalYM":    "2026-04",
      "txID":         "gnosis:0x2922e7fe379ccd",
      "amount":       "-223.91",
      "total":        "-5373.91",
      "currency":     "EURe",
      "type":         "DEBIT",
      "counterparty": "Nicolas Springael SRL",
      "category":     "accounting",
      "collective":   "commonshub",
      "description":  "Accountant 2023-2024 Invoice 147"
    }
  ]
}
```

`uri` + `naturalYM` is the back-reference. The remaining fields are
denormalized so consumers can render a row without re-opening the natural
month's transactions.

## Rebuild pipeline

`chb generate` runs a single global "Rebuilding inbound-spread indexes" phase
after the per-month generates finish (regardless of which month was scoped on
the command line — spreads can target any month, so partial rebuilds would
leave stale entries):

1. Walk every existing `inbound_spreads.json` to record what's currently on
   disk.
2. Walk every `sources/nostr/transaction-annotations.json`, lazy-loading the
   matching `transactions.json` per natural month for denormalized fields.
3. For each annotation with a non-empty `Spread`, emit one `InboundSpread` per
   target month into a `targetYM → []InboundSpread` map.
4. Write each target month's file fresh; delete files in step 1 that don't
   appear in step 3 (no diffing — write-or-delete only).

This is implemented in `cmd/inbound_spreads.go`:

- `scanAnnotationCachesForSpreads(dataDir) → map[targetYM][]InboundSpread`
- `rebuildInboundSpreads(dataDir) error`
- `LoadInboundSpreads(dataDir, year, month) []InboundSpread`

## How consumers apply it

### Monthly report (`Collectives`, `Categories`)

`buildMonthlyReportTaggedFlows` (in `cmd/monthly_report.go`) is allocation-aware:

- For each natural-month transaction:
  - If `len(tx.Spread) > 0`: contribute only the allocation for the current
    month (zero, and skipped, if the spread doesn't include this month).
  - Otherwise: contribute the full amount as before.
- Add every entry from the current month's `inbound_spreads.json` on top.

`Currencies`, `Accounts`, `Tokens`, and the top customers/vendors lists keep
their cash-mode behavior.

### Transactions browser TUI

When a date filter is active (`--since` and `--until`),
`loadFilteredTransactionsWithPII` synthesizes one `TransactionEntry` per
inbound allocation in range. Each virtual entry has:

- `ID = "virtual:<sourceURI>:<targetYM>"`
- `Provider = "spread"`
- `Timestamp` set to the first day of the target month
- `Metadata["virtualSource"] = true` — the marker
- `Metadata["sourceURI"]`, `sourceNaturalYM`, `sourceTxID` — backlinks
- `Metadata["spreadAllocation"]`, `spreadTotal` — for the `applied/total` cell

Rendering:

- Date column gets a `↳` prefix.
- Description column shows `from <YYYY-MM>`.
- Amount column shows `-€223.91/-€5,373.91` (allocation / total), styled by
  sign.
- `commitInlineEdit` short-circuits on virtual rows: edits must happen against
  the source transaction in its natural month.

## Editing flow

```
                               ┌─ relays (canonical, append-only) ─┐
                               │                                    │
TUI date editor → ParseSpreadInput → BuildSpreadEntries
        │
        ├──→ tx.Spread on TransactionEntry
        ├──→ saveTransactionUpdate → transactions.json
        │       (also re-emits ["spread", ym, amt] tags via syncTransactionTags)
        ├──→ persistTransactionAnnotationToNostrSource → annotation cache
        └──→ buildTransactionAnnotationEvent → Nostr publish (kind:1111)
```

After a save:

1. The natural-month `transactions.json` reflects the new spread immediately.
2. The annotation cache reflects it immediately.
3. The published Nostr event carries the spread tags (or the local cache is
   the only authority if there's no Nostr identity configured).
4. The next `chb generate` rebuild picks up the new spread and rewrites the
   target months' `inbound_spreads.json` files (and removes any that no
   longer have inbound).

## Edge cases

- **Spread excludes the natural month.** The natural-month accrual report
  shows zero for that transaction's tag rows — cash arrived but cost was
  recognized elsewhere. Cash sections (`Accounts`, balance) are unaffected.
- **Spread reduced or relocated.** The rebuild writes fresh files for new
  targets and deletes files for orphaned targets — no stale entries linger.
- **Manual non-uniform amounts.** `BuildSpreadEntries` is uniform with
  remainder-on-last; manually editing individual amounts via Nostr is
  honored on read but the TUI editor will overwrite them on the next save.
- **Negative DEBIT distribution.** Sign is preserved by `txSpreadTotal`;
  spread entries are signed and route to the right `In`/`Out` bucket via
  `bumpSigned` in the report builder.
- **Currency.** Spreads are denominated in the source transaction's currency.
  The aggregator treats `EUR`, `EURe`, `EURb` as one accrual currency
  (`isEURCurrency`) but keeps tokens like `CHT` separate.
