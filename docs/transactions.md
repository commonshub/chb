# transactions.json — canonical schema

The shape every provider produces and every consumer can rely on. Lives at
`$DATA_DIR/<YYYY>/<MM>/stewards/transactions.json` (chb's full view; `members/`
and `public/` hold projections with less in them — see audiences.md) and the
mirror in `latest/stewards/`.

## Account-based, not graph-based

Each entry represents **one money-movement event from one account's
perspective**. An internal transfer between two accounts we own (e.g.
Monerium savings → checking) produces **two entries** — one CREDIT-side on
the destination, one DEBIT-side on the source — both sharing the same
transaction id but with their own `accountId`, sign, and `metadata.direction`.

There is **no `from` / `to` / `sender` / `receiver`** field. The schema
encodes direction by `accountId` (one perspective) + `counterpartyId` (the
other side) + the sign of `amount`.

## Field reference

| Field | Type | Convention |
|---|---|---|
| `id` | string | NIP-73 URI (`stripe:txn_…`, `ethereum:100:tx:0x…`, `iban:be46…:tx:<hash>`). Same id can appear twice when an internal transfer hits two of our accounts — disambiguate with `accountId` + `logIndex`. |
| `provider` | string | `stripe` / `etherscan` / `monerium` / `kbcbrussels` / `odoo`. |
| `providerId` | string | Provider-native id (e.g. `txn_…`) when distinct from `id`. |
| `accountId` | string | NIP-73 URI of the account this entry is FROM the perspective of. The amount sign is relative to this account. |
| `counterpartyId` | string | NIP-73 URI of the other side. For mints/burns where the other side is the zero address, this is the token contract URI. |
| `accountSlug` / `accountName` | string | Friendly labels for `accountId`. For token-wide etherscan entries, `accountName` resolves to a known address label when available. |
| `counterparty` | string | Friendly label for the counterparty (customer name, resolved address label, or raw IBAN/address). For Stripe customer txs: `bt.CustomerName`. For etherscan token-wide transfers: the receiver's Nostr-known label, falling back to the raw address. |
| `currency` | string | `EUR`, `EURe`, `EURb`, `CHT`, … |
| `amount` | float | **Signed gross** from `accountId`'s perspective. `amount > 0` ⇔ money flowed INTO `accountId`. `amount < 0` ⇔ money flowed OUT. This is the canonical value used by display, sorting, and rule matchers. |
| `grossAmount` | float | `\|amount\|`. Positive magnitude before any provider fee. |
| `netAmount` | float | Signed amount after provider fees. Equals `amount` for fee-less providers. For Stripe: customer pays `amount`, Stripe takes `fee`, balance moves by `netAmount`. |
| `normalizedAmount` | float | Signed **balance impact** — what the account balance moved by. Same as `netAmount` today; the separate field exists so future currency-normalization (e.g. EUR-equivalent for CHT) can plug in here without disturbing the gross/net pair. |
| `fee` | float | Positive fee magnitude. `0` for providers without separate fees. |
| `type` | string | One of `CREDIT` / `DEBIT` / `MINT` / `BURN` / `INTERNAL`. Redundant with `sign(amount)` for CREDIT/DEBIT/MINT/BURN; `INTERNAL` relies on the sign + `metadata.direction`. Token-wide transfers between two non-tracked addresses are recorded as `DEBIT` from the sender's perspective. |
| `timestamp` | int64 | Unix seconds. |
| `metadata.category` / `metadata.collective` | string | Semantic tags from `rules.json` + Nostr annotations. |
| `metadata.description` | string | Free-text description (Stripe BT description, Monerium memo, KBC narration). Used by `description:` rules. Does NOT carry sender/receiver names. |
| `metadata.direction` | string | For `INTERNAL`/`TRANSFER` only: `CREDIT` or `DEBIT` — the per-account direction. Redundant with `sign(amount)` but kept for legibility. |
| `metadata.kind` | string | Provider-native classifier (Stripe `reporting_category`: `charge`/`fee`/`payout`/`refund`/…). |
| `tags` | `[][]string` | Nostr-style flat tag list (`[["category","food"], …]`). Mirrors what's in `metadata` for downstream Nostr publishing. |
| `spread` | `[]SpreadEntry` | Per-month allocation when amortising — see [txspread.md](txspread.md). |

### Fields that **do not** exist in `transactions.json`

| Not a field | Use instead |
|---|---|
| `from` / `to` | `accountId` (sender perspective) and `counterpartyId` (other side). For mint/burn, the zero address rolls up to the token contract URI. |
| `sender` / `receiver` | Same as above. The rule engine has `sender` and `recipient` match keys that target `tx.counterparty` filtered by direction — they're matcher inputs, not data fields. |
| `fromName` / `toName` | `accountName` (perspective) and `counterparty` (other side). |
| Direction as a boolean | `type` (CREDIT/DEBIT/…) or `sign(amount)`. |
| `TRANSFER` type | Token-wide transfers between two non-tracked addresses are `DEBIT` from the sender's perspective. The receiver is in `counterpartyId`. |

## Signed-amount cheatsheet

| Scenario | `accountId` | `amount` | `type` | `metadata.direction` |
|---|---|---|---|---|
| Customer pays €100 via Stripe (fee €2.90) | `stripe:acct_…` | `+100` | `CREDIT` | — |
| Refund of €100 via Stripe | `stripe:acct_…` | `-100` | `DEBIT` | — |
| Stripe Automatic Tax fee €0.05 | `stripe:acct_…` | `-0.05` | `DEBIT` | — |
| Outgoing payment €73.13 from KBC | `iban:be…` | `-73.13` | `DEBIT` | — |
| Etherscan mint of 10 EURb to savings | `ethereum:100:address:0x…savings` | `+10` | `MINT` | — |
| Etherscan burn of 10 EURb from savings | `ethereum:100:address:0x…savings` | `-10` | `BURN` | — |
| Internal transfer 10000 EURe savings → checking, savings-side entry | `ethereum:100:address:0x…savings` | `-10000` | `INTERNAL` | `DEBIT` |
| Internal transfer 10000 EURe savings → checking, checking-side entry | `ethereum:100:address:0x…checking` | `+10000` | `INTERNAL` | `CREDIT` |
| Token-wide CHT transfer Alice → Bob | `ethereum:42220:address:0x…alice` | `-amount` | `DEBIT` | — |

For the internal-transfer case, the **two entries share the same `id`** (the
on-chain tx hash). Disambiguate by `accountId` (or `logIndex` when multiple
log events live on the same tx hash). Together they sum to zero balance-net
across the two of-our-accounts — that's the invariant tools like reconcile
and balance-verify rely on.

## Provider-level summary

| Provider | `amount` | `grossAmount` | `netAmount` | `fee` |
|---|---|---|---|---|
| Stripe | signed gross | `\|amount\|` | signed net (after Stripe fee) | positive fee |
| KBC | signed gross | `\|amount\|` | = amount (no per-line fee) | `0` |
| Etherscan | signed gross | `\|amount\|` | = amount (no on-chain fee tracked here) | `0` |
| Monerium | signed gross | `\|amount\|` | = amount | `0` |

## Why account-based?

Three reasons the schema went this way:

1. **Account balances are the primary read.** `accounts.go` walks
   transactions and adds signed amounts to per-account totals. Anything
   else (income/expense reports, push-to-Odoo, reconcile) builds on top.
2. **Internal transfers are unambiguous.** Each side has its own row with
   the right sign for that account — no special-case "transfer math" at
   read time.
3. **Provider-agnostic.** Stripe and KBC already produce one row per
   account-side event. Etherscan needed a small fix (signing the amount
   based on `type` and `metadata.direction`) but the model was always
   there: each entry has an `accountId`.

The graph-based alternative ("one row per transfer with `from` + `to` +
positive `amount`") would have required denormalizing every consumer
(balance math, sorting by direction, sign filters, the per-account
report) and there's no read pattern it makes simpler.

## Lifecycle

```
provider archive ──► chb generate ──► transactions.json
                          │
                          ├─► rules.json:        tx → category + collective
                          │
                          └─► odoo_mapping.json: (category, collective, direction)
                                                   → account_code + partner_id
                                                   ──► providers/odoo/pending/<YYYY-MM>.json
```

See [philosophy.md](philosophy.md) for the sync/generate boundary and
[rules.md](rules.md) for the rule + mapping resolution chain.
