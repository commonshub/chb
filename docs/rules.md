# Rules and mapping

Two settings files, two different jobs. Don't conflate them.

## `rules.json` — semantic rules (vendor-agnostic)

Pattern matching against transaction descriptions, IBANs, amounts, and counterparties. Resolves to **category** + **collective** — semantic tags that mean the same thing whether the next system is Odoo, Wave, Xero, or a CSV export.

**Match fields are field-scoped, not free-text.** `description` only looks at `metadata.description` + `metadata.memo`; use `counterparty` (any direction) or `sender`/`recipient` (direction-filtered) to match the counterparty. Mixing the two — e.g. "the vendor name in the bank statement" — needs an explicit `counterparty` rule, not a `description` glob.

**Targets.** A rule with no `target` field defaults to `target: "transaction"` — i.e. fires against ledger transactions. Set `"target": "invoice"` or `"target": "bill"` to apply the same rule engine to invoice/bill rows. Invoice/bill rules match on `title` (glob on the move number — use `*MEM/*` for substring so reversal / credit-note titles like `"Reversal of: MEM/2025/0001"` are also caught), `partner` (glob on customer/vendor display name), and `description` (glob on the concatenated line-item titles of the invoice, e.g. `*room*` catches any invoice whose line items mention a room). Transaction fields like `sender` / `iban` are ignored on move targets, and vice versa. A rule with `match: {}` is a catch-all for its target — useful for default-assignments like "every invoice gets collective=commonshub unless overridden".

The `description` field has per-target evaluation: on transactions it matches `metadata.description` / `metadata.memo`; on invoices / bills it matches the concatenated line items. The intent is "match the text that describes this row" regardless of underlying schema.

**Amount matching.** `amount` is the exact signed-gross match (back-compat: `"amount": -100` matches a €100 expense). `amount_min` / `amount_max` are inclusive bounds on the ABSOLUTE gross — sign-independent. Combine with `direction: "in"` / `"out"` to scope to one direction. Example: `"amount_min": 500, "direction": "in"` catches any incoming tx ≥ €500 regardless of currency representation.

Lives at `$APP_DATA_DIR/settings/rules.json`. Schema (simplified):

```json
[
  {
    "match": {
      "description": "proximus",
      "direction": "out"
    },
    "set": {
      "category": "internet",
      "collective": "commonshub"
    }
  },
  {
    "match": {
      "iban": "BE46000325448336"
    },
    "set": {
      "category": "utilities",
      "collective": "commonshub"
    }
  },
  {
    "match": {
      "amount": -3000,
      "counterparty_contains": "XL"
    },
    "set": {
      "category": "consulting",
      "collective": "brusselspay"
    }
  },

  {
    "target": "invoice",
    "match": { "title": "*MEM/*" },
    "assign": { "category": "membership", "collective": "commonshub" }
  },
  {
    "target": "invoice",
    "match": {},
    "assign": { "collective": "commonshub" }
  }
]
```

Evaluated in file order — most-specific rules first. The first matching rule wins per (collective, category) field; later rules can still fill in a field an earlier rule left blank, which is why catch-all default-assigns go LAST.

Edit with `chb rules edit` (opens in `$EDITOR`) or via the interactive TUI `chb rules` (which lets you preview matches before committing).

## `odoo_mapping.json` — Odoo-specific lookup

Maps a semantic tag (category / collective + direction) to an Odoo `account_code` + `partner_id`. **Lookup table, not a rule engine** — no patterns, just key → value.

Lives at `$APP_DATA_DIR/settings/odoo_mapping.json`. Schema:

```json
[
  {
    "match": {
      "category": "internet",
      "direction": "out"
    },
    "set": {
      "account_code": "616040",
      "account_name": "Internet services"
    }
  },
  {
    "match": {
      "category": "donation"
    },
    "set": {
      "account_code": "740040",
      "account_name": "Donations",
      "partner_id": 2666,
      "partner_name": "Anonymous Donor"
    }
  }
]
```

`account_name` / `partner_name` are human-readable caches so `chb odoo mapping` is reviewable without hitting Odoo. The load-bearing fields are `account_code` (resolved to an account id at push time) and `partner_id`.

Edit with `chb odoo mapping add --category … --account …` (resolves names via Odoo) or `chb odoo mapping edit`.

(The legacy file name `odoo_rules.json` is migrated automatically on first load.)

## How resolution flows

```
provider archive ──► chb generate ──► transactions.json (vendor-agnostic)
                          │
                          ├─► rules.json:        tx → category + collective
                          │
                          └─► odoo_mapping.json: (category, collective, direction)
                                                   → account_code + partner_id
                                                   ──► providers/odoo/pending/<YYYY-MM>.json

chb odoo journals push ◄── reads providers/odoo/pending/ + transactions.json
```

Two important properties of this layout:

1. **`transactions.json` is target-agnostic.** Adding Wave or Xero would mean writing `providers/wave/pending/` alongside `providers/odoo/pending/`; the canonical tx record doesn't change.
2. **Editing a rule or mapping requires `chb generate` before the next push picks it up.** The push path doesn't re-evaluate rules; it trusts pending. This keeps push fast and idempotent.

## Force re-apply onto existing Odoo lines

After fixing a rule or mapping, `chb generate` updates pending — but the lines already in Odoo from earlier pushes still carry the old account/partner. To rewrite them in place:

```bash
chb odoo journals 28 categorize       # journal 28 = KBC
chb odoo journals 28 categorize --dry-run   # preview first
```

This re-applies the analytic_distribution + GL account_id from the current mapping onto every existing line in the journal. It does NOT create new lines — that's `push`'s job.

## See also

- [philosophy.md § Two stages of resolution](philosophy.md#two-stages-of-resolution-rules--mapping--pending) — the architectural rationale.
- [data-model.md § providers/&lt;target&gt;/pending/](data-model.md#providerstargetpending) — the on-disk shape of pending files.
- [cookbook.md § Fix a miscategorised transaction](cookbook.md#fix-a-miscategorised-transaction) — recipe.
