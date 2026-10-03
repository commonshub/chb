# Annual accounts

The association's annual accounts, as filed with the National Bank of
Belgium (NBB): Commons Hub Brussels ASBL, enterprise number 0804.505.132,
formerly Citizen Spring ASBL. Abbreviated schema for associations. chb
archives the documents of each fiscal year, reads the figures by NBB code,
checks them, and publishes the filed accounts as open data.

## Importing a fiscal year

```
chb annual-accounts import balance_sheet_abbr_assoc_31122025.pdf profit_and_loss_abbr_assoc_2025.pdf \
    [trial_balance_2025.pdf] [figures.csv]
chb annual-accounts set 2025 --filed [2026-09-30] [--nbb-ref <reference>] [--nbb-url <url>]
chb annual-accounts                      # list fiscal years, status and checks
```

- One import = one fiscal year. The period end is read from the balance
  sheet ("As of 31/12/2025"). A fiscal year that is not a calendar year
  needs `--period`: the first one ran 1 July 2023 to 31 December 2024, so
  `--period 2023-07-01:2024-12-31 --label 2023`. Without `--period` the
  start is assumed to be 1 January and a check says so.
- The figures come from the abbreviated balance sheet and profit and loss
  PDFs (as exported by Odoo or the NBB), or from a `figures.csv` with one
  `code;amount` per line (for example from the NBB structured export); the
  CSV wins where both give a code.
- Documents are archived unchanged in
  `YYYY/MM/providers/annual-accounts/<period end>/` next to `filing.json`
  (period, status, NBB reference), YYYY/MM being the end of the period.
- **A fiscal year is a draft until `set … --filed`**: drafts are visible to
  stewards only. `--draft` withdraws a fiscal year from the public files.
  `--filed` without a date records a filing whose date is unknown
  (`filedAt: null`).
- **No filed statement yet?** Import the class totals as a `figures.csv`
  with `--figures-source internal-balance`, and the internal balance sheet
  with `--internal`: the figures are published with a note that they come
  from the accountant's internal balance sheet; the internal document stays
  stewards-only.
- **The NBB register.** `chb pull` checks the association's deposits on the
  NBB register once a day. Until a fiscal year appears there, it carries the
  note "not yet visible on the NBB register as of <date>"; once it appears,
  the deposit date and reference fill in `filedAt` and `nbb.reference` when
  they were not set.
- Files dropped in `$DATA_DIR/latest/providers/annual-accounts/` are
  imported as a draft by the hourly `chb pull`.

### What is published

Only the **abbreviated balance sheet and profit and loss** of a **filed**
fiscal year: class-level aggregates by NBB code, no personal data. A trial
balance, an internal balance sheet ("bilan interne": account level, names
current accounts of natural persons, salary lines) and any unrecognised
document stay in the stewards tier. File names with "interne"/"internal"
or "trial" are recognised; `--internal <file>` forces it for any other.

## Files

| path | contents |
|---|---|
| `YYYY/<tier>/annual-accounts.json` | the fiscal year(s) whose period ends in YYYY |
| `latest/<tier>/annual-accounts.json` | every fiscal year (index) |
| `YYYY/public/annual-accounts/<file>.pdf` | the filed abbreviated statements |

Every year has an `annual-accounts.json`; a year without filed accounts has
`"fiscalYears": []`. `public/` and `members/` list filed fiscal years with
the abbreviated statements; `stewards/` lists drafts too, and every
document with its archive path. The archived documents are covered by
`hashes.json` (provider `annual-accounts`). Example (illustrative figures):

```json
{
  "generatedAt": "2026-10-03T12:00:00Z",
  "scope": "year",
  "year": "2025",
  "entity": { "name": "Commons Hub Brussels ASBL", "enterpriseNumber": "0804.505.132", "formerName": "Citizen Spring ASBL" },
  "fiscalYears": [
    {
      "label": "2025",
      "year": "2025",
      "period": { "start": "2025-01-01", "end": "2025-12-31", "months": 12 },
      "status": "filed",
      "filedAt": "2026-09-30",
      "figuresSource": "statements",
      "nbb": { "reference": "…", "url": "https://consult.cbso.nbb.be/consult-enterprise/0804505132",
               "onRegister": true, "depositedAt": "2026-09-30", "checkedAt": "2026-10-03T14:19:06Z" },
      "schema": "abbreviated-association",
      "currency": "EUR",
      "keyFigures": {
        "totalAssets": 150000.00, "fixedAssets": 1500.00, "currentAssets": 148500.00,
        "receivables": 30000.00, "cash": 115000.00, "equity": 80000.00,
        "accumulatedResult": 80000.00, "amountsPayable": 70000.00,
        "turnover": 160000.00, "giftsAndSubsidies": 5000.00, "goodsAndServices": 190000.00,
        "remuneration": 50000.00, "grossMargin": 48000.00, "operatingResult": -8000.00,
        "resultOfThePeriod": -8300.00, "broughtForward": 4700.00, "carriedForward": -3600.00,
        "otherAppropriations": 83600.00
      },
      "figures": { "20/58": 150000.00, "10/15": 80000.00, "9904": -8300.00, "(14)": -3600.00, "…": 0 },
      "labels": { "20/58": "TOTAL ASSETS", "…": "…" },
      "documents": [
        { "kind": "balance-sheet", "file": "balance_sheet_abbr_assoc_31122025.pdf",
          "path": "2025/public/annual-accounts/balance_sheet_abbr_assoc_31122025.pdf",
          "sha256": "…", "bytes": 65000 }
      ],
      "checks": [
        { "level": "warning", "code": "balancing-appropriation", "message": "…", "amount": 83600.00 }
      ]
    }
  ]
}
```

### Key figures

| key | NBB code | |
|---|---|---|
| `totalAssets` | 20/58 | total assets (= total liabilities, 10/49) |
| `fixedAssets` / `currentAssets` | 21/28 / 29/58 | |
| `receivables` | 40/41 | amounts receivable within one year |
| `cash` | 54/58 | cash at bank and in hand |
| `equity` | 10/15 | |
| `accumulatedResult` | 14 | accumulated profits (losses) on the balance sheet |
| `amountsPayable` | 17/49 | |
| `turnover` | 70 | |
| `giftsAndSubsidies` | 73 | membership fees, gifts, legacies and subsidies |
| `goodsAndServices` | 60/61 | |
| `remuneration` | 62 | |
| `grossMargin` | 9900 | |
| `operatingResult` | 9901 | |
| `resultOfThePeriod` | 9904 | |
| `broughtForward` | 14P | result of the previous period brought forward |
| `carriedForward` | (14) | result carried forward (appropriation account) |
| `otherAppropriations` | — | Odoo's "other appropriations of the year" line under 14 |

`figures` holds every code read, as printed, including the appropriation
codes in parentheses (`(9905)`, `(14)`) and Odoo's three uncoded lines under
14 (`14.profitOfTheYear`, `14.otherAppropriations`, `14.previousYears`).
Amounts are in euros; a figure the documents do not show is absent.

### Checks

Published with the figures, never hidden. `level` is `error` (the
statements do not add up), `warning` (they add up but something needs an
explanation) or `info`.

| code | |
|---|---|
| `unbalanced` | total assets ≠ total liabilities |
| `liabilities-sum` | 10/15 + 16 + 17/49 ≠ 10/49 |
| `accumulated-result-sum` | the lines under 14 do not add up to 14 |
| `operating-result`, `result-before-taxes`, `result-of-period` | 9901, 9903, 9904 do not follow from their components |
| `balancing-appropriation` | a non-zero "other appropriations of the year": a balancing entry, not a decision of the general assembly |
| `carried-forward-mismatch` | the appropriation account's (14) ≠ the balance sheet's 14 |
| `gross-margin-unexplained` | 9900 ≠ 70 + 71 + 72 + 73 + 74 + 76A − 60/61 (a component is not shown) |
| `negative-liability` | a liability code with a debit balance |
| `opening-balance-mismatch` | last year's (14) ≠ this year's 14P |
| `no-previous-year` (info) | the previous fiscal year is not imported, so the opening balance is not checked |
| `period-assumed` | the period start was not confirmed |
| `figures-from-internal-balance` (info) | the figures were read from the accountant's internal balance sheet; the filed statement is not available |
| `not-on-nbb-register` (info) | filed, but not yet visible on the NBB register as of the last check |
| `no-figures` | no figure could be read |

## Building pages

- **A year report** (`/2025`): read `2025/public/annual-accounts.json`; show
  the key figures, link the statements (`documents[].path`), and list the
  checks as notes.
- **Finance overview**: `latest/public/annual-accounts.json` for one row per
  fiscal year. Use `period` for the span: a fiscal year can be longer than
  12 months (`months`).
- **Authoritative copy**: link `nbb.url` (the association's page on the NBB
  register) next to ours, and say "not yet on the register" while
  `nbb.onRegister` is false; `sha256` lets anyone check the file is the one
  imported.
- `filedAt` can be `null` (filed, date unknown); `figuresSource` says
  whether the figures come from the statements or the internal balance
  sheet.
