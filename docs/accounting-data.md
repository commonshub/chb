# Expenses, vendors, customers and bookings

What the Hub spends and on what, who it pays, who pays it, and how the rooms
are used: four files per month and per year, in each audience tier, built
by `chb generate` from Odoo and the room calendars. They let a page show,
month by month and for the whole year:

- the vendors and how much each one received, and what was bought, line by
  line (the drinks ordered from DelivCo / Big Bag for the fridge, for
  example);
- the customers and how much each one paid;
- when and how often each room was booked, and what room rental brought in.

## Files

| path | contents |
|---|---|
| `YYYY/MM/<tier>/expenses.json` | every posted vendor bill, vendor credit note and expense claim of the month, line by line |
| `YYYY/MM/<tier>/vendors.json` | one row per vendor: category, documents, totals paid and still due |
| `YYYY/MM/<tier>/customers.json` | one row per customer: income types, invoices, totals received and still due |
| `YYYY/MM/<tier>/bookings.json` | room bookings from the room calendars, room-rental invoice lines, per-room summary |
| `YYYY/<tier>/<same names>` | the same four for the whole year; `bookings.json` adds `months[]`, a per-room summary for each month |

`<tier>` is `public`, `members` or `stewards`. These files are never
mirrored to `latest/`: pick the month or the year. Every month and every
year has all four files: a month without vendor bills has an
`expenses.json` with `"expenses": []`, and so on.

Every file carries `generatedAt`, `scope` (`month` or `year`), `period`
(`2026-08` or `2026`) and `currency` (`EUR`). All totals are in euros:
documents in another currency are converted at Odoo's rate (`totalAmountEUR`
on each expense). Credit notes subtract.

The month bill list `bills.json` from v3.14 is replaced by
`expenses.json`. `latest/<tier>/pending-bills.json` (the "help us pay"
list, [bills.md](bills.md)) is unchanged.

## Who is named where (GDPR)

GDPR protects natural persons, so the rule is about what kind of party it
is. `chb` classifies every Odoo partner:

| type | meaning |
|---|---|
| `organisation` | marked as a company in Odoo, **or** its name carries a legal form (SRL, SA, NV, BV, ASBL, VZW, GmbH, Ltd, Stichting, …) even if Odoo says "person" |
| `sole_trader` | a person registered for VAT (their business identity is public in the company register) |
| `individual` | anyone else |

A bill or invoice addressed to a **contact inside a company** belongs to the
company: "XL Collective SRL, Leen Schelfhout" is published as XL Collective
SRL. In Odoo the contact inherits the company's VAT number, but the person
is not the business. The contact's name is kept for stewards only
(`contact.person`).

| | public | members | stewards |
|---|---|---|---|
| vendors that are organisations | name, VAT number, what they sold (line text), the event a bill is tagged with | = | + contact details, bank account, payments, attachments, Odoo links |
| vendors that are sole traders | name, VAT number, product names; **no** line text, vendor reference or event tag | + line text, reference, event | + contact details, bank account, payments, attachments, Odoo links |
| vendors that are individuals (reimbursements, freelancers without VAT) | type only, merged into one row per category (`individuals`: how many); lines keep the product name, not the free text | name and texts | everything |
| customers that are organisations | name, VAT number, products | = | + contact details and every invoice (`invoiceList`) |
| customers that are sole traders or individuals | type only (`member: true` when they hold a membership), merged into one row per income type | name and products | everything |
| room bookings | room, start, end, hours; the title only when the booking hosts a public event | + title | = |
| payroll lines (accounts 62…, 453/454/455) | amount only, described as "Payroll" | = | full text |
| general-ledger accounts | code and class ("Services and other goods") | + account name | = |

Anonymous rows carry no `id`, so nobody can be followed from month to
month. **Public never links a natural person to an event**: a bill from an
individual or a sole trader loses its `event` tag and its free text ("photos
of the Open Commons Day, 20/09") in public, because together with the name
they would place a person at a date and a place. Organisations keep both.

Below stewards, emails are removed, bank account numbers and BICs are
masked, and Belgian national register numbers are removed, wherever
they appear.

**When a vendor shows as anonymous but should not**, the fix is in Odoo:
mark the partner as a company and fill in its VAT number. The next hourly
run republishes it. A name with a legal form ("DelivCo SRL …") is already
treated as an organisation.

## `expenses.json`

```json
{
  "scope": "month", "period": "2026-09", "currency": "EUR",
  "totals": { "count": 14, "untaxedAmount": 3120.40, "totalAmount": 3588.12, "paidAmount": 2950.00, "amountDue": 638.12 },
  "byCategory": [ { "category": "cold-drinks", "count": 1, "totalAmount": 550.13 } ],
  "expenses": [
    {
      "uri": "odoo:citizen-spring-vzw.odoo.com:citizen-spring-vzw:account.move:8812",
      "id": "b-1f0c…", "number": "CHB-S/2026/09/0013",
      "kind": "bill", "status": "pending",
      "date": "2026-09-18", "dueDate": "2026-10-18",
      "vendor": { "id": "p-00c888a09d", "type": "organisation", "name": "DelivCo SRL (Big Bag Delivery)" },
      "vendorRef": "F2026-0912",
      "description": "Pajottenlander Orange Bio (Casier de 6 x 75cl), …",
      "category": "cold-drinks",
      "currency": "EUR", "untaxedAmount": 489.20, "vatAmount": 60.93,
      "totalAmount": 550.13, "totalAmountEUR": 550.13, "amountDue": 550.13,
      "hasDocument": true,
      "lines": [
        { "description": "Pajottenlander Orange Bio (Casier de 6 x 75cl)", "product": "Pajottenlander Orange Bio",
          "quantity": 1, "unitPrice": 24.16, "untaxedAmount": 24.16, "totalAmount": 25.61, "vatRate": "6%",
          "account": { "code": "604200", "class": "Purchases of goods" } }
      ]
    }
  ]
}
```

- `uri`: the document's global id, `odoo:<host>:<db>:account.move:<id>`
  (`hr.expense` for a claim not booked yet). The same in every tier, in
  `pending-bills.json`, on Nostr and on the website; annotate with it.
- `id`: **deprecated** alias (`b-…`, `x-…`), removed in the next release.
- `note`: the text of a trusted Nostr annotation on this `uri`
  ([website.md](website.md) §9). Annotations also set `category`,
  `collective` and `event`. Dropped in `public/` for a natural person's bill.
- `kind`: `bill`, `credit_note` (the vendor owes us), or `expense` (someone
  paid out of pocket and is reimbursed).
- `status`: `pending`, `partially_paid`, `paid`, `reversed` (cancelled by a
  credit note; excluded from totals), or `submitted` (an expense claim not
  booked yet).
- `number`: our accounting number. `vendorRef`: the vendor's own invoice
  number.
- `category`: our category when Odoo has one, otherwise the account class
  where most of the money went.
- `lines[]`: what was actually bought. `description` is the invoice text,
  `product` the catalogue item.

## `vendors.json`

```json
{
  "scope": "year", "period": "2026",
  "totals": { "count": 56, "untaxedAmount": 58076.87, "totalAmount": 68536.75, "paidAmount": 56441.07, "amountDue": 12095.68 },
  "vendors": [
    { "vendor": { "id": "p-…", "type": "organisation", "name": "Arca Fiduciaire" },
      "category": "Services and other goods", "categories": ["Services and other goods"],
      "documents": 6, "untaxedAmount": 7971.26, "totalAmount": 9645.22, "paidAmount": 1833.90, "amountDue": 7811.32 },
    { "vendor": { "type": "individual" }, "individuals": 4,
      "category": "Services and other goods", "documents": 9, "totalAmount": 6405.90, "paidAmount": 6296.00, "amountDue": 109.90 }
  ]
}
```

Rows are sorted by `totalAmount`, largest first. `totals.count` is the
number of distinct vendors, anonymous ones included.

## `customers.json`

```json
{
  "scope": "year", "period": "2026",
  "totals": { "count": 135, "totalAmount": 151667.17, "paidAmount": 90922.68, "amountDue": 60744.49 },
  "byIncomeType": [ { "category": "membership", "count": 120, "totalAmount": 36103.80 } ],
  "customers": [
    { "customer": { "id": "p-…", "type": "organisation", "name": "Open Collective Inc." },
      "incomeType": "sponsorship", "incomeTypes": ["sponsorship"], "products": ["Sponsorship"],
      "invoices": ["odoo:…:account.move:7001", "odoo:…:account.move:7044", "odoo:…:account.move:7102"], "invoiceCount": 3, "untaxedAmount": 9400, "totalAmount": 9400, "receivedAmount": 9400, "amountDue": 0 },
    { "customer": { "type": "individual", "member": true }, "individuals": 36,
      "incomeType": "membership", "invoices": ["odoo:…:account.move:6120", "…"], "invoiceCount": 52, "totalAmount": 20858.42, "receivedAmount": 19900.00, "amountDue": 958.42 }
  ]
}
```

`invoices[]` lists the URIs of the invoices and credit notes summed in the
row, the anonymous merged rows included (a URI names a document, not a
person), so each invoice can be annotated. `invoiceCount` is their number
(it was `invoices` in v3.16–3.17).

`incomeType` comes from the income account of the invoice lines:

| incomeType | accounts |
|---|---|
| `membership` | 700000, 704200 |
| `room_rental` | 700100 |
| `tickets_events` | 700150 |
| `sponsorship` | 700110, 749001 |
| `donation` | 740040 |
| `reinvoiced_costs` | 700200 |
| `sales_services` | other 70… |
| `other_income` | other 74… |
| `other` | anything else |

Only invoiced income is here. Card payments that never got an invoice, such
as most ticket sales through Stripe, are in `transactions.json`, not
`customers.json`. Some invoices go to collective partners like "Ticket
customers" or "STRIPE TECHNOLOGY EUROPE (Multiple customer stripe)": they
group many buyers.

## `bookings.json`

```json
{
  "scope": "month", "period": "2026-08",
  "rooms": [
    { "room": "ostrom", "roomName": "Ostrom Room", "bookings": 14, "hours": 61.5, "publicBookings": 3,
      "rentalLines": 5, "rentalRevenue": 1650 },
    { "room": "", "roomName": "", "bookings": 0, "hours": 0, "rentalLines": 2, "rentalRevenue": 400 }
  ],
  "bookings": [
    { "room": "ostrom", "roomName": "Ostrom Room", "start": "2026-08-26T18:00:00+02:00", "end": "2026-08-26T22:00:00+02:00",
      "hours": 4, "public": true, "title": "Innerpreneurs Summer gathering", "eventUrl": "https://lu.ma/…" },
    { "room": "satoshi", "roomName": "Satoshi Room", "start": "…", "end": "…", "hours": 2, "public": false }
  ],
  "rentals": [
    { "uri": "odoo:…:account.move:7210", "date": "2026-08-10", "room": "ostrom", "product": "Ostrom Room", "quantity": 4,
      "untaxedAmount": 200, "totalAmount": 242,
      "customer": { "id": "p-…", "type": "organisation", "name": "Open Org ASBL" } }
  ]
}
```

- **`bookings`** come from the room calendars: when a room was occupied.
  `public` is true when the booking hosts an event on the public calendar,
  matched by day and title. Only then does public see the title.
- **`rentals`** come from customer invoice lines on the room-rental income
  account (700100), dated by the invoice. `uri` is the invoice; a trusted
  annotation on it adds `event` and `note` (not in `public/` for an
  individual customer). The room is recognised from the
  product or the line text; `room: ""` means the line does not say which
  room. Invoices and calendar entries are not linked one to one: an invoice
  can cover several days, or a deposit.
- **`rooms`** sums both per room: bookings, hours, public bookings, rental
  lines and rental revenue (untaxed).
- The year file adds `months[]`, the per-room summary for each month.

## Building pages

- **Month view:** read `YYYY/MM/public/vendors.json` and `customers.json`
  for the two lists, and `expenses.json` for the detail of each vendor:
  filter `expenses[]` by `vendor.id`, then show its `lines[]`.
- **Year view:** the same files under `YYYY/public/`.
- **A supplier page**, such as the fridge drinks: filter `expenses[]` by
  `vendor.id` over the year file and list the lines.
- **Rooms:** `bookings.json` `rooms[]` for occupancy and revenue,
  `months[]` from the year file for a chart, `bookings[]` for a calendar.
- **Members-only pages** read `members/` instead: the same files, with
  individuals named.
- **Freshness:** Odoo is pulled hourly. A bill shows as `pending` until it
  is reconciled with its payment in Odoo ([bills.md](bills.md)).

## Chart of accounts and ledger balances

The general ledger as open data: every account of the chart, and for each
calendar year the balance of every account used. A page can show the
books the way an accountant reads them (class by class, account by
account), and anyone can check the annual accounts against them.

| path | contents |
|---|---|
| `latest/<tier>/accounts-chart.json` | every account of the chart (Belgian PCMN, as set up in Odoo), with its labels in every active language |
| `YYYY/<tier>/ledger-balances.json` | per account: opening balance, debit and credit of the year, closing balance |

Both come from Odoo, pulled every hour by `chb odoo pull` (read-only:
`read_group` sums of posted journal items, so drafts never count), and are
written by `chb generate`. Every year from the first posted entry to the
current year has a `ledger-balances.json`; a year has the file even if
nothing was posted (`"accounts": []`). Like the other year files, they are
never mirrored to `latest/` (the chart lives only there).

### `accounts-chart.json`

```json
{
  "generatedAt": "2026-10-03T14:00:00Z",
  "languages": ["en_GB", "fr_BE", "nl_BE"],
  "accounts": [
    {
      "code": "611000",
      "label": "FRAIS ENTRETIEN ET D'AMENAGEMENT DES ESPACES",
      "labels": { "en_GB": "Entretien et réparations des locaux", "fr_BE": "FRAIS ENTRETIEN ET D'AMENAGEMENT DES ESPACES", "nl_BE": "Onderhoudskosten en herstellingen" },
      "class": "6",
      "group": "61",
      "odooGroup": "61",
      "groupName": "61 Services and Other Goods",
      "type": "expense",
      "reconcile": false,
      "deprecated": false,
      "used": true
    }
  ]
}
```

- `class` is the PCMN class (`1` equity … `7` income), `group` the
  two-digit group; `odooGroup`/`groupName` the Odoo account group when
  one is set.
- `type` is Odoo's account type (`asset_cash`, `liability_payable`,
  `income`, `expense`, …); `reconcile` whether items are matched
  (receivables, payables, transfers).
- `used` is true when the account has a balance in some
  `ledger-balances.json`. Use it to hide the hundreds of unused accounts.
- `label` is the label in the database's main language; pick from
  `labels` for the reader's language. Translations are whatever was
  entered in Odoo and can be stale or wrong: fall back to `label`.

### `ledger-balances.json`

```json
{
  "generatedAt": "2026-10-03T14:00:00Z",
  "year": "2025",
  "periodStart": "2025-01-01",
  "periodEnd": "2025-12-31",
  "fiscalStart": "2025-01-01",
  "currency": "EUR",
  "totals": { "label": "Total", "opening": 4739.16, "debit": 3843435.40, "credit": 3843435.40, "closing": 4739.16 },
  "accounts": [
    { "code": "240100", "label": "MATERIEL DE BUREAU", "class": "2", "group": "24",
      "opening": 0.00, "debit": 2150.00, "credit": 0.00, "closing": 2150.00 }
  ]
}
```

- Balances are debit-positive: `closing = opening + debit − credit`.
  Liabilities, equity and income are therefore negative.
- **Opening**: balance-sheet accounts (classes 1–5) carry everything
  posted before `periodStart`. Income and expense accounts (classes 6–7)
  start at `fiscalStart`, the start of the fiscal year containing
  1 January (from the annual accounts periods). It is 1 January except
  when a fiscal year spans two calendar years: FY "2023" ran from
  1 July 2023 to 31 December 2024, so 2024 opens its classes 6–7 with
  July–December 2023.
- `totals.debit` equals `totals.credit` when the books balance.
  `totals.opening` (and so `totals.closing`) is the result of earlier
  fiscal years not yet booked to equity (account 14) in Odoo; it is 0
  once every earlier year is closed.
- The current year changes every hour. A past year changes when the
  accountant posts closing or correcting entries.

### What each tier shows

| | public | members | stewards |
|---|---|---|---|
| chart | every account; accounts named after a person get a neutral label (`"Account of an individual"`) and `"individual": true` | = public | real labels |
| ledger: accounts named after a person (current accounts of directors or members, a person's fees) | merged per two-digit group into one row (`"Current accounts of individuals"`, `"Charges: individuals"`, `"Income: individuals"`), `merged` = the number of accounts | = public | one row per account, real label |
| ledger: payroll (62) | one row `62` for all remuneration and social charges, so no salary can be read | = public | one row per account |
| labels | account numbers and BICs in labels masked | masked | as in Odoo |

An account counts as "named after a person" when its label starts with a
current-account or remuneration prefix (`C/C`, `compte courant`,
`current account`, `rétributions`, `rémunération`) followed by either a
name that is not an organisation (no legal form, not the name of a company
partner in Odoo), or a role one person holds (director, gérant,
administrateur, président, trésorier, secrétaire, zaakvoerder,
bestuurder, voorzitter…): the current account of "the director" is that
person's. Groups (`administrateurs`, `personnel`), taxes and the capital
stay public. The setting `accounting.privateAccounts` (codes) forces an account
private, `accounting.publicAccounts` forces it public.

Totals are identical in every tier: merging changes rows, never amounts.

### Pages

- **Balance sheet / P&L for a year:** group `accounts[]` by `class` (1–5
  balance sheet, 6–7 income statement) and by `group`, show `closing`;
  label each group from the chart.
- **Account page:** the chart entry for its labels; its row in each year's
  `ledger-balances.json` for the history.
- **Check against the annual accounts:** the class totals for the fiscal
  year's end match the figures in `annual-accounts.json`
  ([annual-accounts.md](annual-accounts.md)), allowing for the closing
  entries posted after the filing.
