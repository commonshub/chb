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

- `id`: stable public id, the same as in `pending-bills.json` (`b-…`); an
  expense claim not booked yet has an `x-…` id.
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
      "invoices": 3, "untaxedAmount": 9400, "totalAmount": 9400, "receivedAmount": 9400, "amountDue": 0 },
    { "customer": { "type": "individual", "member": true }, "individuals": 36,
      "incomeType": "membership", "invoices": 52, "totalAmount": 20858.42, "receivedAmount": 19900.00, "amountDue": 958.42 }
  ]
}
```

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
    { "date": "2026-08-10", "room": "ostrom", "product": "Ostrom Room", "quantity": 4,
      "untaxedAmount": 200, "totalAmount": 242,
      "customer": { "id": "p-…", "type": "organisation", "name": "Open Org ASBL" } }
  ]
}
```

- **`bookings`** come from the room calendars: when a room was occupied.
  `public` is true when the booking hosts an event on the public calendar,
  matched by day and title. Only then does public see the title.
- **`rentals`** come from customer invoice lines on the room-rental income
  account (700100), dated by the invoice. The room is recognised from the
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
