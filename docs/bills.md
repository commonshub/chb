# Vendor bills

The Hub's vendor bills live in Odoo. `chb` mirrors them and publishes two
things per audience tier: every bill of a month, paid or not, and the list
of bills **still to pay**. The pending list lets the website show what the
Hub owes and invite people to help cover it.

## Where the data comes from

- `chb bills pull` (part of `chb pull`, hourly on prod) mirrors vendor bills
  and vendor credit notes from Odoo into
  `YYYY/MM/providers/odoo/<db>/bills.json` and `…/private/bills.json`. The
  month is the bill date.
- By default a pull covers the current and previous month. It also finds,
  whatever their date, every bill that is open in Odoo or open in the local
  cache, and fetches again the ones whose state moved. An old bill that gets
  paid therefore leaves the pending list at the next pull. To backfill
  history once: `chb bills pull --since 2024-01`.
- `chb generate` writes the files below. `chb bills pending
  [--public|--members] [--json]` shows the pending list in the terminal.

**Accuracy caveat.** Odoo calls a bill paid only once it is reconciled with
its payment. A bill settled by direct debit or bank transfer stays
"pending" until someone reconciles the bank line with it. Recurring
utilities (electricity, telecom) are the usual suspects. Reconcile before
promoting the list: `chb odoo journals <id> reconcile`.

## Files

| path | contents |
|---|---|
| `YYYY/MM/<tier>/bills.json` | every **posted** bill and vendor credit note dated that month, paid or not |
| `latest/<tier>/pending-bills.json` | every posted bill **still to pay**, whatever its month, newest first |

Drafts are left out because they are not bills yet: they may be
duplicates, have no amount, or still await validation. Cancelled bills are
left out too. The month files are not mirrored to `latest/`, which holds
only the pending list.

Both files share one schema:

```json
{
  "generatedAt": "2026-09-25T12:00:00Z",
  "source": "odoo",
  "scope": "pending",
  "month": "2026-04",
  "currency": "EUR",
  "totals": { "count": 36, "totalAmount": 13042.45, "amountDue": 13042.45 },
  "totalsByCurrency": { "EUR": { "…": 0 }, "USD": { "count": 5, "totalAmount": 102.00, "amountDue": 102.00 } },
  "bills": [
    {
      "id": "b-b7e6ee1b53",
      "number": "CHB-S/2026/09/0011",
      "type": "bill",
      "status": "pending",
      "date": "2026-09-22",
      "dueDate": "2026-09-29",
      "vendor": { "type": "business", "name": "(De pistolei) FDAB", "vat": "BE0829079982" },
      "vendorRef": "260729",
      "description": "Plateau de Luxe",
      "lines": [ { "description": "Plateau de Luxe", "quantity": 36, "untaxedAmount": 122.40, "totalAmount": 129.74, "vatRate": "6%" } ],
      "category": "catering",
      "currency": "EUR",
      "untaxedAmount": 122.40, "vatAmount": 7.34, "totalAmount": 129.74,
      "amountDue": 129.74,
      "hasDocument": true,
      "paymentState": "not_paid"
    }
  ]
}
```

`month` appears only in month files. `scope` is `month` or `pending`.

| field | meaning |
|---|---|
| `id` | stable public id, the same in every tier and on every instance of the same Odoo database. Use it in URLs. It does not reveal the Odoo record id. |
| `number` | our accounting number for the bill. Quote it when paying or sponsoring a bill. |
| `type` | `bill`, or `credit_note` when the vendor owes us. |
| `status` | `pending`, `partially_paid`, `paid` (includes a payment registered but not yet matched to the bank), or `reversed` (cancelled by a credit note). |
| `vendor.type` | `business` when the vendor is a company or is registered for VAT, otherwise `individual`. |
| `amountDue` | what is still to pay, in the bill's currency. 0 once paid. |
| `totals` | EUR bills only: count, total and amount due. Credit notes and reversed bills are not summed. `totalsByCurrency` appears when some bills are in another currency. |
| `category`, `collective`, `event` | how the expense is classified, when Odoo knows. Often empty. |
| `hasDocument` | the vendor's invoice PDF is attached in Odoo. The PDF itself is not published. |
| `paymentState` | Odoo's raw value, for reference. |

## What each audience sees

| | public | members | stewards |
|---|---|---|---|
| amounts, dates, status, category, number, id | ✓ | ✓ | ✓ |
| business vendor: name, VAT number, their invoice number, line descriptions | ✓ | ✓ | ✓ |
| private individual (a volunteer's reimbursement, a freelancer without VAT): name, their reference, line descriptions | — (`vendor: {"type": "individual"}`, lines keep only amounts) | ✓ | ✓ |
| vendor contact details (email, phone, address), vendor bank account, payments, attachment links, Odoo links | — | — | ✓ under `stewards` |

A partner that Odoo names with an email address, such as
`billing@example.org`, is published under that address's domain
(`example.org`). Business VAT numbers are public in the Belgian company
register (KBO/BCE), so they are published.

## Building a "help us pay" page

- Read `latest/public/pending-bills.json` for anyone, or the members file
  behind the Discord `member` role to show who is owed.
- Show one card per bill: vendor, `description`, `amountDue`, `dueDate`,
  with overdue ones first. Compute overdue in the page: `dueDate` < today.
- Headline: `totals.amountDue` (EUR), plus `totalsByCurrency` if present.
- To let someone cover a bill, send them through the Hub's usual payment
  flow (Stripe or a transfer to the Hub's account) with the bill `number`
  as reference. Do not route payments to vendors directly: vendor bank
  details are not published, and the Hub keeps the relationship with its
  suppliers. Covering a bill is a donation to the Hub earmarked for that
  bill. Stewards then pay the vendor and reconcile, and the bill leaves the
  list at the next pull.
- Show the freshness: `generatedAt`. The list follows Odoo within the hour,
  but a bill only disappears once it is reconciled (see the caveat above).
- The month files feed an expense history: `YYYY/MM/public/bills.json`
  lists what was bought that month and what is still open.
