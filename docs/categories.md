# Categories

Every transaction can carry a **category** (`metadata.category`): what the
money was for. Pages group income and expenses by it, so one taxonomy is
shared by transactions, rules, bills and the website.

## The taxonomy: `latest/<tier>/categories.json`

Published, the same in every tier, from `settings/categories.json`:

```json
{
  "categories": [
    { "slug": "rental", "label": "Room rental", "direction": "income", "group": "space",
      "accounts": ["700100", "700003"] },
    { "slug": "internal_transfer", "label": "Internal transfer", "direction": "both",
      "group": "internal", "accounts": ["58"] }
  ]
}
```

- `slug` is what transactions carry. Some legacy slugs stay valid
  (`rentals`, `salaries`, `grant`) so older data and rules keep working.
- `direction`: `income`, `expense`, or `both` (VAT paid or refunded,
  catering bought or sold, internal transfers).
- `group` gathers categories for display: `space`, `people`, `services`,
  `equipment`, `events`, `food`, `taxes`, `finance`, `grants`,
  `membership`, `donations`, `sponsoring`, `internal`, `other`.
- `accounts` are PCMN account-code prefixes (Belgian chart, see
  `accounts-chart.json`) that map to the category; the longest matching
  prefix wins (`611010` printing supplies → `supplies`, `6110…` →
  `maintenance`, any other `6…` → `other-expense`).

Not income or expense: `internal_transfer` (between our own accounts) and
`opening_balance` (the first row of a bank journal). Both have type
`INTERNAL`; leave them out of totals.

## Where a transaction's category comes from

In order; the first one that gives a category wins:

1. the provider (Stripe products and metadata, the Stripe CSV);
2. processors (Luma tickets, Monerium memos…);
3. **rules** (`rules.json`, `rules.local.json`; [rules.md](rules.md));
4. **Odoo**, the consolidated books, when the bank line is reconciled
   there:
   - reconciled with **invoices, bills or other entries**: the category of
     their booking lines, weighted by amount (tax, receivable and payable
     lines left out). Per invoice/bill line: its analytic category when it
     is a known category; else, on customer invoices only, its product
     (categories.json `products`, case-insensitive globs on the product
     name, first match wins: `*room*` → rental, `*coffee*` → catering…);
     else its GL account. Products beat the account because income
     account 700000 carries rooms, catering and coworking as well as
     memberships. Their URIs go to `metadata.documents`. A bill booked to
     444000 (an invoice accrued the year before) gives `accrual`;
   - **not reconciled yet**, but an incoming payment whose memo or
     communication is the Belgian structured communication of one of our
     posted customer invoices (`+++000/0044/21681+++` or `000004421681`:
     the move id and its mod-97 check): that invoice's category, the same
     way, and the invoice goes to `metadata.documents`;
   - booked straight to an account (580000 internal transfer, 451200 VAT,
     455000 salaries, 613105 fees…): the category of that account;
   - suspense (499) gives none.

   These carry `metadata.categorySource: "odoo"`. They are never pushed back
   to Odoo.
5. Anything left is uncategorised: tag it on Nostr
   ([annotations.md](annotations.md)) or add a rule. `chb generate` warns
   ("Categories check") about every commonshub EUR/EURe transaction of the
   current and previous month left without one (internal and excluded rows
   aside), with its date, amount, account and id.

## Membership is strict

Memberships carry no VAT; rentals 21%. They are €10/month (Odoo product
94), €100/year (111) and €200/year for an organisation (104), on account
704200 (MEM journal). So:

- an invoice line is `membership` only when it is one of those products,
  or a membership-named product, **without VAT**; a membership-looking line
  with VAT is not membership (other income). Account 700000 no longer
  means membership: lines there follow their product (rental, coworking,
  catering…);
- the description rules (`*member*`, `*monthly financial contribution*`,
  `*MEM/20*`) only apply to €10/€100/€200 (`amount_in`); Stripe
  subscriptions keep their own rules;
- the "Categories check" flags, for the current and previous month, every
  payment categorised membership of another amount, and every
  membership-looking invoice line (membership product or name, account
  704200) with VAT or another price.

The bank lines' bookings and matches come from the hourly Odoo pull
(`latest/providers/odoo/<db>/journals/`, `statement-matches.json`). When a
line is reconciled later, the next `chb generate` rebuilds that month.

## Coverage: `summary.json` → `coverage`

Each month's `summary.json` says how much of the month's euro income and
expenses (EUR, EURe, EURb; internal transfers and opening balances left
out) is still uncategorised:

```json
"coverage": {
  "currency": "EUR",
  "in": 60703.74, "out": 35995.06,
  "uncategorisedIn": 6479.91, "uncategorisedOut": 18174.5,
  "uncategorisedCount": 22,
  "uncategorisedShare": 0.255,
  "fromOdoo": 17808.22
}
```

`uncategorisedShare` is (uncategorisedIn + uncategorisedOut) / (in + out);
`fromOdoo` is the amount categorised from Odoo bookings.
