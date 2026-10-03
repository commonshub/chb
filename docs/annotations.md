# Annotating transactions and Odoo documents

An **annotation** says what a transaction or an Odoo document is: its
category, the collective it belongs to, the event it relates to, how to
spread it over months, and a short note. Annotations are Nostr events, so
anyone can write them, but the published data only applies annotations
from **trusted** authors. Trusted annotations end up in Odoo, which stays
the single, consolidated source of truth.

## Annotation or comment: lowercase `i` versus uppercase `I`

Both are Nostr events of kind 1111 that point at the same thing through its
URI. The difference is the case of the tags.

| | annotation (data) | comment (discussion) |
|---|---|---|
| purpose | "this payment is category *drinks*, collective *commonshub*" | "who paid for this?", "great event!" |
| tags | `i` (the URI) and `k` (its kind), **lowercase only** | `I`/`K` (uppercase, NIP-22: the thing the thread is about), usually with `i`/`k` too |
| content | the note shown with the record | the message |
| chb | applies the newest trusted one per URI | never applies it; the website shows it as a conversation |

The rule chb follows: **an event carrying an uppercase `I` is a comment and
never changes data**, even if it also carries a lowercase `i`. So when you
build an annotation, never add `I` or `K`.

Annotation:

```json
{
  "kind": 1111,
  "tags": [
    ["i", "odoo:commonshub.odoo.com:commonshub:account.move:15200"],
    ["k", "odoo:account.move"],
    ["category", "drinks"],
    ["collective", "commonshub"],
    ["event", "evt-abc123"]
  ],
  "content": "Fridge restock for the potluck"
}
```

Comment on the same bill, which chb ignores:

```json
{
  "kind": 1111,
  "tags": [
    ["I", "odoo:commonshub.odoo.com:commonshub:account.move:15200"],
    ["K", "odoo:account.move"],
    ["i", "odoo:commonshub.odoo.com:commonshub:account.move:15200"],
    ["k", "odoo:account.move"]
  ],
  "content": "Do we still have the receipt?"
}
```

## Which URI to use

Use the identifier the published files already show. The `k` tag is the
URI's kind, as in the third column.

| record | URI | `k` | where to find it |
|---|---|---|---|
| on-chain transaction (Gnosis = 100, Celo = 42220; Monerium EURe included) | `ethereum:<chainId>:tx:<hash>` | `ethereum:tx` | `transactions.json` `id` |
| Stripe payment, payout, fee | `stripe:txn_…` | `stripe:txn` | `transactions.json` `id` |
| bank transaction (KBC) | `iban:<iban>:tx:<line id>` | `iban:tx` | `transactions.json` `id` |
| vendor bill, vendor credit note | `odoo:<host>:<db>:account.move:<id>` | `odoo:account.move` | `expenses.json` / `pending-bills.json` `uri` |
| customer invoice, credit note | `odoo:<host>:<db>:account.move:<id>` | `odoo:account.move` | `customers.json` `invoices[]`, `bookings.json` rentals `uri` |
| expense claim | `odoo:<host>:<db>:hr.expense:<id>` before it is booked; the bill's `account.move` URI after (both work) | `odoo:hr.expense` | `expenses.json` `uri` |

**Odoo journals.** A line in an Odoo bank journal is one of the
transactions above: annotate the transaction, not the journal line. chb
writes the result on the journal line (see "From Nostr to Odoo"). A
miscellaneous journal entry that is neither a transaction nor a
bill/invoice is not in the published data: an annotation on it is stored
but not applied.

## What to put in it

| tag | value | |
|---|---|---|
| `category` | a category slug from settings `categories.json` | e.g. `drinks`, `room-rental`, `accounting` |
| `collective` | a collective slug from settings `collectives.json` | e.g. `commonshub`, `openletter` |
| `event` | the event id from `events.json` | links an expense or a payment to an event |
| `spread` | `["spread", "YYYY-MM", "<amount>"]`, one tag per month | amortise a transaction over months ([txspread.md](txspread.md)) |
| `exclude` | `["exclude", "<reason>"]` | leave the record out of every total (test mints, duplicates). It stays in `transactions.json`, marked `metadata.excluded: "<reason>"`; `summary.json`, contributors' token totals, the token report and coverage skip it. A newer snapshot without the tag includes it again. `chb nostr annotate <uri> --exclude "test mint"` |
| content | free text, shown as `note` | keep it about the expense, not about people |

An annotation is a **snapshot**: the newest trusted one per URI replaces
the previous one entirely. Repeat the fields you want to keep.

## How to annotate

- **On the website:** the forms sign with the website's key, which is
  trusted.
- **With chb:**

  ```
  chb nostr annotate odoo:commonshub.odoo.com:commonshub:account.move:15200 \
      --category drinks --collective commonshub --note "Fridge restock"
  chb nostr annotate ethereum:100:tx:0xabc… --event evt-abc123
  chb nostr annotate stripe:txn_3Sko… --spread 2026-09:100 --spread 2026-10:100
  ```

  It starts from the current annotation, so you only pass what changes
  (`--clear` starts empty), previews the event and asks before publishing
  (`--dry-run`, `--yes`). It needs a key (`chb setup nostr`).
- **With any Nostr tool**, for example `nak`:

  ```
  nak event -k 1111 \
    -t i=odoo:commonshub.odoo.com:commonshub:account.move:15200 \
    -t k=odoo:account.move -t category=drinks -t collective=commonshub \
    -c "Fridge restock" wss://relay.commonshub.brussels
  ```

Publish on `wss://relay.commonshub.brussels` (settings.json `nostr.relays`).

## Who is trusted

- **Seeds:** settings.json `nostr.trustedAuthors`: the website's key and
  chb's own key by default, plus the key of the chb instance itself.
- **Follows:** an author followed by a seed (its kind 3 contact list) is
  trusted too. To trust someone, a seed follows them; to revoke, it
  unfollows them. One level only: whom they follow is not trusted. Turn it
  off with `nostr.trustFollows: false`.
- **Attestations:** a key a seed attests is trusted too: a kind 31926
  event by the seed with the key in a `p` tag. The website publishes one
  when a member links their browser key to their Discord account:

  ```json
  {"kind": 31926, "tags": [["d", "discord:<user id>"], ["p", "<member key>"],
    ["role", "member"], ["i", "discord:<guild id>"], ["k", "discord"]],
   "content": "{\"name\":\"…\",\"roles\":[\"member\"]}"}
  ```

  It counts only when it carries an allowed role (`role` tags or the
  content's `roles`; `nostr.attestationRoles`, default `member` and
  `steward`) and, when settings.json has `discord.guildId`, names that
  guild in its `i` tag. The newest attestation per (seed, `d`) wins, so
  republishing it without the `p` tag or without the role revokes the
  key at the next pull. One level only. Turn it off with
  `nostr.trustAttestations: false`.
- Every event's signature is checked. The current list is in
  `latest/providers/nostr/trust.json` (seeds, who follows whom, and the
  attested keys with their roles).
- A trust change takes effect at the next pull. Annotations by a
  no-longer-trusted author disappear from the published data at that pull.

Annotations by anyone else are ignored, not rejected: they stay on Nostr
and can be shown as suggestions.

## What happens next

1. **Hourly `chb pull`** fetches the trusted annotations since the last
   pull and files them by URI: transactions in
   `YYYY/MM/providers/nostr/transaction-annotations.json`, Odoo documents in
   `…/odoo-annotations.json`.
2. **`chb generate`**, also hourly, applies them to the published files:
   `transactions.json` (category, collective, event, spread,
   `metadata.note`), `expenses.json`, `pending-bills.json` and room rentals
   in `bookings.json` (category, collective, event, `note`). An annotation
   outranks Odoo and the rules there. Visible on the website within the
   hour.
3. **From Nostr to Odoo**, so Odoo becomes the consolidated truth:
   - transactions in journals chb feeds (Stripe, on-chain, Monerium): the
     hourly `chb odoo sync` writes their category and collective as the
     journal line's analytic distribution;
   - bills, credit notes, invoices, booked expense claims and KBC journal
     lines: `chb odoo annotations push` (preview, `--dry-run`, `--yes`)
     writes the analytic distribution. It only touches records with a
     trusted annotation not yet applied.
   - Each annotation is applied **once**: chb records its event id in
     `latest/providers/odoo/<db>/annotations-applied.json`. If the
     accountant later changes the record in Odoo, Odoo wins until someone
     annotates again.
4. **From Odoo to Nostr:** `chb nostr push bills|invoices` publishes Odoo's
   categorisation as a chb-signed annotation whenever it differs from the
   newest trusted annotation, so the accountant's corrections reach every
   consumer. It waits while a newer annotation is not yet applied to Odoo,
   so it never undoes one.

The result: the newest decision wins, wherever it was made, website, chb
or Odoo, and Odoo and Nostr converge on it.

## Checking

```
chb nostr pull                 # fetch now; prints how many trusted annotations
cat $DATA_DIR/latest/providers/nostr/trust.json
chb odoo annotations push --dry-run
```

An annotation that does not show up is usually by an untrusted key, or uses
uppercase `I`, or a URI chb does not hold (check `transactions.json` `id`
or the document's `uri`), or a category/collective with no Odoo analytic
account (the push warns).
