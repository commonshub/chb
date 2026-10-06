# Members and membership reminders

## Status (Odoo subscriptions first)

`members.json` takes each member from their Odoo subscription (sale order,
`is_subscription`), or from Stripe when there is none. Status: `active`,
`grace` (an invoice unpaid past its due date, or paused — a member until
`graceEndsAt`, 15 days later) or `lapsed` (churned, or grace expired).
Tiers and fields: [website.md §16](website.md).

## Reminders

One email per status change, to members in grace or lapsed in the last 30
days. Reasons: `payment_failed`, `paused` (both mention the grace period),
`ended`; organisations get the `.organisation` variant.

- `chb generate` writes the plan for the current month to
  `latest/providers/members/pending-reminders.json` (stewards-only, 0600):
  who, why, since — no email addresses.
- `chb members remind` previews it; `chb members remind --yes` sends what
  `reminders-sent.json` has not recorded yet (at most 25 per run unless
  `--all`). The address is looked up live: the Odoo partner's email, else
  the Stripe customer's.
- `chb members remind --render-test [payment_failed|paused|ended]
  [--organisation]` prints a sample for a fictitious member.

Configuration (`$APP_DATA_DIR/settings/config.env`):

| key | |
|---|---|
| `RESEND_API_KEY` | Resend API key; from "Commons Hub Brussels <hello@commonshub.brussels>", reply-to hello@commonshub.brussels |
| `RENEW_LINK_SECRET` | shared with the website: signs `https://commonshub.brussels/membership/renew?t=<token>`; token = base64url(JSON {n, c?, p?, o?, x}) + "." + base64url(HMAC-SHA256(secret, the base64url JSON)), valid 30 days |

Templates: `cmd/templates/emails/` is a copy of the website's
`docs/emails/membership-reminder.*` (generated from `buildReminderEmail`).
After the site changes them: `cp <site>/docs/emails/membership-reminder.*
cmd/templates/emails/` and release.

Reminders are not in the hourly cron until the stewards decide to add
`chb members remind --yes` after reviewing a preview.
