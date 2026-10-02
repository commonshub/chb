# Mobilizon

`chb mobilizon` publishes our public Luma events to a Mobilizon group, so they
reach the fediverse without anyone posting them twice. Registration stays on
Luma: every Mobilizon event links to it (`joinOptions: EXTERNAL`).

Mobilizon is a target, like Odoo and Nostr: pull its state, plan the changes
offline, preview, then push.

```
chb calendars sync          # our events, from the Luma calendar feed
chb mobilizon pull          # the group's events → providers/mobilizon/events.json
chb mobilizon generate      # the plan → providers/mobilizon/pending/events.json
chb mobilizon push --dry-run
chb mobilizon push          # asks before writing; --yes when unattended
```

`chb generate` refreshes the plan too, once `chb mobilizon pull` has run.

## Configuration

In `settings/config.env` or the environment:

| Variable | |
| --- | --- |
| `MOBILIZON_EMAIL`, `MOBILIZON_PASSWORD` | an account that is an admin of the group; needed for push |
| `MOBILIZON_URL` | default `https://mobilizon.be` |
| `MOBILIZON_GROUP` | default `commonshub_bxl` |

## What gets planned

Only upcoming events from the Luma calendar. An event is identified by its
Luma URL, which is also the registration link on Mobilizon.

| Situation | Plan |
| --- | --- |
| Not published yet | `create` |
| Published, changed on Luma since | `update` |
| On Mobilizon already with the same Luma link (e.g. posted by hand) | `update`, and from then on we own it |
| On Mobilizon at the same time with a matching title but no Luma link | skipped: posted by hand, left alone |
| Published, gone from Luma, still in the future | `cancel` (status CANCELLED, not deleted) |

A cancellation needs the event's month in the local data, so a narrow
`calendars sync` can't cancel everything outside it.

`providers/mobilizon/published.json` remembers what we pushed: the Mobilizon
ID, a hash of the content and the uploaded cover. Delete an entry to have the
event created again.

## Drafts

`chb mobilizon push --draft` creates new events as drafts, to check them in
the Mobilizon interface first. The next push without `--draft` publishes them.

## Mapping

| Mobilizon | From the event |
| --- | --- |
| `title` | name |
| `description` | description as HTML paragraphs, then the Luma link. Descriptions from the calendar feed are cut short, so the link says the full text is on Luma |
| `beginsOn` / `endsOn` | start / end, in UTC |
| `physicalAddress` | the hub's address and map pin when the location is the hub; otherwise place, street, postcode, city and country from the location text |
| `picture` | the cover image, uploaded again only when it changes |
| `externalParticipationUrl` | the Luma URL |
| `tags` | the event's tags, when it has any |
