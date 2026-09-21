# chb — Commons Hub Brussels CLI

Command-line tool for managing [Commons Hub Brussels](https://commonshub.brussels) data: events, bookings, transactions, messages, and accounting workflows against Odoo / Nostr.

The codebase is organised around a strict three-verb pipeline:

- **`pull`** — fetch from external sources (read-only, remote → local)
- **`generate`** — transform local archives into derived outputs (local-only)
- **`push`** — publish local changes to targets (write-only, local → remote)

See [docs/philosophy.md](docs/philosophy.md) for the contract these verbs uphold, and why we keep them strictly separated.

## Install

The recommended install path is to download a prebuilt binary from GitHub Releases. You do not need Go installed.

See [docs/install.md](docs/install.md) for full instructions.

Fastest install on Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/CommonsHub/chb/main/install.sh | bash
```

Update an existing install:

```bash
chb update
```

`chb update` downloads the latest published binary from GitHub Releases. It does not require Go.

Source-based developer install:

```bash
go install github.com/CommonsHub/chb@latest
```

Or clone and build directly:

```bash
git clone https://github.com/CommonsHub/chb.git
cd chb
go build -o chb .
```

## Hello-world

After install + setup, the day-to-day loop is:

```bash
chb pull                    # fetch latest data from every source
chb generate                # transform into local outputs (no network)
chb odoo journals push      # publish ready changes to Odoo
chb nostr push              # publish ready Nostr annotations
```

Inspect along the way:

```bash
chb accounts                       # bank/payment accounts overview
chb events                         # upcoming events
chb report 2026/05                 # monthly report
chb odoo journals 28 push --dry-run   # see what would change in Odoo
chb nostr pending                  # see what would be published to Nostr
```

## Sources vs targets

Providers split into two roles ([philosophy.md § Sources vs targets](docs/philosophy.md#sources-vs-targets)):

- **Sources** (pull-only): Stripe, KBC Brussels, Etherscan-backed wallets, Monerium, ICS calendars, Discord.
- **Targets** (pull + push, with pending changes): Odoo, Nostr.

`chb pull` runs every source. `chb <target> push` publishes ready changes; the pending entries that drive each push are inspectable as files under `providers/<target>/pending/` (Odoo) or `$APP_DATA_DIR/nostr/outbox/` (Nostr).

## Settings

Settings live under `$APP_DATA_DIR/settings/` (default `~/.chb/settings`):

| File | Purpose |
|---|---|
| `accounts.json` | Bank / payment accounts (Stripe, KBC IBAN, Etherscan wallets, Monerium) — see [docs/accounts.md](docs/accounts.md) |
| `calendars.json` | ICS calendar feeds (not accounts) |
| `categories.json` / `collectives.json` | Allowed values for semantic tagging |
| `rules.json` | Semantic categorisation rules (description/IBAN/amount → category + collective) — see [docs/rules.md](docs/rules.md) |
| `odoo_mapping.json` | Maps semantic tags → Odoo `account_code` + `partner_id` |
| `rooms.json` | Room definitions for booking aggregation |
| `tokens.json` | Token registry for blockchain decoding |
| `config.env` / `settings.json` | API keys + per-feature flags |

## Environment variables

| Variable | Description |
|---|---|
| `APP_DATA_DIR` | App state root (default `~/.chb`) — settings, cached cursors, Nostr outbox. |
| `DATA_DIR` | Generated data root (default `$APP_DATA_DIR/data`) — `YYYY/MM/providers/` (raw archives) + `YYYY/MM/{public,members,stewards}/` (processed, per audience — see docs/audiences.md). |
| `LUMA_API_KEY` | Luma API key (enables rich event data). |
| `ETHERSCAN_API_KEY` | Etherscan / Gnosisscan / Celoscan API key. |
| `STRIPE_SECRET_KEY` | Stripe API key. |
| `DISCORD_BOT_TOKEN` | Discord bot token. |
| `ODOO_URL` / `ODOO_DB` / `ODOO_LOGIN` / `ODOO_API_KEY` | Odoo connection. |
| `MONERIUM_CLIENT_ID` / `MONERIUM_CLIENT_SECRET` | Monerium API credentials. |

## Documentation

- [docs/philosophy.md](docs/philosophy.md) — the load-bearing architecture rules. Read this first.
- [docs/agent-quickstart.md](docs/agent-quickstart.md) — fast on-ramp for AI agents picking up the project.
- [docs/data-model.md](docs/data-model.md) — `TransactionEntry` / `FullEvent` schemas, NIP-73 URIs, pending files.
- [docs/providers.md](docs/providers.md) — how to add a new source provider.
- [docs/processors.md](docs/processors.md) — cross-provider enrichment.
- [docs/rules.md](docs/rules.md) — `rules.json` + `odoo_mapping.json` schemas.
- [docs/accounts.md](docs/accounts.md) — `accounts.json` schema.
- [docs/testing.md](docs/testing.md) — test layout + smoke tests.
- [docs/cookbook.md](docs/cookbook.md) — copy-pasteable recipes for common ops.
- [docs/txspread.md](docs/txspread.md) — `spread` metadata for amortised transactions.

## License

See [Commons Hub Brussels](https://github.com/CommonsHub/commonshub.brussels) for license information.
