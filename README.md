# chb — Commons Hub Brussels CLI

Command-line tool for managing [Commons Hub Brussels](https://commonshub.brussels) data: events, bookings, transactions, messages, and reports.

## Install

The recommended install path is to download a prebuilt binary from GitHub Releases. You do not need Go installed.

See [docs/install.md](docs/install.md) for full instructions.

Fastest install on Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/CommonsHub/chb/main/install.sh | bash
```

Quick example for Linux `amd64`:

```bash
VERSION=v2.2.0
curl -L -o /tmp/chb.tar.gz "https://github.com/CommonsHub/chb/releases/download/${VERSION}/chb_${VERSION#v}_linux_amd64.tar.gz"
tar -xzf /tmp/chb.tar.gz -C /tmp
install /tmp/chb_${VERSION#v}_linux_amd64 /usr/local/bin/chb
chb --version
```

If you want a source-based developer install instead:

```bash
go install github.com/CommonsHub/chb@latest
```

Or clone and build directly:

```bash
git clone https://github.com/CommonsHub/chb.git
cd chb
go build -o chb .
```

## Usage

```
chb <command> [options]

COMMANDS
  events              List upcoming events
  events sync         Fetch events from Luma feeds
  events stats        Show event statistics
  rooms               List all rooms with pricing
  bookings            List upcoming room bookings
  bookings sync       Sync room booking calendars
  bookings stats      Show booking statistics
  transactions sync   Fetch blockchain transactions
  transactions stats  Show transaction statistics
  messages sync       Fetch Discord messages
  messages stats      Show message statistics
  members sync        Fetch membership data from Stripe/Odoo
  sync                Sync everything
  generate            Generate derived data files
  report <period>     Generate monthly/yearly report
  doctor              Audit DATA_DIR integrity

OPTIONS
  --help, -h          Show help
  --version, -v       Show version
```

## Examples

```bash
chb events                        # next 10 upcoming events
chb events sync                   # sync events from Luma
chb events sync 2025/11           # sync events for Nov 2025
chb sync 2025 --force             # resync everything for 2025
chb transactions sync 2025/03     # sync transactions for Mar 2025
chb bookings sync 2025/06         # sync bookings for Jun 2025
chb report 2025/11                # monthly report
```

## Environment Variables

| Variable | Description |
|---|---|
| `DATA_DIR` | Data directory (default: `./data`) |
| `LUMA_API_KEY` | Luma API key (enables rich event data) |
| `ETHERSCAN_API_KEY` | Etherscan/Gnosisscan API key |
| `DISCORD_BOT_TOKEN` | Discord bot token |

## License

See [Commons Hub Brussels](https://github.com/CommonsHub/commonshub.brussels) for license information.
