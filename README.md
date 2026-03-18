# chb — Commons Hub Brussels CLI

Command-line tool for managing [Commons Hub Brussels](https://commonshub.brussels) data: events, bookings, transactions, messages, and reports.

## Install

```bash
go install github.com/CommonsHub/cli/cmd/chb@latest
```

This installs the `chb` binary directly into your `$GOPATH/bin`.

Or clone and build directly:

```bash
git clone https://github.com/CommonsHub/cli.git
cd cli
go build -o chb ./cmd/chb
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
