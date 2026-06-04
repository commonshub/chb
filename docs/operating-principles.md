# Operating principles

Three rules govern how `chb` touches the outside world. They are already how the
tool works — this page states them explicitly so they survive across changes and
contributors. See [philosophy.md](philosophy.md) for the architecture behind them.

## 1. Offline-first — work on the local mirror; if data is missing, sync it first

Everything reads from the local cache under `$DATA_DIR` (default `~/.chb/data`).
Read, analysis, report, and `generate` commands **never fetch inline** — network
access is isolated to the explicit `pull` / `sync` verbs.

- `generate` never hits the network (`providers/` → `generated/`, local-only).
- Reports, stats, reconciliation, the TUIs, etc. read only local caches.
- If data is missing or stale, a command **stops and tells you which sync to run**
  rather than silently fetching — e.g. *"No data found. Run sync first."*,
  *"run `chb pull` first"*, *"`chb calendars sync`"*,
  *"`chb accounts <slug> sync --history`"*. Run that, then re-run the command.

Why: results are deterministic and reproducible, you can work without API keys or
network (see [mirror-mode.md](mirror-mode.md)), and a re-`sync` never changes a
generated output (the sync vs generate hard rule).

**For agents:** never reach for the network to "fill a gap" mid-task. If a cache
is missing, surface the exact `pull`/`sync` command and let it run first.

## 2. Writes go to an outbox, never straight to the remote

Odoo and Nostr are the only writable systems (**targets**). We never write to them
as a side effect of any other command. Instead, changes accumulate locally:

| Target | Outbox (pending changes) | Resolved when |
|---|---|---|
| Odoo | `providers/odoo/pending/<YYYY-MM>.json` (one entry per tx URI) | at `generate` (rules.json + odoo_mapping.json) |
| Nostr | the `outbox/` (signed-but-unsent events) | when an event is queued |

- `push` is the **only** verb that writes to a remote. It publishes the
  outbox/pending and then clears the entries it sent.
- Because resolution happens at `generate` time, the outbox is an exact statement
  of *what the next push will change* — nothing is decided at push time.

## 3. Preview everything; commit only after confirmation

No change reaches Odoo or Nostr without you seeing it first.

- **Preview** the full change set before committing:
  - `--dry-run` on any push / write op (read-only; writes nothing).
  - List what's queued: `chb nostr pending`, and the
    `providers/odoo/pending/<YYYY-MM>.json` files.
- **Commit** only after explicit confirmation — re-run without `--dry-run`, or
  answer the `[y/N]` prompt on interactive operations. The CLI hints the next
  step (e.g. *"↪ Re-run without --dry-run to apply."*).

So the loop for any outward change is always:

```
edit rules / data  →  chb generate            # resolve into the outbox (offline)
                   →  chb … push --dry-run     # preview the full change set
                   →  chb … push               # commit after confirmation
```

These three rules apply to agents and humans alike. When in doubt: stay offline,
queue to the outbox, preview, and wait for a go-ahead before committing.
