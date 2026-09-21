# `chb forecast` — projecting the year's result

```
chb forecast [YYYY] [--baseline YYYY] [--through YYYY-MM] [--method M]
             [--allow-missing] [--verbose] [--json]
```

Projects the full-year **result** (income − expenses) of a year that isn't over
yet, from the transactions already booked plus the baseline year's actuals.

It is a reporting command: local data only, no network, no writes. It reads
`DATA_DIR/<year>/<month>/generated/transactions.json` and nothing else.

## The method

The year is split at the **last closed month**. Everything before the split is
actual; everything after is estimated from the **monthly series**:

```
remainder = typical month × months left × seasonal index
projected = actuals + remainder
```

- **Typical month** — the *median* of the closed months. One outsized month (a
  deposit, an annual insurance bill) moves the median hardly at all, and moves a
  mean by a quarter of itself.
- **Seasonal index** — from the baseline year: how heavy one of its remaining
  months was next to one of its own closed months. `1.0` means the same. This
  is a *shape*, which is far more stable year over year than a level.

The split is always a **whole month**, so the actuals can be re-derived with
`chb income` / `chb expenses`. The month in progress is deliberately *not* an
actual — half a month would drag every run rate down. It is reported separately
and projected with the months after it.

### Why not a year-to-date ratio

The first version of this command projected `A + R × (A ÷ P)` — the baseline's
remainder scaled by this year's level ratio. On live data it projected a
**€7.7M loss**. One category (fridge) had booked **€4.00** in the baseline's
closed window against €7,335 in its remaining months, so the ratio came out at
**×1045** and the remainder at €7.6M.

That is not a tuning problem, it is the estimator: `A ÷ P` divides by a
per-category number that is routinely near zero. Levels now come from the
median month, and the baseline only ever contributes a clamped shape. The same
data now projects **−€82k**, with a −€80k … −€123k range across the methods.

### Guard rails

| Rule | Value | Why |
|---|---|---|
| A line's own baseline must be thick before its shape or ratio is used | ≥ €250, over ≥ 3 distinct months, with ≥ 1 active month on each side of the cutoff | One stray transaction is not a seasonal pattern |
| Factors must land in a band | ×0.25 … ×4.00 | Outside it the number describes a data problem, not next quarter |
| A factor outside the band is **rejected, not capped** | falls back to the direction's shape; the direction falls back to ×1.00 | Capping quietly applies the *maximum* multiplier to exactly the lines that earned the least trust |
| A line with a median month of 0 but real money booked | uses the mean instead | Lumpy lines (quarterly rent, one annual grant) would otherwise project nothing |
| A line with nothing booked this year but a baseline remainder | carries the baseline over at the overall growth rate | A seasonal category that hasn't started yet isn't a zero |

Every fallback is printed in the row's BASIS column (`·overall`, `·rejected`)
and counted in ESTIMATE QUALITY. Nothing is silently adjusted.

## The five methods

All five are always computed on the totals and printed together — **the spread
between them is the uncertainty**. `--method` only chooses which one leads and
which one drives the category rows.

| Method | Remainder | Reach for it when |
|---|---|---|
| `robust` (default) | median closed month × months left × seasonal index | The default central estimate |
| `recent` | median of the last 3 closed months × months left × index | The year turned mid-way and you want the recent level to lead |
| `runrate` | mean month × months left, no seasonality | You want the one-offs included |
| `flat` | the baseline's remaining months, repeated | No growth assumption at all |
| `seasonal` | baseline remainder × this year's level ratio (`A ÷ P`) | Comparison only — this is the estimator described above, now guarded |

## Basis

The forecast counts exactly what `chb income` and `chb expenses` count:

- EUR-family currencies only (`EUR`, `EURe`, `EURb`).
- Internal transfers excluded, on `Type == "INTERNAL"` **and** on
  `category == "internal_transfer"` (older generated files carry only the latter).
- Cash basis, attributed by transaction timestamp in `Europe/Brussels`. No
  spread allocation, no accruals.

That shared filter lives in one function — `countableEURAmount` in
`cmd/forecast.go` — which `chb income` / `chb expenses` also call, so the three
commands cannot drift apart.

**This is not what `chb report <YYYY/MM>` shows per category.** The monthly
summaries carry per-category flows too, but they are a different quantity: they
fold VAT, fees and fiscal-host commissions into `out`, they apply spread
allocations, and they do *not* drop internal transfers. Aggregating the
transactions directly keeps one basis across the three commands, which is what
makes the VERIFY block possible.

## Reading the output

- **RESULT** — the central estimate, plus the range across all five methods.
- **METHODS** — every method on the totals, with the central one marked `▸`.
- **ESTIMATE QUALITY** — what the number is worth: how much is uncategorised,
  which months are outliers, which rows had no usable baseline or a rejected
  factor, and how far the category rows sum from the central estimate.
- **BASIS** — the four windows (A, P, R, F) with transaction counts, the median
  month, and what the month in progress has booked so far.
- **Category tables** — a breakdown for insight, not the estimate. Every row
  satisfies `REMAINDER = PER MONTH × months left × SEASON`, so each is one
  visible multiplication. Their sum (bottom-up) is a second, independent
  estimate; a wide gap to the central one means the category mix is shifting or
  the categorisation is thin.
- **CHECKS** — re-adds the totals from independently summed parts.
- **VERIFY** — the `chb income` / `chb expenses` commands that re-derive every
  input, with the value each must print.

`--verbose` adds the per-month actuals of both years — the series every median
is taken over — and the full list of files read, with each file's `generatedAt`
and row counts. `--json` emits every input, every row (including its `formula`
and `flags`), all five methods, the quality notes, the checks and the verify
commands.

Given the same files, the output is identical every run; `generatedAt` is the
only field that depends on the clock.

## Improving the estimate

The projection is only as good as its inputs, in this order:

1. **Categorise.** If ESTIMATE QUALITY reports a large uncategorised share, the
   category tables are close to meaningless and only the totals are worth
   reading. `chb transactions categorize` and `rules.json` are the levers.
2. **Keep both years complete.** A missing month counts as zero and biases every
   estimator; the command refuses to run over one unless you pass
   `--allow-missing`.
3. **Check the outlier months** the quality block names. A €100k month that was
   a one-off is fine — the median ignores it. One that is the start of a new
   recurring cost is not, and `--method recent` will show you what it implies.

## Missing data

A month with no `generated/transactions.json` would silently count as zero, so
the command stops and names the months and the syncs to run:

```
chb pull --since 2025-03
chb generate --since 2025-03
```

If those months genuinely have no transactions (the org started mid-year, say),
`--allow-missing` proceeds. The missing months are then listed in the quality
notes, in the coverage check and in the JSON payload.

## Examples

```bash
chb forecast                            # project the current year
chb forecast 2026 --verbose             # add per-month actuals and the source files
chb forecast 2026 --method recent       # lead with the last 3 months
chb forecast 2026 --through 2026-06     # pretend the year closed in June
chb forecast 2026 --baseline 2024       # compare against another year
chb forecast 2026 --json | jq .methods  # every method, machine-readable
```
