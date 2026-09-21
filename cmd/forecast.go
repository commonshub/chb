package cmd

// `chb forecast` projects the full-year result (income − expenses) of a year
// that isn't over yet, from the transactions already booked for that year's
// closed months plus the baseline year's actuals.
//
// Four constraints shape this file:
//
//  1. Offline-first — it reads `generated/transactions.json` under DATA_DIR and
//     nothing else. A missing month stops the command and names the sync to run
//     rather than quietly projecting from a hole in the data.
//  2. Verifiable — every printed number is either an input the operator can
//     re-derive with `chb income` / `chb expenses` over the same window (the
//     VERIFY block spells out those commands), or the result of one printed
//     multiplication. The window scan below deliberately mirrors
//     runIncomeExpenseReport's month-file walk and filter so the totals match
//     to the cent.
//  3. Transparent — no hidden smoothing, no dropped outliers, no weighting the
//     output doesn't name. Every row states the method that produced it, and
//     the CHECKS block re-adds the aggregates from independently summed parts.
//  4. Robust — estimators run on MONTHLY series, not on year-to-date ratios.
//     The first version scaled each category by A ÷ P (this year's level over
//     the baseline's), which divides by a per-category number that is often
//     near zero: a category with €4.00 of baseline produced a €7.6M projection.
//     Levels now come from the median month, and the baseline only ever
//     contributes a SHAPE (how heavy the remaining months are), clamped, and
//     only when that category's baseline is thick enough to mean anything.

import (
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Central-estimate methods, selectable with --method. All of them are always
// computed at the total level and printed in the METHODS block; --method only
// chooses which one leads and which one drives the category rows.
const (
	// robust: median closed month × remaining months × seasonal index.
	forecastMethodRobust = "robust"
	// recent: same, but the median of the last few closed months only.
	forecastMethodRecent = "recent"
	// seasonal: the year-to-date level ratio, A + R × (A ÷ P). Kept for
	// comparison; guarded so a thin baseline can't detonate it.
	forecastMethodSeasonal = "seasonal"
	// flat: A + R — the remaining months repeat the baseline unchanged.
	forecastMethodFlat = "flat"
	// runrate: A ÷ closed × remaining — the mean month, no seasonality.
	forecastMethodRunRate = "runrate"
)

// Per-row labels: what actually produced a given line.
const (
	forecastRowRobust   = "median"
	forecastRowRecent   = "recent"
	forecastRowSeasonal = "ratio"
	forecastRowRunRate  = "mean"
	forecastRowFlat     = "flat"
	forecastRowBaseline = "baseline"
	forecastRowClosed   = "closed"
)

// Guard rails. These exist because of a real failure: on live data a category
// with €4.00 of baseline over the closed window scaled a €7,335 remainder by
// ×1045 and projected €7.6M of fridge expenses. A ratio whose denominator is
// noise is noise.
const (
	// A baseline line must have booked at least this much, over at least this
	// many distinct months, before its own shape or ratio is trusted.
	forecastMinBaselineAmount = 250.0
	forecastMinBaselineMonths = 3
	// Seasonal indices and level ratios are clamped into this band. Outside it
	// the number is telling us about a data problem, not about next quarter.
	forecastMinFactor = 0.25
	forecastMaxFactor = 4.0
	// How many closed months the `recent` method looks at.
	forecastRecentMonths = 3
	// A month is flagged as an outlier when it exceeds this many times the
	// median month — it is never dropped, only reported.
	forecastOutlierFactor = 2.5
)

const forecastUncategorized = "(uncategorized)"

// ── Public report shape (also the --json payload) ──────────────────────────

// ForecastFlow is one aggregate over a set of transactions: the countable EUR
// credits, the countable EUR debits, and how many rows went into it.
type ForecastFlow struct {
	Income       float64 `json:"income"`
	Expenses     float64 `json:"expenses"`
	Transactions int     `json:"transactions"`
}

// Result is the net of the flow — the "result" the forecast projects.
func (f ForecastFlow) Result() float64 { return roundCents(f.Income - f.Expenses) }

// ForecastMonth is one month of actuals, attributed by transaction timestamp
// in Europe/Brussels.
type ForecastMonth struct {
	Month        string  `json:"month"` // YYYY-MM
	Income       float64 `json:"income"`
	Expenses     float64 `json:"expenses"`
	Result       float64 `json:"result"`
	Transactions int     `json:"transactions"`
}

// ForecastSource records one month file the scan looked for. Present=false
// means the file is absent — the month contributed nothing.
type ForecastSource struct {
	Month       string `json:"month"` // YYYY-MM
	Path        string `json:"path"`  // relative to DATA_DIR
	Present     bool   `json:"present"`
	Rows        int    `json:"rows"`                  // transactions in the file
	Counted     int    `json:"counted"`               // rows that passed the filter
	GeneratedAt string `json:"generatedAt,omitempty"` // as written by `chb generate`
}

// ForecastEstimate is one projection of the months still to come, with every
// number that produced it.
type ForecastEstimate struct {
	Remainder float64 `json:"remainder"`
	Method    string  `json:"method"`
	// Level is the typical month the estimate multiplies out, and LevelKind
	// says how it was picked (median / mean / recent median).
	Level     float64 `json:"level"`
	LevelKind string  `json:"levelKind,omitempty"`
	// Index is the baseline's seasonal shape: how heavy a remaining month was
	// compared with a month of the closed window. 1.0 = the same.
	Index       float64  `json:"seasonalIndex"`
	IndexSource string   `json:"seasonalIndexSource,omitempty"` // own | overall | none
	Flags       []string `json:"flags,omitempty"`
	Formula     string   `json:"formula"`
}

// ForecastRow is one category's projection.
type ForecastRow struct {
	Category string `json:"category"`
	// A — actuals of the target year over the closed months.
	Actual float64 `json:"actual"`
	// The monthly series A was summed from, oldest first. This is the input
	// the median is taken over; printed with --verbose.
	Monthly []float64 `json:"monthlyActuals"`
	// P / F / R — the baseline year over the closed months, the whole year,
	// and the months still to come (R = F − P).
	BaselineToDate    float64 `json:"baselineToDate"`
	BaselineFullYear  float64 `json:"baselineFullYear"`
	BaselineRemainder float64 `json:"baselineRemainder"`

	Estimate  ForecastEstimate `json:"estimate"`
	Projected float64          `json:"projected"`
}

// ForecastMethodResult is one method applied to the totals. The METHODS block
// prints all of them: the spread between them IS the uncertainty, and hiding it
// behind one number would be the least honest thing this command could do.
type ForecastMethodResult struct {
	Method            string  `json:"method"`
	Label             string  `json:"label"`
	RemainderIncome   float64 `json:"remainderIncome"`
	RemainderExpenses float64 `json:"remainderExpenses"`
	Income            float64 `json:"income"`
	Expenses          float64 `json:"expenses"`
	Result            float64 `json:"result"`
	Central           bool    `json:"central,omitempty"`
	IncomeFormula     string  `json:"incomeFormula"`
	ExpensesFormula   string  `json:"expensesFormula"`
}

// ForecastQualityNote is one line of the ESTIMATE QUALITY block.
type ForecastQualityNote struct {
	Level string `json:"level"` // warn | info
	Text  string `json:"text"`
}

// ForecastCheck is one line of the CHECKS block: a recomputation the reader can
// follow without re-running anything.
type ForecastCheck struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

// ForecastReport is the whole answer, printed or emitted as JSON.
type ForecastReport struct {
	Year            string `json:"year"`
	BaselineYear    string `json:"baselineYear"`
	Method          string `json:"method"`
	Currency        string `json:"currency"`
	Basis           string `json:"basis"`
	ClosedMonths    int    `json:"closedMonths"`
	RemainingMonths int    `json:"remainingMonths"`
	Through         string `json:"through,omitempty"` // last closed month, YYYY-MM
	ClosedLabel     string `json:"closedLabel"`       // e.g. "Jan–Aug"
	RemainingLabel  string `json:"remainingLabel"`    // e.g. "Sep–Dec"
	DataDir         string `json:"dataDir"`
	GeneratedAt     string `json:"generatedAt"` // the only now-dependent field

	Actual            ForecastFlow `json:"actual"`           // target year, closed months
	BaselineToDate    ForecastFlow `json:"baselineToDate"`   // baseline, same months
	BaselineFullYear  ForecastFlow `json:"baselineFullYear"` // baseline, all 12 months
	BaselineRemainder ForecastFlow `json:"baselineRemainder"`

	// Median month of the closed window, the level the robust method uses.
	MedianIncomeMonth   float64 `json:"medianIncomeMonth"`
	MedianExpensesMonth float64 `json:"medianExpensesMonth"`
	// Seasonal index of the baseline's remaining months, per direction.
	SeasonalIndexIncome   float64 `json:"seasonalIndexIncome"`
	SeasonalIndexExpenses float64 `json:"seasonalIndexExpenses"`

	Central ForecastMethodResult   `json:"central"`
	Methods []ForecastMethodResult `json:"methods"`
	RangeLo float64                `json:"rangeLow"`
	RangeHi float64                `json:"rangeHigh"`

	IncomeCategories  []ForecastRow `json:"incomeCategories"`
	ExpenseCategories []ForecastRow `json:"expenseCategories"`
	BottomUpIncome    float64       `json:"bottomUpIncome"`
	BottomUpExpenses  float64       `json:"bottomUpExpenses"`
	BottomUpResult    float64       `json:"bottomUpResult"`

	Months  []ForecastMonth  `json:"months"`
	Sources []ForecastSource `json:"sources"`

	PartialMonth  *ForecastMonth        `json:"partialMonth,omitempty"`
	MissingMonths []string              `json:"missingMonths,omitempty"`
	Quality       []ForecastQualityNote `json:"quality"`
	Checks        []ForecastCheck       `json:"checks"`
	Verify        []string              `json:"verify"`
}

// ── Options ───────────────────────────────────────────────────────────────

type forecastOptions struct {
	Year         int
	Baseline     int
	Through      string // YYYY-MM; empty ⇒ derive from Now
	Method       string
	JSON         bool
	Verbose      bool
	AllowMissing bool
	Now          time.Time
}

// Forecast runs `chb forecast [YYYY] [options]`.
func Forecast(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printForecastHelp()
		return nil
	}
	opts, err := parseForecastArgs(args, time.Now().In(BrusselsTZ()))
	if err != nil {
		return err
	}
	report, err := buildForecastReport(DataDir(), opts)
	if err != nil {
		return err
	}
	if opts.JSON {
		return EmitJSON(report)
	}
	printForecastReport(report, opts.Verbose)
	return nil
}

func parseForecastArgs(args []string, now time.Time) (forecastOptions, error) {
	opts := forecastOptions{
		Method:       forecastMethodRobust,
		JSON:         HasFlag(args, "--json") || GetOption(args, "--format") == "json",
		Verbose:      HasFlag(args, "--verbose", "-v", "--debug"),
		AllowMissing: HasFlag(args, "--allow-missing"),
		Now:          now,
	}

	opts.Year = now.Year()
	if arg, ok := forecastPositionalYear(args); ok {
		y, ok := parseDateSpecYear(strings.TrimSpace(arg))
		if !ok {
			return opts, fmt.Errorf("invalid year %q — `chb forecast` projects a whole year, so pass YYYY (e.g. `chb forecast %d`)", arg, now.Year())
		}
		opts.Year = y
	}

	opts.Baseline = opts.Year - 1
	if raw := strings.TrimSpace(GetOption(args, "--baseline")); raw != "" {
		y, ok := parseDateSpecYear(raw)
		if !ok {
			return opts, fmt.Errorf("invalid --baseline %q (expected YYYY, e.g. --baseline %d)", raw, opts.Year-1)
		}
		if y == opts.Year {
			return opts, fmt.Errorf("--baseline %d is the year being projected; pick a different year", y)
		}
		opts.Baseline = y
	}

	if raw := strings.TrimSpace(GetOption(args, "--through")); raw != "" {
		month, err := parseForecastThrough(raw, opts.Year)
		if err != nil {
			return opts, err
		}
		opts.Through = month
	}

	if raw := strings.TrimSpace(GetOption(args, "--method")); raw != "" {
		switch strings.ToLower(raw) {
		case forecastMethodRobust, forecastMethodRecent, forecastMethodSeasonal,
			forecastMethodFlat, forecastMethodRunRate:
			opts.Method = strings.ToLower(raw)
		case "linear": // the pre-robust name for runrate
			opts.Method = forecastMethodRunRate
		default:
			return opts, fmt.Errorf("unknown --method %q (expected robust, recent, seasonal, flat or runrate)", raw)
		}
	}

	return opts, nil
}

// forecastPositionalYear returns the first bare argument, skipping this
// command's own flags and the values they consume. firstPositionalDateArg only
// knows the shared flag list, which doesn't include --baseline / --through /
// --method — without this, `--through 2026-03` would be read as the year.
func forecastPositionalYear(args []string) (string, bool) {
	valued := map[string]bool{
		"--baseline": true,
		"--through":  true,
		"--method":   true,
		"--format":   true,
	}
	skipNext := false
	for _, a := range args {
		if skipNext {
			skipNext = false
			continue
		}
		if strings.HasPrefix(a, "-") {
			skipNext = valued[a]
			continue
		}
		return a, true
	}
	return "", false
}

// parseForecastThrough accepts YYYY-MM, YYYY/MM and YYYYMM, and requires the
// month to belong to the year being projected — a cutoff outside it would make
// "closed months" meaningless.
func parseForecastThrough(raw string, year int) (string, error) {
	normalized := strings.ReplaceAll(raw, "/", "-")
	if len(normalized) == 6 && allDigits(normalized) {
		normalized = normalized[:4] + "-" + normalized[4:]
	}
	parts := strings.Split(normalized, "-")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid --through %q (expected YYYY-MM, e.g. --through %d-06)", raw, year)
	}
	y, err := strconv.Atoi(parts[0])
	if err != nil {
		return "", fmt.Errorf("invalid --through %q (expected YYYY-MM, e.g. --through %d-06)", raw, year)
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 1 || m > 12 {
		return "", fmt.Errorf("invalid --through %q: %q is not a month 01–12", raw, parts[1])
	}
	if y != year {
		return "", fmt.Errorf("--through %s is outside the forecast year %d — the cutoff must be a month of the year being projected", raw, year)
	}
	return fmt.Sprintf("%04d-%02d", y, m), nil
}

// ── Collection ────────────────────────────────────────────────────────────

// forecastWindow is everything one [from..to] month window of one year
// contributes: totals, per-month and per-category breakdowns (both at month
// granularity, which is what the estimators need), plus which files were read.
type forecastWindow struct {
	Year     int
	From, To int // inclusive month numbers; To < From ⇒ empty window
	Total    ForecastFlow
	Months   map[int]ForecastFlow            // month number → flow
	Cats     map[string]ForecastFlow         // category → window total
	CatMonth map[string]map[int]ForecastFlow // category → month number → flow
	Sources  []ForecastSource
	Missing  []string // YYYY-MM of month files that weren't there
}

// collectForecastWindow sums the countable EUR transactions of `year` over
// months [from..to].
//
// It walks exactly the month files `chb income` / `chb expenses` would walk for
// the same range and applies the same timestamp filter, so a window total here
// equals what those commands print for the matching date range. Keep the two in
// step: the VERIFY block tells operators to compare them.
//
// Note on `chb report`: the monthly summaries carry per-category flows too, but
// they are a different quantity — they fold in VAT, fees, commissions and
// spread allocations, and they do NOT drop internal transfers. Aggregating the
// transactions here keeps one basis across `chb income`, `chb expenses` and
// this command.
func collectForecastWindow(dataDir string, year, from, to int) forecastWindow {
	w := forecastWindow{
		Year:     year,
		From:     from,
		To:       to,
		Months:   map[int]ForecastFlow{},
		Cats:     map[string]ForecastFlow{},
		CatMonth: map[string]map[int]ForecastFlow{},
	}
	if from < 1 || to > 12 || to < from {
		return w
	}
	yearStr := strconv.Itoa(year)
	loc := BrusselsTZ()
	start := time.Date(year, time.Month(from), 1, 0, 0, 0, 0, loc)
	end := time.Date(year, time.Month(to), 1, 0, 0, 0, 0, loc).AddDate(0, 1, 0)

	for m := from; m <= to; m++ {
		monthStr := fmt.Sprintf("%02d", m)
		ym := fmt.Sprintf("%d-%02d", year, m)
		src := ForecastSource{
			Month: ym,
			Path:  displayMonthRelPath(yearStr, monthStr, filepath.Join("generated", "transactions.json")),
		}
		if !fileExists(filepath.Join(dataDir, yearStr, monthStr, "generated", "transactions.json")) {
			w.Sources = append(w.Sources, src)
			w.Missing = append(w.Missing, ym)
			continue
		}
		src.Present = true
		txFile := LoadTransactionsWithPII(dataDir, yearStr, monthStr)
		if txFile == nil {
			// The file is there but unreadable/corrupt. Treat it as missing so
			// the operator is told rather than silently projecting from a hole.
			src.Present = false
			w.Sources = append(w.Sources, src)
			w.Missing = append(w.Missing, ym)
			continue
		}
		src.GeneratedAt = txFile.GeneratedAt
		src.Rows = len(txFile.Transactions)

		for _, tx := range txFile.Transactions {
			amount, ok := countableEURAmount(tx)
			if !ok {
				continue
			}
			ts := time.Unix(tx.Timestamp, 0).In(loc)
			if ts.Before(start) || !ts.Before(end) {
				continue
			}
			src.Counted++
			idx := int(ts.Month())
			cat := strings.TrimSpace(tx.Category)
			if cat == "" {
				cat = forecastUncategorized
			}
			if w.CatMonth[cat] == nil {
				w.CatMonth[cat] = map[int]ForecastFlow{}
			}
			monthFlow := w.Months[idx]
			catFlow := w.Cats[cat]
			catMonthFlow := w.CatMonth[cat][idx]
			if tx.IsIncoming() {
				w.Total.Income = roundReportAmount(w.Total.Income + amount)
				monthFlow.Income = roundReportAmount(monthFlow.Income + amount)
				catFlow.Income = roundReportAmount(catFlow.Income + amount)
				catMonthFlow.Income = roundReportAmount(catMonthFlow.Income + amount)
			} else {
				w.Total.Expenses = roundReportAmount(w.Total.Expenses + amount)
				monthFlow.Expenses = roundReportAmount(monthFlow.Expenses + amount)
				catFlow.Expenses = roundReportAmount(catFlow.Expenses + amount)
				catMonthFlow.Expenses = roundReportAmount(catMonthFlow.Expenses + amount)
			}
			w.Total.Transactions++
			monthFlow.Transactions++
			catFlow.Transactions++
			catMonthFlow.Transactions++
			w.Months[idx] = monthFlow
			w.Cats[cat] = catFlow
			w.CatMonth[cat][idx] = catMonthFlow
		}
		w.Sources = append(w.Sources, src)
	}
	return w
}

// countableEURAmount reports whether a transaction belongs in an income /
// expense aggregate and returns its absolute EUR gross amount.
//
// This is the single definition of "counts as income or expense" shared by
// `chb income`, `chb expenses` and `chb forecast`. Internal transfers are
// excluded twice over — on Type and on category — because files generated
// before Categorizer.Apply coerced the type still carry the old shape.
func countableEURAmount(tx TransactionEntry) (float64, bool) {
	if tx.Type == "INTERNAL" {
		return 0, false
	}
	if strings.EqualFold(strings.TrimSpace(tx.Category), "internal_transfer") {
		return 0, false
	}
	if !isEURCurrency(tx.Currency) {
		return 0, false
	}
	if !tx.IsIncoming() && !tx.IsOutgoing() {
		return 0, false
	}
	amount := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
	if amount == 0 {
		return 0, false
	}
	return amount, true
}

// ── Series ────────────────────────────────────────────────────────────────

// forecastLine is one projectable quantity — a direction total, or one category
// within a direction — carrying the monthly series the estimators run on.
type forecastLine struct {
	Name string
	// Actual (A) and its monthly series over the closed months, oldest first.
	Actual  float64
	Monthly []float64
	// The baseline year: its total over the closed months (P), over the whole
	// year (F), over the months still to come (R = F − P), and its own monthly
	// series over all 12 months.
	BaselineToDate   float64
	BaselineFullYear float64
	BaselineRest     float64
	BaselineMonthly  []float64
}

// forecastTotalLine builds the line for one direction's totals.
func forecastTotalLine(name string, cur, baseFull forecastWindow, pick func(ForecastFlow) float64, closed int) forecastLine {
	l := forecastLine{Name: name}
	for m := 1; m <= closed; m++ {
		v := roundCents(pick(cur.Months[m]))
		l.Monthly = append(l.Monthly, v)
		l.Actual = roundCents(l.Actual + v)
	}
	for m := 1; m <= 12; m++ {
		v := roundCents(pick(baseFull.Months[m]))
		l.BaselineMonthly = append(l.BaselineMonthly, v)
		l.BaselineFullYear = roundCents(l.BaselineFullYear + v)
		if m <= closed {
			l.BaselineToDate = roundCents(l.BaselineToDate + v)
		}
	}
	l.BaselineRest = roundCents(l.BaselineFullYear - l.BaselineToDate)
	return l
}

// forecastCategoryLine builds the line for one category within a direction.
func forecastCategoryLine(cat string, cur, baseFull forecastWindow, pick func(ForecastFlow) float64, closed int) forecastLine {
	l := forecastLine{Name: cat}
	for m := 1; m <= closed; m++ {
		v := roundCents(pick(cur.CatMonth[cat][m]))
		l.Monthly = append(l.Monthly, v)
		l.Actual = roundCents(l.Actual + v)
	}
	for m := 1; m <= 12; m++ {
		v := roundCents(pick(baseFull.CatMonth[cat][m]))
		l.BaselineMonthly = append(l.BaselineMonthly, v)
		l.BaselineFullYear = roundCents(l.BaselineFullYear + v)
		if m <= closed {
			l.BaselineToDate = roundCents(l.BaselineToDate + v)
		}
	}
	l.BaselineRest = roundCents(l.BaselineFullYear - l.BaselineToDate)
	return l
}

// baselineActiveMonths counts the baseline months in [from..to] that booked
// anything. Months, not euros, are what say whether a line is a real recurring
// series or a single stray transaction.
func (l forecastLine) baselineActiveMonths(from, to int) int {
	n := 0
	for m := from; m <= to && m <= len(l.BaselineMonthly); m++ {
		if m >= 1 && l.BaselineMonthly[m-1] > 0 {
			n++
		}
	}
	return n
}

// trustBaselineShape reports whether this line's own baseline is thick enough
// to scale against: enough money, spread over enough months, on both sides of
// the cutoff.
func (l forecastLine) trustBaselineShape(closed int) bool {
	return l.BaselineToDate >= forecastMinBaselineAmount &&
		l.baselineActiveMonths(1, closed) >= forecastMinBaselineMonths &&
		l.baselineActiveMonths(closed+1, 12) >= 1
}

func forecastMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64{}, values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return roundCents(sorted[mid])
	}
	return roundCents((sorted[mid-1] + sorted[mid]) / 2)
}

func forecastLastN(values []float64, n int) []float64 {
	if n <= 0 || len(values) <= n {
		return values
	}
	return values[len(values)-n:]
}

// forecastSeasonalIndex answers "in the baseline year, how heavy was a month of
// the remaining window compared with a month of the closed window?" — 1.0 means
// the same. This is a SHAPE, which is far more stable year over year than a
// level, and it is the only thing the baseline is allowed to contribute to the
// robust estimators.
func forecastSeasonalIndex(baseToDate, baseRest float64, closed, remaining int) (float64, bool) {
	if closed <= 0 || remaining <= 0 || baseToDate <= 0 {
		return 1, false
	}
	return (baseRest / float64(remaining)) / (baseToDate / float64(closed)), true
}

// forecastLevelRatio is A ÷ P for a line, clamped. Only ever used on the totals
// (or borrowed from them), never on a thin category — that division is what
// produced a ×1045 multiplier on live data.
func forecastLevelRatio(l forecastLine) (float64, bool) {
	if l.BaselineToDate <= 0 {
		return 1, false
	}
	ratio := l.Actual / l.BaselineToDate
	if !forecastFactorUsable(ratio) {
		return ratio, false
	}
	return ratio, true
}

// forecastContextFor builds the direction-level fallbacks thin lines borrow,
// and reports anything the reader should know about them.
//
// A factor that had to be capped is not a measurement — it says the two halves
// of the baseline year are too far apart to carry information. Lines that would
// have borrowed it fall back to no adjustment at all (×1.00) instead of
// inheriting the cap, which would multiply every thin row by the maximum.
func forecastContextFor(l forecastLine, closed, remaining int) (forecastContext, []string) {
	var notes []string
	ctx := forecastContext{Index: 1, Ratio: 1}

	if idx, ok := forecastSeasonalIndex(l.BaselineToDate, l.BaselineRest, closed, remaining); ok {
		if forecastFactorUsable(idx) {
			ctx.Index = idx
		} else {
			notes = append(notes, fmt.Sprintf("the overall %s seasonal index came out at ×%.2f, outside ×%.2f…×%.2f", l.Name, idx, forecastMinFactor, forecastMaxFactor))
		}
	}
	if ratio, ok := forecastLevelRatio(l); ok {
		ctx.Ratio = ratio
	} else if l.BaselineToDate > 0 {
		notes = append(notes, fmt.Sprintf("the overall %s growth ratio came out at ×%.2f, outside ×%.2f…×%.2f", l.Name, ratio, forecastMinFactor, forecastMaxFactor))
	}
	return ctx, notes
}

// forecastFactorUsable reports whether a multiplier is inside the band where it
// still carries information.
//
// Outside the band the factor is REJECTED, never capped. Capping looks safe and
// isn't: it quietly applies the maximum to exactly the lines that earned the
// least trust, which is how a category with €4.00 of baseline ended up scaling
// its remainder by the cap. A rejected factor falls back to the next broader
// estimate — a category borrows its direction's shape, a direction falls back
// to no seasonal adjustment at all.
func forecastFactorUsable(v float64) bool {
	return v >= forecastMinFactor && v <= forecastMaxFactor
}

// ── Estimators ────────────────────────────────────────────────────────────

// forecastContext carries the direction-level fallbacks a single line may
// borrow when its own baseline is too thin to mean anything. Both are computed
// on the totals, where the baseline is thick by construction — that is the
// whole point: a shaky line leans on a solid aggregate instead of on its own
// near-zero denominator.
type forecastContext struct {
	Index float64 // seasonal shape: how heavy a remaining month is
	Ratio float64 // level ratio A ÷ P, clamped
}

// estimateForecastLine projects the months still to come for one line.
//
// ctx holds the direction-level seasonal shape and level ratio, computed from
// the totals. A category falls back to them whenever its own baseline is too
// thin to trust — which is exactly the case that used to produce absurd
// projections.
func estimateForecastLine(l forecastLine, closed, remaining int, method string, ctx forecastContext) ForecastEstimate {
	if remaining <= 0 {
		return ForecastEstimate{
			Method:  forecastRowClosed,
			Index:   1,
			Formula: fmt.Sprintf("%s (year complete, nothing to project)", forecastAmount(l.Actual)),
		}
	}
	if closed == 0 {
		// Nothing booked this year yet: the baseline year IS the projection.
		return ForecastEstimate{
			Remainder:   roundCents(l.BaselineRest),
			Method:      forecastRowBaseline,
			Level:       roundCents(l.BaselineRest / float64(remaining)),
			LevelKind:   "baseline month",
			Index:       1,
			IndexSource: "none",
			Formula: fmt.Sprintf("0.00 + %s (the baseline's remaining months, unscaled) = %s",
				forecastAmount(l.BaselineRest), forecastAmount(l.BaselineRest)),
		}
	}

	est := ForecastEstimate{Index: 1, IndexSource: "none"}

	// flat — the baseline's remaining months, repeated. Stated as a per-month
	// level so the table's arithmetic identity (REMAINDER = PER MONTH × months
	// left × SEASON) holds on every row of every method.
	if method == forecastMethodFlat {
		est.Method = forecastRowFlat
		est.Level = roundCents(l.BaselineRest / float64(remaining))
		est.LevelKind = "baseline month"
		est.Remainder = roundCents(l.BaselineRest)
		est.Formula = fmt.Sprintf("%s + %s (the baseline's remaining months, repeated) = %s",
			forecastAmount(l.Actual), forecastAmount(l.BaselineRest),
			forecastAmount(roundCents(l.Actual+l.BaselineRest)))
		return est
	}

	// runrate — the mean month, no seasonality and no baseline at all.
	if method == forecastMethodRunRate {
		est.Method = forecastRowRunRate
		est.Level = roundCents(l.Actual / float64(closed))
		est.LevelKind = "mean"
		est.Remainder = roundCents(est.Level * float64(remaining))
		est.Formula = fmt.Sprintf("%s + %s ÷ %d × %d = %s",
			forecastAmount(l.Actual), forecastAmount(l.Actual), closed, remaining,
			forecastAmount(roundCents(l.Actual+est.Remainder)))
		return est
	}

	// Everything below applies a seasonal shape. Use the line's own only when
	// its baseline is thick enough to mean something; otherwise borrow the
	// direction's, which is measured on the totals.
	trusted := l.trustBaselineShape(closed)
	ownIndex, hasOwn := 0.0, false
	if trusted {
		if idx, ok := forecastSeasonalIndex(l.BaselineToDate, l.BaselineRest, closed, remaining); ok {
			ownIndex, hasOwn = idx, true
		}
	}
	switch {
	case hasOwn && forecastFactorUsable(ownIndex):
		est.Index, est.IndexSource = ownIndex, "own"
	case hasOwn:
		// The line's own shape is outside the band — reject it and borrow the
		// direction's rather than applying the extreme.
		est.Index, est.IndexSource = ctx.Index, "overall"
		est.Flags = append(est.Flags, "rejected-index")
	case ctx.Index > 0:
		est.Index, est.IndexSource = ctx.Index, "overall"
		if l.BaselineFullYear > 0 || l.Actual > 0 {
			est.Flags = append(est.Flags, "thin-baseline")
		}
	}

	// A line that booked nothing at all this year has no level to project. If
	// the baseline booked something in the months still to come — a seasonal
	// category that simply hasn't started — carry that over at the direction's
	// overall growth rather than projecting a flat zero.
	if l.Actual == 0 && l.BaselineRest > 0 {
		est.Method = forecastRowBaseline
		est.Level = roundCents(l.BaselineRest / float64(remaining))
		est.LevelKind = "baseline month"
		est.Index, est.IndexSource = ctx.Ratio, "overall"
		est.Remainder = roundCents(l.BaselineRest * ctx.Ratio)
		est.Formula = fmt.Sprintf("0.00 + %s × %.3f = %s   (nothing booked this year — the baseline's remaining months at the overall growth rate)",
			forecastAmount(l.BaselineRest), ctx.Ratio, forecastAmount(est.Remainder))
		return est
	}

	// seasonal — the year-to-date level ratio. Only meaningful against a thick
	// baseline; otherwise it is a division by noise, so drop to the median
	// estimator rather than printing a number nobody should act on.
	if method == forecastMethodSeasonal {
		ratio := 0.0
		if l.BaselineToDate > 0 {
			ratio = l.Actual / l.BaselineToDate
		}
		if trusted && forecastFactorUsable(ratio) {
			est.Method = forecastRowSeasonal
			est.Index, est.IndexSource = ratio, "own"
			est.Level = roundCents(l.BaselineRest / float64(remaining))
			est.LevelKind = "baseline month"
			est.Remainder = roundCents(l.BaselineRest * ratio)
			est.Formula = fmt.Sprintf("%s + %s × %.3f = %s   (ratio = %s ÷ %s)",
				forecastAmount(l.Actual), forecastAmount(l.BaselineRest), ratio,
				forecastAmount(roundCents(l.Actual+est.Remainder)),
				forecastAmount(l.Actual), forecastAmount(l.BaselineToDate))
			return est
		}
		flag := "thin-baseline"
		if trusted {
			flag = "rejected-ratio"
		}
		if !forecastHasFlag(est.Flags, flag) {
			est.Flags = append(est.Flags, flag)
		}
	}

	// robust / recent (and the seasonal fallback): a median level, so one
	// outsized month can't run away with the projection.
	if method == forecastMethodRecent {
		window := forecastLastN(l.Monthly, forecastRecentMonths)
		est.Method = forecastRowRecent
		est.Level = forecastMedian(window)
		est.LevelKind = fmt.Sprintf("median of the last %d months", len(window))
	} else {
		est.Method = forecastRowRobust
		est.Level = forecastMedian(l.Monthly)
		est.LevelKind = "median month"
	}

	// A lumpy line (quarterly rent, one annual grant) has a median of zero even
	// though real money moved. The mean is the unbiased estimator for a total,
	// so use it rather than projecting nothing.
	if est.Level == 0 && l.Actual > 0 {
		est.Level = roundCents(l.Actual / float64(closed))
		est.LevelKind = "mean (lumpy: the median month is 0)"
	}

	est.Remainder = roundCents(est.Level * float64(remaining) * est.Index)
	est.Formula = fmt.Sprintf("%s + %s × %d × %.2f = %s   (%s)",
		forecastAmount(l.Actual), forecastAmount(est.Level), remaining, est.Index,
		forecastAmount(roundCents(l.Actual+est.Remainder)), est.LevelKind)
	return est
}

// buildForecastRows projects every category of one direction.
func buildForecastRows(cur, baseFull forecastWindow, pick func(ForecastFlow) float64, closed, remaining int, method string, ctx forecastContext) []ForecastRow {
	names := map[string]struct{}{}
	for cat, f := range cur.Cats {
		if pick(f) != 0 {
			names[cat] = struct{}{}
		}
	}
	for cat, f := range baseFull.Cats {
		if pick(f) != 0 {
			names[cat] = struct{}{}
		}
	}

	rows := make([]ForecastRow, 0, len(names))
	for cat := range names {
		line := forecastCategoryLine(cat, cur, baseFull, pick, closed)
		est := estimateForecastLine(line, closed, remaining, method, ctx)
		rows = append(rows, ForecastRow{
			Category:          cat,
			Actual:            line.Actual,
			Monthly:           line.Monthly,
			BaselineToDate:    line.BaselineToDate,
			BaselineFullYear:  line.BaselineFullYear,
			BaselineRemainder: line.BaselineRest,
			Estimate:          est,
			Projected:         roundCents(line.Actual + est.Remainder),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Projected != rows[j].Projected {
			return rows[i].Projected > rows[j].Projected
		}
		return rows[i].Category < rows[j].Category
	})
	return rows
}

func sumForecastRows(rows []ForecastRow, pick func(ForecastRow) float64) float64 {
	total := 0.0
	for _, r := range rows {
		total = roundCents(total + pick(r))
	}
	return total
}

// forecastMethodLabels describes each method in one line, for the METHODS block.
func forecastMethodLabel(method string, closed, remaining int) string {
	switch method {
	case forecastMethodRobust:
		return fmt.Sprintf("median month × %d × seasonal index", remaining)
	case forecastMethodRecent:
		n := forecastRecentMonths
		if closed < n {
			n = closed
		}
		return fmt.Sprintf("median of the last %d months × %d × seasonal index", n, remaining)
	case forecastMethodSeasonal:
		return "baseline remainder × this year's level ratio (A ÷ P)"
	case forecastMethodFlat:
		return "baseline's remaining months, repeated unchanged"
	case forecastMethodRunRate:
		return fmt.Sprintf("mean month so far × %d, no seasonality", remaining)
	}
	return method
}

// ── Report assembly ───────────────────────────────────────────────────────

func buildForecastReport(dataDir string, opts forecastOptions) (*ForecastReport, error) {
	closed, err := resolveForecastClosedMonths(opts)
	if err != nil {
		return nil, err
	}
	remaining := 12 - closed

	cur := collectForecastWindow(dataDir, opts.Year, 1, closed)
	baseYTD := collectForecastWindow(dataDir, opts.Baseline, 1, closed)
	baseFull := collectForecastWindow(dataDir, opts.Baseline, 1, 12)

	missing := append(append([]string{}, cur.Missing...), baseFull.Missing...)
	sort.Strings(missing)
	if len(missing) > 0 && !opts.AllowMissing {
		return nil, forecastMissingDataError(missing)
	}

	// R = F − P, by construction, so the three windows always satisfy
	// F = P + R and the operator can check each side with `chb income`.
	baseRest := ForecastFlow{
		Income:       roundCents(baseFull.Total.Income - baseYTD.Total.Income),
		Expenses:     roundCents(baseFull.Total.Expenses - baseYTD.Total.Expenses),
		Transactions: baseFull.Total.Transactions - baseYTD.Total.Transactions,
	}

	pickIncome := func(f ForecastFlow) float64 { return f.Income }
	pickExpenses := func(f ForecastFlow) float64 { return f.Expenses }

	incomeLine := forecastTotalLine("income", cur, baseFull, pickIncome, closed)
	expenseLine := forecastTotalLine("expenses", cur, baseFull, pickExpenses, closed)

	// The direction-level seasonal index, measured on the totals — where the
	// baseline is thick by construction. Reported as measured; the context
	// below decides whether it is fit to be borrowed by thinner lines.
	incomeIndex, _ := forecastSeasonalIndex(incomeLine.BaselineToDate, incomeLine.BaselineRest, closed, remaining)
	expenseIndex, _ := forecastSeasonalIndex(expenseLine.BaselineToDate, expenseLine.BaselineRest, closed, remaining)
	incomeCtx, incomeCtxNotes := forecastContextFor(incomeLine, closed, remaining)
	expenseCtx, expenseCtxNotes := forecastContextFor(expenseLine, closed, remaining)
	rejectedFactors := append(incomeCtxNotes, expenseCtxNotes...)

	// Every method, applied to the totals. The spread between them is the
	// honest uncertainty of the projection.
	var methods []ForecastMethodResult
	var central ForecastMethodResult
	for _, m := range []string{forecastMethodRobust, forecastMethodRecent, forecastMethodRunRate, forecastMethodFlat, forecastMethodSeasonal} {
		inc := estimateForecastLine(incomeLine, closed, remaining, m, incomeCtx)
		exp := estimateForecastLine(expenseLine, closed, remaining, m, expenseCtx)
		res := ForecastMethodResult{
			Method:            m,
			Label:             forecastMethodLabel(m, closed, remaining),
			RemainderIncome:   inc.Remainder,
			RemainderExpenses: exp.Remainder,
			Income:            roundCents(incomeLine.Actual + inc.Remainder),
			Expenses:          roundCents(expenseLine.Actual + exp.Remainder),
			IncomeFormula:     inc.Formula,
			ExpensesFormula:   exp.Formula,
		}
		res.Result = roundCents(res.Income - res.Expenses)
		if m == opts.Method {
			res.Central = true
			central = res
		}
		methods = append(methods, res)
	}

	incomeRows := buildForecastRows(cur, baseFull, pickIncome, closed, remaining, opts.Method, incomeCtx)
	expenseRows := buildForecastRows(cur, baseFull, pickExpenses, closed, remaining, opts.Method, expenseCtx)
	bottomUpIncome := sumForecastRows(incomeRows, func(r ForecastRow) float64 { return r.Projected })
	bottomUpExpenses := sumForecastRows(expenseRows, func(r ForecastRow) float64 { return r.Projected })

	lo, hi := methods[0].Result, methods[0].Result
	for _, m := range methods {
		lo = math.Min(lo, m.Result)
		hi = math.Max(hi, m.Result)
	}

	report := &ForecastReport{
		Year:                  strconv.Itoa(opts.Year),
		BaselineYear:          strconv.Itoa(opts.Baseline),
		Method:                opts.Method,
		Currency:              "EUR",
		Basis:                 "cash — the same transactions `chb income` / `chb expenses` count (EUR family, internal transfers excluded, attributed by timestamp in Europe/Brussels)",
		ClosedMonths:          closed,
		RemainingMonths:       remaining,
		ClosedLabel:           forecastMonthRangeLabel(1, closed),
		RemainingLabel:        forecastMonthRangeLabel(closed+1, 12),
		DataDir:               dataDir,
		GeneratedAt:           opts.Now.Format(time.RFC3339),
		Actual:                cur.Total,
		BaselineToDate:        baseYTD.Total,
		BaselineFullYear:      baseFull.Total,
		BaselineRemainder:     baseRest,
		MedianIncomeMonth:     forecastMedian(incomeLine.Monthly),
		MedianExpensesMonth:   forecastMedian(expenseLine.Monthly),
		SeasonalIndexIncome:   incomeIndex,
		SeasonalIndexExpenses: expenseIndex,
		Central:               central,
		Methods:               methods,
		RangeLo:               roundCents(lo),
		RangeHi:               roundCents(hi),
		IncomeCategories:      incomeRows,
		ExpenseCategories:     expenseRows,
		BottomUpIncome:        bottomUpIncome,
		BottomUpExpenses:      bottomUpExpenses,
		BottomUpResult:        roundCents(bottomUpIncome - bottomUpExpenses),
		Months:                forecastMonthRows(cur, baseFull),
		Sources:               forecastSources(cur, baseFull),
		MissingMonths:         missing,
	}
	if closed > 0 {
		report.Through = fmt.Sprintf("%d-%02d", opts.Year, closed)
	}
	report.PartialMonth = forecastPartialMonth(dataDir, opts, closed)
	report.Quality = forecastQualityNotes(report, cur, incomeLine, expenseLine, rejectedFactors)
	report.Checks = forecastChecks(report, cur, baseYTD, baseFull, incomeRows, expenseRows)
	report.Verify = forecastVerifyCommands(report, opts)
	return report, nil
}

// resolveForecastClosedMonths decides how many months of the target year count
// as actuals. The boundary is always a whole month: comparing Jan–Aug against
// Jan–Aug is something an operator can re-derive, while a mid-month cutoff
// would need proration nobody can check by hand.
func resolveForecastClosedMonths(opts forecastOptions) (int, error) {
	if opts.Through != "" {
		m, err := strconv.Atoi(opts.Through[5:])
		if err != nil {
			return 0, fmt.Errorf("invalid --through %q", opts.Through)
		}
		return m, nil
	}
	switch {
	case opts.Year < opts.Now.Year():
		return 12, nil
	case opts.Year > opts.Now.Year():
		return 0, nil
	default:
		// The month in progress is not an actual — it goes into the projection
		// together with the months after it.
		return int(opts.Now.Month()) - 1, nil
	}
}

func forecastMissingDataError(missing []string) error {
	shown := make([]string, 0, len(missing))
	for _, m := range missing {
		shown = append(shown, strings.ReplaceAll(m, "-", "/"))
	}
	if len(shown) > 6 {
		shown = append(append([]string{}, shown[:6]...), fmt.Sprintf("… (+%d more)", len(missing)-6))
	}
	// One month: name it directly. Several: a --since window covers them all in
	// one pass, including any gap between them.
	scope := strings.ReplaceAll(missing[0], "-", "/")
	if len(missing) > 1 {
		scope = "--since " + missing[0]
	}
	return fmt.Errorf(`no generated/transactions.json for %d month(s): %s

A forecast over a hole in the data would silently count those months as zero.
Pull and generate them first:
  chb pull %s
  chb generate %s

If those months genuinely have no transactions, re-run with --allow-missing`,
		len(missing), strings.Join(shown, ", "), scope, scope)
}

// forecastPartialMonth reports what the month in progress has booked so far.
// It is deliberately NOT part of the actuals: including half a month would
// drag every run rate down. Shown so the operator can sanity-check the
// projection against what's already on the account.
func forecastPartialMonth(dataDir string, opts forecastOptions, closed int) *ForecastMonth {
	if opts.Year != opts.Now.Year() || closed >= 12 {
		return nil
	}
	m := closed + 1
	if m != int(opts.Now.Month()) {
		// Only the month actually in progress is "partial"; a --through cutoff
		// in the past leaves whole closed months in the projected window.
		return nil
	}
	w := collectForecastWindow(dataDir, opts.Year, m, m)
	if len(w.Missing) > 0 {
		return nil
	}
	return &ForecastMonth{
		Month:        fmt.Sprintf("%d-%02d", opts.Year, m),
		Income:       roundCents(w.Total.Income),
		Expenses:     roundCents(w.Total.Expenses),
		Result:       w.Total.Result(),
		Transactions: w.Total.Transactions,
	}
}

// forecastSources lists every month file the scan looked for, oldest first, so
// the operator can walk the inputs in the order a ledger would.
func forecastSources(windows ...forecastWindow) []ForecastSource {
	var out []ForecastSource
	for _, w := range windows {
		out = append(out, w.Sources...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Month < out[j].Month })
	return out
}

func forecastMonthRows(cur, baseFull forecastWindow) []ForecastMonth {
	rows := []ForecastMonth{}
	for _, w := range []forecastWindow{baseFull, cur} {
		for m := w.From; m <= w.To; m++ {
			f := w.Months[m]
			rows = append(rows, ForecastMonth{
				Month:        fmt.Sprintf("%d-%02d", w.Year, m),
				Income:       roundCents(f.Income),
				Expenses:     roundCents(f.Expenses),
				Result:       f.Result(),
				Transactions: f.Transactions,
			})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Month < rows[j].Month })
	return rows
}

// forecastQualityNotes is the part of the report that says how much the number
// above it is worth. Categorisation coverage, outlier months and thin baselines
// are the three things that actually decide whether a projection means anything.
func forecastQualityNotes(r *ForecastReport, cur forecastWindow, incomeLine, expenseLine forecastLine, rejectedFactors []string) []ForecastQualityNote {
	var notes []ForecastQualityNote
	warn := func(format string, args ...interface{}) {
		notes = append(notes, ForecastQualityNote{Level: "warn", Text: fmt.Sprintf(format, args...)})
	}
	info := func(format string, args ...interface{}) {
		notes = append(notes, ForecastQualityNote{Level: "info", Text: fmt.Sprintf(format, args...)})
	}

	switch r.ClosedMonths {
	case 0:
		warn("No closed month of %s yet — the projection is %s's actuals, unscaled.", r.Year, r.BaselineYear)
	case 12:
		info("%s is complete: the figures below are actuals, not a projection.", r.Year)
	case 1, 2:
		warn("Only %d closed month(s) of data — every estimator is volatile this early in the year.", r.ClosedMonths)
	}

	// Categorisation coverage. This is the single biggest driver of whether the
	// category tables are worth reading.
	uncatIncome := cur.Cats[forecastUncategorized].Income
	uncatExpenses := cur.Cats[forecastUncategorized].Expenses
	if share := forecastShare(uncatIncome, cur.Total.Income); share >= 0.2 {
		warn("%.0f%% of %s %s income is uncategorised (%s of %s) — read the total, not the income rows.",
			share*100, r.Year, r.ClosedLabel, forecastAmount(uncatIncome), forecastAmount(cur.Total.Income))
	}
	if share := forecastShare(uncatExpenses, cur.Total.Expenses); share >= 0.2 {
		warn("%.0f%% of %s %s expenses are uncategorised (%s of %s) — read the total, not the expense rows.",
			share*100, r.Year, r.ClosedLabel, forecastAmount(uncatExpenses), forecastAmount(cur.Total.Expenses))
	}

	// Outlier months: never dropped, always named, because a mean-based run
	// rate inherits them and a median-based one does not.
	for _, o := range forecastOutlierMonths(incomeLine, r.Year, "income") {
		warn("%s", o)
	}
	for _, o := range forecastOutlierMonths(expenseLine, r.Year, "expenses") {
		warn("%s", o)
	}

	for _, note := range rejectedFactors {
		warn("Rejected: %s — it was discarded rather than capped, so the rows that would have borrowed it carry no seasonal adjustment.", note)
	}

	thin, rejected := 0, 0
	for _, row := range append(append([]ForecastRow{}, r.IncomeCategories...), r.ExpenseCategories...) {
		if forecastHasFlag(row.Estimate.Flags, "thin-baseline") {
			thin++
		}
		if forecastHasFlag(row.Estimate.Flags, "rejected-index") || forecastHasFlag(row.Estimate.Flags, "rejected-ratio") {
			rejected++
		}
	}
	if thin > 0 {
		info("%d category row(s) borrow the overall seasonal shape: their own %s baseline is under %s EUR or spans fewer than %d months.",
			thin, r.BaselineYear, forecastAmount(forecastMinBaselineAmount), forecastMinBaselineMonths)
	}
	if rejected > 0 {
		info("%d category row(s) had their own factor rejected for landing outside ×%.2f…×%.2f, and borrow the overall shape instead.",
			rejected, forecastMinFactor, forecastMaxFactor)
	}

	if spread := roundCents(r.BottomUpResult - r.Central.Result); math.Abs(spread) > 0.005 {
		info("Category rows sum to %s against the %s central estimate of %s — a spread of %s.",
			forecastSigned(r.BottomUpResult), r.Method, forecastSigned(r.Central.Result), forecastSigned(spread))
	}
	if len(r.MissingMonths) > 0 {
		warn("Counted as zero (--allow-missing): %s.", strings.Join(r.MissingMonths, ", "))
	}
	return notes
}

func forecastShare(part, whole float64) float64 {
	if whole <= 0 {
		return 0
	}
	return part / whole
}

func forecastHasFlag(flags []string, want string) bool {
	for _, f := range flags {
		if f == want {
			return true
		}
	}
	return false
}

// forecastOutlierMonths names the closed months that tower over the median one.
func forecastOutlierMonths(l forecastLine, year, label string) []string {
	median := forecastMedian(l.Monthly)
	if median <= 0 {
		return nil
	}
	var out []string
	for i, v := range l.Monthly {
		if v > median*forecastOutlierFactor {
			out = append(out, fmt.Sprintf("Outlier month %s-%02d: %s %s against a %s median — a mean-based run rate inherits it, the median does not.",
				year, i+1, label, forecastAmount(v), forecastAmount(median)))
		}
	}
	return out
}

func forecastChecks(r *ForecastReport, cur, baseYTD, baseFull forecastWindow, incomeRows, expenseRows []ForecastRow) []ForecastCheck {
	checks := []ForecastCheck{}

	checks = append(checks, forecastCoverageCheck(r.Year+" "+r.ClosedLabel+" month files", cur))
	checks = append(checks, forecastCoverageCheck(r.BaselineYear+" full year month files", baseFull))

	// Independently re-add the category rows and the monthly rows; both must
	// reproduce the window totals the header prints.
	checks = append(checks,
		forecastValueCheck(r.Year+" actual income = Σ category rows",
			cur.Total.Income, sumForecastRows(incomeRows, func(x ForecastRow) float64 { return x.Actual })),
		forecastValueCheck(r.Year+" actual expenses = Σ category rows",
			cur.Total.Expenses, sumForecastRows(expenseRows, func(x ForecastRow) float64 { return x.Actual })),
		forecastValueCheck(r.Year+" actual income = Σ monthly rows",
			cur.Total.Income, forecastSumMonths(r.Months, r.Year, func(m ForecastMonth) float64 { return m.Income })),
		forecastValueCheck(r.Year+" actual expenses = Σ monthly rows",
			cur.Total.Expenses, forecastSumMonths(r.Months, r.Year, func(m ForecastMonth) float64 { return m.Expenses })),
		forecastValueCheck(r.BaselineYear+" full year income = "+r.ClosedLabel+" + "+r.RemainingLabel,
			baseFull.Total.Income, roundCents(baseYTD.Total.Income+r.BaselineRemainder.Income)),
		forecastValueCheck(r.BaselineYear+" full year expenses = "+r.ClosedLabel+" + "+r.RemainingLabel,
			baseFull.Total.Expenses, roundCents(baseYTD.Total.Expenses+r.BaselineRemainder.Expenses)),
		forecastValueCheck("central projected income = actual + projected remainder",
			r.Central.Income, roundCents(cur.Total.Income+r.Central.RemainderIncome)),
		forecastValueCheck("central projected expenses = actual + projected remainder",
			r.Central.Expenses, roundCents(cur.Total.Expenses+r.Central.RemainderExpenses)),
		forecastValueCheck("central projected result = income − expenses",
			r.Central.Result, roundCents(r.Central.Income-r.Central.Expenses)),
	)
	return checks
}

func forecastCoverageCheck(name string, w forecastWindow) ForecastCheck {
	total := w.To - w.From + 1
	if total < 0 {
		total = 0
	}
	present := total - len(w.Missing)
	c := ForecastCheck{
		Name:   name,
		OK:     len(w.Missing) == 0,
		Detail: fmt.Sprintf("%d/%d present", present, total),
	}
	if !c.OK {
		c.Detail += " — missing " + strings.Join(w.Missing, ", ")
	}
	return c
}

func forecastValueCheck(name string, got, want float64) ForecastCheck {
	return ForecastCheck{
		Name:   name,
		OK:     math.Abs(got-want) < 0.005,
		Detail: fmt.Sprintf("%s = %s", forecastAmount(got), forecastAmount(want)),
	}
}

func forecastSumMonths(months []ForecastMonth, year string, pick func(ForecastMonth) float64) float64 {
	total := 0.0
	for _, m := range months {
		if strings.HasPrefix(m.Month, year+"-") {
			total = roundCents(total + pick(m))
		}
	}
	return total
}

// forecastVerifyCommands lists the commands that re-derive each input of the
// projection, with the value each one must print. Anything that can't be
// checked this way isn't an input — it's arithmetic the CHECKS block re-adds.
func forecastVerifyCommands(r *ForecastReport, opts forecastOptions) []string {
	type step struct{ command, expect string }
	var steps []step
	if r.ClosedMonths > 0 {
		window := forecastDateRangeArg(opts.Year, 1, r.ClosedMonths)
		baseWindow := forecastDateRangeArg(opts.Baseline, 1, r.ClosedMonths)
		steps = append(steps,
			step{"chb income " + window, fmt.Sprintf("%s EUR (A, actual income)", forecastAmount(r.Actual.Income))},
			step{"chb expenses " + window, fmt.Sprintf("%s EUR (A, actual expenses)", forecastAmount(r.Actual.Expenses))},
			step{"chb income " + baseWindow, fmt.Sprintf("%s EUR (P, baseline income)", forecastAmount(r.BaselineToDate.Income))},
			step{"chb expenses " + baseWindow, fmt.Sprintf("%s EUR (P, baseline expenses)", forecastAmount(r.BaselineToDate.Expenses))},
		)
	}
	steps = append(steps,
		step{"chb income " + r.BaselineYear, fmt.Sprintf("%s EUR (F, baseline income)", forecastAmount(r.BaselineFullYear.Income))},
		step{"chb expenses " + r.BaselineYear, fmt.Sprintf("%s EUR (F, baseline expenses)", forecastAmount(r.BaselineFullYear.Expenses))},
		step{"chb forecast " + r.Year + " --verbose", "the monthly series every median is taken over"},
		step{"chb forecast " + r.Year + " --json", "every input above, machine-readable"},
	)

	width := 0
	for _, s := range steps {
		if len(s.command) > width {
			width = len(s.command)
		}
	}
	out := make([]string, 0, len(steps))
	for _, s := range steps {
		out = append(out, fmt.Sprintf("%-*s  → %s", width, s.command, s.expect))
	}
	return out
}

// forecastDateRangeArg renders a whole-month window as the YYYYMMDD-YYYYMMDD
// argument `chb income` / `chb expenses` accept.
func forecastDateRangeArg(year, from, to int) string {
	start := time.Date(year, time.Month(from), 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(year, time.Month(to), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, -1)
	return start.Format("20060102") + "-" + end.Format("20060102")
}

// ── Formatting helpers ────────────────────────────────────────────────────

func forecastAmount(v float64) string {
	if v < 0 {
		return "-" + fmtNumber(math.Abs(v))
	}
	return fmtNumber(v)
}

func forecastSigned(v float64) string {
	if v < 0 {
		return "-" + fmtNumber(math.Abs(v))
	}
	return "+" + fmtNumber(v)
}

func forecastFactor(v float64) string {
	if v == 0 {
		return "—"
	}
	return fmt.Sprintf("×%.2f", v)
}

// forecastMonthRangeLabel renders "Jan–Aug", "Sep–Dec", "Dec" or "—".
func forecastMonthRangeLabel(from, to int) string {
	if from > to || from < 1 || to > 12 {
		return "—"
	}
	if from == to {
		return time.Month(from).String()[:3]
	}
	return time.Month(from).String()[:3] + "–" + time.Month(to).String()[:3]
}

// ── Printing ──────────────────────────────────────────────────────────────

func printForecastReport(r *ForecastReport, verbose bool) {
	f := Fmt
	fmt.Printf("\n%s📈 Projected %s result — %s%s\n", f.Bold, r.Year, r.Central.Label, f.Reset)
	if r.ClosedMonths > 0 {
		fmt.Printf("   %sActuals %s %s (%d of 12 months closed) · baseline %s · basis: cash, EUR%s\n",
			f.Dim, r.Year, r.ClosedLabel, r.ClosedMonths, r.BaselineYear, f.Reset)
	} else {
		fmt.Printf("   %sNo closed month of %s · baseline %s · basis: cash, EUR%s\n", f.Dim, r.Year, r.BaselineYear, f.Reset)
	}
	fmt.Printf("   %sSource: %s/<year>/<month>/generated/transactions.json%s\n", f.Dim, r.DataDir, f.Reset)

	printForecastResult(r)
	printForecastMethods(r)
	printForecastQuality(r)
	printForecastBasis(r)
	printForecastCategoryTable("Income", r.IncomeCategories, r, f.Green)
	printForecastCategoryTable("Expenses", r.ExpenseCategories, r, f.Red)
	if verbose {
		printForecastMonths(r)
		printForecastSources(r)
	}
	printForecastChecks(r)
	printForecastVerify(r)

	if !verbose {
		fmt.Printf("%s↪ Per-month actuals and the files behind them: chb forecast %s --verbose%s\n", f.Dim, r.Year, f.Reset)
	}
	fmt.Println()
}

func printForecastResult(r *ForecastReport) {
	f := Fmt
	color := f.Green
	if r.Central.Result < 0 {
		color = f.Red
	}
	fmt.Printf("\n%sRESULT%s %s(income − expenses, EUR — what's left at year end)%s\n", f.Bold, f.Reset, f.Dim, f.Reset)
	fmt.Printf("  %-40s %16s\n", fmt.Sprintf("actual     %s %s", r.Year, r.ClosedLabel), forecastSigned(r.Actual.Result()))
	fmt.Printf("  %-40s %16s\n", fmt.Sprintf("projected  %s %s", r.Year, r.RemainingLabel),
		forecastSigned(roundCents(r.Central.RemainderIncome-r.Central.RemainderExpenses)))
	fmt.Printf("  %s%s%s\n", f.Dim, strings.Repeat("─", 57), f.Reset)
	fmt.Printf("  %s%-40s %s%16s%s\n", f.Bold, fmt.Sprintf("projected  %s full year", r.Year), color, forecastSigned(r.Central.Result), f.Reset)
	fmt.Printf("  %s%-40s %16s%s\n", f.Dim, fmt.Sprintf("range across the methods below"),
		forecastSigned(r.RangeLo)+" … "+forecastSigned(r.RangeHi), f.Reset)
	fmt.Printf("  %-40s %16s\n", fmt.Sprintf("actual     %s full year", r.BaselineYear), forecastSigned(r.BaselineFullYear.Result()))
	fmt.Printf("  %-40s %16s\n", fmt.Sprintf("change vs %s", r.BaselineYear),
		forecastSigned(roundCents(r.Central.Result-r.BaselineFullYear.Result())))
	fmt.Printf("\n  %sprojected income %s − expenses %s = %s%s\n", f.Dim,
		forecastAmount(r.Central.Income), forecastAmount(r.Central.Expenses),
		forecastSigned(r.Central.Result), f.Reset)
}

func printForecastMethods(r *ForecastReport) {
	f := Fmt
	fmt.Printf("\n%sMETHODS%s %s(the same actuals, five ways to finish the year — the spread is the uncertainty)%s\n", f.Bold, f.Reset, f.Dim, f.Reset)
	fmt.Printf("  %s%-9s %14s %14s %15s  %s%s\n", f.Dim, "METHOD", "INCOME", "EXPENSES", "RESULT", "HOW", f.Reset)
	for _, m := range r.Methods {
		marker, bold, reset := "  ", "", ""
		if m.Central {
			marker, bold, reset = "▸ ", f.Bold, f.Reset
		}
		fmt.Printf("%s%s%-9s %14s %14s %15s%s  %s%s%s\n",
			marker, bold, m.Method,
			forecastAmount(m.Income), forecastAmount(m.Expenses), forecastSigned(m.Result), reset,
			f.Dim, m.Label, f.Reset)
	}
	fmt.Printf("  %s▸ = the central estimate (--method %s). Seasonal index: income %s, expenses %s — how heavy %s %s was per month next to %s.%s\n",
		f.Dim, r.Method, forecastFactor(r.SeasonalIndexIncome), forecastFactor(r.SeasonalIndexExpenses),
		r.BaselineYear, r.RemainingLabel, r.ClosedLabel, f.Reset)
}

func printForecastQuality(r *ForecastReport) {
	if len(r.Quality) == 0 {
		return
	}
	f := Fmt
	fmt.Printf("\n%sESTIMATE QUALITY%s %s(what the number above is worth)%s\n", f.Bold, f.Reset, f.Dim, f.Reset)
	for _, n := range r.Quality {
		if n.Level == "warn" {
			fmt.Printf("  %s⚠ %s%s\n", f.Yellow, n.Text, f.Reset)
			continue
		}
		fmt.Printf("  %s· %s%s\n", f.Dim, n.Text, f.Reset)
	}
}

func printForecastBasis(r *ForecastReport) {
	f := Fmt
	fmt.Printf("\n%sBASIS%s  %s(EUR)%s\n", f.Bold, f.Reset, f.Dim, f.Reset)
	fmt.Printf("  %s%-34s %14s %14s %14s %7s%s\n", f.Dim, "", "INCOME", "EXPENSES", "RESULT", "TXS", f.Reset)
	printForecastBasisRow(fmt.Sprintf("A  %s actual %s", r.Year, r.ClosedLabel), r.Actual)
	printForecastBasisRow(fmt.Sprintf("P  %s actual %s", r.BaselineYear, r.ClosedLabel), r.BaselineToDate)
	printForecastBasisRow(fmt.Sprintf("R  %s actual %s", r.BaselineYear, r.RemainingLabel), r.BaselineRemainder)
	printForecastBasisRow(fmt.Sprintf("F  %s actual full year", r.BaselineYear), r.BaselineFullYear)
	fmt.Printf("  %smedian month %s %s:  income %s   expenses %s%s\n", f.Dim, r.Year, r.ClosedLabel,
		forecastAmount(r.MedianIncomeMonth), forecastAmount(r.MedianExpensesMonth), f.Reset)

	if r.PartialMonth != nil {
		fmt.Printf("  %s%s so far (not in A — it belongs to the projected months): in %s · out %s · result %s · %d txs%s\n",
			f.Dim, r.PartialMonth.Month,
			forecastAmount(r.PartialMonth.Income), forecastAmount(r.PartialMonth.Expenses),
			forecastSigned(r.PartialMonth.Result), r.PartialMonth.Transactions, f.Reset)
	}
}

func printForecastBasisRow(label string, flow ForecastFlow) {
	fmt.Printf("  %-34s %14s %14s %14s %7d\n",
		label,
		forecastAmount(flow.Income),
		forecastAmount(flow.Expenses),
		forecastSigned(flow.Result()),
		flow.Transactions)
}

func printForecastCategoryTable(title string, rows []ForecastRow, r *ForecastReport, color string) {
	f := Fmt
	fmt.Printf("\n%s%s%s %sby category — a breakdown for insight; the totals above are the estimate%s\n", f.Bold, title, f.Reset, f.Dim, f.Reset)
	if len(rows) == 0 {
		fmt.Printf("  %sNothing booked in %s or %s.%s\n", f.Dim, r.Year, r.BaselineYear, f.Reset)
		return
	}
	fmt.Printf("  %s%-22s %13s %12s %8s %13s %13s %13s  %s%s\n",
		f.Dim, "CATEGORY", "A "+r.ClosedLabel, "PER MONTH", "SEASON", "REMAINDER", "PROJECTED", r.BaselineYear+" FULL", "BASIS", f.Reset)
	for _, row := range rows {
		fmt.Printf("  %-22s %13s %12s %8s %13s %s%13s%s %13s  %s%s%s\n",
			truncateCategory(row.Category, 22),
			forecastAmount(row.Actual),
			forecastAmount(row.Estimate.Level),
			forecastFactor(row.Estimate.Index),
			forecastAmount(row.Estimate.Remainder),
			color, forecastAmount(row.Projected), f.Reset,
			forecastAmount(row.BaselineFullYear),
			f.Dim, forecastRowBasis(row), f.Reset)
	}
	fmt.Printf("  %s%-22s %13s %12s %8s %13s %13s %13s%s\n",
		f.Bold, "TOTAL (bottom-up)",
		forecastAmount(sumForecastRows(rows, func(x ForecastRow) float64 { return x.Actual })),
		"", "",
		forecastAmount(sumForecastRows(rows, func(x ForecastRow) float64 { return x.Estimate.Remainder })),
		forecastAmount(sumForecastRows(rows, func(x ForecastRow) float64 { return x.Projected })),
		forecastAmount(sumForecastRows(rows, func(x ForecastRow) float64 { return x.BaselineFullYear })),
		f.Reset)
	printForecastLegend(rows, r)
}

// forecastRowBasis compresses "how this row was estimated" into one cell.
func forecastRowBasis(row ForecastRow) string {
	basis := row.Estimate.Method
	if row.Estimate.IndexSource == "overall" {
		basis += "·overall"
	}
	for _, flag := range row.Estimate.Flags {
		if flag == "rejected-index" || flag == "rejected-ratio" {
			basis += "·rejected"
		}
	}
	return basis
}

// printForecastLegend defines every column and spells out the rule behind each
// basis that actually appears in the table above.
func printForecastLegend(rows []ForecastRow, r *ForecastReport) {
	f := Fmt
	fmt.Printf("  %sPER MONTH = the typical closed month · SEASON = how heavy a %s month was in %s · REMAINDER = PER MONTH × %d × SEASON%s\n",
		f.Dim, r.RemainingLabel, r.BaselineYear, r.RemainingMonths, f.Reset)
	seen := map[string]bool{}
	var used []string
	for _, row := range rows {
		if !seen[row.Estimate.Method] {
			seen[row.Estimate.Method] = true
			used = append(used, row.Estimate.Method)
		}
	}
	sort.Strings(used)
	for _, method := range used {
		fmt.Printf("  %s%-9s %s%s\n", f.Dim, method, forecastBasisExplanation(method, r), f.Reset)
	}
	if forecastAnyFlag(rows, "thin-baseline") {
		fmt.Printf("  %s·overall  this category's %s baseline is under %s or spans fewer than %d months, so it borrows the overall seasonal shape%s\n",
			f.Dim, r.BaselineYear, forecastAmount(forecastMinBaselineAmount), forecastMinBaselineMonths, f.Reset)
	}
	if forecastAnyFlag(rows, "rejected-index") || forecastAnyFlag(rows, "rejected-ratio") {
		fmt.Printf("  %s·rejected this line's own factor fell outside ×%.2f … ×%.2f, so it was discarded (not capped) and the overall shape used instead%s\n",
			f.Dim, forecastMinFactor, forecastMaxFactor, f.Reset)
	}
}

func forecastAnyFlag(rows []ForecastRow, flag string) bool {
	for _, r := range rows {
		if forecastHasFlag(r.Estimate.Flags, flag) {
			return true
		}
	}
	return false
}

func forecastBasisExplanation(method string, r *ForecastReport) string {
	switch method {
	case forecastRowRobust:
		return "median of the closed months — one big month can't run away with the projection"
	case forecastRowRecent:
		return fmt.Sprintf("median of the last %d closed months — weights how the year is actually going", forecastRecentMonths)
	case forecastRowRunRate:
		return fmt.Sprintf("mean month (A ÷ %d) — includes every one-off", r.ClosedMonths)
	case forecastRowSeasonal:
		return "baseline remainder × this year's level ratio"
	case forecastRowFlat:
		return "the baseline's remaining months, repeated unchanged"
	case forecastRowBaseline:
		return "nothing booked this year — the baseline carried over"
	case forecastRowClosed:
		return "year complete — nothing to project"
	}
	return ""
}

func printForecastMonths(r *ForecastReport) {
	f := Fmt
	fmt.Printf("\n%sMONTHLY ACTUALS%s %s(EUR, attributed by timestamp in Europe/Brussels — the series every median is taken over)%s\n", f.Bold, f.Reset, f.Dim, f.Reset)
	fmt.Printf("  %s%-10s %14s %14s %14s %7s%s\n", f.Dim, "MONTH", "INCOME", "EXPENSES", "RESULT", "TXS", f.Reset)
	for _, m := range r.Months {
		fmt.Printf("  %-10s %14s %14s %14s %7d\n",
			m.Month, forecastAmount(m.Income), forecastAmount(m.Expenses), forecastSigned(m.Result), m.Transactions)
	}
}

func printForecastSources(r *ForecastReport) {
	f := Fmt
	fmt.Printf("\n%sSOURCES%s %s(every file this forecast read)%s\n", f.Bold, f.Reset, f.Dim, f.Reset)
	for _, s := range r.Sources {
		mark := f.Green + "✓" + f.Reset
		detail := fmt.Sprintf("%d rows, %d counted", s.Rows, s.Counted)
		if !s.Present {
			mark = f.Red + "✗" + f.Reset
			detail = "missing"
		}
		generated := s.GeneratedAt
		if generated != "" {
			generated = " · generated " + generated
		}
		fmt.Printf("  %s %-44s %s%s%s\n", mark, s.Path, f.Dim, detail+generated, f.Reset)
	}
}

func printForecastChecks(r *ForecastReport) {
	f := Fmt
	fmt.Printf("\n%sCHECKS%s %s(recomputed from the parts above)%s\n", f.Bold, f.Reset, f.Dim, f.Reset)
	for _, c := range r.Checks {
		mark := f.Green + "✓" + f.Reset
		if !c.OK {
			mark = f.Red + "✗" + f.Reset
		}
		fmt.Printf("  %s %-58s %s%s%s\n", mark, c.Name, f.Dim, c.Detail, f.Reset)
	}
}

func printForecastVerify(r *ForecastReport) {
	f := Fmt
	fmt.Printf("\n%sVERIFY%s %s(re-derive every input independently)%s\n", f.Bold, f.Reset, f.Dim, f.Reset)
	for _, cmdLine := range r.Verify {
		fmt.Printf("  %s$ %s%s\n", f.Dim, cmdLine, f.Reset)
	}
	fmt.Println()
}

func printForecastHelp() {
	f := Fmt
	year := time.Now().In(BrusselsTZ()).Year()
	fmt.Printf(`
%s📈 chb forecast%s — project this year's result from booked transactions

%sUSAGE%s
  %schb forecast%s [YYYY] [--baseline YYYY] [--through YYYY-MM] [--method M] [--json]

%sHOW IT WORKS%s
  The year is split at the last closed month: everything before it is actual,
  everything after is estimated. Estimators run on the MONTHLY series, never on
  year-to-date ratios:

    %sremainder = typical month × months left × seasonal index%s

  The typical month is the %smedian%s of the closed months, so one outsized month
  can't run away with the projection. The baseline year contributes only a
  SHAPE — how heavy its remaining months were next to its own closed ones — and
  only when that line's baseline is thick enough (at least %s EUR over at least
  %d months). A factor landing outside ×%.2f…×%.2f is rejected, never capped:
  the line borrows the overall shape instead of inheriting the extreme.

  Five methods are always computed on the totals and printed together: the
  spread between them is the honest uncertainty. %s--method%s picks which one leads.

%sARGUMENTS%s
  %sYYYY%s                  Year to project (default: %d)

%sOPTIONS%s
  %s--method robust%s       median month × months left × seasonal index (default)
  %s--method recent%s       median of the last %d closed months — catches a turning year
  %s--method runrate%s      mean month × months left, no seasonality
  %s--method flat%s         the baseline's remaining months, repeated unchanged
  %s--method seasonal%s     baseline remainder × this year's level ratio (A ÷ P)
  %s--baseline YYYY%s       Comparison year (default: the year before)
  %s--through YYYY-MM%s     Treat this month as the last closed one
  %s--allow-missing%s       Proceed when a month has no generated/transactions.json
  %s--verbose%s, %s-v%s         Add per-month actuals and the list of files read
  %s--json%s                Emit every input and result as JSON
  %s--help%s, %s-h%s            Show this help

%sREADING IT%s
  • RESULT is the central estimate plus the range across all five methods.
  • ESTIMATE QUALITY says what that number is worth: how much is uncategorised,
    which months are outliers, which rows have no usable baseline.
  • The category tables are a breakdown for insight. Their sum (bottom-up) is a
    second, independent estimate — a wide gap to the central one means the
    category mix is shifting or the categorisation is thin.
  • CHECKS re-adds the totals from the parts; VERIFY re-derives every input
    with %schb income%s / %schb expenses%s.

%sBASIS%s
  • Local data only — no network. Reads generated/transactions.json under DATA_DIR.
  • Cash basis: the same transactions %schb income%s / %schb expenses%s count
    (EUR family only, internal transfers excluded, timestamps in Europe/Brussels).
    Note this is NOT what %schb report%s shows per category — those figures fold in
    VAT, fees, commissions and spread allocations.
  • The month in progress is NOT an actual — it is reported separately and
    projected together with the months after it.
  • Deterministic: same files in, same numbers out.

%sEXAMPLES%s
%s%s%s
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset, f.Cyan, f.Reset,
		f.Bold, f.Reset, f.Cyan, f.Reset, f.Bold, f.Reset,
		forecastAmount(forecastMinBaselineAmount), forecastMinBaselineMonths, forecastMinFactor, forecastMaxFactor,
		f.Yellow, f.Reset,
		f.Bold, f.Reset, f.Yellow, f.Reset, year,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset, forecastRecentMonths, f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Bold, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Bold, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Dim, forecastHelpExamples(year), f.Reset,
	)
}

// forecastHelpExamples keeps the comment column aligned whatever the current
// year's digits do to the command widths.
func forecastHelpExamples(year int) string {
	examples := [][2]string{
		{"chb forecast", "project the current year"},
		{fmt.Sprintf("chb forecast %d", year), "project a specific year"},
		{fmt.Sprintf("chb forecast %d --verbose", year), "add per-month actuals and the files read"},
		{fmt.Sprintf("chb forecast %d --method recent", year), "lead with the last 3 months"},
		{fmt.Sprintf("chb forecast %d --through %d-06", year, year), "pretend the year closed in June"},
		{fmt.Sprintf("chb forecast %d --baseline %d", year, year-2), "compare against another year"},
		{fmt.Sprintf("chb forecast %d --json | jq .methods", year), "every method, machine-readable"},
	}
	width := 0
	for _, e := range examples {
		if len(e[0]) > width {
			width = len(e[0])
		}
	}
	var b strings.Builder
	for i, e := range examples {
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "  $ %-*s  # %s", width, e[0], e[1])
	}
	return b.String()
}
