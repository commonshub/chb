package cmd

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Fixture shape, hand-checkable end to end:
//
//	2025 (baseline)  membership 100/mo Jan–Jun, 200/mo Jul–Dec   → P 600   R 1200  F 1800
//	                 ticket     60/mo Jul–Dec                    → P   0   R  360  F  360
//	                 rent       50/mo all year (expense)         → P 300   R  300  F  600
//	2026 (target)    membership 180/mo Jan–Jun                   → A 1080
//	                 grants     300 once in March                → A  300
//	                 rent       50/mo Jan–Jun (expense)          → A  300
//
// with the cutoff at 2026-06 (6 closed months, 6 remaining).
func writeForecastFixtures(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()

	for m := 1; m <= 12; m++ {
		txs := []string{
			forecastTx(t, "mem", 2025, m, "CREDIT", forecastBaselineMembership(m), "membership"),
			forecastTx(t, "rent", 2025, m, "DEBIT", 50, "rent"),
		}
		if m >= 7 {
			txs = append(txs, forecastTx(t, "tick", 2025, m, "CREDIT", 60, "ticket"))
		}
		writeForecastMonth(t, dataDir, 2025, m, txs)
	}
	for m := 1; m <= 6; m++ {
		txs := []string{
			forecastTx(t, "mem", 2026, m, "CREDIT", 180, "membership"),
			forecastTx(t, "rent", 2026, m, "DEBIT", 50, "rent"),
		}
		if m == 3 {
			txs = append(txs, forecastTx(t, "grant", 2026, m, "CREDIT", 300, "grants"))
		}
		writeForecastMonth(t, dataDir, 2026, m, txs)
	}
	return dataDir
}

func forecastBaselineMembership(month int) float64 {
	if month >= 7 {
		return 200
	}
	return 100
}

func forecastTx(t *testing.T, id string, year, month int, txType string, amount float64, category string) string {
	t.Helper()
	ts := time.Date(year, time.Month(month), 15, 12, 0, 0, 0, BrusselsTZ()).Unix()
	return fmt.Sprintf(`{"id":"test:%s-%d-%02d","provider":"test","currency":"EUR","amount":%.2f,"grossAmount":%.2f,"type":%q,"timestamp":%d,"metadata":{"category":%q}}`,
		id, year, month, amount, amount, txType, ts, category)
}

func writeForecastMonth(t *testing.T, dataDir string, year, month int, txs []string) {
	t.Helper()
	path := filepath.Join(dataDir, fmt.Sprintf("%d", year), fmt.Sprintf("%02d", month), "generated", "transactions.json")
	writeJSONFixture(t, path, fmt.Sprintf(`{"year":"%d","month":"%02d","generatedAt":"%d-%02d-01T00:00:00+01:00","transactions":[%s]}`,
		year, month, year, month, strings.Join(txs, ",")))
}

func appendForecastTx(t *testing.T, dataDir string, year, month int, txs ...string) {
	t.Helper()
	existing := LoadTransactionsWithPII(dataDir, fmt.Sprintf("%d", year), fmt.Sprintf("%02d", month))
	var rows []string
	if existing != nil {
		for _, tx := range existing.Transactions {
			cat := tx.Category
			typ := tx.Type
			rows = append(rows, fmt.Sprintf(`{"id":%q,"provider":"test","currency":%q,"amount":%.2f,"grossAmount":%.2f,"type":%q,"timestamp":%d,"metadata":{"category":%q}}`,
				tx.ID, tx.Currency, tx.GrossAmount, tx.GrossAmount, typ, tx.Timestamp, cat))
		}
	}
	rows = append(rows, txs...)
	writeForecastMonth(t, dataDir, year, month, rows)
}

func forecastTestOptions() forecastOptions {
	return forecastOptions{
		Year:     2026,
		Baseline: 2025,
		Method:   forecastMethodRobust,
		// Mid-July: June is the last closed month, so the cutoff lands on 6.
		Now: time.Date(2026, 7, 15, 9, 0, 0, 0, BrusselsTZ()),
	}
}

func buildForecastForTest(t *testing.T, dataDir string, opts forecastOptions) *ForecastReport {
	t.Helper()
	report, err := buildForecastReport(dataDir, opts)
	if err != nil {
		t.Fatalf("buildForecastReport: %v", err)
	}
	return report
}

func assertForecastAmount(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 0.005 {
		t.Errorf("%s = %.2f, want %.2f", name, got, want)
	}
}

func forecastRowFor(t *testing.T, rows []ForecastRow, category string) ForecastRow {
	t.Helper()
	for _, r := range rows {
		if r.Category == category {
			return r
		}
	}
	t.Fatalf("no %q row in %+v", category, rows)
	return ForecastRow{}
}

func forecastMethodFor(t *testing.T, r *ForecastReport, method string) ForecastMethodResult {
	t.Helper()
	for _, m := range r.Methods {
		if m.Method == method {
			return m
		}
	}
	t.Fatalf("no %q method in %+v", method, r.Methods)
	return ForecastMethodResult{}
}

func TestForecastRobustProjection(t *testing.T) {
	report := buildForecastForTest(t, writeForecastFixtures(t), forecastTestOptions())

	if report.ClosedMonths != 6 || report.RemainingMonths != 6 {
		t.Fatalf("closed/remaining = %d/%d, want 6/6", report.ClosedMonths, report.RemainingMonths)
	}

	// The windows the projection is built from.
	assertForecastAmount(t, "A income", report.Actual.Income, 1380)    // 6×180 + 300
	assertForecastAmount(t, "A expenses", report.Actual.Expenses, 300) // 6×50
	assertForecastAmount(t, "P income", report.BaselineToDate.Income, 600)
	assertForecastAmount(t, "F income", report.BaselineFullYear.Income, 2160) // 1800 + 360
	assertForecastAmount(t, "R income", report.BaselineRemainder.Income, 1560)

	// Median month: Jan–Jun income is 180,180,480,180,180,180 → 180, NOT the
	// 230 mean that March's one-off grant would produce.
	assertForecastAmount(t, "median income month", report.MedianIncomeMonth, 180)
	assertForecastAmount(t, "median expense month", report.MedianExpensesMonth, 50)

	// Seasonal index on the totals: (1560 ÷ 6) ÷ (600 ÷ 6) = 2.6 for income,
	// flat expenses give 1.0.
	assertForecastAmount(t, "income seasonal index", report.SeasonalIndexIncome, 2.6)
	assertForecastAmount(t, "expense seasonal index", report.SeasonalIndexExpenses, 1.0)

	// Central (robust): 180 × 6 × 2.6 = 2808 more income; 50 × 6 × 1.0 = 300 more expenses.
	assertForecastAmount(t, "central remainder income", report.Central.RemainderIncome, 2808)
	assertForecastAmount(t, "central income", report.Central.Income, 4188)
	assertForecastAmount(t, "central expenses", report.Central.Expenses, 600)
	assertForecastAmount(t, "central result", report.Central.Result, 3588)
	if !report.Central.Central || report.Central.Method != forecastMethodRobust {
		t.Errorf("central = %+v, want the robust method flagged", report.Central)
	}

	// membership has a thick baseline of its own: (1200÷6) ÷ (600÷6) = ×2.0.
	membership := forecastRowFor(t, report.IncomeCategories, "membership")
	if membership.Estimate.IndexSource != "own" {
		t.Errorf("membership index source = %q, want own", membership.Estimate.IndexSource)
	}
	assertForecastAmount(t, "membership index", membership.Estimate.Index, 2.0)
	assertForecastAmount(t, "membership level", membership.Estimate.Level, 180)
	assertForecastAmount(t, "membership projected", membership.Projected, 3240) // 1080 + 180×6×2

	// grants is a single March one-off: the median month is 0, so the mean
	// stands in, and with no baseline it borrows the overall shape.
	grants := forecastRowFor(t, report.IncomeCategories, "grants")
	if !strings.HasPrefix(grants.Estimate.LevelKind, "mean (lumpy") {
		t.Errorf("grants level kind = %q, want the lumpy mean fallback", grants.Estimate.LevelKind)
	}
	if grants.Estimate.IndexSource != "overall" || !forecastHasFlag(grants.Estimate.Flags, "thin-baseline") {
		t.Errorf("grants estimate = %+v, want the overall shape and a thin-baseline flag", grants.Estimate)
	}

	// ticket booked nothing this year but the baseline's remaining months did:
	// carried over at the overall level ratio (1380 ÷ 600 = 2.3).
	ticket := forecastRowFor(t, report.IncomeCategories, "ticket")
	if ticket.Estimate.Method != forecastRowBaseline {
		t.Errorf("ticket method = %q, want baseline carry-over", ticket.Estimate.Method)
	}
	assertForecastAmount(t, "ticket projected", ticket.Projected, 828) // 360 × 2.3

	for _, c := range report.Checks {
		if !c.OK {
			t.Errorf("check %q failed: %s", c.Name, c.Detail)
		}
	}
}

// The bug this command shipped with: on live data a fridge category with €4.00
// of baseline over the closed window scaled a €7,335 remainder by ×1045 and
// projected €7.6M. The guard rails must make that arithmetically impossible.
func TestForecastThinBaselineCannotExplode(t *testing.T) {
	dataDir := writeForecastFixtures(t)
	// One stray €4 baseline transaction in January…
	appendForecastTx(t, dataDir, 2025, 1, forecastTx(t, "fridge-stray", 2025, 1, "DEBIT", 4, "fridge"))
	// …against a real €7,335 in the baseline's remaining months…
	for m := 7; m <= 12; m++ {
		appendForecastTx(t, dataDir, 2025, m, forecastTx(t, "fridge", 2025, m, "DEBIT", 1222.53, "fridge"))
	}
	// …and this year running at ~€523/month.
	for m := 1; m <= 6; m++ {
		appendForecastTx(t, dataDir, 2026, m, forecastTx(t, "fridge", 2026, m, "DEBIT", 523, "fridge"))
	}

	report := buildForecastForTest(t, dataDir, forecastTestOptions())
	fridge := forecastRowFor(t, report.ExpenseCategories, "fridge")

	// P = 4.00 over a single month: nowhere near thick enough to scale against.
	assertForecastAmount(t, "fridge P", fridge.BaselineToDate, 4)
	if fridge.Estimate.IndexSource != "overall" {
		t.Errorf("fridge index source = %q, want overall — its own baseline is one €4 transaction", fridge.Estimate.IndexSource)
	}
	if !forecastHasFlag(fridge.Estimate.Flags, "thin-baseline") {
		t.Errorf("fridge flags = %v, want thin-baseline", fridge.Estimate.Flags)
	}

	// The projection has to stay in the neighbourhood of the run rate: €523/mo
	// booked, 6 months left, so anything past ~2× the naive €6,276 is a bug.
	runRate := 523.0 * 6
	if fridge.Estimate.Remainder > runRate*2 {
		t.Errorf("fridge remainder = %.2f, want at most %.2f (2× the run rate)", fridge.Estimate.Remainder, runRate*2)
	}
	assertForecastAmount(t, "fridge level", fridge.Estimate.Level, 523)

	// And the same must hold for every method, including the level-ratio one
	// that produced the original blow-up.
	for _, method := range []string{forecastMethodRobust, forecastMethodRecent, forecastMethodSeasonal, forecastMethodFlat, forecastMethodRunRate} {
		opts := forecastTestOptions()
		opts.Method = method
		r := buildForecastForTest(t, dataDir, opts)
		row := forecastRowFor(t, r.ExpenseCategories, "fridge")
		if row.Projected > 30000 {
			t.Errorf("--method %s projects %.2f of fridge expenses from a €4.00 baseline", method, row.Projected)
		}
	}
}

// Out-of-band factors are rejected, never capped: capping would apply the
// maximum multiplier to exactly the lines that earned the least trust.
func TestForecastFactorsOutsideTheBandAreRejected(t *testing.T) {
	for _, tc := range []struct {
		in   float64
		want bool
	}{
		{1045.6, false},
		{0.001, false},
		{2.5, true},
		{forecastMinFactor, true},
		{forecastMaxFactor, true},
	} {
		if got := forecastFactorUsable(tc.in); got != tc.want {
			t.Errorf("forecastFactorUsable(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// The median is the whole point of the robust method: one outsized month must
// not drag the projection with it, and it must be named in ESTIMATE QUALITY.
func TestForecastMedianResistsAnOutlierMonth(t *testing.T) {
	dataDir := writeForecastFixtures(t)
	before := buildForecastForTest(t, dataDir, forecastTestOptions())

	// A single €100,000 month, ~100× the usual expense level.
	appendForecastTx(t, dataDir, 2026, 5, forecastTx(t, "boom", 2026, 5, "DEBIT", 100000, "equipment"))
	after := buildForecastForTest(t, dataDir, forecastTestOptions())

	if after.Actual.Expenses <= before.Actual.Expenses {
		t.Fatal("the outlier should raise the actuals")
	}
	// Actuals rise by the full 100k; the projection of the REMAINING months
	// must not, because the median month is unchanged.
	assertForecastAmount(t, "central remainder expenses", after.Central.RemainderExpenses, before.Central.RemainderExpenses)

	// The mean-based method does inherit it — that contrast is the point.
	runrate := forecastMethodFor(t, after, forecastMethodRunRate)
	if runrate.RemainderExpenses <= after.Central.RemainderExpenses {
		t.Errorf("runrate remainder %.2f should exceed the robust %.2f after a 100k month",
			runrate.RemainderExpenses, after.Central.RemainderExpenses)
	}

	var flagged bool
	for _, n := range after.Quality {
		if strings.Contains(n.Text, "Outlier month 2026-05") {
			flagged = true
		}
	}
	if !flagged {
		t.Errorf("expected the outlier month to be named in ESTIMATE QUALITY, got %+v", after.Quality)
	}
}

// `recent` exists to catch a year that turns mid-way. A collapsing second
// quarter has to show up as a lower estimate than the all-months median.
func TestForecastRecentMethodCatchesADecliningYear(t *testing.T) {
	dataDir := t.TempDir()
	for m := 1; m <= 12; m++ {
		writeForecastMonth(t, dataDir, 2025, m, []string{
			forecastTx(t, "inc", 2025, m, "CREDIT", 1000, "membership"),
		})
	}
	// 2026: 1000/month for three months, then 100/month.
	for m := 1; m <= 6; m++ {
		amount := 1000.0
		if m > 3 {
			amount = 100
		}
		writeForecastMonth(t, dataDir, 2026, m, []string{
			forecastTx(t, "inc", 2026, m, "CREDIT", amount, "membership"),
		})
	}

	report := buildForecastForTest(t, dataDir, forecastTestOptions())
	robust := forecastMethodFor(t, report, forecastMethodRobust)
	recent := forecastMethodFor(t, report, forecastMethodRecent)

	// Median of all six months is 550; median of the last three is 100.
	assertForecastAmount(t, "robust remainder", robust.RemainderIncome, 550*6)
	assertForecastAmount(t, "recent remainder", recent.RemainderIncome, 100*6)
	if recent.Result >= robust.Result {
		t.Errorf("recent (%.2f) should be below robust (%.2f) in a declining year", recent.Result, robust.Result)
	}
}

// Every printed row must satisfy REMAINDER = PER MONTH × months left × SEASON,
// for every method — that identity is what makes the table checkable by hand.
func TestForecastRowArithmeticIsSelfConsistent(t *testing.T) {
	dataDir := writeForecastFixtures(t)
	for _, method := range []string{forecastMethodRobust, forecastMethodRecent, forecastMethodSeasonal, forecastMethodFlat, forecastMethodRunRate} {
		opts := forecastTestOptions()
		opts.Method = method
		report := buildForecastForTest(t, dataDir, opts)
		for _, row := range append(append([]ForecastRow{}, report.IncomeCategories...), report.ExpenseCategories...) {
			want := roundCents(row.Estimate.Level * float64(report.RemainingMonths) * row.Estimate.Index)
			if math.Abs(row.Estimate.Remainder-want) > 0.02 {
				t.Errorf("--method %s, %s: remainder %.2f ≠ level %.2f × %d × %.4f = %.2f",
					method, row.Category, row.Estimate.Remainder, row.Estimate.Level,
					report.RemainingMonths, row.Estimate.Index, want)
			}
			if math.Abs(row.Projected-roundCents(row.Actual+row.Estimate.Remainder)) > 0.005 {
				t.Errorf("--method %s, %s: projected %.2f ≠ actual + remainder", method, row.Category, row.Projected)
			}
		}
	}
}

func TestForecastReportsAllMethodsAndTheirRange(t *testing.T) {
	report := buildForecastForTest(t, writeForecastFixtures(t), forecastTestOptions())

	if len(report.Methods) != 5 {
		t.Fatalf("methods = %d, want 5", len(report.Methods))
	}
	lo, hi := math.Inf(1), math.Inf(-1)
	centrals := 0
	for _, m := range report.Methods {
		lo = math.Min(lo, m.Result)
		hi = math.Max(hi, m.Result)
		if m.Central {
			centrals++
		}
		if m.Label == "" || m.IncomeFormula == "" {
			t.Errorf("method %q is missing its label or formula", m.Method)
		}
	}
	if centrals != 1 {
		t.Errorf("central methods = %d, want exactly 1", centrals)
	}
	assertForecastAmount(t, "range low", report.RangeLo, lo)
	assertForecastAmount(t, "range high", report.RangeHi, hi)
	if report.RangeLo > report.Central.Result || report.Central.Result > report.RangeHi {
		t.Errorf("central %.2f falls outside the range %.2f … %.2f", report.Central.Result, report.RangeLo, report.RangeHi)
	}
}

// A report whose categories are mostly "(uncategorized)" must say so — that is
// the difference between a breakdown worth reading and one that isn't.
func TestForecastWarnsWhenCategorisationIsThin(t *testing.T) {
	dataDir := t.TempDir()
	for m := 1; m <= 12; m++ {
		writeForecastMonth(t, dataDir, 2025, m, []string{
			forecastTx(t, "out", 2025, m, "DEBIT", 1000, ""),
		})
	}
	for m := 1; m <= 6; m++ {
		writeForecastMonth(t, dataDir, 2026, m, []string{
			forecastTx(t, "out", 2026, m, "DEBIT", 900, ""),
			forecastTx(t, "known", 2026, m, "DEBIT", 100, "rent"),
		})
	}

	report := buildForecastForTest(t, dataDir, forecastTestOptions())
	var warned bool
	for _, n := range report.Quality {
		if n.Level == "warn" && strings.Contains(n.Text, "uncategorised") && strings.Contains(n.Text, "expenses") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("expected an uncategorised-expenses warning, got %+v", report.Quality)
	}
}

// The forecast must count exactly what `chb income` / `chb expenses` count:
// internal transfers and non-EUR currencies stay out of both.
func TestForecastExcludesInternalTransfersAndNonEUR(t *testing.T) {
	dataDir := writeForecastFixtures(t)
	writeForecastMonth(t, dataDir, 2026, 4, []string{
		forecastTx(t, "mem", 2026, 4, "CREDIT", 180, "membership"),
		forecastTx(t, "rent", 2026, 4, "DEBIT", 50, "rent"),
		// Flagged only by type…
		forecastTx(t, "sweep", 2026, 4, "INTERNAL", 5000, "membership"),
		// …only by category (files generated before the type was coerced)…
		forecastTx(t, "sweep2", 2026, 4, "CREDIT", 7000, "internal_transfer"),
		// …and a currency outside the EUR family.
		strings.Replace(forecastTx(t, "usd", 2026, 4, "CREDIT", 900, "grants"), `"currency":"EUR"`, `"currency":"USD"`, 1),
	})

	report := buildForecastForTest(t, dataDir, forecastTestOptions())
	assertForecastAmount(t, "A income", report.Actual.Income, 1380)
	assertForecastAmount(t, "A expenses", report.Actual.Expenses, 300)
	// 6 membership + 1 grant + 6 rent; the internal and USD rows never count.
	if report.Actual.Transactions != 13 {
		t.Errorf("counted transactions = %d, want 13", report.Actual.Transactions)
	}
}

func TestForecastStopsOnMissingMonth(t *testing.T) {
	dataDir := writeForecastFixtures(t)
	if err := removeForecastMonth(dataDir, 2026, 5); err != nil {
		t.Fatalf("remove month: %v", err)
	}

	_, err := buildForecastReport(dataDir, forecastTestOptions())
	if err == nil {
		t.Fatal("expected an error when a closed month has no transactions.json")
	}
	for _, want := range []string{"2026/05", "chb pull", "chb generate", "--allow-missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}

	opts := forecastTestOptions()
	opts.AllowMissing = true
	report := buildForecastForTest(t, dataDir, opts)
	if len(report.MissingMonths) != 1 || report.MissingMonths[0] != "2026-05" {
		t.Fatalf("missing months = %v, want [2026-05]", report.MissingMonths)
	}
	// May's 180 + 50 are gone; everything else is unchanged.
	assertForecastAmount(t, "A income", report.Actual.Income, 1200)
	assertForecastAmount(t, "A expenses", report.Actual.Expenses, 250)
	var coverage *ForecastCheck
	for i := range report.Checks {
		if strings.Contains(report.Checks[i].Name, "2026") && strings.Contains(report.Checks[i].Name, "month files") {
			coverage = &report.Checks[i]
		}
	}
	if coverage == nil || coverage.OK || !strings.Contains(coverage.Detail, "2026-05") {
		t.Errorf("coverage check = %+v, want a failing check naming 2026-05", coverage)
	}
}

func TestForecastCompleteYearReportsActuals(t *testing.T) {
	opts := forecastTestOptions()
	opts.Now = time.Date(2027, 2, 1, 9, 0, 0, 0, BrusselsTZ()) // 2026 is over
	opts.AllowMissing = true                                   // fixtures stop at 2026-06

	report := buildForecastForTest(t, writeForecastFixtures(t), opts)
	if report.ClosedMonths != 12 || report.RemainingMonths != 0 {
		t.Fatalf("closed/remaining = %d/%d, want 12/0", report.ClosedMonths, report.RemainingMonths)
	}
	for _, row := range append(report.IncomeCategories, report.ExpenseCategories...) {
		if row.Estimate.Method != forecastRowClosed {
			t.Errorf("%s method = %q, want closed", row.Category, row.Estimate.Method)
		}
		assertForecastAmount(t, row.Category+" projected", row.Projected, row.Actual)
	}
	assertForecastAmount(t, "central result", report.Central.Result, report.Actual.Result())
	if report.PartialMonth != nil {
		t.Errorf("partial month = %+v, want nil for a closed year", report.PartialMonth)
	}
}

func TestForecastPartialMonthIsReportedButNotCounted(t *testing.T) {
	dataDir := writeForecastFixtures(t)
	writeForecastMonth(t, dataDir, 2026, 7, []string{
		forecastTx(t, "mem", 2026, 7, "CREDIT", 180, "membership"),
	})

	report := buildForecastForTest(t, dataDir, forecastTestOptions())
	assertForecastAmount(t, "A income", report.Actual.Income, 1380) // July stays out
	if report.PartialMonth == nil {
		t.Fatal("expected the month in progress to be reported")
	}
	if report.PartialMonth.Month != "2026-07" {
		t.Errorf("partial month = %q, want 2026-07", report.PartialMonth.Month)
	}
	assertForecastAmount(t, "partial income", report.PartialMonth.Income, 180)
}

func TestForecastNothingClosedYetFallsBackToTheBaseline(t *testing.T) {
	opts := forecastTestOptions()
	opts.Now = time.Date(2026, 1, 10, 9, 0, 0, 0, BrusselsTZ())
	opts.AllowMissing = true

	report := buildForecastForTest(t, writeForecastFixtures(t), opts)
	if report.ClosedMonths != 0 {
		t.Fatalf("closed = %d, want 0", report.ClosedMonths)
	}
	// With nothing booked, every method must land on the baseline year itself.
	assertForecastAmount(t, "central income", report.Central.Income, report.BaselineFullYear.Income)
	assertForecastAmount(t, "central expenses", report.Central.Expenses, report.BaselineFullYear.Expenses)
	assertForecastAmount(t, "central result", report.Central.Result, report.BaselineFullYear.Result())
}

func TestForecastMedianAndLastN(t *testing.T) {
	cases := []struct {
		in   []float64
		want float64
	}{
		{nil, 0},
		{[]float64{5}, 5},
		{[]float64{1, 2, 3}, 2},
		{[]float64{1, 2, 3, 100}, 2.5},
		{[]float64{100, 1, 2, 3}, 2.5}, // order must not matter
	}
	for _, tc := range cases {
		if got := forecastMedian(tc.in); math.Abs(got-tc.want) > 0.005 {
			t.Errorf("forecastMedian(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if got := forecastLastN([]float64{1, 2, 3, 4, 5}, 3); len(got) != 3 || got[0] != 3 {
		t.Errorf("forecastLastN = %v, want the last three", got)
	}
	if got := forecastLastN([]float64{1, 2}, 3); len(got) != 2 {
		t.Errorf("forecastLastN with fewer values = %v, want all of them", got)
	}
}

func TestForecastSeasonalIndex(t *testing.T) {
	// Baseline booked 600 over 6 closed months (100/mo) and 1560 over the 6
	// remaining ones (260/mo): the remaining months are 2.6× as heavy.
	idx, ok := forecastSeasonalIndex(600, 1560, 6, 6)
	if !ok {
		t.Fatal("expected a usable index")
	}
	assertForecastAmount(t, "index", idx, 2.6)

	if _, ok := forecastSeasonalIndex(0, 500, 6, 6); ok {
		t.Error("an index with no baseline to date must report itself unusable")
	}
	if idx, _ := forecastSeasonalIndex(0, 500, 6, 6); idx != 1 {
		t.Error("the unusable index must be neutral (1.0), never zero")
	}
}

func TestParseForecastArgs(t *testing.T) {
	now := time.Date(2026, 9, 17, 9, 0, 0, 0, BrusselsTZ())

	opts, err := parseForecastArgs(nil, now)
	if err != nil {
		t.Fatalf("no args: %v", err)
	}
	if opts.Year != 2026 || opts.Baseline != 2025 || opts.Method != forecastMethodRobust {
		t.Errorf("defaults = %d/%d/%s, want 2026/2025/robust", opts.Year, opts.Baseline, opts.Method)
	}

	// --through's value must not be mistaken for the positional year.
	opts, err = parseForecastArgs([]string{"--through", "2026-03"}, now)
	if err != nil {
		t.Fatalf("--through: %v", err)
	}
	if opts.Year != 2026 || opts.Through != "2026-03" {
		t.Errorf("year/through = %d/%q, want 2026/2026-03", opts.Year, opts.Through)
	}
	for _, form := range []string{"2026/03", "202603", "2026-3"} {
		opts, err := parseForecastArgs([]string{"--through", form}, now)
		if err != nil || opts.Through != "2026-03" {
			t.Errorf("--through %s → %q, %v; want 2026-03", form, opts.Through, err)
		}
	}

	opts, err = parseForecastArgs([]string{"2024", "--baseline", "2022", "--method", "flat"}, now)
	if err != nil {
		t.Fatalf("explicit args: %v", err)
	}
	if opts.Year != 2024 || opts.Baseline != 2022 || opts.Method != forecastMethodFlat {
		t.Errorf("got %d/%d/%s, want 2024/2022/flat", opts.Year, opts.Baseline, opts.Method)
	}

	// `linear` was the pre-robust name for the mean run rate; keep it working.
	if opts, err := parseForecastArgs([]string{"--method", "linear"}, now); err != nil || opts.Method != forecastMethodRunRate {
		t.Errorf("--method linear → %q, %v; want runrate", opts.Method, err)
	}

	for _, args := range [][]string{
		{"2026/05"},              // a month, not a year
		{"--method", "vibes"},    // unknown method
		{"--baseline", "2026"},   // same as the year being projected
		{"--through", "2025-03"}, // outside the forecast year
		{"--through", "2026-13"}, // not a month
	} {
		if _, err := parseForecastArgs(args, now); err == nil {
			t.Errorf("parseForecastArgs(%v) = nil error, want a usage error", args)
		}
	}
}

func TestResolveForecastClosedMonths(t *testing.T) {
	now := time.Date(2026, 9, 17, 9, 0, 0, 0, BrusselsTZ())
	cases := []struct {
		name string
		opts forecastOptions
		want int
	}{
		{"current year stops at the last closed month", forecastOptions{Year: 2026, Now: now}, 8},
		{"past year is complete", forecastOptions{Year: 2025, Now: now}, 12},
		{"future year has nothing closed", forecastOptions{Year: 2027, Now: now}, 0},
		{"january has nothing closed", forecastOptions{Year: 2026, Now: time.Date(2026, 1, 20, 9, 0, 0, 0, BrusselsTZ())}, 0},
		{"--through wins", forecastOptions{Year: 2026, Through: "2026-04", Now: now}, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveForecastClosedMonths(tc.opts)
			if err != nil {
				t.Fatalf("resolveForecastClosedMonths: %v", err)
			}
			if got != tc.want {
				t.Errorf("closed months = %d, want %d", got, tc.want)
			}
		})
	}
}

// The VERIFY block is only useful if the ranges it prints are the ones
// `chb income` / `chb expenses` actually accept for the same window.
func TestForecastVerifyCommandsRederiveTheInputs(t *testing.T) {
	report := buildForecastForTest(t, writeForecastFixtures(t), forecastTestOptions())

	joined := strings.Join(report.Verify, "\n")
	for _, want := range []string{
		"chb income 20260101-20260630",
		"chb expenses 20260101-20260630",
		"chb income 20250101-20250630",
		"chb income 2025",
		"1,380.00 EUR",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("verify block does not mention %q:\n%s", want, joined)
		}
	}

	// And the range really is the window the report measured.
	spec, ok := ParseDateRangeSpec(forecastDateRangeArg(2026, 1, 6))
	if !ok {
		t.Fatalf("chb income cannot parse %q", forecastDateRangeArg(2026, 1, 6))
	}
	if got := spec.Start.Format("2006-01-02"); got != "2026-01-01" {
		t.Errorf("range start = %s, want 2026-01-01", got)
	}
	if got := spec.End.Add(-time.Nanosecond).Format("2006-01-02"); got != "2026-06-30" {
		t.Errorf("range end = %s, want 2026-06-30", got)
	}
}

func TestForecastMonthRangeLabel(t *testing.T) {
	cases := []struct {
		from, to int
		want     string
	}{
		{1, 8, "Jan–Aug"},
		{9, 12, "Sep–Dec"},
		{12, 12, "Dec"},
		{13, 12, "—"},
		{1, 0, "—"},
	}
	for _, tc := range cases {
		if got := forecastMonthRangeLabel(tc.from, tc.to); got != tc.want {
			t.Errorf("forecastMonthRangeLabel(%d, %d) = %q, want %q", tc.from, tc.to, got, tc.want)
		}
	}
}

func removeForecastMonth(dataDir string, year, month int) error {
	return os.RemoveAll(filepath.Join(dataDir, fmt.Sprintf("%d", year), fmt.Sprintf("%02d", month)))
}
