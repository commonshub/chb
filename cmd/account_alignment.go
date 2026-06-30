package cmd

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// accountAlignment captures the three balances that must agree for an account
// linked to Odoo: its own latest/local balance, its linked Odoo *journal*
// balance, and its *GL account* balance. Any divergence beyond a cent means the
// local mirror, the journal, and the ledger account have drifted apart.
type accountAlignment struct {
	slug     string
	currency string

	local   float64
	journal float64
	gl      float64

	hasLocal   bool
	hasJournal bool
	hasGL      bool
}

// alignmentEpsilon is the largest gap (in account currency) still considered
// "aligned" — a cent, to absorb rounding.
const alignmentEpsilon = 0.01

// linked reports whether the account participates in the alignment check at all
// (it must have a journal and/or a GL account configured).
func (a accountAlignment) linked() bool { return a.hasJournal || a.hasGL }

// comparable reports whether there are at least two legs to compare.
func (a accountAlignment) comparable() bool {
	n := 0
	for _, h := range []bool{a.hasLocal, a.hasJournal, a.hasGL} {
		if h {
			n++
		}
	}
	return n >= 2
}

// aligned returns true when every available leg agrees within a cent. With
// fewer than two legs it's trivially aligned (nothing to compare).
func (a accountAlignment) aligned() bool { return a.worstDelta() <= alignmentEpsilon }

type alignmentLeg struct {
	name string
	val  float64
	has  bool
}

func (a accountAlignment) legs() []alignmentLeg {
	return []alignmentLeg{
		{"local", a.local, a.hasLocal},
		{"journal", a.journal, a.hasJournal},
		{"GL", a.gl, a.hasGL},
	}
}

// worstDelta is the largest pairwise gap among the available legs.
func (a accountAlignment) worstDelta() float64 {
	legs := a.legs()
	worst := 0.0
	for i := 0; i < len(legs); i++ {
		if !legs[i].has {
			continue
		}
		for j := i + 1; j < len(legs); j++ {
			if !legs[j].has {
				continue
			}
			if d := math.Abs(legs[i].val - legs[j].val); d > worst {
				worst = d
			}
		}
	}
	return worst
}

// diffNote describes the widest divergence, e.g. "journal vs GL off by 1,250.00 EUR".
func (a accountAlignment) diffNote() string {
	legs := a.legs()
	worst := 0.0
	wi, wj := -1, -1
	for i := 0; i < len(legs); i++ {
		if !legs[i].has {
			continue
		}
		for j := i + 1; j < len(legs); j++ {
			if !legs[j].has {
				continue
			}
			if d := math.Abs(legs[i].val - legs[j].val); d > worst {
				worst, wi, wj = d, i, j
			}
		}
	}
	if wi < 0 {
		return ""
	}
	return fmt.Sprintf("%s vs %s off by %s", legs[wi].name, legs[wj].name, formatBalancePlain(worst, a.currency))
}

// computeAccountAlignment fills the journal and GL legs for an account whose
// local balance is already known. The journal leg comes from the local
// journal-lines cache (or live Odoo when live=true). The GL leg needs a live
// Odoo query, so it is only filled when live=true — cached runs compare local
// vs journal only. asOf (YYYY-MM-DD, or "") bounds the GL query; "" = latest.
func computeAccountAlignment(acc *AccountConfig, currency string, local float64, hasLocal, live bool, asOf string, creds *OdooCredentials, uid int) accountAlignment {
	al := accountAlignment{slug: acc.Slug, currency: currency, local: local, hasLocal: hasLocal}
	if acc.OdooJournalID > 0 {
		if snap, ok := accountJournalSnapshot(acc, currency, live); ok {
			al.journal, al.hasJournal = snap.Balance, true
		}
	}
	if acc.OdooGlAccountCode != "" && live && creds != nil && uid != 0 {
		if gl, ok, err := fetchOdooAccountByCode(creds, uid, acc.OdooGlAccountCode); err == nil && ok {
			if bal, _, _, err := fetchOdooAccountBalanceAt(creds, uid, gl.ID, asOf); err == nil {
				al.gl, al.hasGL = bal, true
			}
		}
	}
	return al
}

// misalignedAccounts returns the linked, comparable accounts whose legs disagree,
// sorted worst-divergence first.
func misalignedAccounts(aligns map[string]accountAlignment) []accountAlignment {
	var out []accountAlignment
	for _, a := range aligns {
		if a.linked() && a.comparable() && !a.aligned() {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].worstDelta() > out[j].worstDelta() })
	return out
}

// printAlignmentSummary prints the footer block listing misaligned accounts.
// `live` toggles the hint about the GL leg needing a live query.
func printAlignmentSummary(aligns map[string]accountAlignment, live bool) {
	bad := misalignedAccounts(aligns)
	if len(bad) == 0 {
		// Only claim "all aligned" if at least one account was actually checked.
		checked := 0
		for _, a := range aligns {
			if a.linked() && a.comparable() {
				checked++
			}
		}
		if checked > 0 {
			hint := ""
			if !live {
				hint = "  " + Fmt.Dim + "(local vs journal; add --live to include GL accounts)" + Fmt.Reset
			}
			fmt.Printf("  %s✓ %d linked account(s) aligned with their journal & GL account%s%s\n",
				Fmt.Green, checked, Fmt.Reset, hint)
		}
		return
	}
	fmt.Printf("  %s⚠ %d account(s) not aligned with their journal/GL account:%s\n", Fmt.Yellow, len(bad), Fmt.Reset)
	for _, a := range bad {
		fmt.Printf("    %s%s%s — %s%s%s\n", Fmt.Bold, a.slug, Fmt.Reset, Fmt.Yellow, a.diffNote(), Fmt.Reset)
	}
	if !live {
		fmt.Printf("    %s(local vs journal only — add --live to also check GL accounts)%s\n", Fmt.Dim, Fmt.Reset)
	}
}

// alignmentMarker is the inline flag appended next to a misaligned account's
// balance in the compact table ("" when aligned or not checked).
func alignmentMarker(a accountAlignment) string {
	if a.linked() && a.comparable() && !a.aligned() {
		return "  " + Fmt.Yellow + "⚠ " + a.diffNote() + Fmt.Reset
	}
	return ""
}

// printAccountAlignmentDetail renders the per-account local/journal/GL breakdown
// used in the --details view.
func printAccountAlignmentDetail(indent string, a accountAlignment) {
	if !a.linked() {
		return
	}
	status := Fmt.Green + "aligned" + Fmt.Reset
	if a.comparable() && !a.aligned() {
		status = Fmt.Yellow + "⚠ " + a.diffNote() + Fmt.Reset
	} else if !a.comparable() {
		status = Fmt.Dim + "needs --live for GL" + Fmt.Reset
	}
	parts := []string{}
	if a.hasLocal {
		parts = append(parts, "local "+formatBalancePlain(a.local, a.currency))
	}
	if a.hasJournal {
		parts = append(parts, "journal "+formatBalancePlain(a.journal, a.currency))
	}
	if a.hasGL {
		parts = append(parts, "GL "+formatBalancePlain(a.gl, "EUR"))
	}
	printAccountField(indent, "Alignment", fmt.Sprintf("%s  %s(%s)%s", status, Fmt.Dim, strings.Join(parts, " · "), Fmt.Reset))
}
