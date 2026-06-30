# Account alignment audit — local vs Odoo GL

_Generated 2026-06-29. Compares each account's **local balance** (computed from the
locally-cached source transactions) against its **GL balance** in Odoo (the sum of
posted `account.move.line` entries on the journal's default GL account), using the
new tooling: `chb accounts <slug>`, `chb accounts balance <period>`, and
`chb odoo accounts <code> balance <period>`._

All figures are pegged 1:1 (EURe/EURb ≈ EUR), so the cross-currency comparison is valid.

## Summary

| Account | GL code | Local (now) | GL (now) | Δ (GL − local) | Status |
|---|---|---:|---:|---:|---|
| savings | 551 | 5,090.13 | 5,090.13 | 0.00 | ✅ aligned |
| checking | 550010 | 1,933.34 | 1,933.34 | 0.00 | ✅ aligned |
| eoa | 550018 | 0.00 | 0.00 | 0.00 | ✅ aligned |
| citizen-wallet | 550006 | 0.00 | 0.00 | 0.00 | ✅ aligned |
| stripe | 550014 | 1,261.92 | 1,262.05 | +0.13 | 🟢 negligible |
| coffee | 550017 | 9.15 | 9.15 | 0.00 | 🟢 GL ok (journal view off −0.65) |
| fridge | 550016 | 1,069.43 | 1,077.52 | +8.09 | 🟡 minor |
| kbc | 550003 | 402.31 | 238.09 | **−164.22** | 🟠 moderate |
| chb-safe | 550015 | 9,089.19 | 41,039.75 | **+31,950.56** | 🔴 severe |
| 202605-savings-hacked | 550013 | 0.00 | −64,304.42 | **−64,304.42** | 🔴 severe |

Four accounts are materially misaligned: **202605-savings-hacked**, **chb-safe**,
**kbc**, and (minor) **fridge**.

## Is there a missing starting-balance movement? — partially confirmed

The hypothesis is **correct for one account and disproven for the others.**

Three accounts have a `2025-01-01` cutoff (`odooSyncSince`) and are supposed to carry
a manual opening-balance entry equal to their 2024-12-31 closing balance. Checking the
GL balance straddling the cutoff:

| Account | local @ 2024-12-31 (needed opening) | GL @ 2024-12-31 | GL @ 2025-01-01 | Opening entry? |
|---|---:|---:|---:|---|
| stripe | 9,326.90 | 0.00 | **9,326.90** | ✅ present & correct (dated 2025-01-01) |
| kbc | 10,051.46 | 0.00 | **10,051.46** | ✅ present & correct (dated 2025-01-01) |
| 202605-savings-hacked | 115,525.44 | 0.00 | **0.00** | ❌ exists but **DRAFT (move #30766)** — uncounted |

So stripe and kbc already have a correct opening entry (it lands on 2025-01-01, which is
why a "@ end of 2024" cut reads 0). The hacked account **does have an opening entry of
+115,525.44 (move #30766, "Solde d'ouverture") — but it was left in `draft`**, and Odoo's
GL only sums *posted* lines, so the +115,525.44 is invisible. Effectively missing until
posted. The kbc and chb-safe discrepancies have other causes (below).

---

## Per-account findings & fixes

### 🔴 202605-savings-hacked (550013) — missing opening + corrupted drain entries

**Evidence.** For most of 2025 the GL equals `local − 115,525.44` *exactly*:

| Date | local | GL | local − 115,525.44 |
|---|---:|---:|---:|
| 2025-03-31 | 51,401.23 | −64,124.21 | −64,124.21 ✓ |
| 2025-05-31 | 97,593.78 | −17,931.66 | −17,931.66 ✓ |
| 2025-06-30 | 87,659.94 | −27,865.50 | −27,865.50 ✓ |
| 2025-12-31 | 100,864.78 | **+36,560.36** | −14,660.66 ✗ |
| 2026-06-30 | 0.00 | **−64,304.42** | −115,525.44 ✗ |

**Culprit (two layers).**
1. The **opening balance of +115,525.44 (2024-12-31) is missing** — it alone explains the
   gap through mid-2025.
2. From H2-2025 a further **+51,221.02 of spurious entries** entered the GL (the residual
   `115,525.44 − 64,304.42 = 51,221.02`), so the drain/theft is only partly and
   incorrectly booked. The account was drained to 0 on-chain, but Odoo ends at −64,304.42
   (a nonsensical negative asset).

**Fix — post the draft opening (no rebuild, no new entry).** The opening already exists —
`push --startingBalance` correctly found it and reported `✓ matches`, so it did **not**
create a duplicate. The entry is just **unposted**: move **#30766** sits in `draft`, so the
GL doesn't count it. Post it and the +115,525.44 lands:

```
# Odoo UI: Accounting → Journal 47 → the "Solde d'ouverture" 2025-01-01 entry → Post
# (or post move #30766 directly)
```

After posting, the GL goes from −64,304.42 to **+51,221.02** (it realigns everything through
Nov-2025). Two follow-ups remain to reach 0:

- **Dec-2025 gap of +51,221.02 — 4 manual entries in *another* journal.** Journal 47 itself
  is clean: its 61 December lines sum to **+30,241.52**, exactly the on-chain reality, and all
  are import-id matched. The surplus is **4 manual year-end entries dated 2025-12-31 in the
  "OPERATIONS DIVERSES" journal**, posted to account 550013:
    - `#30803` +762.26 and `#31158` −762.26 (vs 580000) — cancel each other, harmless.
    - **`#31160` +35,731.83** (vs 580000 internal transfers) — manual reclassification onto 550013.
    - **`#31162` +15,489.19** (vs **550015 / chb-safe**) — moves 15,489.19 off chb-safe onto 550013.

  `chb odoo journals 47 fix` **cannot** see these — they aren't journal-47 statement lines,
  they're manual entries in a different journal that merely *post to* GL account 550013, and
  `fix` is scoped to a journal's own bank lines. Review `#31160` and `#31162` in Odoo directly
  (Accounting → Misc. Operations); if erroneous, reverse them. Removing the two real ones —
  plus posting the opening — lands 550013 at **0**.
- **Theft as a loss** — reclassify the drains from the asset account to a **loss/expense**
  account so the 115,525.44 reads as a realised loss rather than vanished cash.

> Why didn't `fix` already fix it? Two gaps, both now actionable:
> 1. `createOpeningBalanceLine` created the statement line but never posted its move — so a
>    chb-created opening is left in draft. **Fixed**: it now `action_post`s the move.
> 2. The convergence's `✓ opening matches` check compares only the *amount*, not the posted
>    state — so it green-lit a draft opening. `fix`/convergence should detect an unposted
>    opening and post it (needs the journal-lines cache to carry move state). _Proposed._

### 🔴 chb-safe (550015) — wrong-signed / duplicated movement in Q3 2024

**Evidence.** GL tracked local *perfectly* until 2024-06-30, then diverged by a **constant
+47,439.75** that has persisted ever since:

| Date | GL | local | Δ |
|---|---:|---:|---:|
| 2024-06-30 | 52,439.75 | 52,439.75 | 0.00 ✓ |
| 2024-09-30 | 76,180.22 | 28,740.47 | +47,439.75 |
| 2024-12-31 | 56,528.94 | 9,089.19 | +47,439.75 |
| now | 41,039.75 | 9,089.19 | +31,950.56 |

A **constant** offset that appears in one quarter is the signature of a single bad entry.
Over Q3-2024 local fell ~23.7k (real expenses) while the GL *rose* ~23.7k — i.e. one
movement of ≈ **23,719.88 was booked with the wrong sign** (an outflow recorded as an
inflow), which double-counts to exactly 47,439.75. The current 31,950.56 gap is that same
error minus genuine 2025 activity Odoo has that the local cache doesn't.

This account has **no `odooSyncSince`** and its on-chain history is fully knowable; the live
on-chain read (9,089.19) confirms local is the truth and Odoo is wrong.

> Connected to the hacked account: part of chb-safe's 2025 drift is manual entry **`#31162`**
> (2025-12-31, OPERATIONS DIVERSES) which moved **15,489.19 off 550015 onto the hacked
> account's 550013**. Same entry, two symptoms — fixing it helps both.

**Fix — surgical, no rebuild.** The journal tracks local correctly apart from one mis-signed
entry, which is exactly what `fix` / `fix-amounts` targets (they rewrite lines whose
sign/amount disagree with local, matched by `unique_import_id`, and drop unmatched orphans):

```
chb accounts chb-safe pull --history && chb generate   # make sure the local cache is complete first
chb odoo journals 50 fix --dry-run                     # detects the mis-signed Q3-2024 line + any orphans
chb odoo journals 50 fix-amounts --dry-run             # or just the amount step
chb odoo journals 50 fix-amounts                       # apply
```

The live on-chain read agrees with local at 9,089.19, which is good evidence local is
complete — but re-pull first, because if the multisig had any 2025 activity the local cache
doesn't yet hold, `fix` would mistake the real Odoo lines for orphans. Only fall back to a
`push --startingBalance 2024-01-01 --force` rebuild if `fix` can't match the bad line (e.g.
it's a manual entry with no import-id).

### 🟠 kbc (550003) — local CSV ahead of the manually-maintained Odoo journal

**Not an opening-balance problem** — the +10,051.46 opening is present and correct.
KBC is flagged `odooSourceOfTruth` (CHB must **not** push to it; the journal is maintained
directly in Odoo). The local CSV holds 188 transactions (→ 2026-05-15); the Odoo journal
holds 179. The **9-transaction / 164.22 difference** is recent KBC activity present in the
local CSV but **not yet entered in Odoo** (or a duplicate on one side).

**Fix (manual — do not push).** Reconcile the two sets and enter the missing rows in Odoo:
```
chb accounts kbc transactions --csv          # local CSV rows
chb odoo journals 28 ...                      # compare against Odoo journal 28
```
Lowest priority — small amount, and the divergence is expected for a source-of-truth journal
that simply lags the bank export.

### 🟡 fridge (550016) — minor EURb counting drift

GL 1,077.52 vs local 1,069.43 (**+8.09**); the live on-chain read is 1,083.41. The journal's
*statement-line* view reads only 86.78 because ~990 of the balance sits in non-statement
move lines. This is the known EURb duplicate-`logIndex` / fridge-counting quirk, not an
opening issue. Low priority — re-run a full history sync and regenerate, then re-check:
```
chb accounts fridge pull --history && chb generate
chb accounts fridge            # re-verify the four balances line up
```

### 🟢 stripe (550014) & coffee (550017) — negligible

- **stripe**: opening correct; GL 1,262.05 vs local 1,261.92 = **+0.13** sub-euro residue
  (fee/rounding). The Stripe journal is structurally busy (4,227 GL lines) but currently
  lands on the right balance. No action needed beyond watching the 13-cent residue.
- **coffee**: GL **matches** local (9.15); only the statement-line view is −0.65 off. No action.

### ✅ Aligned: savings, checking, eoa, citizen-wallet

GL equals local to the cent. These on-chain accounts start from genesis (no opening entry
needed) and their full history is synced.

---

## Recommended order of work

1. **202605-savings-hacked** — add the opening via `push --startingBalance 2025-01-01` (no force),
   then `journals 47 fix` the Dec-2025 stray entry, then book the theft as a loss. (🔴 biggest, 64k wrong)
2. **chb-safe** — `journals 50 fix` / `fix-amounts` to correct the mis-signed Q3-2024 line. (🔴 32k wrong)
3. **kbc** — manually enter the ~9 missing 2026 rows in Odoo. (🟠 164 wrong, source-of-truth)
4. **fridge** — full re-sync + regenerate. (🟡 8 wrong)
5. stripe 13c residue, coffee statement-view — monitor only.

None of these needs a `--force` rebuild. The order of preference is always:
**(1) `fix` / `fix-amounts`** (surgical, matched by import-id) → **(2) `push --startingBalance`**
(adds/corrects the opening, in place) → **(3) `--force`** (wipe + rebuild) only as a last resort
when a bad line can't be matched.

> ⚠️ Always run with `--dry-run` first, confirm the plan, and target the **test** instance
> before prod. `--startingBalance <date>` computes the opening from local history and is the
> non-destructive way to add/correct the opening on cutoff (`odooSyncSince`) accounts.

## Note — could `fix` do all of it?

`fix` already detects the missing/wrong opening on cutoff journals and reports it, but it only
*auto-creates* the starting-balance entry on `odooSourceOfTruth` journals. A small enhancement
would let `fix` also offer to create the opening on any `odooSyncSince` journal (reusing the
same convergence plan), so `chb odoo journals <id> fix` becomes a true one-stop repair for the
hacked account. Worth doing if we expect more cutoff journals to drift like this.

**Blind spot — entries from other journals (now covered).** `fix` is scoped to a journal's
own bank statement lines, so it can't see manual entries booked in *other* journals against the
same GL account (the +51,221.02 above). This is now addressed by **`chb odoo accounts <code>
fix [--dry-run]`**, which reconciles the GL account to its local source and decomposes the gap:
it posts unposted own-journal drafts (e.g. an opening left in draft) and **flags foreign-journal
entries with direct Odoo URLs** for manual review. Running it on 550013 surfaces
`#31160`/`#31162` immediately. This closes the **journal balance ≠ GL balance** gap whenever
foreign-journal entries touch the account.
