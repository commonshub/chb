#!/usr/bin/env bash
# One-off decisions from the 2023-2024 review (categorise_chb_2023_2024.py),
# as Nostr annotations. The general decisions are rules in cmd/defaults/rules.json
# (note: "2023-2024 review (WinBooks)").
# Needs a chb build with `nostr annotate` and a trusted steward key
# (chb setup nostr). Dry run by default; pass --yes to publish.
set -euo pipefail
CHB=${CHB:-chb}
MODE=${1:---dry-run}

$CHB nostr annotate ethereum:137:tx:0xa5524ff7537a4ef263888d2fbcc9d0923fe970e4f359acb8117389e73a7169cf \
  --category internal_transfer \
  --note "Monerium EURe v1 -> v2 migration: v2 minted the wallet's v1 balance (May grants 37,700.68 + 5,214.82 from EOA - 37,602.77 to savings = 5,312.73)" "$MODE"

$CHB nostr annotate ethereum:100:tx:0x2032a1171768fc41ad325f8671b4eb145ca485ac10e5916825554e95136778d7 \
  --category refund --note "EUR 1,600 paid and returned (expense 217644)" "$MODE"

$CHB nostr annotate ethereum:100:tx:0x3c0b091ab94ab067f82657090413ff3a23883a4c7ccd4d3628760481ae89c757 \
  --category donation --note "\"Don Be Impact\" (Solifin): a gift" "$MODE"

$CHB nostr annotate stripe:txn_3PRamsFAhaWeDyow0MF0in3M \
  --category other-income \
  --note "Stripe invoice A60E2C44-0001, Citizen Wallet service; WinBooks books EUR 1,850 ex VAT" "$MODE"

$CHB nostr annotate stripe:txn_3PLn9mFAhaWeDyow2csalrZU \
  --category ticket --note "EUR 176.32 incl. 21% VAT (145.72 + 30.60); no product on the Stripe charge" "$MODE"
