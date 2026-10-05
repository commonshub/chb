package cmd

// Import ids for transactions with several transfers of one account.
//
// Odoo already holds such transfers under position-based ids (":0", ":1")
// whose order was not stable, and under hand-made ids (":log10", a
// truncated hash). chb never renames them: each local transfer, known by
// its real log index, takes the id of the existing Odoo line it matches —
// first an exact ":log<N>" id, then the same amount, in line-id order —
// and a transfer without a line gets <chain>:<account>:<hash>:log<N>.
// Extra Odoo lines (a duplicate kept on purpose) are left alone.

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

func onchainLogImportID(chain, account, hash string, logIndex int) string {
	return fmt.Sprintf("%s:%s:%s:log%d", strings.ToLower(chain), strings.ToLower(account), strings.ToLower(hash), logIndex)
}

// matchMultiTransferLines assigns import ids to the events of one
// transaction (amounts signed, sorted by log index) from the Odoo lines of
// that transaction. Returns one id per event ("" = none matched).
func matchMultiTransferLines(events []TransactionEntry, lines []OdooCacheLine) []string {
	ids := make([]string, len(events))
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].ID < lines[j].ID })
	used := make([]bool, len(lines))
	// 1. exact :log<N>
	for i, e := range events {
		suffix := fmt.Sprintf(":log%d", e.LogIndex)
		for j, l := range lines {
			if !used[j] && strings.HasSuffix(strings.ToLower(l.UniqueImportID), suffix) {
				ids[i], used[j] = l.UniqueImportID, true
				break
			}
		}
	}
	// 2. same amount, in line-id order
	for i, e := range events {
		if ids[i] != "" {
			continue
		}
		for j, l := range lines {
			if !used[j] && math.Abs(l.Amount-e.Amount) < 0.005 {
				ids[i], used[j] = l.UniqueImportID, true
				break
			}
		}
	}
	return ids
}

var onchainJournalLinesCache = map[int][]OdooCacheLine{}

func resolveMultiTransferImportIDs(transactions []TransactionEntry, idx []int, accountSlug, chain, account string) {
	if len(idx) == 0 || account == "" {
		return
	}
	var lines []OdooCacheLine
	for _, acc := range LoadAccountConfigs() {
		if strings.EqualFold(acc.Slug, accountSlug) && acc.OdooJournalID > 0 {
			if cached, ok := onchainJournalLinesCache[acc.OdooJournalID]; ok {
				lines = cached
			} else {
				lines, _ = loadLatestOdooJournalLinesCache(acc.OdooJournalID)
				onchainJournalLinesCache[acc.OdooJournalID] = lines
			}
			break
		}
	}
	byHash := map[string][]int{}
	var hashes []string
	for _, i := range idx {
		h := strings.ToLower(transactions[i].TxHash)
		if _, ok := byHash[h]; !ok {
			hashes = append(hashes, h)
		}
		byHash[h] = append(byHash[h], i)
	}
	prefix := strings.ToLower(chain) + ":" + strings.ToLower(account) + ":"
	for _, h := range hashes {
		group := byHash[h]
		sort.SliceStable(group, func(a, b int) bool { return transactions[group[a]].LogIndex < transactions[group[b]].LogIndex })
		short := h
		if len(short) > 18 {
			short = short[:18]
		}
		var candidates []OdooCacheLine
		for _, l := range lines {
			id := strings.ToLower(l.UniqueImportID)
			if strings.HasPrefix(id, prefix) && strings.Contains(id, short) {
				candidates = append(candidates, l)
			}
		}
		events := make([]TransactionEntry, len(group))
		for k, i := range group {
			events[k] = transactions[i]
		}
		ids := matchMultiTransferLines(events, candidates)
		for k, i := range group {
			if ids[k] != "" {
				transactions[i].ImportID = ids[k]
			} else {
				transactions[i].ImportID = onchainLogImportID(chain, account, h, transactions[i].LogIndex)
			}
		}
	}
}
