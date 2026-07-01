package cmd

import "testing"

// row builds a res.partner.bank search_read row as Odoo returns it: id as a
// number, partner_id as [id, name] or false.
func pbRow(id int, partnerID int) map[string]interface{} {
	var p interface{} = false
	if partnerID > 0 {
		p = []interface{}{float64(partnerID), "P"}
	}
	return map[string]interface{}{"id": float64(id), "partner_id": p}
}

func TestPickPartnerBankRow(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		if id, pid := pickPartnerBankRow(nil, "X"); id != 0 || pid != 0 {
			t.Fatalf("want 0,0 got %d,%d", id, pid)
		}
	})

	t.Run("single", func(t *testing.T) {
		id, pid := pickPartnerBankRow([]map[string]interface{}{pbRow(227, 269)}, "X")
		if id != 227 || pid != 269 {
			t.Fatalf("want 227,269 got %d,%d", id, pid)
		}
	})

	t.Run("lowest id with a partner wins", func(t *testing.T) {
		// rows arrive id asc; both have partners → lowest id.
		rows := []map[string]interface{}{pbRow(227, 269), pbRow(2011, 229)}
		id, pid := pickPartnerBankRow(rows, "X")
		if id != 227 || pid != 269 {
			t.Fatalf("want 227,269 got %d,%d", id, pid)
		}
	})

	t.Run("orphan row without a partner is skipped", func(t *testing.T) {
		rows := []map[string]interface{}{pbRow(10, 0), pbRow(227, 269)}
		id, pid := pickPartnerBankRow(rows, "X")
		if id != 227 || pid != 269 {
			t.Fatalf("want 227,269 got %d,%d", id, pid)
		}
	})

	t.Run("all orphans → first row, no partner", func(t *testing.T) {
		rows := []map[string]interface{}{pbRow(10, 0), pbRow(11, 0)}
		id, pid := pickPartnerBankRow(rows, "X")
		if id != 10 || pid != 0 {
			t.Fatalf("want 10,0 got %d,%d", id, pid)
		}
	})
}
