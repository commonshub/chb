package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureMonthFileSet(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	// 2025/11 has data; 2025/12 is a gap; 2026/01 has only an old stewards
	// events.json (generated before the tier split).
	writeFile(t, filepath.Join(dataDir, "2025", "11", "providers", "stripe", "x.json"), `{}`)
	writeFile(t, filepath.Join(dataDir, "2025", "11", "stewards", "transactions.json"),
		`{"year":"2025","month":"11","transactions":[{"id":"t1","provider":"kbcbrussels","metadata":{"description":"DOE JOHN BE56 0016 9232 9088"}}]}`)
	writeFile(t, filepath.Join(dataDir, "2026", "01", "stewards", "events.json"),
		`{"month":"2026-01","events":[{"id":"e1","name":"Assembly","startAt":"2026-01-10T18:00:00+01:00","source":"luma","guests":[{"name":"Guest"}]}]}`)
	writeFile(t, filepath.Join(dataDir, "latest", "public", "events.json"), `{"upcoming":true}`)

	n, err := ensureMonthFileSet(dataDir)
	if err != nil || n == 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	for _, ym := range []string{"2025/11", "2025/12", "2026/01"} {
		for _, tier := range []string{"public", "members", "stewards"} {
			for _, spec := range monthFileSpecs {
				p := filepath.Join(dataDir, ym, tier, spec.rel)
				data, err := os.ReadFile(p)
				if err != nil {
					t.Errorf("%s missing", p)
					continue
				}
				if strings.HasSuffix(spec.rel, ".json") && !json.Valid(data) {
					t.Errorf("%s is not valid JSON", p)
				}
			}
		}
	}
	for _, y := range []string{"2025", "2026"} {
		for _, spec := range yearFileSpecs {
			if _, err := os.Stat(filepath.Join(dataDir, y, "public", spec.rel)); err != nil {
				t.Errorf("%s/public/%s missing", y, spec.rel)
			}
		}
	}

	// Projection, not copy: the public transactions lose the narration.
	pub, _ := os.ReadFile(filepath.Join(dataDir, "2025", "11", "public", "transactions.json"))
	if strings.Contains(string(pub), "DOE JOHN") || !strings.Contains(string(pub), `"t1"`) {
		t.Errorf("public transactions = %s", pub)
	}
	// Old stewards events reach public, without guests.
	ev, _ := os.ReadFile(filepath.Join(dataDir, "2026", "01", "public", "events.json"))
	if !strings.Contains(string(ev), "Assembly") || strings.Contains(string(ev), "Guest") {
		t.Errorf("public events = %s", ev)
	}
	yev, _ := os.ReadFile(filepath.Join(dataDir, "2026", "public", "events.json"))
	if !strings.Contains(string(yev), "Assembly") {
		t.Errorf("year events = %s", yev)
	}
	// Empty months carry empty lists, not null.
	empty, _ := os.ReadFile(filepath.Join(dataDir, "2025", "12", "public", "expenses.json"))
	if !strings.Contains(string(empty), `"expenses": []`) {
		t.Errorf("empty expenses = %s", empty)
	}
	door, _ := os.ReadFile(filepath.Join(dataDir, "2025", "12", "public", "door.json"))
	if strings.Contains(string(door), "openers\": [") {
		t.Errorf("public door.json keeps its public shape (counts): %s", door)
	}
	// latest/ is never touched.
	if l, _ := os.ReadFile(filepath.Join(dataDir, "latest", "public", "events.json")); string(l) != `{"upcoming":true}` {
		t.Errorf("latest/ was overwritten: %s", l)
	}
	// Idempotent.
	if n, _ := ensureMonthFileSet(dataDir); n != 0 {
		t.Errorf("second run wrote %d files", n)
	}
}

// Year files never mirror to latest/: latest/contributors.json is the top
// contributors list, not the last year processed.
func TestYearFilesDoNotMirrorToLatest(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	writeTiersSame(dataDir, "latest", "", "activitygrid.json", []byte(`{"years":[]}`))
	writeTiersSame(dataDir, "2027", "", "activitygrid.json", []byte(`{"year":"2027","months":[]}`))
	got, _ := os.ReadFile(filepath.Join(dataDir, "latest", "public", "activitygrid.json"))
	if string(got) != `{"years":[]}` {
		t.Errorf("year file overwrote latest/: %s", got)
	}
	writeTiersSame(dataDir, "2026", "08", "summary.json", []byte(`{"month":"08"}`))
	if got, _ := os.ReadFile(filepath.Join(dataDir, "latest", "public", "summary.json")); string(got) != `{"month":"08"}` {
		t.Errorf("month files still mirror to latest/: %s", got)
	}
}
