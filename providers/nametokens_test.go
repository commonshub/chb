package providers

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"
)

func TestNameTokens(t *testing.T) {
	h := func(tok string) string {
		s := sha256.Sum256([]byte("name:" + tok + "salt"))
		return fmt.Sprintf("%x", s)[:16]
	}
	want := []string{h("dean"), h("miriam")}
	if want[0] > want[1] {
		want[0], want[1] = want[1], want[0]
	}
	for _, name := range []string{"Dean Miriam", "MIRIAM  dean", "Miriam-Dean, Miriam", "Dean J. Miriam"} {
		if got := NameTokens(name, "salt"); !reflect.DeepEqual(got, want) {
			t.Errorf("%q: got %v want %v", name, got, want)
		}
	}
	if got := NameTokens("Zoë Ångström", "salt"); len(got) != 2 || !reflect.DeepEqual(got, NameTokens("zoe angstrom", "salt")) {
		t.Errorf("accents: %v", got)
	}
	if NameTokens("Dean Miriam", "") != nil || NameTokens("", "salt") != nil {
		t.Error("empty salt or name must give no tokens")
	}
	// Fixed vector shared with the website's implementation.
	if h("dean") != "80fc3a1415d322ba" {
		t.Errorf("vector: %s", h("dean"))
	}
}
