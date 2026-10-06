package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveConfigEnvKeepsUnknownKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.env")
	if err := os.WriteFile(path, []byte("ODOO_URL=https://x.odoo.com\nRESEND_API_KEY=re_123\nRENEW_LINK_SECRET=s3cret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env := loadConfigEnv(path)
	env["EMAIL_HASH_SALT"] = "prod-abc"
	if err := saveConfigEnv(path, env); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	for _, want := range []string{"ODOO_URL=https://x.odoo.com", "RESEND_API_KEY=re_123", "RENEW_LINK_SECRET=s3cret", "EMAIL_HASH_SALT=prod-abc"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("lost %q:\n%s", want, data)
		}
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0600 {
		t.Errorf("mode %v", fi.Mode().Perm())
	}
}
