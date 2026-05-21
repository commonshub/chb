package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// wrapOdooAuthError relabels the wrapped error so a transport-layer
// rate-limit doesn't get reported as an auth failure (which used to send
// the operator chasing wrong-credentials ghosts). The underlying odoosource
// already detects HTTP 429 and surfaces "rate-limited by Odoo"; we just
// avoid prepending "Odoo authentication failed:" in that case.
func wrapOdooAuthError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	// Already actionable / self-describing — don't smother it with a
	// generic "Odoo authentication failed:" prefix.
	if strings.Contains(msg, "rate-limited by Odoo") ||
		strings.HasPrefix(msg, "Odoo ") ||
		strings.HasPrefix(msg, "could not reach Odoo") {
		return err
	}
	return fmt.Errorf("Odoo authentication failed: %v", err)
}

func odooDBFromURL(odooURL string) string {
	return odoosource.DBFromURL(odooURL)
}

// OdooDBFromURL is the exported alias used by main.go's global-flag handler
// to derive the DB slug from a URL when only one of the two was specified.
func OdooDBFromURL(odooURL string) string {
	return odoosource.DBFromURL(odooURL)
}

func odooAuth(odooURL, db, login, password string) (int, error) {
	return odoosource.Auth(odooURL, db, login, password)
}

func odooExec(odooURL, db string, uid int, password, model, method string, args []interface{}, kwargs map[string]interface{}) (json.RawMessage, error) {
	if isMutatingOdooMethod(method) {
		printOdooWriteBannerOnce(odooURL, db)
	}
	return odoosource.Exec(odooURL, db, uid, password, model, method, args, kwargs)
}

// isMutatingOdooMethod conservatively classifies Odoo RPC methods.
// Known read-only methods return false; everything else (write/create/
// unlink, action_*, button_*, reconcile, register_payment, custom server
// methods) is assumed to mutate. Erring toward "mutating" keeps the
// safety banner accurate even when new server methods are introduced.
func isMutatingOdooMethod(method string) bool {
	switch method {
	case "search", "search_read", "search_count", "read", "read_group",
		"name_search", "name_get", "fields_get", "default_get",
		"get_metadata", "_form_view_action":
		return false
	}
	return true
}

// odooWriteBannerPrinted is reset to false at process start so the
// banner prints once per `chb …` invocation, immediately before the
// first write hits Odoo. Tests and long-lived processes (none today)
// would need to reset it manually.
var odooWriteBannerPrinted bool

// printOdooWriteBannerOnce prints a high-visibility banner identifying
// the Odoo target before the first mutating RPC. Idempotent — only
// emits on the first call per process. CLI handlers that are about to
// write should call this in their header rather than wait for the
// odooExec hook below, so the operator sees the target *before* any
// confirm prompt.
func printOdooWriteBannerOnce(odooURL, db string) {
	if odooWriteBannerPrinted {
		return
	}
	odooWriteBannerPrinted = true
	host := OdooHost(odooURL)
	if host == "" {
		host = odooURL
	}
	if db == "" {
		db = "(default)"
	}
	fmt.Fprintf(os.Stderr, "\n  %s● Odoo target:%s %s%s%s  %s(db: %s)%s\n",
		Fmt.Yellow, Fmt.Reset,
		Fmt.Bold, host, Fmt.Reset,
		Fmt.Dim, db, Fmt.Reset)
}
