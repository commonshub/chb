package odoo

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type MembershipProduct struct {
	ID       int
	Name     string
	Interval string
}

type MembershipAmount struct {
	Value    float64 `json:"value"`
	Decimals int     `json:"decimals"`
	Currency string  `json:"currency"`
}

type MembershipPayment struct {
	Date   string           `json:"date"`
	Amount MembershipAmount `json:"amount"`
	Status string           `json:"status"`
	URL    string           `json:"url,omitempty"`
}

type MembershipSubscription struct {
	ID                 string             `json:"id"`
	Source             string             `json:"source"`
	EmailHash          string             `json:"emailHash"`
	FirstName          string             `json:"firstName"`
	LastName           string             `json:"lastName"`
	Plan               string             `json:"plan"`
	Amount             MembershipAmount   `json:"amount"`
	Interval           string             `json:"interval"`
	Status             string             `json:"status"`
	CurrentPeriodStart string             `json:"currentPeriodStart"`
	CurrentPeriodEnd   string             `json:"currentPeriodEnd"`
	LatestPayment      *MembershipPayment `json:"latestPayment"`
	SubscriptionURL    string             `json:"subscriptionUrl,omitempty"`
	CreatedAt          string             `json:"createdAt"`
	Discord            *string            `json:"discord"`
	IsOrganization     bool               `json:"isOrganization,omitempty"`
	ProductID          interface{}        `json:"productId,omitempty"`
}

type MembershipSnapshot struct {
	Provider      string                   `json:"provider"`
	FetchedAt     string                   `json:"fetchedAt"`
	Subscriptions []MembershipSubscription `json:"subscriptions"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			Debug string `json:"debug"`
		} `json:"data"`
	} `json:"error"`
}

var sourceEmailPattern = regexp.MustCompile(`(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b`)

func BuildMembershipSnapshot(products []MembershipProduct, odooURL, login, password, salt string) (MembershipSnapshot, error) {
	db := dbFromURL(odooURL)
	empty := MembershipSnapshot{Provider: Source, FetchedAt: time.Now().UTC().Format(time.RFC3339)}

	uid, err := auth(odooURL, db, login, password)
	if err != nil {
		return empty, err
	}

	templateIDs := make([]interface{}, len(products))
	for i, p := range products {
		templateIDs[i] = p.ID
	}

	ppResult, err := exec(odooURL, db, uid, password, "product.product", "search",
		[]interface{}{[]interface{}{[]interface{}{"product_tmpl_id", "in", templateIDs}}}, nil)
	if err != nil {
		return empty, fmt.Errorf("product search: %w", err)
	}
	var ppIDs []int
	_ = json.Unmarshal(ppResult, &ppIDs)
	if len(ppIDs) == 0 {
		return empty, nil
	}

	ppIDsIface := intSliceToIface(ppIDs)
	orderResult, err := exec(odooURL, db, uid, password, "sale.order", "search",
		[]interface{}{[]interface{}{
			[]interface{}{"is_subscription", "=", true},
			[]interface{}{"order_line.product_id", "in", ppIDsIface},
			[]interface{}{"subscription_state", "in", []string{"3_progress", "4_paused"}},
		}}, nil)
	if err != nil {
		return empty, fmt.Errorf("order search: %w", err)
	}
	var orderIDs []int
	_ = json.Unmarshal(orderResult, &orderIDs)
	if len(orderIDs) == 0 {
		return empty, nil
	}

	ordersRaw, err := exec(odooURL, db, uid, password, "sale.order", "read", []interface{}{intSliceToIface(orderIDs)}, map[string]interface{}{
		"fields": []string{"id", "name", "partner_id", "subscription_state", "start_date", "next_invoice_date", "recurring_monthly", "invoice_ids", "order_line"},
	})
	if err != nil {
		return empty, fmt.Errorf("order read: %w", err)
	}
	var orders []map[string]interface{}
	_ = json.Unmarshal(ordersRaw, &orders)

	partnerIDs := map[int]bool{}
	invoiceIDs := map[int]bool{}
	lineIDs := map[int]bool{}
	for _, order := range orders {
		if pid, ok := order["partner_id"].([]interface{}); ok && len(pid) > 0 {
			partnerIDs[int(pid[0].(float64))] = true
		}
		for _, id := range floatList(order["invoice_ids"]) {
			invoiceIDs[id] = true
		}
		for _, id := range floatList(order["order_line"]) {
			lineIDs[id] = true
		}
	}

	invoiceMap := map[int]map[string]interface{}{}
	if len(invoiceIDs) > 0 {
		invRaw, err := exec(odooURL, db, uid, password, "account.move", "read", []interface{}{mapKeys(invoiceIDs)}, map[string]interface{}{
			"fields": []string{"id", "invoice_date", "amount_total", "payment_state"},
		})
		if err == nil {
			var invs []map[string]interface{}
			_ = json.Unmarshal(invRaw, &invs)
			for _, inv := range invs {
				paymentState, _ := inv["payment_state"].(string)
				if paymentState != "paid" && paymentState != "in_payment" {
					continue
				}
				invoiceMap[int(inv["id"].(float64))] = inv
			}
		}
	}

	partnerMap := map[int]map[string]interface{}{}
	if len(partnerIDs) > 0 {
		pRaw, err := exec(odooURL, db, uid, password, "res.partner", "read", []interface{}{mapKeys(partnerIDs)}, map[string]interface{}{
			"fields": []string{"id", "name", "email", "is_company"},
		})
		if err == nil {
			var partners []map[string]interface{}
			_ = json.Unmarshal(pRaw, &partners)
			for _, p := range partners {
				partnerMap[int(p["id"].(float64))] = p
			}
		}
	}

	orderToTemplate := map[int]int{}
	if len(lineIDs) > 0 {
		linesRaw, err := exec(odooURL, db, uid, password, "sale.order.line", "read", []interface{}{mapKeys(lineIDs)}, map[string]interface{}{
			"fields": []string{"id", "order_id", "product_id"},
		})
		if err == nil {
			var lines []map[string]interface{}
			_ = json.Unmarshal(linesRaw, &lines)
			productIDs := map[int]bool{}
			ordersByProduct := map[int][]int{}
			for _, line := range lines {
				orderRef, ok1 := line["order_id"].([]interface{})
				productRef, ok2 := line["product_id"].([]interface{})
				if !ok1 || !ok2 || len(orderRef) == 0 || len(productRef) == 0 {
					continue
				}
				orderID := int(orderRef[0].(float64))
				productID := int(productRef[0].(float64))
				productIDs[productID] = true
				ordersByProduct[productID] = append(ordersByProduct[productID], orderID)
			}
			ppRaw, err := exec(odooURL, db, uid, password, "product.product", "read", []interface{}{mapKeys(productIDs)}, map[string]interface{}{
				"fields": []string{"id", "product_tmpl_id"},
			})
			if err == nil {
				var productsRaw []map[string]interface{}
				_ = json.Unmarshal(ppRaw, &productsRaw)
				for _, product := range productsRaw {
					productID := int(product["id"].(float64))
					tmplRef, ok := product["product_tmpl_id"].([]interface{})
					if !ok || len(tmplRef) == 0 {
						continue
					}
					tmplID := int(tmplRef[0].(float64))
					for _, configured := range products {
						if configured.ID == tmplID {
							for _, orderID := range ordersByProduct[productID] {
								orderToTemplate[orderID] = tmplID
							}
						}
					}
				}
			}
		}
	}

	return buildMembershipSnapshot(products, odooURL, salt, orders, partnerMap, invoiceMap, orderToTemplate), nil
}

func buildMembershipSnapshot(products []MembershipProduct, odooURL, salt string, orders []map[string]interface{}, partnerMap, invoiceMap map[int]map[string]interface{}, orderToTemplate map[int]int) MembershipSnapshot {
	var subs []MembershipSubscription
	for _, order := range orders {
		orderID := int(order["id"].(float64))
		partnerID := 0
		if pid, ok := order["partner_id"].([]interface{}); ok && len(pid) > 0 {
			partnerID = int(pid[0].(float64))
		}
		partner := partnerMap[partnerID]
		if partner == nil {
			continue
		}

		email, _ := partner["email"].(string)
		emailHash := email
		if email != "" {
			emailHash = hashEmail(email, salt)
		} else {
			emailHash = fmt.Sprintf("odoo-noemail-%d", orderID)
		}

		partnerName, _ := partner["name"].(string)
		firstName, lastName := splitSourceName(partnerName)
		isCompany, _ := partner["is_company"].(bool)

		tmplID := orderToTemplate[orderID]
		interval := "month"
		for _, product := range products {
			if product.ID == tmplID {
				interval = product.Interval
				break
			}
		}

		subState, _ := order["subscription_state"].(string)
		status := "active"
		if subState == "4_paused" {
			status = "paused"
		}

		recurringMonthly, _ := order["recurring_monthly"].(float64)
		totalAmount := recurringMonthly
		if interval == "year" {
			totalAmount = recurringMonthly * 12
		}

		startDate, _ := order["start_date"].(string)
		nextInvoice, _ := order["next_invoice_date"].(string)

		var latestPayment *MembershipPayment
		if invIDs, ok := order["invoice_ids"].([]interface{}); ok {
			var bestDate string
			for _, iid := range invIDs {
				inv := invoiceMap[int(iid.(float64))]
				if inv == nil {
					continue
				}
				invDate, _ := inv["invoice_date"].(string)
				if invDate > bestDate {
					bestDate = invDate
					invAmount, _ := inv["amount_total"].(float64)
					latestPayment = &MembershipPayment{
						Date:   invDate,
						Amount: MembershipAmount{Value: invAmount, Decimals: 2, Currency: "EUR"},
						Status: "succeeded",
						URL:    fmt.Sprintf("%s/web#id=%d&model=account.move&view_type=form", odooURL, int(iid.(float64))),
					}
				}
			}
		}

		isOrg := isCompany || tmplID == 104
		subs = append(subs, MembershipSubscription{
			ID:                 fmt.Sprintf("odoo-%d", orderID),
			Source:             Source,
			EmailHash:          emailHash,
			FirstName:          firstName,
			LastName:           lastName,
			Plan:               interval + "ly",
			Amount:             MembershipAmount{Value: math.Round(totalAmount*100) / 100, Decimals: 2, Currency: "EUR"},
			Interval:           interval,
			Status:             status,
			CurrentPeriodStart: startDate,
			CurrentPeriodEnd:   nextInvoice,
			LatestPayment:      latestPayment,
			SubscriptionURL:    fmt.Sprintf("%s/web#id=%d&model=sale.order&view_type=form", odooURL, orderID),
			CreatedAt:          startDate,
			IsOrganization:     isOrg,
			ProductID:          tmplID,
		})
	}

	return MembershipSnapshot{
		Provider:      Source,
		FetchedAt:     time.Now().UTC().Format(time.RFC3339),
		Subscriptions: subs,
	}
}

// rpcMaxRetries / rpcRetryBaseDelay control how long we wait when Odoo
// returns an HTTP 429 (SaaS rate limit). Each retry doubles the delay,
// capped at 30s — total budget ~63s with the default settings, enough to
// ride out the usual 30–60s burst window Odoo SaaS imposes.
const rpcMaxRetries = 5
const rpcRetryBaseDelay = 2 * time.Second

// rpcHTTPClient bounds every JSON-RPC request with a per-call deadline.
// Go's http.Post uses http.DefaultClient, which has NO timeout: a SaaS
// instance that accepts the TCP connection but never sends a response body
// would hang the caller indefinitely. Under `chb sync` that freezes the
// whole hourly cron on whatever step it reached (e.g. "verifying cache
// freshness"), which is exactly the failure this guards against. A finite
// deadline turns the stall into a clean, surfaced error (handled by
// friendlyOdooNetworkError) so the run fails fast and the next cron retries.
//
// The default is generous enough for large paginated reads; override with
// ODOO_RPC_TIMEOUT_SECONDS for an unusually slow instance (0 / unset uses
// the default).
const defaultRPCHTTPTimeout = 120 * time.Second

var rpcHTTPClient = &http.Client{Timeout: resolveRPCHTTPTimeout()}

func resolveRPCHTTPTimeout() time.Duration {
	if v := strings.TrimSpace(os.Getenv("ODOO_RPC_TIMEOUT_SECONDS")); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultRPCHTTPTimeout
}

func rpc(odooURL, service, method string, args []interface{}) (json.RawMessage, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "call",
		"params": map[string]interface{}{
			"service": service,
			"method":  method,
			"args":    args,
		},
		"id": time.Now().UnixNano(),
	}
	data, _ := json.Marshal(payload)

	delay := rpcRetryBaseDelay
	for attempt := 0; ; attempt++ {
		resp, err := rpcHTTPClient.Post(odooURL+"/jsonrpc", "application/json", bytes.NewReader(data))
		if err != nil {
			return nil, friendlyOdooNetworkError(odooURL, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// HTTP 429: Odoo SaaS rate-limit. Honour Retry-After when present,
		// otherwise back off exponentially. Don't surface the HTML page —
		// that's noise; the operator wants to know "we're being throttled,
		// waiting Ns".
		if resp.StatusCode == 429 && attempt < rpcMaxRetries {
			wait := delay
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, err := time.ParseDuration(ra + "s"); err == nil && secs > 0 {
					wait = secs
				}
			}
			if wait > 30*time.Second {
				wait = 30 * time.Second
			}
			time.Sleep(wait)
			delay *= 2
			continue
		}

		var rpcResp rpcResponse
		if err := json.Unmarshal(body, &rpcResp); err != nil {
			if resp.StatusCode == 429 {
				return nil, fmt.Errorf("rate-limited by Odoo (HTTP 429) after %d retries — try again in a minute", attempt)
			}
			return nil, friendlyOdooTransportError(odooURL, resp.StatusCode, body)
		}
		return handleRPCResponse(rpcResp)
	}
}

// friendlyOdooNetworkError turns the raw transport error (DNS failure,
// connection refused, TLS error, timeout) into a one-line actionable
// message that names the host and points at the env var to fix.
func friendlyOdooNetworkError(odooURL string, err error) error {
	host := odooURL
	if u, perr := url.Parse(odooURL); perr == nil && u.Host != "" {
		host = u.Host
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "no such host"), strings.Contains(msg, "dns"):
		return fmt.Errorf("Odoo host %s cannot be resolved (DNS lookup failed). "+
			"Check ODOO_URL in $APP_DATA_DIR/settings/config.env", host)
	case strings.Contains(msg, "connection refused"):
		return fmt.Errorf("Odoo at %s refused the connection — the service may be down", host)
	case strings.Contains(msg, "deadline exceeded"), strings.Contains(msg, "timeout"):
		return fmt.Errorf("Odoo at %s timed out — the instance is slow or unreachable; try again in a moment", host)
	case strings.Contains(msg, "x509"), strings.Contains(msg, "tls"), strings.Contains(msg, "certificate"):
		return fmt.Errorf("Odoo at %s has a TLS/certificate problem: %v", host, err)
	}
	return fmt.Errorf("could not reach Odoo at %s: %v", host, err)
}

// friendlyOdooTransportError turns a non-JSON RPC response into an
// actionable error string. Detects the common SaaS failure modes
// (deleted instance redirect, generic 404, login form, server error)
// so operators don't have to wade through HTML to figure out what
// went wrong. Falls back to a truncated preview when nothing matches.
func friendlyOdooTransportError(odooURL string, status int, body []byte) error {
	host := odooURL
	if u, err := url.Parse(odooURL); err == nil && u.Host != "" {
		host = u.Host
	}
	preview := strings.ToLower(string(body))

	// Odoo SaaS serves a JS redirect to /typo when the subdomain doesn't
	// exist anymore (instance deleted or never existed). The body looks
	// like `<script>window.location = 'https://www.odoo.com/typo?…'</script>`.
	if status == 404 && strings.Contains(preview, "odoo.com/typo") {
		return fmt.Errorf("Odoo instance %s does not exist (or has been removed). "+
			"Check ODOO_URL in $APP_DATA_DIR/settings/config.env, "+
			"or pass --odoo-url=<other-instance> for this call", host)
	}
	if status == 404 {
		return fmt.Errorf("Odoo URL %s returned 404 — the JSON-RPC endpoint is not reachable at this host. "+
			"Verify ODOO_URL points at a live Odoo instance", odooURL)
	}
	// HTML login form ⇒ the host is alive but the request didn't auth.
	// Usually a wrong DB or session expiry.
	if strings.Contains(preview, "<title>odoo") || strings.Contains(preview, "name=\"login\"") {
		return fmt.Errorf("Odoo at %s returned an HTML login page instead of JSON-RPC — "+
			"the database name is likely wrong (set --odoo-db or ODOO_DATABASE)", host)
	}
	if status >= 500 {
		return fmt.Errorf("Odoo at %s is unhealthy (HTTP %d) — try again in a moment", host, status)
	}
	// Generic fallback: short preview, no HTML noise.
	snippet := strings.Join(strings.Fields(string(body)), " ")
	if len(snippet) > 160 {
		snippet = snippet[:160] + "…"
	}
	return fmt.Errorf("Odoo at %s returned an unexpected response (HTTP %d): %s", host, status, snippet)
}

// handleRPCResponse extracts the result or formats the Odoo-side error.
// Split out from rpc() so the retry loop above stays linear.
func handleRPCResponse(rpcResp rpcResponse) (json.RawMessage, error) {
	if rpcResp.Error != nil {
		msg := rpcResp.Error.Message
		if rpcResp.Error.Data.Debug != "" {
			// The debug field is a Python traceback. The most informative
			// line is the last non-empty one (the actual exception
			// message). Walk back to skip trailing blanks.
			lines := strings.Split(rpcResp.Error.Data.Debug, "\n")
			for i := len(lines) - 1; i >= 0; i-- {
				if s := strings.TrimSpace(lines[i]); s != "" {
					msg = s
					break
				}
			}
		}
		if msg == "" {
			msg = "(empty error response)"
		}
		return nil, friendlyOdooRPCError(msg)
	}
	return rpcResp.Result, nil
}

// friendlyOdooRPCError takes the (often Python-traceback) error string
// Odoo returns inside a JSON-RPC envelope and turns the common ones into
// short actionable messages. Unrecognised errors fall through with a
// neutral "odoo error: ..." prefix so the original info is still there.
func friendlyOdooRPCError(msg string) error {
	low := strings.ToLower(msg)
	// `psycopg2.OperationalError: ... database "X" does not exist`
	if strings.Contains(low, "database") && strings.Contains(low, "does not exist") {
		if m := dbNameInError.FindStringSubmatch(msg); len(m) > 1 {
			return fmt.Errorf("Odoo database %q does not exist on this server. "+
				"Set ODOO_DATABASE (or pass --odoo-db=<name>) to a valid DB", m[1])
		}
		return fmt.Errorf("Odoo database does not exist on this server. " +
			"Set ODOO_DATABASE (or pass --odoo-db=<name>) to a valid DB")
	}
	// AccessDenied / wrong credentials
	if strings.Contains(low, "access denied") || strings.Contains(low, "accessdenied") ||
		strings.Contains(low, "invalid login") || strings.Contains(low, "wrong login/password") {
		return fmt.Errorf("Odoo rejected the credentials — check ODOO_LOGIN and ODOO_PASSWORD")
	}
	return fmt.Errorf("odoo error: %s", msg)
}

var dbNameInError = regexp.MustCompile(`database\s+"([^"]+)"\s+does not exist`)

func RPC(odooURL, service, method string, args []interface{}) (json.RawMessage, error) {
	return rpc(odooURL, service, method, args)
}

func dbFromURL(odooURL string) string {
	u := strings.TrimPrefix(odooURL, "https://")
	u = strings.TrimPrefix(u, "http://")
	u = strings.Split(u, ".")[0]
	return u
}

func DBFromURL(odooURL string) string {
	return dbFromURL(odooURL)
}

func auth(odooURL, db, login, password string) (int, error) {
	result, err := rpc(odooURL, "common", "authenticate", []interface{}{
		db, login, password, map[string]interface{}{},
	})
	if err != nil {
		return 0, err
	}
	var uid int
	if err := json.Unmarshal(result, &uid); err != nil || uid == 0 {
		return 0, fmt.Errorf("Odoo rejected the credentials — check ODOO_LOGIN and ODOO_PASSWORD (login=%s, db=%s)", login, db)
	}
	return uid, nil
}

func Auth(odooURL, db, login, password string) (int, error) {
	return auth(odooURL, db, login, password)
}

func exec(odooURL, db string, uid int, password, model, method string, args []interface{}, kwargs map[string]interface{}) (json.RawMessage, error) {
	callArgs := []interface{}{db, uid, password, model, method, args}
	if kwargs == nil {
		kwargs = map[string]interface{}{}
	}
	callArgs = append(callArgs, kwargs)
	return rpc(odooURL, "object", "execute_kw", callArgs)
}

func Exec(odooURL, db string, uid int, password, model, method string, args []interface{}, kwargs map[string]interface{}) (json.RawMessage, error) {
	return exec(odooURL, db, uid, password, model, method, args, kwargs)
}

func intSliceToIface(ids []int) []interface{} {
	out := make([]interface{}, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}

func mapKeys(m map[int]bool) []interface{} {
	out := make([]interface{}, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func floatList(v interface{}) []int {
	var out []int
	items, ok := v.([]interface{})
	if !ok {
		return out
	}
	for _, x := range items {
		if f, ok := x.(float64); ok {
			out = append(out, int(f))
		}
	}
	return out
}

func hashEmail(email, salt string) string {
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(email)) + salt))
	return fmt.Sprintf("%x", h)
}

func splitSourceName(name string) (string, string) {
	sanitized := strings.Join(sourceEmailPattern.Split(name, -1), " ")
	sanitized = strings.Join(strings.Fields(sanitized), " ")
	if sanitized == "" {
		return "Member", ""
	}
	parts := strings.Fields(sanitized)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], strings.Join(parts[1:], " ")
}
