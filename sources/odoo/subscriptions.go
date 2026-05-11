package odoo

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
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
	resp, err := http.Post(odooURL+"/jsonrpc", "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var rpcResp rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, err
	}
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
		return nil, fmt.Errorf("odoo error: %s", msg)
	}
	return rpcResp.Result, nil
}

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
		return 0, fmt.Errorf("auth failed (uid=0)")
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
