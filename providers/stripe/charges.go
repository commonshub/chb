package stripe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/CommonsHub/chb/providers"
)

var httpClient = &http.Client{Timeout: 20 * time.Second}

// ChargeData holds Stripe provider objects related to balance transactions.
type ChargeData struct {
	FetchedAt      string             `json:"fetchedAt"`
	Charges        map[string]*Charge `json:"charges"`
	RefundToCharge map[string]string  `json:"refundToCharge,omitempty"`
}

// Charge holds data from a Stripe charge object and checkout session.
type Charge struct {
	ID              string            `json:"id"`
	CustomerID      string            `json:"customerId,omitempty"`
	CustomerName    string            `json:"customerName,omitempty"`
	CustomerEmail   string            `json:"customerEmail,omitempty"`
	BillingName     string            `json:"billingName,omitempty"`
	BillingEmail    string            `json:"billingEmail,omitempty"`
	ReceiptEmail    string            `json:"receiptEmail,omitempty"`
	Description     string            `json:"description,omitempty"`
	Application     string            `json:"application,omitempty"`
	ApplicationName string            `json:"applicationName,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	CustomFields    map[string]string `json:"customFields,omitempty"`
	PaymentLink     string            `json:"paymentLink,omitempty"`
	ProductID       string            `json:"productId,omitempty"`
	ProductName     string            `json:"productName,omitempty"`
	// Enriched is true once the checkout session and its line items were
	// fetched successfully (or there was no session to fetch). The backfill
	// re-fetches any charge that isn't enriched yet, so a charge whose session
	// data was lost (e.g. a failed/incomplete earlier fetch) is recovered, and
	// the pass still converges once everything succeeds.
	Enriched bool `json:"enriched,omitempty"`
}

var KnownApps = map[string]string{
	"ca_HB0JKrk4R6zGWt4fAD9M6iutRhuBdFqd": "luma",
	"ca_68FQ4jN0XMVhxpnk6gAptwvx90S9VYXF": "opencollective",
}

func FetchCharges(apiKey, accountID string, chargeIDs []string) (map[string]*Charge, error) {
	return FetchChargesWithProgress(apiKey, accountID, chargeIDs, nil)
}

func FetchChargesWithProgress(apiKey, accountID string, chargeIDs []string, progress providers.ProgressFunc) (map[string]*Charge, error) {
	result := map[string]*Charge{}
	if len(chargeIDs) == 0 {
		return result, nil
	}

	seen := map[string]bool{}
	var unique []string
	for _, id := range chargeIDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		unique = append(unique, id)
	}

	for i, chargeID := range unique {
		// Report every charge: this loop can run for hundreds of charges
		// (especially the product backfill), so a per-charge counter is what
		// keeps the user from thinking it has hung.
		if progress != nil {
			progress(providers.ProgressEvent{
				Source:  Source,
				Step:    "fetch_charges",
				Detail:  "charge_session",
				Current: i + 1,
				Total:   len(unique),
			})
		}
		// Retry transient failures (rate limits, network blips) instead of
		// silently dropping the charge — a dropped charge loses the customer /
		// metadata / paymentLink the classifier needs, so the tx falls back to
		// a default category. FillMissingCharges catches anything still absent.
		var charge *Charge
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			if charge, err = fetchSingleCharge(apiKey, accountID, chargeID); err == nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
		}
		if err != nil {
			continue
		}
		result[chargeID] = charge
		time.Sleep(100 * time.Millisecond)
	}

	return result, nil
}

// FillMissingCharges scans every archived balance transaction, finds the
// charge-kind ones whose charge object is not in the local archive, fetches the
// missing charges, and saves them back into their month files. This makes
// `chb accounts stripe pull` self-healing: a charge dropped by a transient
// fetch error on an earlier run is recovered, so classification always has the
// customer / metadata / paymentLink it needs. Returns the number recovered.
func FillMissingCharges(dataDir, apiKey, accountID string, progress providers.ProgressFunc) (int, error) {
	// Collect every charge id referenced by a BT, grouped by the BT's month,
	// and the set of charge ids already on disk.
	chargeMonth := map[string]string{} // chargeID -> "YYYY-MM"
	have := map[string]bool{}
	needEnrich := map[string]bool{} // present locally but session/product not yet fetched
	years, err := os.ReadDir(dataDir)
	if err != nil {
		return 0, err
	}
	for _, y := range years {
		if !y.IsDir() {
			continue
		}
		months, err := os.ReadDir(filepath.Join(dataDir, y.Name()))
		if err != nil {
			continue
		}
		for _, m := range months {
			if !m.IsDir() {
				continue
			}
			ym := y.Name() + "-" + m.Name()
			localCharges, _ := LoadChargeData(dataDir, y.Name(), m.Name())
			for id, ch := range localCharges {
				have[id] = true
				// A charge whose checkout session / product hasn't been
				// successfully fetched yet (predates product fetching, or an
				// earlier fetch failed) is re-fetched once to backfill the
				// session (payment link, metadata) and product. The Enriched
				// flag makes this converge — once fetched, it's left alone.
				if ch != nil && !ch.Enriched {
					needEnrich[id] = true
					if _, seen := chargeMonth[id]; !seen {
						chargeMonth[id] = ym
					}
				}
			}
			path := TransactionCachePath(dataDir, y.Name(), m.Name())
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				continue
			}
			var cache CacheFile
			if json.Unmarshal(data, &cache) != nil {
				continue
			}
			for _, tx := range cache.Transactions {
				if cid := ExtractChargeID(tx.Source); cid != "" {
					if _, seen := chargeMonth[cid]; !seen {
						chargeMonth[cid] = ym
					}
				}
			}
		}
	}

	var missing []string
	for cid := range chargeMonth {
		if !have[cid] || needEnrich[cid] {
			missing = append(missing, cid)
		}
	}
	if len(missing) == 0 {
		return 0, nil
	}

	// Announce the count up front — the scan above is silent and the fetch
	// below can take minutes, so tell the caller how much work is queued.
	if progress != nil {
		progress(providers.ProgressEvent{
			Source: Source,
			Step:   "fetch_charges",
			Detail: "charge_backfill_begin",
			Total:  len(missing),
		})
	}

	charges, err := FetchChargesWithProgress(apiKey, accountID, missing, progress)
	if err != nil {
		return 0, err
	}
	// Group recovered charges by their BT's month and merge into each file.
	byMonth := map[string]map[string]*Charge{}
	for cid, ch := range charges {
		ym := chargeMonth[cid]
		if byMonth[ym] == nil {
			byMonth[ym] = map[string]*Charge{}
		}
		byMonth[ym][cid] = ch
	}
	recovered := 0
	for ym, monthCharges := range byMonth {
		parts := strings.Split(ym, "-")
		if len(parts) != 2 {
			continue
		}
		existing, refunds := LoadChargeData(dataDir, parts[0], parts[1])
		if existing == nil {
			existing = map[string]*Charge{}
		}
		for id, ch := range monthCharges {
			existing[id] = ch
			recovered++
		}
		if err := SaveChargeData(dataDir, parts[0], parts[1], existing, refunds); err != nil {
			return recovered, err
		}
	}
	return recovered, nil
}

func fetchSingleCharge(apiKey, accountID, chargeID string) (*Charge, error) {
	chargeURL := fmt.Sprintf("https://api.stripe.com/v1/charges/%s?expand[]=customer&expand[]=payment_intent", chargeID)
	raw, err := get(apiKey, accountID, chargeURL)
	if err != nil {
		return nil, err
	}

	var chargeResp struct {
		ID          string `json:"id"`
		Description string `json:"description"`
		Application string `json:"application"`
		Customer    *struct {
			ID    string `json:"id"`
			Name  string `json:"name"`
			Email string `json:"email"`
		} `json:"customer"`
		BillingDetails struct {
			Name  string `json:"name"`
			Email string `json:"email"`
		} `json:"billing_details"`
		ReceiptEmail  string            `json:"receipt_email"`
		Metadata      map[string]string `json:"metadata"`
		PaymentIntent interface{}       `json:"payment_intent"`
	}

	if err := json.Unmarshal(raw, &chargeResp); err != nil {
		return nil, err
	}

	charge := &Charge{
		ID:           chargeResp.ID,
		Description:  chargeResp.Description,
		Application:  chargeResp.Application,
		BillingName:  chargeResp.BillingDetails.Name,
		BillingEmail: chargeResp.BillingDetails.Email,
		ReceiptEmail: chargeResp.ReceiptEmail,
		Metadata:     chargeResp.Metadata,
	}
	if charge.Metadata == nil {
		charge.Metadata = map[string]string{}
	}

	if chargeResp.Customer != nil {
		charge.CustomerID = chargeResp.Customer.ID
		charge.CustomerName = chargeResp.Customer.Name
		charge.CustomerEmail = chargeResp.Customer.Email
	}
	if chargeResp.Application != "" {
		if name, ok := KnownApps[chargeResp.Application]; ok {
			charge.ApplicationName = name
		} else {
			charge.ApplicationName = chargeResp.Application
		}
	}
	piID := ""
	if piObj, ok := chargeResp.PaymentIntent.(map[string]interface{}); ok {
		if id, ok := piObj["id"].(string); ok {
			piID = id
		}
		if piMeta, ok := piObj["metadata"].(map[string]interface{}); ok {
			for k, v := range piMeta {
				if s, ok := v.(string); ok && s != "" {
					charge.Metadata[k] = s
				}
			}
		}
	} else if piStr, ok := chargeResp.PaymentIntent.(string); ok {
		piID = piStr
	}

	// enriched stays true unless a session/line-items fetch we attempt fails
	// transiently — then the charge is left un-enriched so the backfill retries.
	enriched := true
	if piID != "" {
		// NOTE: line_items is NOT expandable on the checkout-sessions *list*
		// endpoint — passing expand[]=data.line_items there makes Stripe reject
		// the whole request, dropping payment_link/metadata too. So list the
		// session plainly here, then fetch its line items (with the product)
		// from the dedicated per-session endpoint below.
		sessionURL := fmt.Sprintf("https://api.stripe.com/v1/checkout/sessions?payment_intent=%s", piID)
		sessionRaw, err := get(apiKey, accountID, sessionURL)
		if err != nil {
			enriched = false
		}
		if err == nil {
			var sessionResp struct {
				Data []struct {
					ID           string            `json:"id"`
					Metadata     map[string]string `json:"metadata"`
					PaymentLink  string            `json:"payment_link"`
					CustomFields []struct {
						Key   string `json:"key"`
						Label struct {
							Custom string `json:"custom"`
						} `json:"label"`
						Text *struct {
							Value string `json:"value"`
						} `json:"text"`
						Dropdown *struct {
							Value string `json:"value"`
						} `json:"dropdown"`
					} `json:"custom_fields"`
				} `json:"data"`
			}
			if json.Unmarshal(sessionRaw, &sessionResp) == nil && len(sessionResp.Data) > 0 {
				session := sessionResp.Data[0]
				for k, v := range session.Metadata {
					if v != "" {
						charge.Metadata[k] = v
					}
				}
				if len(session.CustomFields) > 0 {
					charge.CustomFields = map[string]string{}
					for _, cf := range session.CustomFields {
						label := cf.Label.Custom
						if label == "" {
							label = cf.Key
						}
						value := ""
						if cf.Text != nil {
							value = cf.Text.Value
						} else if cf.Dropdown != nil {
							value = cf.Dropdown.Value
						}
						if value != "" {
							charge.CustomFields[label] = value
						}
					}
				}
				charge.PaymentLink = session.PaymentLink
				// Line items (and the product behind them) ARE expandable on the
				// per-session endpoint — fetch what was actually bought.
				if session.ID != "" {
					liURL := fmt.Sprintf("https://api.stripe.com/v1/checkout/sessions/%s/line_items?limit=1&expand[]=data.price.product", session.ID)
					liRaw, lerr := get(apiKey, accountID, liURL)
					if lerr != nil {
						enriched = false
					}
					if lerr == nil {
						var liResp struct {
							Data []struct {
								Description string `json:"description"`
								Price       struct {
									Product struct {
										ID   string `json:"id"`
										Name string `json:"name"`
									} `json:"product"`
								} `json:"price"`
							} `json:"data"`
						}
						if json.Unmarshal(liRaw, &liResp) == nil && len(liResp.Data) > 0 {
							li := liResp.Data[0]
							charge.ProductID = li.Price.Product.ID
							charge.ProductName = li.Price.Product.Name
							if charge.ProductName == "" {
								charge.ProductName = li.Description
							}
						}
					}
				}
			}
		}
	}

	charge.Enriched = enriched
	return charge, nil
}

func FetchRefundChargeID(apiKey, accountID, refundID string) string {
	raw, err := get(apiKey, accountID, fmt.Sprintf("https://api.stripe.com/v1/refunds/%s", refundID))
	if err != nil {
		return ""
	}
	var refund struct {
		Charge string `json:"charge"`
	}
	if json.Unmarshal(raw, &refund) == nil {
		return refund.Charge
	}
	return ""
}

func get(apiKey, accountID, url string) (json.RawMessage, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if accountID != "" {
		req.Header.Set("Stripe-Account", accountID)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		time.Sleep(2 * time.Second)
		resp2, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp2.Body.Close()
		resp = resp2
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("stripe API returned %d", resp.StatusCode)
	}

	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func (c *Charge) BestName() string {
	if c.BillingName != "" {
		return c.BillingName
	}
	if c.CustomerName != "" {
		return c.CustomerName
	}
	return ""
}

func (c *Charge) BestEmail() string {
	if c.BillingEmail != "" {
		return c.BillingEmail
	}
	if c.CustomerEmail != "" {
		return c.CustomerEmail
	}
	if c.ReceiptEmail != "" {
		return c.ReceiptEmail
	}
	return ""
}

func LoadChargeData(dataDir, year, month string) (map[string]*Charge, map[string]string) {
	data, err := os.ReadFile(Path(dataDir, year, month, ChargesFile))
	if err != nil {
		return nil, nil
	}
	var chargeData ChargeData
	if json.Unmarshal(data, &chargeData) != nil {
		return nil, nil
	}
	return chargeData.Charges, chargeData.RefundToCharge
}

func LoadCustomerData(dataDir, year, month string) map[string]*CustomerPII {
	data, err := os.ReadFile(Path(dataDir, year, month, CustomersFile))
	if err != nil {
		return nil
	}
	var customerData CustomerData
	if json.Unmarshal(data, &customerData) != nil {
		return nil
	}
	return customerData.Customers
}

func ExtractSourceID(source json.RawMessage) string {
	if len(source) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(source, &s) == nil {
		return s
	}
	var obj struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(source, &obj) == nil {
		return obj.ID
	}
	return ""
}

func ExtractChargeID(source json.RawMessage) string {
	id := ExtractSourceID(source)
	// ch_ is a card charge; py_ is the charge object Stripe issues for
	// non-card payments (SEPA, Bancontact, etc.). Both are fetchable via
	// /v1/charges/<id> and carry the customer / checkout-session / product.
	if strings.HasPrefix(id, "ch_") || strings.HasPrefix(id, "py_") {
		return id
	}
	return ""
}
