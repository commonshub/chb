package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// StripeChargeEnrichment holds private customer/app data from Stripe charges.
// Stored in finance/stripe/private/charges.json per month.
type StripeChargeEnrichment struct {
	FetchedAt      string                   `json:"fetchedAt"`
	Charges        map[string]*StripeCharge `json:"charges"`                  // keyed by charge ID (ch_...)
	RefundToCharge map[string]string        `json:"refundToCharge,omitempty"` // re_... → ch_...
}

// StripeCharge holds enrichment data from a Stripe charge object + checkout session.
type StripeCharge struct {
	ID              string            `json:"id"`
	CustomerName    string            `json:"customerName,omitempty"`
	CustomerEmail   string            `json:"customerEmail,omitempty"`
	BillingName     string            `json:"billingName,omitempty"`
	BillingEmail    string            `json:"billingEmail,omitempty"`
	ReceiptEmail    string            `json:"receiptEmail,omitempty"`
	Description     string            `json:"description,omitempty"`
	Application     string            `json:"application,omitempty"`     // ca_... ID
	ApplicationName string            `json:"applicationName,omitempty"` // resolved name
	Metadata        map[string]string `json:"metadata,omitempty"`        // charge + session metadata merged
	CustomFields    map[string]string `json:"customFields,omitempty"`    // checkout session custom fields
	PaymentMethod   string            `json:"paymentMethod,omitempty"`   // e.g. "visa ****4242"
	PaymentLink     string            `json:"paymentLink,omitempty"`     // plink_... ID
}

// Known Stripe Connect application IDs → names
var knownStripeApps = map[string]string{
	"ca_HB0JKrk4R6zGWt4fAD9M6iutRhuBdFqd": "Luma",
	"ca_68FQ4jN0XMVhxpnk6gAptwvx90S9VYXF": "Open Collective",
}

// fetchStripeCharges fetches charge details for a list of charge IDs.
func fetchStripeCharges(apiKey, accountID string, chargeIDs []string) (map[string]*StripeCharge, error) {
	result := map[string]*StripeCharge{}
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
	chargeIDs = unique

	// Batch fetch charges (100 at a time with customer expansion)
	for i := 0; i < len(chargeIDs); i += 100 {
		end := i + 100
		if end > len(chargeIDs) {
			end = len(chargeIDs)
		}
		batch := chargeIDs[i:end]

		for j, chargeID := range batch {
			if j == 0 || (i+j+1)%10 == 0 || i+j+1 == len(chargeIDs) {
				fmt.Printf(" %s%d/%d%s", Fmt.Dim, i+j+1, len(chargeIDs), Fmt.Reset)
			}
			charge, err := fetchSingleCharge(apiKey, accountID, chargeID)
			if err != nil {
				continue // skip failures silently
			}
			result[chargeID] = charge
			time.Sleep(100 * time.Millisecond) // rate limit
		}
	}

	return result, nil
}

func fetchSingleCharge(apiKey, accountID, chargeID string) (*StripeCharge, error) {
	// Fetch charge with customer + payment_intent expanded
	chargeURL := fmt.Sprintf("https://api.stripe.com/v1/charges/%s?expand[]=customer&expand[]=payment_intent", chargeID)

	raw, err := stripeGet(apiKey, accountID, chargeURL)
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
		ReceiptEmail         string            `json:"receipt_email"`
		Metadata             map[string]string `json:"metadata"`
		PaymentMethodDetails *struct {
			Card *struct {
				Brand string `json:"brand"`
				Last4 string `json:"last4"`
			} `json:"card"`
		} `json:"payment_method_details"`
		PaymentIntent interface{} `json:"payment_intent"` // expanded object or string ID
	}

	if err := json.Unmarshal(raw, &chargeResp); err != nil {
		return nil, err
	}

	charge := &StripeCharge{
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
		charge.CustomerName = chargeResp.Customer.Name
		charge.CustomerEmail = chargeResp.Customer.Email
	}

	// Resolve application name
	if chargeResp.Application != "" {
		if name, ok := knownStripeApps[chargeResp.Application]; ok {
			charge.ApplicationName = name
		} else {
			charge.ApplicationName = chargeResp.Application
		}
	}

	// Payment method
	if chargeResp.PaymentMethodDetails != nil && chargeResp.PaymentMethodDetails.Card != nil {
		card := chargeResp.PaymentMethodDetails.Card
		charge.PaymentMethod = fmt.Sprintf("%s ****%s", card.Brand, card.Last4)
	}

	// Extract payment intent ID to fetch checkout session
	piID := ""
	if piObj, ok := chargeResp.PaymentIntent.(map[string]interface{}); ok {
		if id, ok := piObj["id"].(string); ok {
			piID = id
		}
		// Also merge PI metadata
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

	// Fetch checkout session (has metadata from payment links + custom fields)
	if piID != "" {
		sessionURL := fmt.Sprintf("https://api.stripe.com/v1/checkout/sessions?payment_intent=%s", piID)
		sessionRaw, err := stripeGet(apiKey, accountID, sessionURL)
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

				// Merge session metadata (e.g. collective from payment link)
				for k, v := range session.Metadata {
					if v != "" {
						charge.Metadata[k] = v
					}
				}

				// Extract custom fields
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
			}
		}
	}

	return charge, nil
}

// stripeGet makes an authenticated GET request to the Stripe API with retry on 429.
func stripeGet(apiKey, accountID, url string) (json.RawMessage, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if accountID != "" {
		req.Header.Set("Stripe-Account", accountID)
	}

	resp, err := stripeHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		time.Sleep(2 * time.Second)
		resp2, err := stripeHTTPClient.Do(req)
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

// BestName returns the best available customer name from a charge.
func (c *StripeCharge) BestName() string {
	if c.BillingName != "" {
		return c.BillingName
	}
	if c.CustomerName != "" {
		return c.CustomerName
	}
	return ""
}

// BestEmail returns the best available email from a charge.
func (c *StripeCharge) BestEmail() string {
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

// LoadStripeChargeEnrichment reads the private charge data for a month.
func LoadStripeChargeEnrichment(dataDir, year, month string) (map[string]*StripeCharge, map[string]string) {
	path := filepath.Join(dataDir, year, month, "finance", "stripe", "private", "charges.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil
	}
	var enrichment StripeChargeEnrichment
	if json.Unmarshal(data, &enrichment) != nil {
		return nil, nil
	}
	return enrichment.Charges, enrichment.RefundToCharge
}

// SaveStripeChargeEnrichment writes the private charge data for a month.
func SaveStripeChargeEnrichment(dataDir, year, month string, charges map[string]*StripeCharge, refundToCharge map[string]string) {
	enrichment := StripeChargeEnrichment{
		FetchedAt:      time.Now().UTC().Format(time.RFC3339),
		Charges:        charges,
		RefundToCharge: refundToCharge,
	}
	data, _ := json.MarshalIndent(enrichment, "", "  ")
	relPath := filepath.Join("finance", "stripe", "private", "charges.json")
	_ = writeDataFile(filepath.Join(dataDir, year, month, relPath), data)
	// Also write to latest
	_ = writeDataFile(filepath.Join(dataDir, "latest", relPath), data)
}

// loadStripeCustomerData reads the private customer PII for a month.
func loadStripeCustomerData(dataDir, year, month string) map[string]*StripeCustomerPII {
	path := filepath.Join(dataDir, year, month, "finance", "stripe", "private", "customers.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var customerData StripeCustomerData
	if json.Unmarshal(data, &customerData) != nil {
		return nil
	}
	return customerData.Customers
}

// extractSourceID extracts the source ID from a Stripe balance transaction's source field.
// Returns the ID and its prefix (e.g. "ch_", "re_", "po_").
func extractSourceID(source json.RawMessage) string {
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

// extractChargeID extracts a charge ID from a Stripe balance transaction's source field.
func extractChargeID(source json.RawMessage) string {
	id := extractSourceID(source)
	if strings.HasPrefix(id, "ch_") {
		return id
	}
	return ""
}

// fetchRefundChargeID fetches the original charge ID for a refund.
func fetchRefundChargeID(apiKey, accountID, refundID string) string {
	raw, err := stripeGet(apiKey, accountID, fmt.Sprintf("https://api.stripe.com/v1/refunds/%s", refundID))
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
