package stripe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/CommonsHub/chb/sources"
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
	PaymentMethod   string            `json:"paymentMethod,omitempty"`
	PaymentLink     string            `json:"paymentLink,omitempty"`
}

var KnownApps = map[string]string{
	"ca_HB0JKrk4R6zGWt4fAD9M6iutRhuBdFqd": "Luma",
	"ca_68FQ4jN0XMVhxpnk6gAptwvx90S9VYXF": "Open Collective",
}

func FetchCharges(apiKey, accountID string, chargeIDs []string) (map[string]*Charge, error) {
	return FetchChargesWithProgress(apiKey, accountID, chargeIDs, nil)
}

func FetchChargesWithProgress(apiKey, accountID string, chargeIDs []string, progress sources.ProgressFunc) (map[string]*Charge, error) {
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
		if progress != nil && (i == 0 || (i+1)%10 == 0 || i+1 == len(unique)) {
			progress(sources.ProgressEvent{
				Source:  Source,
				Step:    "fetch_charges",
				Detail:  "charge_session",
				Current: i + 1,
				Total:   len(unique),
			})
		}
		charge, err := fetchSingleCharge(apiKey, accountID, chargeID)
		if err != nil {
			continue
		}
		result[chargeID] = charge
		time.Sleep(100 * time.Millisecond)
	}

	return result, nil
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
		ReceiptEmail         string            `json:"receipt_email"`
		Metadata             map[string]string `json:"metadata"`
		PaymentMethodDetails *struct {
			Card *struct {
				Brand string `json:"brand"`
				Last4 string `json:"last4"`
			} `json:"card"`
		} `json:"payment_method_details"`
		PaymentIntent interface{} `json:"payment_intent"`
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
	if chargeResp.PaymentMethodDetails != nil && chargeResp.PaymentMethodDetails.Card != nil {
		card := chargeResp.PaymentMethodDetails.Card
		charge.PaymentMethod = fmt.Sprintf("%s ****%s", card.Brand, card.Last4)
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

	if piID != "" {
		sessionURL := fmt.Sprintf("https://api.stripe.com/v1/checkout/sessions?payment_intent=%s", piID)
		sessionRaw, err := get(apiKey, accountID, sessionURL)
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
			}
		}
	}

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
	if strings.HasPrefix(id, "ch_") {
		return id
	}
	return ""
}
