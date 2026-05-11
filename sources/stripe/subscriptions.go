package stripe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Subscription struct {
	ID                 string `json:"id"`
	Status             string `json:"status"`
	Customer           string `json:"customer"`
	CurrentPeriodStart int64  `json:"current_period_start"`
	CurrentPeriodEnd   int64  `json:"current_period_end"`
	Created            int64  `json:"created"`
	CanceledAt         *int64 `json:"canceled_at"`
	EndedAt            *int64 `json:"ended_at"`
	Items              struct {
		Data []SubscriptionItem `json:"data"`
	} `json:"items"`
	Metadata      map[string]string `json:"metadata"`
	LatestInvoice json.RawMessage   `json:"latest_invoice"`
}

type SubscriptionItem struct {
	Price Price `json:"price"`
}

type Price struct {
	ID         string `json:"id"`
	UnitAmount int64  `json:"unit_amount"`
	Currency   string `json:"currency"`
	Recurring  struct {
		Interval      string `json:"interval"`
		IntervalCount int    `json:"interval_count"`
	} `json:"recurring"`
	Product string `json:"product"`
}

type Customer struct {
	ID       string            `json:"id"`
	Email    string            `json:"email"`
	Name     *string           `json:"name"`
	Metadata map[string]string `json:"metadata"`
}

type Invoice struct {
	ID                string `json:"id"`
	Status            string `json:"status"`
	AmountPaid        int64  `json:"amount_paid"`
	Currency          string `json:"currency"`
	Created           int64  `json:"created"`
	HostedInvoiceURL  string `json:"hosted_invoice_url"`
	StatusTransitions struct {
		PaidAt *int64 `json:"paid_at"`
	} `json:"status_transitions"`
}

func FetchSubscriptions(apiKey, productID string) ([]Subscription, error) {
	var all []Subscription
	startingAfter := ""

	for {
		url := "https://api.stripe.com/v1/subscriptions?limit=100&status=all&expand[]=data.latest_invoice"
		if startingAfter != "" {
			url += "&starting_after=" + startingAfter
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == 429 {
			resp.Body.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			return nil, fmt.Errorf("Stripe API %d", resp.StatusCode)
		}

		var result struct {
			Data    []Subscription `json:"data"`
			HasMore bool           `json:"has_more"`
		}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()

		for _, sub := range result.Data {
			for _, item := range sub.Items.Data {
				if item.Price.Product == productID {
					all = append(all, sub)
					break
				}
			}
		}

		if !result.HasMore || len(result.Data) == 0 {
			break
		}
		startingAfter = result.Data[len(result.Data)-1].ID
		time.Sleep(200 * time.Millisecond)
	}

	return all, nil
}

func FetchCustomer(apiKey, customerID string) *Customer {
	url := "https://api.stripe.com/v1/customers/" + customerID
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil
	}

	var cust Customer
	json.NewDecoder(resp.Body).Decode(&cust)
	return &cust
}
