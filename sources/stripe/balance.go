package stripe

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func FetchBalance(apiKey string) (float64, error) {
	if apiKey == "" {
		return 0, fmt.Errorf("STRIPE_SECRET_KEY not set")
	}

	req, err := http.NewRequest("GET", "https://api.stripe.com/v1/balance", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("stripe API returned %d", resp.StatusCode)
	}

	var result struct {
		Available []struct {
			Amount   int64  `json:"amount"`
			Currency string `json:"currency"`
		} `json:"available"`
		Pending []struct {
			Amount   int64  `json:"amount"`
			Currency string `json:"currency"`
		} `json:"pending"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	var total int64
	for _, a := range result.Available {
		total += a.Amount
	}
	for _, p := range result.Pending {
		total += p.Amount
	}
	return float64(total) / 100, nil
}
