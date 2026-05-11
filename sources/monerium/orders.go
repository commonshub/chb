package monerium

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func Authenticate(clientID, clientSecret, environment string) (string, error) {
	baseURL := "https://api.monerium.app"
	if environment == "sandbox" {
		baseURL = "https://api.monerium.dev"
	}

	data := fmt.Sprintf("grant_type=client_credentials&client_id=%s&client_secret=%s", clientID, clientSecret)
	req, err := http.NewRequest("POST", baseURL+"/auth/token", strings.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("auth request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		var errResp struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&errResp)
		return "", fmt.Errorf("auth failed (%d): %s", resp.StatusCode, errResp.Error)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("failed to decode token: %w", err)
	}

	return tokenResp.AccessToken, nil
}

func FetchOrders(accessToken, address, environment string) ([]Order, error) {
	baseURL := "https://api.monerium.app"
	if environment == "sandbox" {
		baseURL = "https://api.monerium.dev"
	}

	url := fmt.Sprintf("%s/orders?address=%s", baseURL, address)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/vnd.monerium.api-v2+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("API error: %d", resp.StatusCode)
	}

	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	var orders []Order
	if err := json.Unmarshal(raw, &orders); err != nil {
		var wrapped struct {
			Orders []Order `json:"orders"`
		}
		if err := json.Unmarshal(raw, &wrapped); err != nil {
			return nil, fmt.Errorf("failed to parse orders: %w", err)
		}
		orders = wrapped.Orders
	}

	return orders, nil
}

func GroupByMonth(orders []Order, tz *time.Location) map[string][]Order {
	if tz == nil {
		tz = time.UTC
	}
	byMonth := make(map[string][]Order)

	for _, order := range orders {
		dateStr := order.Meta.PlacedAt
		if dateStr == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, dateStr)
		if err != nil {
			t, err = time.Parse(time.RFC3339Nano, dateStr)
			if err != nil {
				continue
			}
		}
		t = t.In(tz)
		ym := fmt.Sprintf("%d-%02d", t.Year(), t.Month())
		byMonth[ym] = append(byMonth[ym], order)
	}

	return byMonth
}
