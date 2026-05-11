package stripe

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/CommonsHub/chb/sources"
)

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []sources.File {
	return []sources.File{
		{Name: BalanceTransactionsFile, Description: "Monthly Stripe balance transactions.", Private: true},
		{Name: ChargesFile, Description: "Monthly Stripe charge, refund, checkout session, and application data.", Private: true},
		{Name: CustomersFile, Description: "Monthly Stripe customer PII keyed by balance transaction.", Private: true},
		{Name: SubscriptionsFile, Description: "Monthly Stripe membership subscription snapshot.", Private: true},
		{Name: PayoutsFile, Description: "Latest Stripe payout summary derived from archived balance transactions.", Private: true},
	}
}

func WriteJSON(dataDir, year, month string, v interface{}, elems ...string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	path := Path(dataDir, year, month, elems...)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	_ = os.Chmod(filepath.Dir(path), 0700)
	_ = os.Chmod(path, 0600)
	return nil
}

func SaveChargeData(dataDir, year, month string, charges map[string]*Charge, refundToCharge map[string]string) error {
	return WriteJSON(dataDir, year, month, ChargeData{
		FetchedAt:      time.Now().UTC().Format(time.RFC3339),
		Charges:        charges,
		RefundToCharge: refundToCharge,
	}, ChargesFile)
}
