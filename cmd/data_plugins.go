package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type PluginEnvVar struct {
	Name        string
	Description string
	Required    bool
}

type DataPlugin interface {
	Name() string
	EnvVars() []PluginEnvVar
	WarmUp(*PluginContext) error
	AugmentTransaction(*PluginContext, *TransactionEntry) error
	AugmentEvent(*PluginContext, *FullEvent) error
	Flush(*PluginContext) error
}

type PluginContext struct {
	DataDir    string
	Year       string
	Month      string
	HTTPClient *http.Client
}

func newPluginContext(dataDir, year, month string) *PluginContext {
	return &PluginContext{
		DataDir:    dataDir,
		Year:       year,
		Month:      month,
		HTTPClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func registeredDataPlugins() []DataPlugin {
	return []DataPlugin{
		newLumaStripePlugin(),
	}
}

func (ctx *PluginContext) ReadPublicJSON(plugin, name string, v interface{}) error {
	return ctx.readJSON(filepath.Join("plugins", plugin, name), v)
}

func (ctx *PluginContext) WritePublicJSON(plugin, name string, v interface{}) error {
	return ctx.writeJSON(filepath.Join("plugins", plugin, name), v)
}

func (ctx *PluginContext) ReadPrivateJSON(plugin, name string, v interface{}) error {
	return ctx.readJSON(filepath.Join("plugins", plugin, "private", name), v)
}

func (ctx *PluginContext) WritePrivateJSON(plugin, name string, v interface{}) error {
	return ctx.writeJSON(filepath.Join("plugins", plugin, "private", name), v)
}

func (ctx *PluginContext) readJSON(relPath string, v interface{}) error {
	data, err := os.ReadFile(filepath.Join(ctx.DataDir, ctx.Year, ctx.Month, relPath))
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func (ctx *PluginContext) writeJSON(relPath string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeMonthFile(ctx.DataDir, ctx.Year, ctx.Month, relPath, data)
}

func runTransactionPlugins(dataDir, year, month string, txs []TransactionEntry) {
	plugins := registeredDataPlugins()
	if len(plugins) == 0 {
		return
	}
	label := year
	if month != "" {
		label = year + "-" + month
	}
	ctx := newPluginContext(dataDir, year, month)
	for _, plugin := range plugins {
		fmt.Printf("    %s%s: applying plugin %s to %d transaction(s)%s\n", Fmt.Dim, label, plugin.Name(), len(txs), Fmt.Reset)
		if err := plugin.WarmUp(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: plugin %s warm-up failed: %v\n", plugin.Name(), err)
			continue
		}
		for i := range txs {
			if err := plugin.AugmentTransaction(ctx, &txs[i]); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: plugin %s transaction %s failed: %v\n", plugin.Name(), txs[i].ID, err)
			}
		}
		if err := plugin.Flush(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: plugin %s flush failed: %v\n", plugin.Name(), err)
		}
	}
}

func runEventPlugins(dataDir, year, month string, events []FullEvent) {
	plugins := registeredDataPlugins()
	if len(plugins) == 0 {
		return
	}
	ctx := newPluginContext(dataDir, year, month)
	for _, plugin := range plugins {
		if err := plugin.WarmUp(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: plugin %s warm-up failed: %v\n", plugin.Name(), err)
			continue
		}
		for i := range events {
			if err := plugin.AugmentEvent(ctx, &events[i]); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: plugin %s event %s failed: %v\n", plugin.Name(), events[i].ID, err)
			}
		}
		if err := plugin.Flush(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: plugin %s flush failed: %v\n", plugin.Name(), err)
		}
	}
}
