package cmd

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type ProcessorEnvVar struct {
	Name        string
	Description string
	Required    bool
}

type DataProcessor interface {
	Name() string
	EnvVars() []ProcessorEnvVar
	WarmUp(*ProcessorContext) error
	ProcessTransaction(*ProcessorContext, *TransactionEntry) error
	ProcessEvent(*ProcessorContext, *FullEvent) error
	Flush(*ProcessorContext) error
}

type ProcessorContext struct {
	DataDir    string
	Year       string
	Month      string
	HTTPClient *http.Client
}

func newProcessorContext(dataDir, year, month string) *ProcessorContext {
	return &ProcessorContext{
		DataDir:    dataDir,
		Year:       year,
		Month:      month,
		HTTPClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func registeredDataProcessors() []DataProcessor {
	return []DataProcessor{
		newLumaStripeProcessor(),
		newMoneriumProcessor(),
	}
}

func registeredDataProcessorNames() []string {
	processors := registeredDataProcessors()
	names := make([]string, 0, len(processors))
	for _, processor := range processors {
		names = append(names, processor.Name())
	}
	return names
}

func (ctx *ProcessorContext) ReadPublicJSON(processor, name string, v interface{}) error {
	return ctx.readJSON(filepath.Join("processors", processor, name), v)
}

func (ctx *ProcessorContext) WritePublicJSON(processor, name string, v interface{}) error {
	return ctx.writeJSON(filepath.Join("processors", processor, name), v)
}

func (ctx *ProcessorContext) ReadPrivateJSON(processor, name string, v interface{}) error {
	return ctx.readJSON(filepath.Join("processors", processor, "private", name), v)
}

func (ctx *ProcessorContext) WritePrivateJSON(processor, name string, v interface{}) error {
	return ctx.writeJSON(filepath.Join("processors", processor, "private", name), v)
}

func (ctx *ProcessorContext) readJSON(relPath string, v interface{}) error {
	data, err := os.ReadFile(filepath.Join(ctx.DataDir, ctx.Year, ctx.Month, relPath))
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func (ctx *ProcessorContext) writeJSON(relPath string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeMonthFile(ctx.DataDir, ctx.Year, ctx.Month, relPath, data)
}

func runTransactionProcessors(dataDir, year, month string, txs []TransactionEntry) {
	processors := registeredDataProcessors()
	if len(processors) == 0 {
		return
	}
	ctx := newProcessorContext(dataDir, year, month)
	for _, processor := range processors {
		if err := processor.WarmUp(ctx); err != nil {
			LogWarningf("Warning: processor %s warm-up failed: %v", processor.Name(), err)
			continue
		}
		for i := range txs {
			if err := processor.ProcessTransaction(ctx, &txs[i]); err != nil {
				LogWarningf("Warning: processor %s transaction %s failed: %v", processor.Name(), txs[i].ID, err)
			}
		}
		if err := processor.Flush(ctx); err != nil {
			LogWarningf("Warning: processor %s flush failed: %v", processor.Name(), err)
		}
	}
}

func runEventProcessors(dataDir, year, month string, events []FullEvent) {
	processors := registeredDataProcessors()
	if len(processors) == 0 {
		return
	}
	ctx := newProcessorContext(dataDir, year, month)
	for _, processor := range processors {
		if err := processor.WarmUp(ctx); err != nil {
			LogWarningf("Warning: processor %s warm-up failed: %v", processor.Name(), err)
			continue
		}
		for i := range events {
			if err := processor.ProcessEvent(ctx, &events[i]); err != nil {
				LogWarningf("Warning: processor %s event %s failed: %v", processor.Name(), events[i].ID, err)
			}
		}
		if err := processor.Flush(ctx); err != nil {
			LogWarningf("Warning: processor %s flush failed: %v", processor.Name(), err)
		}
	}
}
