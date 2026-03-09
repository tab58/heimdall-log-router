package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/tbright/heimdall/internal/plugin"
)

const httpTimeout = 10 * time.Second

// WebhookPlugin implements plugin.Plugin by POSTing payloads as JSON to a URL.
type WebhookPlugin struct {
	name   string
	url    string
	client *http.Client
}

// NewWebhookPlugin creates a WebhookPlugin. Requires "url" in config.
func NewWebhookPlugin(name string, config map[string]string) (*WebhookPlugin, error) {
	if config == nil {
		return nil, fmt.Errorf("webhook plugin %q: config is nil", name)
	}
	url := config["url"]
	if url == "" {
		return nil, fmt.Errorf("webhook plugin %q: missing required config key \"url\"", name)
	}
	return &WebhookPlugin{
		name:   name,
		url:    url,
		client: &http.Client{Timeout: httpTimeout},
	}, nil
}

func (w *WebhookPlugin) Name() string {
	return w.name
}

func (w *WebhookPlugin) Start() error {
	return nil
}

func (w *WebhookPlugin) Send(ctx context.Context, payload plugin.PluginPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("webhook %q: marshal error: %w", w.name, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("webhook %q: request error: %w", w.name, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook %q: send error: %w", w.name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook %q: non-2xx response: %d", w.name, resp.StatusCode)
	}
	return nil
}

func (w *WebhookPlugin) Shutdown(_ context.Context) error {
	return nil
}
