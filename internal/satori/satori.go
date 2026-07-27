package satori

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is a minimal Satori HTTP API client.
type Client struct {
	baseURL    string
	apiKeyName string
	apiKey     string
	httpClient *http.Client
}

// Config holds Satori connection settings.
type Config struct {
	URL        string
	APIKeyName string
	APIKey     string
	TimeoutMs  int64
}

// NewClient creates a Satori client from config.
func NewClient(cfg Config) *Client {
	timeout := time.Duration(cfg.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		baseURL:    cfg.URL,
		apiKeyName: cfg.APIKeyName,
		apiKey:     cfg.APIKey,
		httpClient: &http.Client{Timeout: timeout},
	}
}

func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	if c == nil || c.baseURL == "" {
		return fmt.Errorf("satori not configured")
	}
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKeyName != "" && c.apiKey != "" {
		req.SetBasicAuth(c.apiKeyName, c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("satori %s: %s", resp.Status, string(msg))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// FlagsList returns feature flags for identities.
func (c *Client) FlagsList(ctx context.Context, identities []map[string]string) (map[string]interface{}, error) {
	var out map[string]interface{}
	err := c.post(ctx, "/v1/flags", map[string]any{"identities": identities}, &out)
	return out, err
}

// EventsPublish publishes analytics events.
func (c *Client) EventsPublish(ctx context.Context, events []map[string]any) error {
	return c.post(ctx, "/v1/event", map[string]any{"events": events}, nil)
}

// ConsoleDirectMessageSend sends a direct message from console.
func (c *Client) ConsoleDirectMessageSend(ctx context.Context, identityID, title, body string) error {
	return c.post(ctx, "/v1/console/message", map[string]any{
		"identity_id": identityID,
		"title":       title,
		"body":        body,
	}, nil)
}

// MessageTemplate is a Satori console message template.
type MessageTemplate struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Title string `json:"title"`
	Value string `json:"value"`
}

// ConsoleMessageTemplatesList lists message templates from Satori.
func (c *Client) ConsoleMessageTemplatesList(ctx context.Context, limit int, cursor string) ([]MessageTemplate, string, error) {
	if c == nil || c.baseURL == "" {
		return nil, "", fmt.Errorf("satori not configured")
	}
	if limit <= 0 {
		limit = 100
	}
	path := fmt.Sprintf("/v1/console/message-template?limit=%d", limit)
	if cursor != "" {
		path += "&cursor=" + cursor
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, "", err
	}
	if c.apiKeyName != "" && c.apiKey != "" {
		req.SetBasicAuth(c.apiKeyName, c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("satori %s: %s", resp.Status, string(msg))
	}
	var out struct {
		Templates []MessageTemplate `json:"templates"`
		Cursor    string            `json:"cursor"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, "", err
	}
	return out.Templates, out.Cursor, nil
}
