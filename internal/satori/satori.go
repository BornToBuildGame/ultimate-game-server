package satori

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Satori is the reference-shaped LiveOps client surface.
type Satori interface {
	Authenticate(ctx context.Context, id string, defaultProperties, customProperties map[string]string, noSession bool, ipAddress ...string) (*Properties, error)
	IdentityDelete(ctx context.Context, id string) error
	PropertiesGet(ctx context.Context, id string) (*Properties, error)
	PropertiesUpdate(ctx context.Context, id string, properties *PropertiesUpdate) error
	EventsPublish(ctx context.Context, id string, events []*Event, ipAddress ...string) error
	ServerEventsPublish(ctx context.Context, events []*Event, ipAddress ...string) error
	ExperimentsList(ctx context.Context, id string, names, labels []string) (*ExperimentList, error)
	FlagsList(ctx context.Context, id string, names, labels []string) (*FlagList, error)
	FlagsOverridesList(ctx context.Context, id string, names, labels []string) (*FlagOverridesList, error)
	LiveEventsList(ctx context.Context, id string, names, labels []string, pastRunCount, futureRunCount int32, startTimeSec, endTimeSec int64) (*LiveEventList, error)
	LiveEventJoin(ctx context.Context, id, liveEventId string) error
	MessagesList(ctx context.Context, id string, limit int, forward bool, cursor string, messageIDs []string) (*MessageList, error)
	MessageUpdate(ctx context.Context, id, messageId string, readTime, consumeTime int64) error
	MessageDelete(ctx context.Context, id, messageId string) error
	ConsoleDirectMessageSend(ctx context.Context, templateId string, recipientIDs []string, integrations []MessageIntegration, persist bool, channels map[MessageIntegration]*MessageIntegrationChannels, templateOverride *MessageTemplateOverride) (*MessageSendResults, error)
}

// Client is the HTTP Satori API client.
type Client struct {
	baseURL    string
	apiKeyName string
	apiKey     string
	signingKey string
	httpClient *http.Client
}

// Config holds Satori connection settings.
type Config struct {
	URL        string
	APIKeyName string
	APIKey     string
	SigningKey string
	TimeoutMs  int64
}

// NewClient creates a Satori client from config.
func NewClient(cfg Config) *Client {
	timeout := time.Duration(cfg.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	signing := cfg.SigningKey
	if signing == "" {
		signing = cfg.APIKey
	}
	return &Client{
		baseURL:    strings.TrimRight(cfg.URL, "/"),
		apiKeyName: cfg.APIKeyName,
		apiKey:     cfg.APIKey,
		signingKey: signing,
		httpClient: &http.Client{Timeout: timeout},
	}
}

var _ Satori = (*Client)(nil)

type sessionTokenClaims struct {
	SessionID  string `json:"sid,omitempty"`
	IdentityId string `json:"iid,omitempty"`
	ExpiresAt  int64  `json:"exp,omitempty"`
	IssuedAt   int64  `json:"iat,omitempty"`
	ApiKeyName string `json:"api,omitempty"`
	jwt.RegisteredClaims
}

func (c *Client) generateToken(id string) (string, error) {
	if c == nil || c.signingKey == "" {
		return "", fmt.Errorf("satori not configured")
	}
	now := time.Now().UTC()
	claims := sessionTokenClaims{
		SessionID:  uuid.NewString(),
		IdentityId: id,
		ExpiresAt:  now.Add(time.Hour).Unix(),
		IssuedAt:   now.Unix(),
		ApiKeyName: c.apiKeyName,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return t.SignedString([]byte(c.signingKey))
}

func (c *Client) do(ctx context.Context, method, path string, authToken string, query url.Values, body any, out any, ipAddress ...string) error {
	if c == nil || c.baseURL == "" {
		return fmt.Errorf("satori not configured")
	}
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	} else if c.apiKeyName != "" && c.apiKey != "" {
		req.SetBasicAuth(c.apiKeyName, c.apiKey)
	}
	if len(ipAddress) > 0 && ipAddress[0] != "" {
		req.Header.Set("X-Forwarded-For", ipAddress[0])
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
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) sessionDo(ctx context.Context, id, method, path string, query url.Values, body, out any, ipAddress ...string) error {
	tok, err := c.generateToken(id)
	if err != nil {
		return err
	}
	return c.do(ctx, method, path, tok, query, body, out, ipAddress...)
}

// Authenticate creates or updates a Satori identity.
func (c *Client) Authenticate(ctx context.Context, id string, defaultProperties, customProperties map[string]string, noSession bool, ipAddress ...string) (*Properties, error) {
	var props struct {
		Properties Properties `json:"properties"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/authenticate", "", nil, map[string]any{
		"id":         id,
		"default":    defaultProperties,
		"custom":     customProperties,
		"no_session": noSession,
	}, &props, ipAddress...)
	if err != nil {
		return nil, err
	}
	return &props.Properties, nil
}

// IdentityDelete deletes an identity and associated data.
func (c *Client) IdentityDelete(ctx context.Context, id string) error {
	return c.sessionDo(ctx, id, http.MethodDelete, "/v1/identity", nil, nil, nil)
}

// PropertiesGet returns identity properties.
func (c *Client) PropertiesGet(ctx context.Context, id string) (*Properties, error) {
	var props Properties
	if err := c.sessionDo(ctx, id, http.MethodGet, "/v1/properties", nil, nil, &props); err != nil {
		return nil, err
	}
	return &props, nil
}

// PropertiesUpdate updates identity properties.
func (c *Client) PropertiesUpdate(ctx context.Context, id string, properties *PropertiesUpdate) error {
	return c.sessionDo(ctx, id, http.MethodPut, "/v1/properties", nil, properties, nil)
}

// EventsPublish publishes analytics events for an identity.
func (c *Client) EventsPublish(ctx context.Context, id string, events []*Event, ipAddress ...string) error {
	for _, e := range events {
		if e != nil && e.IdentityId == "" {
			e.IdentityId = id
		}
	}
	return c.sessionDo(ctx, id, http.MethodPost, "/v1/event", nil, map[string]any{"events": events}, nil, ipAddress...)
}

// ServerEventsPublish publishes server-side events (no identity session).
func (c *Client) ServerEventsPublish(ctx context.Context, events []*Event, ipAddress ...string) error {
	return c.do(ctx, http.MethodPost, "/v1/server-event", "", nil, map[string]any{"events": events}, nil, ipAddress...)
}

// ExperimentsList lists experiments for an identity.
func (c *Client) ExperimentsList(ctx context.Context, id string, names, labels []string) (*ExperimentList, error) {
	q := url.Values{}
	for _, n := range names {
		q.Add("names", n)
	}
	for _, l := range labels {
		q.Add("labels", l)
	}
	var out ExperimentList
	if err := c.sessionDo(ctx, id, http.MethodGet, "/v1/experiment", q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FlagsList lists feature flags for an identity.
func (c *Client) FlagsList(ctx context.Context, id string, names, labels []string) (*FlagList, error) {
	q := url.Values{}
	for _, n := range names {
		q.Add("names", n)
	}
	for _, l := range labels {
		q.Add("labels", l)
	}
	var out FlagList
	if err := c.sessionDo(ctx, id, http.MethodGet, "/v1/flag", q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FlagsOverridesList lists flag overrides for an identity.
func (c *Client) FlagsOverridesList(ctx context.Context, id string, names, labels []string) (*FlagOverridesList, error) {
	q := url.Values{}
	for _, n := range names {
		q.Add("names", n)
	}
	for _, l := range labels {
		q.Add("labels", l)
	}
	var out FlagOverridesList
	if err := c.sessionDo(ctx, id, http.MethodGet, "/v1/flag/override", q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// LiveEventsList lists live events for an identity.
func (c *Client) LiveEventsList(ctx context.Context, id string, names, labels []string, pastRunCount, futureRunCount int32, startTimeSec, endTimeSec int64) (*LiveEventList, error) {
	q := url.Values{}
	for _, n := range names {
		q.Add("names", n)
	}
	for _, l := range labels {
		q.Add("labels", l)
	}
	if pastRunCount != 0 {
		q.Set("past_run_count", strconv.FormatInt(int64(pastRunCount), 10))
	}
	if futureRunCount != 0 {
		q.Set("future_run_count", strconv.FormatInt(int64(futureRunCount), 10))
	}
	if startTimeSec != 0 {
		q.Set("start_time_sec", strconv.FormatInt(startTimeSec, 10))
	}
	if endTimeSec != 0 {
		q.Set("end_time_sec", strconv.FormatInt(endTimeSec, 10))
	}
	var out LiveEventList
	if err := c.sessionDo(ctx, id, http.MethodGet, "/v1/live-event", q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// LiveEventJoin joins a live event.
func (c *Client) LiveEventJoin(ctx context.Context, id, liveEventId string) error {
	path := "/v1/live-event/" + url.PathEscape(liveEventId) + "/participation"
	return c.sessionDo(ctx, id, http.MethodPost, path, nil, nil, nil)
}

// MessagesList lists messages for an identity.
func (c *Client) MessagesList(ctx context.Context, id string, limit int, forward bool, cursor string, messageIDs []string) (*MessageList, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	q.Set("forward", strconv.FormatBool(forward))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	for _, mid := range messageIDs {
		q.Add("message_ids", mid)
	}
	var out MessageList
	if err := c.sessionDo(ctx, id, http.MethodGet, "/v1/message", q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// MessageUpdate updates read/consume times on a message.
func (c *Client) MessageUpdate(ctx context.Context, id, messageId string, readTime, consumeTime int64) error {
	path := "/v1/message/" + url.PathEscape(messageId)
	return c.sessionDo(ctx, id, http.MethodPut, path, nil, map[string]any{
		"read_time":    readTime,
		"consume_time": consumeTime,
	}, nil)
}

// MessageDelete deletes a message.
func (c *Client) MessageDelete(ctx context.Context, id, messageId string) error {
	path := "/v1/message/" + url.PathEscape(messageId)
	return c.sessionDo(ctx, id, http.MethodDelete, path, nil, nil, nil)
}

// ConsoleDirectMessageSend sends a console direct message (reference-shaped).
func (c *Client) ConsoleDirectMessageSend(ctx context.Context, templateId string, recipientIDs []string, integrations []MessageIntegration, persist bool, channels map[MessageIntegration]*MessageIntegrationChannels, templateOverride *MessageTemplateOverride) (*MessageSendResults, error) {
	body := map[string]any{
		"template_id":   templateId,
		"recipient_ids": recipientIDs,
		"persist":       persist,
	}
	if len(integrations) > 0 {
		body["integrations"] = integrations
	}
	if channels != nil {
		body["channels"] = channels
	}
	if templateOverride != nil {
		body["template_override"] = templateOverride
	}
	var out MessageSendResults
	if err := c.do(ctx, http.MethodPost, "/v1/console/message-direct", "", nil, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ConsoleDirectMessageSendSimple is a convenience wrapper used by console HTTP/gRPC.
func (c *Client) ConsoleDirectMessageSendSimple(ctx context.Context, identityID, title, body string) error {
	return c.do(ctx, http.MethodPost, "/v1/console/message", "", nil, map[string]any{
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
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	var out struct {
		Templates []MessageTemplate `json:"templates"`
		Cursor    string            `json:"cursor"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/console/message-template", "", q, nil, &out); err != nil {
		// Fallback legacy path alias.
		if err2 := c.do(ctx, http.MethodGet, "/v1/console/template", "", q, nil, &out); err2 != nil {
			return nil, "", err
		}
	}
	return out.Templates, out.Cursor, nil
}

// FlagsListLegacy supports the prior multi-identity map shape used by early Lua bindings.
func (c *Client) FlagsListLegacy(ctx context.Context, identities []map[string]string) (map[string]interface{}, error) {
	var out map[string]interface{}
	err := c.do(ctx, http.MethodPost, "/v1/flags", "", nil, map[string]any{"identities": identities}, &out)
	return out, err
}

// EventsPublishLegacy supports the prior untyped events map shape.
func (c *Client) EventsPublishLegacy(ctx context.Context, events []map[string]any) error {
	return c.do(ctx, http.MethodPost, "/v1/event", "", nil, map[string]any{"events": events}, nil)
}
