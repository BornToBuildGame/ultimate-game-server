package satori

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientAuthenticateFlagsEventsMessages(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/authenticate", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"properties": map[string]any{"default": map[string]string{"tier": "gold"}},
		})
	})
	mux.HandleFunc("/v1/flag", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(FlagList{Flags: []*Flag{{Name: "feat", Value: "on"}}})
	})
	mux.HandleFunc("/v1/event", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v1/message", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(MessageList{Messages: []*Message{{Id: "m1", Title: "hi"}}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClient(Config{URL: srv.URL, APIKeyName: "key", APIKey: "secret", SigningKey: "sign"})
	ctx := context.Background()

	props, err := c.Authenticate(ctx, "id-1", map[string]string{"a": "1"}, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if props == nil || props.Default["tier"] != "gold" {
		t.Fatalf("props=%v", props)
	}

	flags, err := c.FlagsList(ctx, "id-1", []string{"feat"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if flags == nil || len(flags.Flags) != 1 || flags.Flags[0].Name != "feat" {
		t.Fatalf("flags=%v", flags)
	}

	if err := c.EventsPublish(ctx, "id-1", []*Event{{Name: "login", Value: "1"}}); err != nil {
		t.Fatal(err)
	}

	msgs, err := c.MessagesList(ctx, "id-1", 10, true, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if msgs == nil || len(msgs.Messages) != 1 {
		t.Fatalf("msgs=%v", msgs)
	}
}
