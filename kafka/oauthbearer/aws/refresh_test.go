package aws

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// fakeHandle records calls to the OAUTHBEARER setters and lets the test
// inject an error from SetOAuthBearerToken to simulate librdkafka's own
// regex-based token validation rejecting the token.
type fakeHandle struct {
	setCalls  []kafka.OAuthBearerToken
	failCalls []string
	setErr    error
}

func (f *fakeHandle) SetOAuthBearerToken(t kafka.OAuthBearerToken) error {
	f.setCalls = append(f.setCalls, t)
	return f.setErr
}

func (f *fakeHandle) SetOAuthBearerTokenFailure(errstr string) error {
	f.failCalls = append(f.failCalls, errstr)
	return nil
}

func TestRefresh_HappyPath(t *testing.T) {
	jwt := makeJWT(t, map[string]any{"sub": "arn:aws:iam::123456789012:role/R"})
	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return xmlResponse(200, stsSuccessXML(jwt, "2026-04-21T06:06:47Z")), nil
		},
	}
	tp := newTestProvider(t, Config{}, fake)
	h := &fakeHandle{}

	tp.Refresh(context.Background(), h)

	if len(h.setCalls) != 1 {
		t.Fatalf("setCalls: got %d, want 1", len(h.setCalls))
	}
	if len(h.failCalls) != 0 {
		t.Errorf("failCalls should be empty on happy path, got %v", h.failCalls)
	}
	if h.setCalls[0].TokenValue != jwt {
		t.Errorf("TokenValue mismatch")
	}
	if h.setCalls[0].Principal != "arn:aws:iam::123456789012:role/R" {
		t.Errorf("Principal: got %q", h.setCalls[0].Principal)
	}
}

func TestRefresh_STSFailure(t *testing.T) {
	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return xmlResponse(403, stsErrorXML("AccessDenied", "denied")), nil
		},
	}
	tp := newTestProvider(t, Config{}, fake)
	h := &fakeHandle{}

	tp.Refresh(context.Background(), h)

	if len(h.setCalls) != 0 {
		t.Errorf("setCalls should be empty on STS failure, got %v", h.setCalls)
	}
	if len(h.failCalls) != 1 {
		t.Fatalf("failCalls: got %d, want 1", len(h.failCalls))
	}
	if !strings.Contains(h.failCalls[0], "sts GetWebIdentityToken") {
		t.Errorf("failure message should carry operation prefix: %q", h.failCalls[0])
	}
	if !strings.Contains(h.failCalls[0], "AccessDenied") {
		t.Errorf("failure message should surface STS code: %q", h.failCalls[0])
	}
}

func TestRefresh_TokenParseFailure(t *testing.T) {
	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return xmlResponse(200, stsSuccessXML("not-a-jwt", "2026-04-21T06:06:47Z")), nil
		},
	}
	tp := newTestProvider(t, Config{}, fake)
	h := &fakeHandle{}

	tp.Refresh(context.Background(), h)

	if len(h.setCalls) != 0 {
		t.Errorf("setCalls should be empty when JWT can't be parsed, got %v", h.setCalls)
	}
	if len(h.failCalls) != 1 {
		t.Fatalf("failCalls: got %d, want 1", len(h.failCalls))
	}
	if !strings.Contains(h.failCalls[0], "parse JWT sub") {
		t.Errorf("failure message should carry parse prefix: %q", h.failCalls[0])
	}
}

// When SetOAuthBearerToken itself returns an error — e.g. librdkafka's
// OAUTHBEARER regex rejects the token — Refresh must fall through to
// SetOAuthBearerTokenFailure so librdkafka re-emits the refresh event.
func TestRefresh_SetTokenRejected(t *testing.T) {
	jwt := makeJWT(t, map[string]any{"sub": "arn:aws:iam::123456789012:role/R"})
	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return xmlResponse(200, stsSuccessXML(jwt, "2026-04-21T06:06:47Z")), nil
		},
	}
	tp := newTestProvider(t, Config{}, fake)
	h := &fakeHandle{setErr: fmt.Errorf("token value does not match regex")}

	tp.Refresh(context.Background(), h)

	if len(h.setCalls) != 1 {
		t.Errorf("SetOAuthBearerToken should still be attempted, got %d", len(h.setCalls))
	}
	if len(h.failCalls) != 1 {
		t.Fatalf("failCalls: got %d, want 1", len(h.failCalls))
	}
	if !strings.Contains(h.failCalls[0], "regex") {
		t.Errorf("failure should carry underlying rejection: %q", h.failCalls[0])
	}
}
