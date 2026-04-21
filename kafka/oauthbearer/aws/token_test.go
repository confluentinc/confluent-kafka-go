package aws

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// fakeHTTPClient intercepts AWS SDK HTTP requests. responder returns the
// canned response; captured request bodies are available for wire-level
// assertions via lastForm.
type fakeHTTPClient struct {
	responder func(*http.Request) (*http.Response, error)
	mu        sync.Mutex
	bodies    [][]byte
}

func (f *fakeHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
		f.bodies = append(f.bodies, body)
		f.mu.Unlock()
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	return f.responder(req)
}

// lastForm parses the most recent captured request body as form-urlencoded
// parameters. Fails the test if no request was captured.
func (f *fakeHTTPClient) lastForm(t *testing.T) url.Values {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.bodies) == 0 {
		t.Fatal("no request captured")
	}
	v, err := url.ParseQuery(string(f.bodies[len(f.bodies)-1]))
	if err != nil {
		t.Fatalf("parse form: %v", err)
	}
	return v
}

func xmlResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:        http.Header{"Content-Type": []string{"text/xml"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

type stubCreds struct{}

func (stubCreds) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     "AKID-TEST",
		SecretAccessKey: "SECRET-TEST",
	}, nil
}

// newTestProvider wires a TokenProvider to the given fake HTTP client and
// stub credentials so no real network or credential resolution happens.
func newTestProvider(t *testing.T, cfg Config, fake *fakeHTTPClient) *TokenProvider {
	t.Helper()
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Audience == "" {
		cfg.Audience = "test-audience"
	}
	cfg.AWSConfigOptions = append(cfg.AWSConfigOptions,
		config.WithHTTPClient(fake),
		config.WithCredentialsProvider(stubCreds{}),
	)
	tp, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return tp
}

func stsSuccessXML(jwt, expiration string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<GetWebIdentityTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetWebIdentityTokenResult>
    <WebIdentityToken>` + jwt + `</WebIdentityToken>
    <Expiration>` + expiration + `</Expiration>
  </GetWebIdentityTokenResult>
  <ResponseMetadata>
    <RequestId>test-request-id</RequestId>
  </ResponseMetadata>
</GetWebIdentityTokenResponse>`
}

func stsErrorXML(code, message string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Sender</Type>
    <Code>` + code + `</Code>
    <Message>` + message + `</Message>
  </Error>
  <RequestId>error-request-id</RequestId>
</ErrorResponse>`
}

// -------- Tests --------

func TestToken_HappyPath(t *testing.T) {
	const wantPrincipal = "arn:aws:iam::123456789012:role/TestRole"
	jwt := makeJWT(t, map[string]any{
		"sub": wantPrincipal,
		"iss": "https://708975691912.tokens.sts.global.api.aws",
		"aud": "test-audience",
	})
	const expStr = "2026-04-21T06:06:47.641000Z"

	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return xmlResponse(200, stsSuccessXML(jwt, expStr)), nil
		},
	}

	tp := newTestProvider(t,
		Config{Region: "eu-north-1", Audience: "test-audience"},
		fake)

	tok, err := tp.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if tok.TokenValue != jwt {
		t.Errorf("TokenValue: got %q, want minted JWT", tok.TokenValue)
	}
	if tok.Principal != wantPrincipal {
		t.Errorf("Principal: got %q, want %q", tok.Principal, wantPrincipal)
	}
	wantExp, err := time.Parse(time.RFC3339Nano, expStr)
	if err != nil {
		t.Fatalf("parse expected expiration: %v", err)
	}
	if !tok.Expiration.Equal(wantExp) {
		t.Errorf("Expiration: got %s, want %s", tok.Expiration, wantExp)
	}
}

func TestToken_AccessDenied(t *testing.T) {
	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return xmlResponse(403, stsErrorXML(
				"AccessDenied",
				"User ... is not authorized to perform: sts:GetWebIdentityToken",
			)), nil
		},
	}
	tp := newTestProvider(t, Config{}, fake)
	_, err := tp.Token(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "sts GetWebIdentityToken") {
		t.Errorf("error should mention operation: %v", err)
	}
	if !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("error should surface the STS error code: %v", err)
	}
}

func TestToken_FederationNotEnabled(t *testing.T) {
	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return xmlResponse(400, stsErrorXML(
				"ValidationError",
				"Outbound identity federation is not enabled for this account",
			)), nil
		},
	}
	tp := newTestProvider(t, Config{}, fake)
	_, err := tp.Token(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "not enabled") {
		t.Errorf("error should carry upstream message: %v", err)
	}
}

func TestToken_MalformedJWT(t *testing.T) {
	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return xmlResponse(200, stsSuccessXML("not-a-jwt", "2026-04-21T06:06:47Z")), nil
		},
	}
	tp := newTestProvider(t, Config{}, fake)
	_, err := tp.Token(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "parse JWT sub") {
		t.Errorf("error should be wrapped with parse prefix: %v", err)
	}
}

func TestToken_NetworkFailure(t *testing.T) {
	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	tp := newTestProvider(t, Config{}, fake)
	_, err := tp.Token(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "sts GetWebIdentityToken") {
		t.Errorf("error should be wrapped with operation prefix: %v", err)
	}
}

func TestToken_DurationPassthrough(t *testing.T) {
	jwt := makeJWT(t, map[string]any{"sub": "arn:aws:iam::123456789012:role/X"})
	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return xmlResponse(200, stsSuccessXML(jwt, "2026-04-21T06:06:47Z")), nil
		},
	}
	tp := newTestProvider(t,
		Config{Region: "us-east-1", Audience: "test-audience", Duration: 15 * time.Minute},
		fake)

	if _, err := tp.Token(context.Background()); err != nil {
		t.Fatalf("Token: %v", err)
	}

	form := fake.lastForm(t)
	if got := form.Get("DurationSeconds"); got != "900" {
		t.Errorf("DurationSeconds: got %q, want 900", got)
	}
	if got := form.Get("Action"); got != "GetWebIdentityToken" {
		t.Errorf("Action: got %q", got)
	}
	if got := form.Get("Audience.member.1"); got != "test-audience" {
		t.Errorf("Audience.member.1: got %q", got)
	}
	if got := form.Get("SigningAlgorithm"); got != "ES384" {
		t.Errorf("SigningAlgorithm: got %q", got)
	}
}

func TestToken_FractionalExpiration(t *testing.T) {
	jwt := makeJWT(t, map[string]any{"sub": "arn:aws:iam::123456789012:role/X"})
	// Exact shape from live STS probe data (2026-04-21): microsecond
	// precision with trailing Z.
	const fractional = "2026-04-21T06:06:47.641000Z"

	fake := &fakeHTTPClient{
		responder: func(*http.Request) (*http.Response, error) {
			return xmlResponse(200, stsSuccessXML(jwt, fractional)), nil
		},
	}
	tp := newTestProvider(t, Config{}, fake)

	tok, err := tp.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	wantExp, err := time.Parse(time.RFC3339Nano, fractional)
	if err != nil {
		t.Fatalf("parse expected expiration: %v", err)
	}
	if !tok.Expiration.Equal(wantExp) {
		t.Errorf("Expiration: got %s, want %s", tok.Expiration, wantExp)
	}
}
