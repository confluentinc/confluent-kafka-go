package aws

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// -------- Config validation --------

func TestConfig_Validate_MissingRegion(t *testing.T) {
	c := Config{Audience: "x"}
	err := c.validate()
	if err == nil || !strings.Contains(err.Error(), "Region") {
		t.Fatalf("want Region error, got %v", err)
	}
}

func TestConfig_Validate_MissingAudience(t *testing.T) {
	c := Config{Region: "us-east-1"}
	err := c.validate()
	if err == nil || !strings.Contains(err.Error(), "Audience") {
		t.Fatalf("want Audience error, got %v", err)
	}
}

func TestConfig_Validate_BadSigningAlgorithm(t *testing.T) {
	c := Config{Region: "us-east-1", Audience: "x", SigningAlgorithm: "HS256"}
	err := c.validate()
	if err == nil || !strings.Contains(err.Error(), "SigningAlgorithm") {
		t.Fatalf("want SigningAlgorithm error, got %v", err)
	}
}

func TestConfig_Validate_SigningAlgorithmAccepted(t *testing.T) {
	for _, alg := range []string{"", "ES384", "RS256"} {
		c := Config{Region: "us-east-1", Audience: "x", SigningAlgorithm: alg}
		if err := c.validate(); err != nil {
			t.Errorf("alg %q should be accepted, got %v", alg, err)
		}
	}
}

func TestConfig_Validate_DurationTooSmall(t *testing.T) {
	c := Config{Region: "us-east-1", Audience: "x", Duration: 30 * time.Second}
	err := c.validate()
	if err == nil || !strings.Contains(err.Error(), "Duration") {
		t.Fatalf("want Duration error, got %v", err)
	}
}

func TestConfig_Validate_DurationTooLarge(t *testing.T) {
	c := Config{Region: "us-east-1", Audience: "x", Duration: 2 * time.Hour}
	err := c.validate()
	if err == nil || !strings.Contains(err.Error(), "Duration") {
		t.Fatalf("want Duration error, got %v", err)
	}
}

func TestConfig_Validate_DurationBoundariesAccepted(t *testing.T) {
	for _, d := range []time.Duration{minDuration, 5 * time.Minute, maxDuration, 0} {
		c := Config{Region: "us-east-1", Audience: "x", Duration: d}
		if err := c.validate(); err != nil {
			t.Errorf("duration %s should be accepted, got %v", d, err)
		}
	}
}

// -------- applyDefaults --------

func TestConfig_ApplyDefaults_ZeroValues(t *testing.T) {
	c := Config{Region: "us-east-1", Audience: "x"}
	c.applyDefaults()
	if c.SigningAlgorithm != "ES384" {
		t.Errorf("SigningAlgorithm: got %q, want ES384", c.SigningAlgorithm)
	}
	if c.Duration != 5*time.Minute {
		t.Errorf("Duration: got %s, want 5m", c.Duration)
	}
}

func TestConfig_ApplyDefaults_NonZeroPassthrough(t *testing.T) {
	c := Config{
		Region:           "us-east-1",
		Audience:         "x",
		SigningAlgorithm: "RS256",
		Duration:         20 * time.Minute,
	}
	c.applyDefaults()
	if c.SigningAlgorithm != "RS256" {
		t.Errorf("user SigningAlgorithm overwritten")
	}
	if c.Duration != 20*time.Minute {
		t.Errorf("user Duration overwritten")
	}
}

// -------- New() --------

func TestNew_Happy(t *testing.T) {
	tp, err := New(context.Background(), Config{
		Region:   "us-east-1",
		Audience: "https://example.com/oidc",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if tp == nil {
		t.Fatal("New returned nil provider")
	}
	if tp.cfg.SigningAlgorithm != "ES384" {
		t.Errorf("defaults not applied: %+v", tp.cfg)
	}
	if tp.sts == nil {
		t.Error("sts client not constructed")
	}
}

func TestNew_ValidationErrorSurfaced(t *testing.T) {
	_, err := New(context.Background(), Config{Audience: "x"})
	if err == nil || !strings.Contains(err.Error(), "Region") {
		t.Fatalf("want validation error, got %v", err)
	}
}

// recordingHTTPClient implements config/aws.HTTPClient for injection tests.
type recordingHTTPClient struct {
	called int64
}

func (r *recordingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	atomic.AddInt64(&r.called, 1)
	// Returning an error here is fine — we only verify the client is wired,
	// the actual HTTP round-trip is tested in M4.
	return nil, http.ErrUseLastResponse
}

func TestNew_AWSConfigOptionsHonored(t *testing.T) {
	client := &recordingHTTPClient{}
	_, err := New(context.Background(), Config{
		Region:   "us-east-1",
		Audience: "x",
		AWSConfigOptions: []func(*config.LoadOptions) error{
			config.WithHTTPClient(client),
		},
	})
	if err != nil {
		t.Fatalf("New should accept AWSConfigOptions: %v", err)
	}
	// Plumbing is verified end-to-end in M4 when an actual STS call is made.
	// Here we only assert the option was accepted without error.
}

func TestNew_STSEndpointOverride(t *testing.T) {
	const override = "https://sts.example.internal"
	tp, err := New(context.Background(), Config{
		Region:      "us-east-1",
		Audience:    "x",
		STSEndpoint: override,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	opts := tp.sts.Options()
	if opts.BaseEndpoint == nil {
		t.Fatal("BaseEndpoint not set on sts.Client")
	}
	if *opts.BaseEndpoint != override {
		t.Errorf("BaseEndpoint: got %q, want %q", *opts.BaseEndpoint, override)
	}
}

func TestNew_NoEndpointSetByDefault(t *testing.T) {
	tp, err := New(context.Background(), Config{
		Region:   "us-east-1",
		Audience: "x",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if tp.sts.Options().BaseEndpoint != nil {
		t.Errorf("BaseEndpoint should be nil when STSEndpoint is unset")
	}
}

// -------- Lazy credential resolution (critical gate for M3) --------

// trackingCreds records whether Retrieve was called. Used to prove New()
// does not eagerly resolve credentials.
type trackingCreds struct {
	called int64
}

func (t *trackingCreds) Retrieve(ctx context.Context) (aws.Credentials, error) {
	atomic.AddInt64(&t.called, 1)
	return aws.Credentials{
		AccessKeyID:     "AKID-TEST",
		SecretAccessKey: "SECRET-TEST",
	}, nil
}

func TestNew_NoEagerCredentialResolution(t *testing.T) {
	tc := &trackingCreds{}
	_, err := New(context.Background(), Config{
		Region:   "us-east-1",
		Audience: "x",
		AWSConfigOptions: []func(*config.LoadOptions) error{
			config.WithCredentialsProvider(tc),
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if n := atomic.LoadInt64(&tc.called); n != 0 {
		t.Fatalf("credentials were resolved during New() (%d calls); "+
			"lazy-resolution invariant violated", n)
	}
}
