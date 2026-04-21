//go:build integration

// +build integration

package aws

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// rolePrincipalRE matches both bare-role ARNs (the expected shape per probe
// data from 2026-04-21) and assumed-role ARNs, leaving room for future AWS
// shape changes without a test-code bump.
var rolePrincipalRE = regexp.MustCompile(`^arn:aws:(iam|sts)::\d+:(role|assumed-role)/.+$`)

// TestGetWebIdentityToken_Real mints a real JWT from STS and asserts the
// response shape. Double-gated: requires the `integration` build tag AND
// the RUN_AWS_STS_REAL=1 env var, so neither `go test ./...` nor a stray
// `-tags integration` will accidentally hit AWS.
//
// See TESTING.md in this directory for environment setup.
func TestGetWebIdentityToken_Real(t *testing.T) {
	if os.Getenv("RUN_AWS_STS_REAL") != "1" {
		t.Skip("set RUN_AWS_STS_REAL=1 to run (requires AWS environment with sts:GetWebIdentityToken permission)")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		t.Fatal("AWS_REGION must be set")
	}
	audience := os.Getenv("STS_AUDIENCE")
	if audience == "" {
		t.Fatal("STS_AUDIENCE must be set")
	}

	cfg := Config{
		Region:   region,
		Audience: audience,
	}
	if s := os.Getenv("STS_DURATION_SECONDS"); s != "" {
		sec, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("STS_DURATION_SECONDS parse: %v", err)
		}
		cfg.Duration = time.Duration(sec) * time.Second
	}

	ctx := context.Background()
	provider, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tok, err := provider.Token(ctx)
	if err != nil {
		t.Fatalf("Token: %v", err)
	}

	// ---- TokenValue is a 3-segment base64url-encoded JWT ----

	if tok.TokenValue == "" {
		t.Fatal("TokenValue empty")
	}
	parts := strings.Split(tok.TokenValue, ".")
	if len(parts) != 3 {
		t.Fatalf("TokenValue has %d segments, expected 3", len(parts))
	}
	for i, part := range parts {
		if _, err := base64.RawURLEncoding.DecodeString(part); err != nil {
			t.Errorf("segment %d not base64url: %v", i, err)
		}
	}
	t.Logf("TokenValue: %d bytes, 3 segments", len(tok.TokenValue))

	// ---- Expiration in the future ----

	if !tok.Expiration.After(time.Now()) {
		t.Errorf("Expiration %s is not after now %s", tok.Expiration, time.Now())
	}
	t.Logf("Expiration: %s (%.0fs from now)",
		tok.Expiration.Format(time.RFC3339Nano),
		time.Until(tok.Expiration).Seconds())

	// ---- Principal matches expected ARN shape ----

	if !rolePrincipalRE.MatchString(tok.Principal) {
		t.Errorf("Principal %q does not match pattern %s", tok.Principal, rolePrincipalRE)
	}
	t.Logf("Principal: %s", tok.Principal)

	// ---- JWT payload sanity (iss, sub, aud) ----

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode JWT payload: %v", err)
	}
	var claims struct {
		Iss string `json:"iss"`
		Aud any    `json:"aud"`
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}

	if !strings.HasPrefix(claims.Iss, "https://") {
		t.Errorf("iss %q should start with https://", claims.Iss)
	}
	if !strings.Contains(claims.Iss, ".tokens.sts.") {
		t.Errorf("iss %q should match AWS issuer URL pattern (contain .tokens.sts.)", claims.Iss)
	}
	if claims.Sub != tok.Principal {
		t.Errorf("JWT sub %q != reported Principal %q", claims.Sub, tok.Principal)
	}

	t.Logf("iss: %s", claims.Iss)
	t.Logf("aud: %v", claims.Aud)
	t.Logf("sub: %s", claims.Sub)
}
