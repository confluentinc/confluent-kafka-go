package aws

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

// makeJWT builds a token with the given payload. Header and signature
// contents are irrelevant — subFromJWT only decodes the middle segment.
func makeJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES384","typ":"JWT"}`))
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	body := base64.RawURLEncoding.EncodeToString(payload)
	sig := base64.RawURLEncoding.EncodeToString([]byte("fake-signature"))
	return strings.Join([]string{header, body, sig}, ".")
}

func TestSubFromJWT_RoleARN(t *testing.T) {
	want := "arn:aws:iam::123456789012:role/MyRole"
	tok := makeJWT(t, map[string]any{
		"sub": want,
		"iss": "https://708975691912.tokens.sts.global.api.aws",
		"aud": "https://confluent.cloud/oidc",
	})
	got, err := subFromJWT(tok)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSubFromJWT_AssumedRoleARN(t *testing.T) {
	want := "arn:aws:sts::123456789012:assumed-role/MyRole/session-name"
	tok := makeJWT(t, map[string]any{"sub": want})
	got, err := subFromJWT(tok)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSubFromJWT_MissingSub(t *testing.T) {
	tok := makeJWT(t, map[string]any{
		"iss": "https://example.tokens.sts.global.api.aws",
	})
	_, err := subFromJWT(tok)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "sub") {
		t.Errorf("error %q should mention 'sub'", err.Error())
	}
}

func TestSubFromJWT_EmptySub(t *testing.T) {
	tok := makeJWT(t, map[string]any{"sub": ""})
	if _, err := subFromJWT(tok); err == nil {
		t.Fatal("expected error for empty sub, got nil")
	}
}

func TestSubFromJWT_TwoSegments(t *testing.T) {
	_, err := subFromJWT("aaa.bbb")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "segments") {
		t.Errorf("error %q should mention 'segments'", err.Error())
	}
}

func TestSubFromJWT_FourSegments(t *testing.T) {
	_, err := subFromJWT("aaa.bbb.ccc.ddd")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "segments") {
		t.Errorf("error %q should mention 'segments'", err.Error())
	}
}

func TestSubFromJWT_MalformedBase64(t *testing.T) {
	if _, err := subFromJWT("header.!!!not-base64!!!.signature"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSubFromJWT_MalformedJSON(t *testing.T) {
	notJSON := base64.RawURLEncoding.EncodeToString([]byte("not valid json"))
	if _, err := subFromJWT("header." + notJSON + ".sig"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSubFromJWT_Empty(t *testing.T) {
	_, err := subFromJWT("")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error %q should mention 'empty'", err.Error())
	}
}

func TestSubFromJWT_Oversized(t *testing.T) {
	big := strings.Repeat("a", maxJWTSize+1)
	_, err := subFromJWT(big)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "max size") {
		t.Errorf("error %q should mention 'max size'", err.Error())
	}
}
