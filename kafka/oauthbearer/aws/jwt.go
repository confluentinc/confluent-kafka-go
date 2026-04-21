package aws

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// maxJWTSize bounds the input to subFromJWT. Real AWS GetWebIdentityToken
// JWTs are ~1.5KB; 8KB leaves headroom for future claim growth while
// preventing attacker-controlled allocation from untrusted input.
const maxJWTSize = 8 * 1024

// subFromJWT extracts the "sub" claim from a JWT without verifying the
// signature. AWS already signed the token; verification is the relying
// party's responsibility. We only need sub for OAuthBearerToken.Principal,
// which librdkafka uses for client-side identification in logs.
func subFromJWT(token string) (string, error) {
	if token == "" {
		return "", errors.New("empty token")
	}
	if len(token) > maxJWTSize {
		return "", fmt.Errorf("token exceeds max size of %d bytes", maxJWTSize)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("token has %d segments, expected 3", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("decode payload: %w", err)
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("unmarshal payload: %w", err)
	}
	if claims.Sub == "" {
		return "", errors.New("sub claim missing or empty")
	}
	return claims.Sub, nil
}
