package aws

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Config holds the STS GetWebIdentityToken parameters.
type Config struct {
	// Region is the AWS region whose STS regional endpoint will be used.
	// Required. No default — misconfiguration here should fail loudly.
	Region string

	// Audience is the audience claim baked into the minted JWT. Required.
	Audience string

	// SigningAlgorithm selects the JWT signing algorithm STS will use.
	// Allowed: "" (defaults to "ES384"), "ES384", "RS256".
	SigningAlgorithm string

	// Duration is the requested token lifetime. Zero value defaults to
	// 5 minutes. Bounded to [1m, 1h] by AWS.
	Duration time.Duration

	// STSEndpoint optionally overrides the default regional STS endpoint.
	// Use for FIPS endpoints or VPC-private STS endpoints.
	STSEndpoint string

	// AWSConfigOptions are forwarded to config.LoadDefaultConfig. Use this
	// to inject a mock HTTPClient in tests, pin a specific profile, etc.
	// Nil means default credential chain (env → web_identity → ECS → IMDS).
	AWSConfigOptions []func(*config.LoadOptions) error
}

const (
	defaultSigningAlgorithm = "ES384"
	defaultDuration         = 5 * time.Minute
	minDuration             = 1 * time.Minute
	maxDuration             = 1 * time.Hour
)

func (c *Config) validate() error {
	if c.Region == "" {
		return fmt.Errorf("Region is required")
	}
	if c.Audience == "" {
		return fmt.Errorf("Audience is required")
	}
	switch c.SigningAlgorithm {
	case "", "ES384", "RS256":
	default:
		return fmt.Errorf("SigningAlgorithm %q not supported (want \"ES384\" or \"RS256\")", c.SigningAlgorithm)
	}
	if c.Duration != 0 && (c.Duration < minDuration || c.Duration > maxDuration) {
		return fmt.Errorf("Duration %s out of range [%s, %s]", c.Duration, minDuration, maxDuration)
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.SigningAlgorithm == "" {
		c.SigningAlgorithm = defaultSigningAlgorithm
	}
	if c.Duration == 0 {
		c.Duration = defaultDuration
	}
}

// TokenProvider fetches OAUTHBEARER tokens via AWS STS GetWebIdentityToken.
// Safe for concurrent use.
//
// TokenProvider does not cache tokens — librdkafka drives refresh cadence via
// the OAuthBearerTokenRefresh event based on each token's expiration, so
// internal caching would be redundant and risk returning stale tokens.
type TokenProvider struct {
	cfg Config
	sts *sts.Client
}

// New constructs a TokenProvider. Credential resolution is lazy: the AWS
// credential chain is built during New(), but credentials are not fetched
// until the first Token() call. This matches AWS SDK convention across
// .NET / Go / Python / JS — all four construct clients cheaply and resolve
// credentials on first API call, caching afterward.
func New(ctx context.Context, cfg Config) (*TokenProvider, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()

	loadOpts := append(
		[]func(*config.LoadOptions) error{config.WithRegion(cfg.Region)},
		cfg.AWSConfigOptions...,
	)
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var stsOpts []func(*sts.Options)
	if cfg.STSEndpoint != "" {
		ep := cfg.STSEndpoint
		stsOpts = append(stsOpts, func(o *sts.Options) {
			o.BaseEndpoint = aws.String(ep)
		})
	}

	return &TokenProvider{
		cfg: cfg,
		sts: sts.NewFromConfig(awsCfg, stsOpts...),
	}, nil
}

// Token fetches a fresh JWT via STS GetWebIdentityToken and shapes it into a
// kafka.OAuthBearerToken. Callers driving their own event loop invoke this
// directly, then pass the result to SetOAuthBearerToken. Callers using the
// Refresh convenience get this called for them.
func (p *TokenProvider) Token(ctx context.Context) (kafka.OAuthBearerToken, error) {
	out, err := p.sts.GetWebIdentityToken(ctx, &sts.GetWebIdentityTokenInput{
		Audience:         []string{p.cfg.Audience},
		SigningAlgorithm: aws.String(p.cfg.SigningAlgorithm),
		DurationSeconds:  aws.Int32(int32(p.cfg.Duration / time.Second)),
	})
	if err != nil {
		return kafka.OAuthBearerToken{}, fmt.Errorf("sts GetWebIdentityToken: %w", err)
	}

	principal, err := subFromJWT(aws.ToString(out.WebIdentityToken))
	if err != nil {
		return kafka.OAuthBearerToken{}, fmt.Errorf("parse JWT sub: %w", err)
	}

	return kafka.OAuthBearerToken{
		TokenValue: aws.ToString(out.WebIdentityToken),
		Expiration: aws.ToTime(out.Expiration),
		Principal:  principal,
	}, nil
}

// Handle is the subset of *kafka.Producer / *kafka.Consumer / *kafka.AdminClient
// needed to deliver a token. All three types implement it with identical
// method signatures — see kafka/producer.go, kafka/consumer.go, kafka/adminapi.go.
type Handle interface {
	SetOAuthBearerToken(kafka.OAuthBearerToken) error
	SetOAuthBearerTokenFailure(string) error
}

// Refresh fetches a fresh token and delivers it to h. Intended to be called
// from within an OAuthBearerTokenRefresh event handler:
//
//	case kafka.OAuthBearerTokenRefresh:
//	    provider.Refresh(ctx, client)
//
// On any failure — STS error, JWT parse error, or a token rejected by the
// client's own OAUTHBEARER validation — it calls SetOAuthBearerTokenFailure,
// which schedules librdkafka to re-emit the refresh event ~10s later.
func (p *TokenProvider) Refresh(ctx context.Context, h Handle) {
	tok, err := p.Token(ctx)
	if err != nil {
		_ = h.SetOAuthBearerTokenFailure(err.Error())
		return
	}
	if err := h.SetOAuthBearerToken(tok); err != nil {
		_ = h.SetOAuthBearerTokenFailure(err.Error())
	}
}
