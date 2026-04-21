# Design: AWS STS `GetWebIdentityToken` OAUTHBEARER Provider

**Status:** Implemented. See [IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md](IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md) for milestone tracking and the real-AWS validation result.
**Owner:** prashah@confluent.io
**Last updated:** 2026-04-21

## 1. Context

AWS shipped **IAM Outbound Identity Federation** (GA 2025-11-19), which exposes a new STS API, `GetWebIdentityToken`. This lets AWS principals mint short-lived OIDC JWTs that external OIDC-compatible services (e.g. Confluent Cloud) can verify against an AWS-hosted JWKS. AWS acts as the OIDC IdP; the external service is the relying party.

Confluent customers running Kafka clients on AWS (EC2, EKS, ECS/Fargate, Lambda) want to use their AWS identity to authenticate to OIDC-gated Kafka endpoints without maintaining long-lived secrets.

A parallel effort in librdkafka implements this in C for language-agnostic reach (see that repo's `DESIGN_AWS_OAUTHBEARER_V1.md`). This document describes the **complementary Go-layer integration** that lets users who prefer a pure-Go path opt in without waiting for or depending on the librdkafka-side work.

## 2. Decision

Add an **optional, separately-versioned Go submodule** at:

```
kafka/oauthbearer/aws/    →    github.com/confluentinc/confluent-kafka-go/v2/kafka/oauthbearer/aws
```

that wraps the AWS SDK for Go v2 to call `sts:GetWebIdentityToken` and shape the result into a `kafka.OAuthBearerToken`. Users plug it into the existing `OAuthBearerTokenRefresh` event they already handle — one line inside their poll loop.

The submodule has its own `go.mod` declaring the AWS SDK dependency. **Users who never import the submodule see zero change to their dependency graph.**

## 3. Rejected alternatives

### 3a. Build tag on the main `kafka/` package (`-tags awsiam`)

Add build-tag-gated files like `kafka/oauthbearer_aws.go` directly in the main package.

**Why rejected:** even though a gated file isn't compiled without the tag, declaring its imports forces `aws-sdk-go-v2/service/sts` into the root [go.mod](go.mod) as a direct `require`. That's a regression compared to today: verified via `go mod why github.com/aws/aws-sdk-go-v2` — a user importing only `kafka/` does **not** currently resolve the SDK into their build (it only appears as a test-only transitive via `testcontainers` → `docker/compose` → `docker/buildx`). Option A would break that property for every non-opting user.

### 3b. librdkafka-native implementation only

Relies on the parallel librdkafka C-layer work landing. Users stuck on older librdkafka, or who prefer Go-controlled credential handling (AWS SDK's config chain, role chaining, SSO), get nothing.

### 3c. Interface-in-main + plugin registration via `init()`

Define a `TokenProvider` interface in `kafka/`, have AWS subpackage register via `init()`. Elegant, but doesn't solve the dependency-graph question — the user still has to import the subpackage, and the interface surface would need thoughtful stabilization. Can be layered on top of this submodule later without breaking it.

## 4. Verification performed (2026-04-21)

### 4a. `GetWebIdentityToken` is available in `aws-sdk-go-v2/service/sts`

- **Minimum version:** `v1.41.0` (bisected via GitHub content API; absent in `v1.40.x` and earlier, present in `v1.41.0` and all subsequent tags).
- **Current latest:** `v1.42.0`.
- Struct shapes from v1.42.0:
  ```go
  type GetWebIdentityTokenInput struct {
      Audience         []string
      SigningAlgorithm *string   // "ES384" or "RS256"
      DurationSeconds  *int32
  }
  type GetWebIdentityTokenOutput struct {
      Expiration       *time.Time  // SDK handles RFC3339 + fractional seconds
      WebIdentityToken *string
  }
  ```
- Uses the AWS query (XML) protocol internally, consistent with the live wire captures from the librdkafka probe work.

The main module pins `aws-sdk-go-v2/service/sts v1.28.6` (via schemaregistry/awskms). The submodule has its own `go.mod`, so pinning `>= v1.41.0` there does not conflict.

### 4b. Producer/Consumer/AdminClient share identical OAUTHBEARER method signatures

| Type | Method | Source |
|---|---|---|
| `*Producer` | `SetOAuthBearerToken(OAuthBearerToken) error` | [kafka/producer.go:768](kafka/producer.go#L768) |
| `*Producer` | `SetOAuthBearerTokenFailure(string) error` | [kafka/producer.go:783](kafka/producer.go#L783) |
| `*Consumer` | `SetOAuthBearerToken(OAuthBearerToken) error` | [kafka/consumer.go:878](kafka/consumer.go#L878) |
| `*Consumer` | `SetOAuthBearerTokenFailure(string) error` | [kafka/consumer.go:893](kafka/consumer.go#L893) |
| `*AdminClient` | `SetOAuthBearerToken(OAuthBearerToken) error` | [kafka/adminapi.go:2328](kafka/adminapi.go#L2328) |
| `*AdminClient` | `SetOAuthBearerTokenFailure(string) error` | [kafka/adminapi.go:2343](kafka/adminapi.go#L2343) |

All six are exported and delegate to the same internal `handle.setOAuthBearerToken` / `handle.setOAuthBearerTokenFailure`, so a single `Handle` interface binds all three client types.

## 5. Architecture

### 5a. Directory layout

```
confluent-kafka-go/
├── go.mod                          ← unchanged; no new AWS deps
├── go.work                         ← NEW (optional; local dev only)
└── kafka/
    ├── consumer.go                 ← existing hooks unchanged
    ├── producer.go
    ├── adminapi.go
    └── oauthbearer/
        └── aws/                    ← NEW submodule
            ├── go.mod              ← declares aws-sdk-go-v2/service/sts ≥ v1.41.0
            ├── go.sum
            ├── doc.go              ← package documentation
            ├── provider.go         ← Config, TokenProvider, New, Token, Refresh
            ├── provider_test.go    ← unit tests w/ injected HTTPClient
            ├── jwt.go              ← ~20 LoC sub-claim extractor
            ├── jwt_test.go
            └── example_test.go     ← Godoc-renderable runnable example
```

### 5b. Submodule `go.mod`

```go
module github.com/confluentinc/confluent-kafka-go/v2/kafka/oauthbearer/aws

go 1.24.3

require (
    github.com/confluentinc/confluent-kafka-go/v2 v2.14.1
    github.com/aws/aws-sdk-go-v2 v1.42.0
    github.com/aws/aws-sdk-go-v2/config v1.30.0
    github.com/aws/aws-sdk-go-v2/service/sts v1.42.0
)
```

(Exact version pins are TBD at implementation time; `service/sts` floor is `v1.41.0`.)

### 5c. Public API surface

```go
package aws

// Config holds the STS GetWebIdentityToken parameters.
type Config struct {
    Region           string        // required; no default — fail loudly
    Audience         string        // required
    SigningAlgorithm string        // "ES384" (default) or "RS256"
    Duration         time.Duration // 60s–3600s; default 300s
    STSEndpoint      string        // optional override (FIPS, VPC endpoint)

    // Forwarded to config.LoadDefaultConfig. Nil => default credential chain
    // (env → web_identity → ECS → IMDS). Use this to inject a mock HTTPClient
    // in tests, pin a specific profile, etc.
    AWSConfigOptions []func(*config.LoadOptions) error
}

// TokenProvider fetches OAUTHBEARER tokens via STS GetWebIdentityToken.
// Safe for concurrent use.
type TokenProvider struct {
    cfg Config
    sts *sts.Client
}

// New constructs a TokenProvider, resolving AWS credentials eagerly so
// configuration errors surface at startup rather than on first token fetch.
func New(ctx context.Context, cfg Config) (*TokenProvider, error)

// Token fetches a fresh JWT. Callers driving their own event loop use this
// directly, then pass the result to SetOAuthBearerToken.
func (p *TokenProvider) Token(ctx context.Context) (kafka.OAuthBearerToken, error)

// Handle is the subset of *Consumer / *Producer / *AdminClient needed to
// deliver a token. All three already implement it (see §4b).
type Handle interface {
    SetOAuthBearerToken(kafka.OAuthBearerToken) error
    SetOAuthBearerTokenFailure(string) error
}

// Refresh is a convenience wrapper: fetch + set-or-fail. Intended to be called
// from within an OAuthBearerTokenRefresh event handler.
func (p *TokenProvider) Refresh(ctx context.Context, h Handle)
```

### 5d. `Token()` internal flow

```go
func (p *TokenProvider) Token(ctx context.Context) (kafka.OAuthBearerToken, error) {
    out, err := p.sts.GetWebIdentityToken(ctx, &sts.GetWebIdentityTokenInput{
        Audience:         []string{p.cfg.Audience},
        SigningAlgorithm: aws.String(p.cfg.SigningAlgorithm),
        DurationSeconds:  aws.Int32(int32(p.cfg.Duration / time.Second)),
    })
    if err != nil {
        return kafka.OAuthBearerToken{}, fmt.Errorf("sts GetWebIdentityToken: %w", err)
    }

    principal, err := subFromJWT(*out.WebIdentityToken)
    if err != nil {
        return kafka.OAuthBearerToken{}, fmt.Errorf("parse JWT sub: %w", err)
    }

    return kafka.OAuthBearerToken{
        TokenValue: *out.WebIdentityToken,
        Expiration: *out.Expiration,  // SDK parsed already
        Principal:  principal,        // bare role ARN
    }, nil
}
```

All the work that would have been ~850 LoC of C in the librdkafka-native path — SigV4 signing, credential chain, XML parsing, region resolution, retries — is delegated to the AWS SDK.

### 5e. JWT handling

The `sub` claim (bare role ARN per live STS response) is used for `OAuthBearerToken.Principal`. Extraction is a ~20-LoC base64url + `json.Unmarshal` helper in `jwt.go`; no new JWT library dep. Verification is unnecessary — AWS already signed it, and `Principal` is only used for client-side identification in logs.

## 6. User-side integration pattern

```go
import (
    "context"
    "time"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    oauthaws "github.com/confluentinc/confluent-kafka-go/v2/kafka/oauthbearer/aws"
)

func main() {
    ctx := context.Background()

    provider, err := oauthaws.New(ctx, oauthaws.Config{
        Region:   "eu-north-1",
        Audience: "https://confluent.cloud/oidc",
        Duration: time.Hour,
    })
    if err != nil {
        log.Fatal(err)
    }

    c, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": "...",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "OAUTHBEARER",
        "group.id":          "g1",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    c.SubscribeTopics([]string{"t"}, nil)
    for {
        switch ev := c.Poll(1000).(type) {
        case kafka.OAuthBearerTokenRefresh:
            provider.Refresh(ctx, c)   // one-line integration
        case *kafka.Message:
            // ...
        }
    }
}
```

librdkafka drives the refresh cadence: it emits `OAuthBearerTokenRefresh` based on token expiration, and on `SetOAuthBearerTokenFailure` re-emits after ~10 seconds. No goroutines or timers required in user code.

## 7. Local development — `go.work`

Without special handling, the submodule would resolve `confluent-kafka-go/v2` to a published version, which means local changes to `kafka/` aren't visible while hacking on the submodule.

**Chosen approach:** `go.work` at the repo root.

```go
// go.work
go 1.24.3

use (
    .
    ./kafka/oauthbearer/aws
)
```

During local development both modules resolve against the working tree. At release time, the submodule's own `go.mod` — which has no `replace` directive — is what users consume; `go.work` is ignored outside the repo per Go semantics.

Alternative (rejected): `replace github.com/confluentinc/confluent-kafka-go/v2 => ../../..` inside the submodule's `go.mod`, mirroring the [examples/go.mod](examples/go.mod#L5) pattern. Works, but the `replace` must be stripped before tagging or consumers get a broken module. Workspaces avoid that edit step entirely.

## 8. Release and versioning policy

**Policy:** lockstep tagging with the main module.

- When the main module is tagged `v2.X.Y`, also tag `kafka/oauthbearer/aws/v2.X.Y`.
- The submodule's `go.mod` pins the main module at the matching version.
- Users always know which pair is compatible without having to consult a compatibility matrix.
- Cost: one extra `git tag` at release time; no-op submodule releases when nothing changed.

Release scripts in [kafka/README.md:74](kafka/README.md#L74) gain one step: tag the submodule after the main module.

Independent semver (e.g. `kafka/oauthbearer/aws/v0.1.0`) was considered but rejected — Confluent release cadence favors predictability over submodule autonomy.

## 9. Testing strategy

Three layers:

1. **Unit** (`provider_test.go`) — inject a fake `HTTPClient` via `Config.AWSConfigOptions`; return canned STS XML responses; assert token shaping. Fast, no network, no Docker.
2. **JWT extractor** (`jwt_test.go`) — fixtures covering role ARN, assumed-role ARN, missing `sub`, malformed base64, oversized tokens.
3. **Integration (opt-in)** — `TestGetWebIdentityToken_Real` gated on `RUN_AWS_STS_REAL=1` env var, running against real STS with whatever credentials the default chain finds. Runs on the existing EC2 test box (`eu-north-1`, role `ktrue-iam-sts-test-role`); off by default in CI.

The project-wide test runner at [mk/Makefile](mk/Makefile) already traverses all `go.mod`-rooted directories, so the submodule's tests pick up automatically.

## 10. Operational notes

- **Region must be explicit.** No silent default, no IMDS sniffing. Misconfigured region is a startup error. (Matches the librdkafka-side decision.)
- **`DurationSeconds` bounds:** 60–3600 (AWS-enforced). Defaults to 300.
- **Enablement prerequisite:** the AWS account must have called `iam:EnableOutboundWebIdentityFederation` once (admin action, not the client's concern). First `GetWebIdentityToken` on an un-enabled account fails with a distinct error code, surfaced verbatim in `SetOAuthBearerTokenFailure`.
- **FIPS / VPC endpoints:** supported via `Config.STSEndpoint` override.
- **Lambda:** the default credential chain picks up `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` that Lambda injects. Lambda's execution role needs `sts:GetWebIdentityToken` permission; VPC-bound Lambdas need egress to the STS regional endpoint.

## 11. Decisions and open items

Execution plan with milestones is in [IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md](IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md).

**Decided:**

1. **Submodule path naming: `kafka/oauthbearer/aws`.** Leaves room for future `kafka/oauthbearer/azure`, `kafka/oauthbearer/gcp`. Locked.
2. **Credential resolution: lazy.** `New()` is syntactic — it validates config and constructs the `*sts.Client`, but does NOT pre-fetch credentials. First call to `Token()` triggers the SDK's credential chain, which then caches internally. Rationale: matches AWS SDK convention across .NET / Go / Python / JS (all four construct clients cheaply, resolve credentials on first API call, cache afterward). Avoids blocking `New()` on network calls (IMDS, web-identity file I/O).

**Still open:**

3. **Token caching.** The AWS SDK does not cache `GetWebIdentityToken` responses (it caches *credentials*, not the outbound JWT). Since librdkafka drives refresh via the event, caching inside `TokenProvider` is redundant — worth a one-liner comment in the provider doc explaining why, so future readers don't try to "optimize" by adding one.
4. **CHANGELOG + README integration.** Add a new top-level "Optional integrations" section to the README pointing at the submodule, and a `CHANGELOG.md` entry at first release (both tracked as M8 in the implementation plan).

## 12. References

- AWS IAM Outbound Identity Federation announcement (2025-11-19).
- AWS SDK for Go v2 `service/sts` ≥ v1.41.0 — `GetWebIdentityToken` operation.
- Confluent librdkafka-native design (see that repo's `DESIGN_AWS_OAUTHBEARER_V1.md`) — complementary work, identical wire protocol, shared probe results.
- [kafka/consumer.go:868](kafka/consumer.go#L868), [kafka/producer.go:768](kafka/producer.go#L768), [kafka/adminapi.go:2318](kafka/adminapi.go#L2318) — existing OAUTHBEARER hook site.
- [kafka/kafka.go:231](kafka/kafka.go#L231) — `OAuthBearerTokenRefresh` event documentation.
