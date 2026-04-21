# Implementation Plan: AWS OAUTHBEARER Submodule

**Companion document to** [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md).
**Status:** Complete — M1 through M8 delivered. Real-AWS validation passed on 2026-04-21 against `ktrue-iam-sts-test-role` in `eu-north-1` (account 708975691912); 1256-byte JWT minted, all assertions green.
**Last updated:** 2026-04-21.

This document describes the execution path. For design rationale, public API shape, and rejected alternatives, see the design doc.

## Locked decisions

| Decision | Value |
|---|---|
| Submodule path | `kafka/oauthbearer/aws` |
| Module path | `github.com/confluentinc/confluent-kafka-go/v2/kafka/oauthbearer/aws` |
| Credential resolution | **Lazy** — construct client cheaply, resolve on first `Token()` call. Matches AWS SDK convention across .NET / Go / Python / JS. |
| AWS SDK pins (starting point) | `aws-sdk-go-v2 v1.42.0`, `aws-sdk-go-v2/config v1.30.x`, `aws-sdk-go-v2/service/sts v1.42.0` (floor: `sts >= v1.41.0`) |
| Versioning policy | Lockstep with main module |
| Local dev layout | `go.work` at repo root; no `replace` in submodule's published `go.mod` |
| Release target | Deferred — picked at M8. Implementation is unblocked until then. |
| E2E validation | Driven manually by owner after implementation completes. M7 provides the opt-in test scaffold. |

## Critical path

```
M1 ──┬─→ M3 ──→ M4 ──→ M5 ──┬─→ M7 ──→ M8
     │                      │
     └─→ M2 (parallel)      └─→ M6 (parallel)
```

**Total effort:** ~13–20 focused hours. Deliverable in 2–3 working days solo, or split between a pair as M1+M2 and M3–M5.

## Non-negotiable gates enforced at every step

1. **Zero-cost-for-non-opt-in preserved.** After every PR: run `go mod why github.com/aws/aws-sdk-go-v2` from a minimal kafka-only consumer (e.g. [examples/docker_aws_lambda_example/](examples/docker_aws_lambda_example/)) and confirm `(main module does not need package github.com/aws/aws-sdk-go-v2)`. If that ever changes, roll back.
2. **No `replace` directive in the submodule's published `go.mod`.** `go.work` handles local dev; `replace` leaks into released tags.
3. **Lockstep release discipline.** Main module `v2.X.Y` → submodule `kafka/oauthbearer/aws/v2.X.Y`, always. Enforced via release checklist at M8.

---

## M1 — Scaffolding & empty-compile gate

**Estimated effort:** 1–2 hours
**Parallelizable with:** M2
**Blocks:** M3, M4, M5

### Deliverables

- `kafka/oauthbearer/aws/go.mod` with deps:
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
- `kafka/oauthbearer/aws/doc.go` — package-level comment (prose fleshed out at M6).
- `go.work` at repo root:
  ```go
  go 1.24.3

  use (
      .
      ./kafka/oauthbearer/aws
  )
  ```
- Empty `provider.go` with package declaration only.

### Exit criteria

- `go build ./...` from the repo root: clean.
- `go build ./...` from inside the submodule: clean.
- Zero-cost gate: `go mod why github.com/aws/aws-sdk-go-v2` from `examples/docker_aws_lambda_example/` still returns "main module does not need".
- `make -f mk/Makefile "go test"` and `"go vet"` traverse the submodule without errors (may require a tweak to the subdir walker in [mk/Makefile](mk/Makefile); verify and patch if needed).

---

## M2 — JWT `sub` extractor

**Estimated effort:** 1–2 hours
**Parallelizable with:** M1 (fully independent)
**Blocks:** M4

### Deliverables

- `kafka/oauthbearer/aws/jwt.go`:
  ```go
  // subFromJWT extracts the "sub" claim from an unverified JWT payload.
  // AWS already signed this token; verification isn't our job — we just
  // need the claim for OAuthBearerToken.Principal.
  func subFromJWT(token string) (string, error)
  ```
  Implementation: split on `.`, base64url-decode the middle segment, `json.Unmarshal` into `struct{ Sub string \`json:"sub"\` }`. Zero deps beyond stdlib.
- `kafka/oauthbearer/aws/jwt_test.go` covering:
  - Valid role ARN — `arn:aws:iam::123456789012:role/MyRole`.
  - Valid assumed-role ARN — different ARN shape.
  - Missing `sub` claim — returns descriptive error.
  - Token with fewer than 3 dot-separated segments.
  - Token with more than 3 segments.
  - Malformed base64 in middle segment.
  - Malformed JSON in decoded payload.
  - Empty string input.
  - Oversized input guard (reject tokens > ~8KB to avoid attacker-controlled allocation; document rationale).

### Exit criteria

- `go test ./...` green inside submodule, covering all cases above.
- Function is unexported — only used by `provider.go`.

---

## M3 — `Config` + `New()` (lazy-resolution variant)

**Estimated effort:** 2–3 hours
**Depends on:** M1
**Blocks:** M4

### Deliverables

- `Config` struct per design §5c.
- `(*Config).validate()` — private helper:
  - `Region` required (non-empty, no silent default).
  - `Audience` required (non-empty).
  - `SigningAlgorithm` must be `""`, `"ES384"`, or `"RS256"`. Empty → default `"ES384"`.
  - `Duration` — either zero (→ default 300s) or in `[60s, 3600s]`.
- `(*Config).applyDefaults()` — fills zero-valued fields after validation passes.
- `New(ctx context.Context, cfg Config) (*TokenProvider, error)`:
  ```go
  func New(ctx context.Context, cfg Config) (*TokenProvider, error) {
      if err := cfg.validate(); err != nil {
          return nil, err
      }
      cfg.applyDefaults()

      awsCfg, err := config.LoadDefaultConfig(ctx,
          append([]func(*config.LoadOptions) error{
              config.WithRegion(cfg.Region),
          }, cfg.AWSConfigOptions...)...,
      )
      if err != nil {
          return nil, fmt.Errorf("load AWS config: %w", err)
      }

      stsOpts := []func(*sts.Options){}
      if cfg.STSEndpoint != "" {
          stsOpts = append(stsOpts, func(o *sts.Options) {
              o.BaseEndpoint = aws.String(cfg.STSEndpoint)
          })
      }

      return &TokenProvider{
          cfg: cfg,
          sts: sts.NewFromConfig(awsCfg, stsOpts...),
      }, nil
  }
  ```
- **No eager credential resolution.** `LoadDefaultConfig` builds the credential chain but does not call `Retrieve()`. First `Token()` call triggers actual credential fetching via the SDK.

### Tests (`provider_test.go`)

- Validation errors: missing `Region`, missing `Audience`, invalid `SigningAlgorithm`, `Duration` below 60s, `Duration` above 3600s.
- Defaults applied: zero `SigningAlgorithm` → `"ES384"`, zero `Duration` → 300s.
- `AWSConfigOptions` honored: inject `config.WithHTTPClient(fakeClient)` and confirm `fakeClient` is the one the STS client uses (observed by routing a `GetCallerIdentity` call through and asserting the fake handler was called).
- `STSEndpoint` override plumbed through to `sts.Options.BaseEndpoint`.
- **New() does NOT fetch credentials**: use an `AWSConfigOptions` entry that installs a credential provider whose `Retrieve` method panics or sets a flag; verify `New()` returns successfully and the flag is unset.

### Exit criteria

- All tests pass.
- No network calls from `New()` (verified by the credential-panic test).

---

## M4 — `Token()` + STS wire mocking

**Estimated effort:** 3–4 hours
**Depends on:** M2, M3
**Blocks:** M5, M7

### Deliverables

- `Token(ctx context.Context) (kafka.OAuthBearerToken, error)` per design §5d, with the `aws.String(...)` correction for `SigningAlgorithm`:
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
  ```

### Tests — with injected `HTTPClient` returning canned STS XML

Use real-shape XML captured from the librdkafka probe work (namespace `https://sts.amazonaws.com/doc/2011-06-15/`):

- **Happy path:** XML envelope with `WebIdentityToken` = a valid 3-segment JWT whose middle segment decodes to `{"sub": "arn:aws:iam::123456789012:role/TestRole", ...}`, and `Expiration` = `2026-04-21T06:06:47.641000+00:00` (RFC 3339 with fractional seconds, matching probe data). Assert all three fields of returned `OAuthBearerToken`.
- **STS error — AccessDenied:**
  ```xml
  <ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <Error>
      <Type>Sender</Type>
      <Code>AccessDenied</Code>
      <Message>User: ... is not authorized to perform: sts:GetWebIdentityToken</Message>
    </Error>
    <RequestId>...</RequestId>
  </ErrorResponse>
  ```
  Assert error contains `"AccessDenied"` and the operation wrap prefix `"sts GetWebIdentityToken:"`.
- **STS error — account not enabled:** same shape with `Code` indicating OutboundWebIdentityFederation not enabled. Surfaces cleanly.
- **Malformed JWT in response:** `WebIdentityToken` is `not-a-jwt`. Error wraps with `"parse JWT sub:"`.
- **Network failure:** injected `HTTPClient` returns a transport error. Error propagates.
- **Duration passthrough:** configure `Duration = 900s`, assert the outgoing request body contains `DurationSeconds=900` (parse the form-urlencoded body in the fake handler).

### Exit criteria

- All tests pass.
- No real AWS calls.
- Error messages include enough context for operator diagnosis without leaking credentials or tokens.

---

## M5 — `Handle` interface + `Refresh()` helper

**Estimated effort:** 1–2 hours
**Depends on:** M4
**Blocks:** M7

### Deliverables

- `Handle` interface per design §5c.
- `Refresh(ctx context.Context, h Handle)` method:
  ```go
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
  ```
- Compile-time satisfaction assertion in a new file `handle_assert_test.go`:
  ```go
  var (
      _ Handle = (*kafka.Producer)(nil)
      _ Handle = (*kafka.Consumer)(nil)
      _ Handle = (*kafka.AdminClient)(nil)
  )
  ```

### Tests

- Fake Handle recording calls (`type fakeHandle struct { setCalls []kafka.OAuthBearerToken; failCalls []string }`):
  - `Refresh` on happy path: asserts `setCalls` has exactly one entry with the expected token; `failCalls` is empty.
  - `Refresh` when `Token()` fails (STS error injected via M4 mocks): `setCalls` empty, `failCalls` has one entry with the wrapped error string.
  - `Refresh` when `SetOAuthBearerToken` itself returns an error (simulating librdkafka Principal/Extensions regex rejection): `setCalls` has one entry, `failCalls` has one entry with the SetOAuthBearerToken error.

### Exit criteria

- All tests pass.
- Compile-time assertion succeeds (guarantees future kafka package refactors can't silently break the Handle contract).

---

## M6 — Godoc-renderable example + doc.go polish

**Estimated effort:** 1 hour
**Depends on:** M5
**Parallelizable with:** M7

### Deliverables

- `kafka/oauthbearer/aws/example_test.go` with `ExampleTokenProvider_Refresh` showing the full event-loop integration from design §6. Must compile and execute via `go test` (the `go test` runner validates `Example*` functions with `// Output:` directives — can use `// Output:` with empty expected output if the example doesn't naturally print).
- `kafka/oauthbearer/aws/doc.go` fleshed out:
  - Two-paragraph package overview.
  - Minimum SDK version note (`aws-sdk-go-v2/service/sts >= v1.41.0`).
  - Pointer to [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) and [IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md](IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md).
  - One-liner comment explaining why `TokenProvider` does not cache the JWT (librdkafka drives refresh cadence; internal caching would be redundant and create staleness hazards).

### Exit criteria

- Godoc renders cleanly (verified via `go doc -all ./kafka/oauthbearer/aws`).
- `go test ./...` passes including the example function.

---

## M7 — Real-AWS integration test (scaffold for manual E2E)

**Estimated effort:** 2–3 hours (test code + one validation run)
**Depends on:** M4
**Parallelizable with:** M6

### Deliverables

- `kafka/oauthbearer/aws/provider_real_test.go`:
  ```go
  //go:build integration

  func TestGetWebIdentityToken_Real(t *testing.T) {
      if os.Getenv("RUN_AWS_STS_REAL") != "1" {
          t.Skip("set RUN_AWS_STS_REAL=1 to run")
      }
      // ... construct provider, call Token, assert shape
  }
  ```
  Gated on build tag `integration` AND env var, so neither default `go test` nor `go test -tags integration` in an unconfigured environment accidentally tries to hit AWS.
- Assertions:
  - `TokenValue` non-empty, 3 base64url segments.
  - `Expiration.After(time.Now())`.
  - `Principal` matches `^arn:aws:iam::\d+:role/.+$`.
  - Bonus: decode the JWT and verify `iss` is `https://*.tokens.sts.global.api.aws` (from probe findings).
- Reproduce recipe in a new file `kafka/oauthbearer/aws/TESTING.md`:
  - Required env vars (`AWS_REGION`, `RUN_AWS_STS_REAL=1`, optionally `AUDIENCE`).
  - EC2 role prerequisites (`sts:GetWebIdentityToken` permission, account-level `EnableOutboundWebIdentityFederation` executed).
  - Command: `RUN_AWS_STS_REAL=1 go test -tags integration -run TestGetWebIdentityToken_Real -v ./kafka/oauthbearer/aws/`.

### Scope boundary

This milestone verifies we MINT a valid token. It does NOT verify the token is accepted by a Kafka broker — that requires Confluent Cloud OIDC trust configuration, which is an admin action outside this implementation's scope. Broker-side acceptance is the manual E2E step the owner will drive.

### Exit criteria

- Scaffold code compiles under `-tags integration`.
- Owner runs the test on EC2 and reports green. Artifact: test output attached to PR.

---

## M8 — Release wiring

**Estimated effort:** 2–3 hours
**Depends on:** M1–M7 all complete

### Deliverables

1. **Release checklist update** — add a submodule-tagging step to [kafka/README.md:74](kafka/README.md#L74) Release process section:
   ```
   ### Tag the AWS OAUTHBEARER submodule (if present)

       $ git tag kafka/oauthbearer/aws/vX.Y.Z
       $ git push --dry-run origin kafka/oauthbearer/aws/vX.Y.Z
   ```
2. **CHANGELOG entry** — add to [CHANGELOG.md](CHANGELOG.md) under the target version:
   ```
   ## Go Client

   ### Enhancements

   - Added optional AWS OAUTHBEARER token provider submodule at
     `kafka/oauthbearer/aws`. See DESIGN_AWS_OAUTHBEARER.md for details.
     This submodule is opt-in; users not importing it see zero change
     in their dependency graph.
   ```
3. **README.md — new "Optional integrations" section** after the existing `librdkafka` section, pointing at the submodule and the design doc. Explicitly call out that it does not affect the main module's dependency surface.
4. **Verify project-wide tooling**:
   - `make -f mk/Makefile "go test"` traverses the submodule.
   - `make -f mk/Makefile "go vet"` traverses the submodule.
   - `make -f mk/Makefile docs` handles (or explicitly skips) the submodule.
5. **Release rehearsal** (local):
   - Tag `kafka/oauthbearer/aws/v2.15.0-rc.0` on a throwaway branch.
   - Point a scratch project at it: `go get github.com/confluentinc/confluent-kafka-go/v2/kafka/oauthbearer/aws@v2.15.0-rc.0`.
   - Confirm `go build` succeeds.
   - Delete the rehearsal tag.

### Exit criteria

- Release checklist updated and reviewed.
- CHANGELOG entry merged.
- README section merged.
- All four make targets clean.
- Rehearsal tag resolution confirmed (then deleted).

---

## Open items for later (not blocking implementation)

1. **Release target version** — pick at M8. Leaning toward whatever minor version is cut after implementation merges.
2. **Extension to other high-level clients** — confluent-kafka-python, confluent-kafka-javascript, confluent-kafka-dotnet. Out of scope here; pattern is documented in the project-level memory so future work can reuse.
3. **Additional token providers** — `kafka/oauthbearer/azure`, `kafka/oauthbearer/gcp`. The path naming reserves space for these; implementations are independent future work.

## References

- [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) — full design doc.
- [kafka/producer.go:768](kafka/producer.go#L768), [kafka/consumer.go:878](kafka/consumer.go#L878), [kafka/adminapi.go:2328](kafka/adminapi.go#L2328) — `SetOAuthBearerToken` / `SetOAuthBearerTokenFailure` sites.
- [kafka/kafka.go:231](kafka/kafka.go#L231) — `OAuthBearerTokenRefresh` event docs.
- [examples/go.mod](examples/go.mod) — nested-module precedent.
