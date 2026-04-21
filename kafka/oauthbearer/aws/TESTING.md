# Real-AWS integration testing

This submodule includes a build-tag-gated test that mints a real JWT from
AWS STS `GetWebIdentityToken`. It requires a live AWS environment and is
off by default.

## Prerequisites

### 1. AWS account configuration (one-time, by an admin)

The AWS account must have Outbound Identity Federation enabled:

```sh
aws iam enable-outbound-web-identity-federation
```

If this has not been run, `GetWebIdentityToken` returns a `ValidationError`
whose message names the disabled feature. The failure surfaces verbatim to
`SetOAuthBearerTokenFailure`.

### 2. Execution environment

Run the test from an environment where the AWS SDK default credential chain
can resolve role credentials. Typical options:

- **EC2 instance** with an IAM role attached (IMDSv2 provides credentials)
- **EKS pod** with IRSA (`AWS_WEB_IDENTITY_TOKEN_FILE` set by Kubernetes)
- **EKS Pod Identity** (`AWS_CONTAINER_CREDENTIALS_FULL_URI` + file-token auth)
- **ECS / Fargate task** with a task role
- **Lambda** (env credentials auto-injected by the runtime)
- **Developer laptop** with `AWS_PROFILE` or static env credentials

The role used must have the `sts:GetWebIdentityToken` permission:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "sts:GetWebIdentityToken",
    "Resource": "*"
  }]
}
```

### 3. Build tag + env var gates

The test is gated on **both** the `integration` build tag and the
`RUN_AWS_STS_REAL=1` env var. Neither alone will trigger it:

| Command | Behavior |
|---|---|
| `go test ./...` | Build tag excludes the file entirely |
| `go test -tags integration ./...` | File compiles; test skips on env var check |
| `RUN_AWS_STS_REAL=1 go test ./...` | Build tag still excludes — safe |
| `RUN_AWS_STS_REAL=1 go test -tags integration ./...` | Runs for real |

## Running

```sh
export AWS_REGION=eu-north-1
export STS_AUDIENCE="https://confluent.cloud/oidc"
export RUN_AWS_STS_REAL=1

go test -tags integration -run TestGetWebIdentityToken_Real -v \
    github.com/confluentinc/confluent-kafka-go/v2/kafka/oauthbearer/aws
```

### Optional env vars

| Variable | Purpose |
|---|---|
| `STS_DURATION_SECONDS` | Override the default 300s token lifetime (must be in `[60, 3600]`) |

## What the test verifies

On success, the test asserts:

1. **TokenValue** is a 3-segment base64url-encoded JWT.
2. **Expiration** is strictly after `time.Now()`.
3. **Principal** matches `^arn:aws:(iam|sts)::\d+:(role|assumed-role)/.+$`.
4. **JWT `iss` claim** starts with `https://` and contains `.tokens.sts.`
   (the AWS OIDC issuer URL pattern, per live probe data).
5. **JWT `sub` claim** matches the reported `Principal`.

All of these are logged via `t.Logf` along with token byte size, expiration
delta, and the issuer URL — useful for post-mortem debugging.

## Scope boundary

This test verifies that the submodule correctly mints a JWT via real AWS.
It does **not** verify that the minted JWT is accepted by a Kafka broker —
that requires the broker's OIDC trust configuration to include the AWS
JWKS, which is an admin action outside this test's scope.

End-to-end validation against an OIDC-configured Confluent Cloud cluster
is a separate manual step, owner-driven.
