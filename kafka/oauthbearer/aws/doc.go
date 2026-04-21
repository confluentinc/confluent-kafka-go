// Package aws provides an OAUTHBEARER token provider backed by AWS STS
// GetWebIdentityToken. It lets Go Kafka clients running on AWS authenticate
// to OIDC-gated brokers (e.g. Confluent Cloud) using their AWS IAM identity,
// with no long-lived secrets.
//
// # Usage
//
// Construct a TokenProvider once at startup and call Refresh from the
// OAuthBearerTokenRefresh event handler of your Producer, Consumer, or
// AdminClient. librdkafka drives the refresh cadence based on each token's
// Expiration claim, so no timers or goroutines are needed in user code.
//
// See ExampleTokenProvider_Refresh for a complete wiring example.
//
// # Optional dependency
//
// This submodule is published as a separate Go module. Users who do not
// import it see zero change in their dependency graph: the parent
// confluent-kafka-go module does not declare any AWS SDK dependency.
// A kafka-only consumer pulls no aws-sdk-go-v2 packages into its build.
//
// # Requirements
//
// Requires aws-sdk-go-v2/service/sts v1.41.0 or later — the minimum version
// that exposes the GetWebIdentityToken operation (the underlying AWS IAM
// Outbound Identity Federation feature GA'd 2025-11-19).
//
// The caller's AWS account must have called iam:EnableOutboundWebIdentityFederation
// once; the first GetWebIdentityToken call on an un-enabled account fails
// with a distinct error that surfaces verbatim to SetOAuthBearerTokenFailure.
//
// # No caching
//
// TokenProvider intentionally does not cache tokens. librdkafka drives
// refresh cadence via the OAuthBearerTokenRefresh event based on each
// token's Expiration claim, so an internal cache would be redundant and
// risk returning stale tokens when the upstream state (clock skew, role
// permissions, account configuration) has changed since a previous mint.
//
// # See also
//
// DESIGN_AWS_OAUTHBEARER.md and IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md at
// the confluent-kafka-go repo root for design rationale and milestone plan.
package aws
