/**
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package internal

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rest"
	"golang.org/x/oauth2/clientcredentials"
)

// Relative Confluent Schema Registry REST API endpoints as described in the Confluent documentation
// https://docs.confluent.io/current/schema-registry/docs/api.html
const (
	Base                     = ".."
	Schemas                  = "/schemas/ids/%d"
	Contexts                 = "/contexts"
	SchemasBySubject         = "/schemas/ids/%d?subject=%s"
	SubjectsAndVersionsByID  = "/schemas/ids/%d/versions"
	SchemasByGUID            = "/schemas/guids/%s"
	Subject                  = "/subjects"
	Subjects                 = Subject + "/%s"
	SubjectsNormalize        = Subject + "/%s?normalize=%t"
	SubjectsNormalizeDeleted = Subject + "/%s?normalize=%t&deleted=%t"
	SubjectsDelete           = Subjects + "?permanent=%t"
	LatestWithMetadata       = Subjects + "/metadata?deleted=%t%s"
	Version                  = Subjects + "/versions"
	VersionNormalize         = Subjects + "/versions?normalize=%t"
	Versions                 = Version + "/%v"
	VersionsIncludeDeleted   = Versions + "?deleted=%t"
	VersionsDelete           = Versions + "?permanent=%t"
	SubjectCompatibility     = "/compatibility" + Version
	Compatibility            = "/compatibility" + Versions
	Config                   = "/config"
	SubjectConfig            = Config + "/%s"
	SubjectConfigDefault     = SubjectConfig + "?defaultToGlobal=%t"
	Mode                     = "/mode"
	SubjectMode              = Mode + "/%s"

	Keks          = "/dek-registry/v1/keks"
	KekByName     = Keks + "/%s?deleted=%t"
	Deks          = Keks + "/%s/deks"
	DeksBySubject = Deks + "/%s?algorithm=%s&deleted=%t"
	DeksByVersion = Deks + "/%s/versions/%v?algorithm=%s&deleted=%t"

	Associations                 = "/associations"
	AssociationsBySubject        = Associations + "/subjects/%s"
	AssociationsByResourceID     = Associations + "/resources/%s"
	AssociationsDeleteByResource = AssociationsByResourceID

	TargetSRClusterKey      = "Target-Sr-Cluster"
	TargetIdentityPoolIDKey = "Confluent-Identity-Pool-Id"
)

// API represents a REST API request
type API struct {
	method    string
	endpoint  string
	arguments []interface{}
	body      interface{}
}

// NewRequest returns new Confluent Schema Registry API request */
func NewRequest(method string, endpoint string, body interface{}, arguments ...interface{}) *API {
	return &API{
		method:    method,
		endpoint:  endpoint,
		arguments: arguments,
		body:      body,
	}
}

/*
* HTTP error codes/ SR int:error_code:
*	402: Invalid {resource}
*	404: {resource} not found
*		- 40401 - Subject not found
*		- 40402 - SchemaMetadata not found
*		- 40403 - Schema not found
*	422: Invalid {resource}
*		- 42201 - Invalid Schema
*		- 42202 - Invalid SchemaMetadata
*	500: Internal Server Error (something broke between SR and Kafka)
*		- 50001 - Error in backend(kafka)
*		- 50002 - Operation timed out
*		- 50003 - Error forwarding request to SR leader
 */

// RestService represents a REST client
type RestService struct {
	urls                         []*url.URL
	headers                      http.Header
	maxRetries                   int
	retriesWaitMs                int
	retriesMaxWaitMs             int
	ceilingRetries               int
	authenticationHeaderProvider AuthenticationHeaderProvider
	*http.Client
}

// NewRestService returns a new REST client for the Confluent Schema Registry
func NewRestService(conf *ClientConfig) (*RestService, error) {
	urlConf := conf.SchemaRegistryURL
	urlStrs := strings.Split(urlConf, ",")
	urls := make([]*url.URL, len(urlStrs))
	for i, urlStr := range urlStrs {
		u, err := url.Parse(strings.TrimSpace(urlStr))
		if err != nil {
			return nil, err
		}
		urls[i] = u
	}

	headers := http.Header{}

	headers.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	authenticationHeaderProvider, err := NewAuthenticationHeaderProvider(urls[0], conf)
	if err != nil {
		return nil, err
	}

	if conf.HTTPClient == nil {
		transport, err := configureTransport(conf)
		if err != nil {
			return nil, err
		}

		timeout := conf.RequestTimeoutMs

		conf.HTTPClient = &http.Client{
			Transport: transport,
			Timeout:   time.Duration(timeout) * time.Millisecond,
		}
	}

	return &RestService{
		urls:                         urls,
		headers:                      headers,
		maxRetries:                   conf.MaxRetries,
		retriesWaitMs:                conf.RetriesWaitMs,
		retriesMaxWaitMs:             conf.RetriesMaxWaitMs,
		ceilingRetries:               int(math.Log2(float64(conf.RetriesMaxWaitMs) / float64(conf.RetriesWaitMs))),
		Client:                       conf.HTTPClient,
		authenticationHeaderProvider: authenticationHeaderProvider,
	}, nil
}

// encodeBasicAuth adds a basic http authentication header to the provided header
func encodeBasicAuth(userinfo string) string {
	return base64.StdEncoding.EncodeToString([]byte(userinfo))
}

// ConfigureTLS populates tlsConf
func ConfigureTLS(conf *ClientConfig, tlsConf *tls.Config) error {
	certFile := conf.SslCertificateLocation
	keyFile := conf.SslKeyLocation
	caFile := conf.SslCaLocation
	unsafe := conf.SslDisableEndpointVerification

	var err error
	if certFile != "" {
		if keyFile == "" {
			return errors.New(
				"SslKeyLocation needs to be provided if using SslCertificateLocation")
		}
		var cert tls.Certificate
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return err
		}
		tlsConf.Certificates = []tls.Certificate{cert}
	}

	if caFile != "" {
		if unsafe {
			log.Println("WARN: endpoint verification is currently disabled. " +
				"This feature should be configured for development purposes only")
		}
		var caCert []byte
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return err
		}

		tlsConf.RootCAs = x509.NewCertPool()
		if !tlsConf.RootCAs.AppendCertsFromPEM(caCert) {
			return fmt.Errorf("could not parse certificate from %s", caFile)
		}
	}

	tlsConf.BuildNameToCertificate()

	return err
}

// configureTransport returns a new Transport for use by the Confluent Schema Registry REST client
func configureTransport(conf *ClientConfig) (*http.Transport, error) {

	// Exposed for testing purposes only. In production properly formed certificates should be used
	// https://tools.ietf.org/html/rfc2818#section-3
	tlsConfig := &tls.Config{}
	if err := ConfigureTLS(conf, tlsConfig); err != nil {
		return nil, err
	}

	timeout := conf.ConnectionTimeoutMs

	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout: time.Duration(timeout) * time.Millisecond,
		}).Dial,
		TLSClientConfig: tlsConfig,
	}, nil
}

// create creates a new BasicAuthenticationHeaderProvider
// that uses the URL to set the Basic Authentication header
func createURLAuthHeaderProvider(service *url.URL) (AuthenticationHeaderProvider, error) {
	return NewBasicAuthenticationHeaderProvider(encodeBasicAuth(service.User.String())), nil
}

// createSASLAuthHeaderProvider creates a new BasicAuthenticationHeaderProvider
// that uses the SASL_INHERIT source to set the Basic Authentication header
func createSASLAuthHeaderProvider(conf *ClientConfig) (AuthenticationHeaderProvider, error) {
	mech := conf.SaslMechanism
	if strings.ToUpper(mech) == "GSSAPI" {
		return nil, fmt.Errorf("SASL_INHERIT support PLAIN and SCRAM SASL mechanisms only")
	}

	user := conf.SaslUsername
	pass := conf.SaslPassword
	if user == "" || pass == "" {
		return nil, fmt.Errorf("SASL_INHERIT requires both sasl.username and sasl.password be set")
	}

	return NewBasicAuthenticationHeaderProvider(encodeBasicAuth(fmt.Sprintf("%s:%s", user, pass))), nil
}

// createUSERINFOAuthHeaderProvider creates a new BasicAuthenticationHeaderProvider
// that uses the userinfo string to set the Basic Authentication header
func createUSERINFOAuthHeaderProvider(conf *ClientConfig) (AuthenticationHeaderProvider, error) {
	auth := conf.BasicAuthUserInfo
	if auth == "" {
		return nil, fmt.Errorf("USER_INFO source configured without basic.auth.user.info ")
	}

	return NewBasicAuthenticationHeaderProvider(encodeBasicAuth(auth)), nil
}

// checkIdentityPoolIDAndLogicalCluster checks if identity pool id and logical cluster are set
func checkIdentityPoolIDAndLogicalCluster(conf *ClientConfig) error {
	if conf.BearerAuthIdentityPoolID == "" {
		return fmt.Errorf("bearer.auth.identity.pool.id must be specified when bearer.auth.credentials.source is" +
			" specified with STATIC_TOKEN or OAUTHBEARER")
	}
	if conf.BearerAuthLogicalCluster == "" {
		return fmt.Errorf("bearer.auth.logical.cluster must be specified when bearer.auth.credentials.source is" +
			" specified with STATIC_TOKEN or OAUTHBEARER")

	}
	return nil
}

// checkBearerOAuthFields checks if the bearer auth fields are set
func checkBearerOAuthFields(conf *ClientConfig) error {
	if conf.AuthenticationHeaderProvider != nil {
		return fmt.Errorf("cannot have bearer.auth.credentials.source oauthbearer " +
			"with custom authentication header provider")
	}

	if conf.BearerAuthIssuerEndpointURL == "" {
		return fmt.Errorf("bearer.auth.issuer.endpoint.url must be specified when bearer.auth.credentials.source is" +
			" specified with OAUTHBEARER")
	}

	if conf.BearerAuthClientID == "" {
		return fmt.Errorf("bearer.auth.client.id must be specified when bearer.auth.credentials.source is" +
			" specified with OAUTHBEARER")
	}

	if conf.BearerAuthClientSecret == "" {
		return fmt.Errorf("bearer.auth.client.secret must be specified when bearer.auth.credentials.source is" +
			" specified with OAUTHBEARER")
	}

	err := checkIdentityPoolIDAndLogicalCluster(conf)
	if err != nil {
		return err
	}

	return nil
}

// createStaticTokenAuthHeaderProvider creates a new StaticTokenAuthenticationHeaderProvider
func createStaticTokenAuthHeaderProvider(conf *ClientConfig) (AuthenticationHeaderProvider, error) {
	bearerToken := conf.BearerAuthToken
	if len(bearerToken) == 0 {
		return nil, fmt.Errorf("config bearer.auth.token must be specified when bearer.auth.credentials.source is" +
			" specified with STATIC_TOKEN")
	}

	if conf.AuthenticationHeaderProvider != nil {
		return nil, fmt.Errorf("cannot have bearer.auth.credentials.source static_token " +
			"with custom authentication header provider")
	}

	// TODO: Enable these lines for major version 3 release, since static token does not check for
	// identity pool id and logical cluster at the moment

	// err := checkIdentityPoolIDAndLogicalCluster(conf)
	// if err != nil {
	// 	return nil, err
	// }

	return NewStaticTokenAuthenticationHeaderProvider(bearerToken,
		conf.BearerAuthIdentityPoolID,
		conf.BearerAuthLogicalCluster), nil
}

// createBearerOAuthHeaderProvider creates a new BearerTokenAuthenticationHeaderProvider
func createBearerOAuthHeaderProvider(conf *ClientConfig) (AuthenticationHeaderProvider, error) {
	err := checkBearerOAuthFields(conf)
	if err != nil {
		return nil, err
	}

	tokenFetcher := &clientcredentials.Config{
		ClientID:     conf.BearerAuthClientID,
		ClientSecret: conf.BearerAuthClientSecret,
		TokenURL:     conf.BearerAuthIssuerEndpointURL,
		Scopes:       conf.BearerAuthScopes,
	}

	authenticationHeaderProvider := NewBearerTokenAuthenticationHeaderProvider(
		conf.BearerAuthIdentityPoolID,
		conf.BearerAuthLogicalCluster,
		tokenFetcher,
		conf.MaxRetries,
		conf.RetriesWaitMs,
		conf.RetriesMaxWaitMs,
	)

	return authenticationHeaderProvider, nil
}

// handleCustomAuthenticationHeaderProvider handles custom authentication header provider
func handleCustomAuthenticationHeaderProvider(conf *ClientConfig) (AuthenticationHeaderProvider, error) {
	if conf.AuthenticationHeaderProvider == nil {
		return nil, fmt.Errorf("cannot have bearer.auth.credentials.source custom " +
			"with no custom authentication header provider")
	}

	return conf.AuthenticationHeaderProvider, nil
}

// NewAuthenticationHeaderProvider returns a base64 encoded userinfo string identified on the configured credentials source
func NewAuthenticationHeaderProvider(service *url.URL, conf *ClientConfig) (AuthenticationHeaderProvider, error) {
	// Remove userinfo from url regardless of source to avoid confusion/conflicts
	defer func() {
		service.User = nil
	}()

	basicSource := conf.BasicAuthCredentialsSource
	bearerSource := conf.BearerAuthCredentialsSource

	var err error
	var provider AuthenticationHeaderProvider
	if len(basicSource) != 0 && len(bearerSource) != 0 {
		return nil, fmt.Errorf("only one of basic.auth.credentials.source or bearer.auth.credentials.source" +
			" may be specified")
	} else if len(basicSource) != 0 {
		switch strings.ToUpper(basicSource) {
		case "URL":
			provider, err = createURLAuthHeaderProvider(service)
		case "SASL_INHERIT":
			provider, err = createSASLAuthHeaderProvider(conf)
		case "USER_INFO":
			provider, err = createUSERINFOAuthHeaderProvider(conf)
		default:
			err = fmt.Errorf("unrecognized value for basic.auth.credentials.source %s", basicSource)
		}
	} else if len(bearerSource) != 0 {
		switch strings.ToUpper(bearerSource) {
		case "STATIC_TOKEN":
			provider, err = createStaticTokenAuthHeaderProvider(conf)
		case "OAUTHBEARER":
			provider, err = createBearerOAuthHeaderProvider(conf)
		case "CUSTOM":
			provider, err = handleCustomAuthenticationHeaderProvider(conf)
		default:
			err = fmt.Errorf("unrecognized value for bearer.auth.credentials.source %s", bearerSource)
		}
	}

	return provider, err
}

// HandleRequest sends a request to the Schema Registry, iterating over the list of URLs
func (rs *RestService) HandleRequest(request *API, response interface{}) error {
	var resp *http.Response
	var err error
	for i, u := range rs.urls {
		resp, err = rs.HandleHTTPRequest(u, request)
		if err != nil {
			if i == len(rs.urls)-1 {
				return err
			}
			continue
		}
		if isSuccess(resp.StatusCode) || !isRetriable(resp.StatusCode) || i >= rs.maxRetries {
			break
		}
	}
	defer resp.Body.Close()
	if isSuccess(resp.StatusCode) {
		if err = json.NewDecoder(resp.Body).Decode(response); err != nil {
			return err
		}
		return nil
	}

	var failure rest.Error
	if err = json.NewDecoder(resp.Body).Decode(&failure); err != nil {
		return err
	}

	return &failure
}

// SetAuthenticationHeaders sets the authentication headers on the request
func SetAuthenticationHeaders(provider AuthenticationHeaderProvider, headers *http.Header) error {
	if provider == nil {
		return nil
	}
	authHeader, err := provider.GetAuthenticationHeader()
	if err != nil {
		return err
	}
	headers.Set("Authorization", authHeader)

	identityPoolID, err := provider.GetIdentityPoolID()
	if err != nil {
		return err
	}
	if len(identityPoolID) > 0 {
		headers.Set("Confluent-Identity-Pool-Id", identityPoolID)
	}

	logicalCluster, err := provider.GetLogicalCluster()
	if err != nil {
		return err
	}
	if len(logicalCluster) > 0 {
		headers.Set("Target-Sr-Cluster", logicalCluster)
	}

	return nil
}

// HandleHTTPRequest sends a HTTP(S) request to the Schema Registry, placing results into the response object
func (rs *RestService) HandleHTTPRequest(url *url.URL, request *API) (*http.Response, error) {
	urlPath := path.Join(url.Path, fmt.Sprintf(request.endpoint, request.arguments...))
	endpoint, err := url.Parse(urlPath)
	if err != nil {
		return nil, err
	}

	var outbuf io.Reader
	if request.body != nil {
		body, err := json.Marshal(request.body)
		if err != nil {
			return nil, err
		}
		outbuf = bytes.NewBuffer(body)
	}

	var req *http.Request
	var resp *http.Response

	err = SetAuthenticationHeaders(rs.authenticationHeaderProvider, &rs.headers)

	if err != nil {
		return nil, err
	}

	for i := 0; i < rs.maxRetries+1; i++ {

		req, err = http.NewRequest(
			request.method,
			endpoint.String(),
			outbuf,
		)
		req.Header = rs.headers

		resp, err = rs.Do(req)
		if err != nil {
			return nil, err
		}

		if isSuccess(resp.StatusCode) || !isRetriable(resp.StatusCode) || i >= rs.maxRetries {
			return resp, nil
		}

		time.Sleep(fullJitter(i, rs.ceilingRetries, rs.retriesMaxWaitMs, rs.retriesWaitMs))
	}
	return nil, fmt.Errorf("failed to send request after %d retries", rs.maxRetries)
}

func fullJitter(retriesAttempted, ceilingRetries, retriesMaxWaitMs, retriesWaitMs int) time.Duration {
	if retriesAttempted > ceilingRetries {
		return time.Duration(retriesMaxWaitMs) * time.Millisecond
	}
	b := rand.Float64()
	ri := int64(1 << uint64(retriesAttempted))
	delayMs := b * float64(ri) * float64(retriesWaitMs)
	return time.Duration(delayMs) * time.Millisecond
}

func isSuccess(statusCode int) bool {
	return statusCode >= 200 && statusCode <= 299
}

func isRetriable(statusCode int) bool {
	return statusCode == 408 || statusCode == 429 ||
		statusCode == 500 || statusCode == 502 || statusCode == 503 || statusCode == 504
}
