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

package schemaregistry

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
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// Relative Confluent Schema Registry REST API endpoints as described in the Confluent documentation
// https://docs.confluent.io/current/schema-registry/docs/api.html
const (
	base              = ".."
	schemas           = "/schemas/ids/%d"
	schemasBySubject  = "/schemas/ids/%d?subject=%s"
	subject           = "/subjects"
	subjects          = subject + "/%s"
	subjectsNormalize = subject + "/%s?normalize=%t"
	subjectsDelete    = subjects + "?permanent=%t"
	version           = subjects + "/versions"
	versionNormalize  = subjects + "/versions?normalize=%t"
	versions          = version + "/%v"
	versionsDelete    = versions + "?permanent=%t"
	compatibility     = "/compatibility" + versions
	config            = "/config"
	subjectConfig     = config + "/%s"
	mode              = "/mode"
	modeConfig        = mode + "/%s"
)

// REST API request
type api struct {
	method    string
	endpoint  string
	arguments []interface{}
	body      interface{}
}

// newRequest returns new Confluent Schema Registry API request */
func newRequest(method string, endpoint string, body interface{}, arguments ...interface{}) *api {
	return &api{
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

// RestError represents a Schema Registry HTTP Error response
type RestError struct {
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

// Error implements the errors.Error interface
func (err *RestError) Error() string {
	return fmt.Sprintf("schema registry request failed error code: %d: %s", err.Code, err.Message)
}

type restService struct {
	url     *url.URL
	headers http.Header
	*http.Client
}

// newRestService returns a new REST client for the Confluent Schema Registry
func newRestService(conf *Config) (*restService, error) {
	urlConf := conf.SchemaRegistryURL
	u, err := url.Parse(urlConf)

	if err != nil {
		return nil, err
	}

	headers, err := newAuthHeader(u, conf)
	if err != nil {
		return nil, err
	}

	headers.Add("Content-Type", "application/vnd.schemaregistry.v1+json")
	if err != nil {
		return nil, err
	}

	transport, err := configureTransport(conf)
	if err != nil {
		return nil, err
	}

	timeout := conf.RequestTimeoutMs

	return &restService{
		url:     u,
		headers: headers,
		Client: &http.Client{
			Transport: transport,
			Timeout:   time.Duration(timeout) * time.Millisecond,
		},
	}, nil
}

// encodeBasicAuth adds a basic http authentication header to the provided header
func encodeBasicAuth(userinfo string) string {
	return base64.StdEncoding.EncodeToString([]byte(userinfo))
}

// configureTLS populates tlsConf
func configureTLS(conf *Config, tlsConf *tls.Config) error {
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
func configureTransport(conf *Config) (*http.Transport, error) {

	// Exposed for testing purposes only. In production properly formed certificates should be used
	// https://tools.ietf.org/html/rfc2818#section-3
	tlsConfig := &tls.Config{}
	if err := configureTLS(conf, tlsConfig); err != nil {
		return nil, err
	}

	timeout := conf.ConnectionTimeoutMs

	return &http.Transport{
		Dial: (&net.Dialer{
			Timeout: time.Duration(timeout) * time.Millisecond,
		}).Dial,
		TLSClientConfig: tlsConfig,
	}, nil
}

// configureURLAuth copies the url userinfo into a basic HTTP auth authorization header
func configureURLAuth(service *url.URL, header http.Header) error {
	header.Add("Authorization", fmt.Sprintf("Basic %s", encodeBasicAuth(service.User.String())))
	return nil
}

// configureSASLAuth copies the sasl username and password into a HTTP basic authorization header
func configureSASLAuth(conf *Config, header http.Header) error {
	mech := conf.SaslMechanism
	if strings.ToUpper(mech) == "GSSAPI" {
		return fmt.Errorf("SASL_INHERIT support PLAIN and SCRAM SASL mechanisms only")
	}

	user := conf.SaslUsername
	pass := conf.SaslPassword
	if user == "" || pass == "" {
		return fmt.Errorf("SASL_INHERIT requires both sasl.username and sasl.password be set")
	}

	header.Add("Authorization", fmt.Sprintf("Basic %s", encodeBasicAuth(fmt.Sprintf("%s:%s", user, pass))))
	return nil
}

// configureUSERINFOAuth copies basic.auth.user.info
func configureUSERINFOAuth(conf *Config, header http.Header) error {
	auth := conf.BasicAuthUserInfo
	if auth == "" {
		return fmt.Errorf("USER_INFO source configured without basic.auth.user.info ")
	}

	header.Add("Authorization", fmt.Sprintf("Basic %s", encodeBasicAuth(auth)))
	return nil

}

// newAuthHeader returns a base64 encoded userinfo string identified on the configured credentials source
func newAuthHeader(service *url.URL, conf *Config) (http.Header, error) {
	// Remove userinfo from url regardless of source to avoid confusion/conflicts
	defer func() {
		service.User = nil
	}()

	source := conf.BasicAuthCredentialsSource

	header := http.Header{}

	var err error
	switch strings.ToUpper(source) {
	case "URL":
		err = configureURLAuth(service, header)
	case "SASL_INHERIT":
		err = configureSASLAuth(conf, header)
	case "USER_INFO":
		err = configureUSERINFOAuth(conf, header)
	default:
		err = fmt.Errorf("unrecognized value for basic.auth.credentials.source %s", source)
	}
	return header, err
}

// handleRequest sends a HTTP(S) request to the Schema Registry, placing results into the response object
func (rs *restService) handleRequest(request *api, response interface{}) error {
	urlPath := path.Join(rs.url.Path, fmt.Sprintf(request.endpoint, request.arguments...))
	endpoint, err := rs.url.Parse(urlPath)
	if err != nil {
		return err
	}

	var readCloser io.ReadCloser
	if request.body != nil {
		outbuf, err := json.Marshal(request.body)
		if err != nil {
			return err
		}
		readCloser = ioutil.NopCloser(bytes.NewBuffer(outbuf))
	}

	req := &http.Request{
		Method: request.method,
		URL:    endpoint,
		Body:   readCloser,
		Header: rs.headers,
	}

	resp, err := rs.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		if err = json.NewDecoder(resp.Body).Decode(response); err != nil {
			return err
		}
		return nil
	}

	var failure RestError
	if err := json.NewDecoder(resp.Body).Decode(&failure); err != nil {
		return err
	}

	return &failure
}
