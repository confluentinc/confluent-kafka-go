package schemaregistry

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
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
	version           = subjects + "/versions"
	versionNormalize  = subjects + "/versions?normalize=%t"
	versions          = version + "/%v"
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
func newRestService(conf *ConfigMap) (*restService, error) {
	urlConf, err := conf.Get("schema.registry.url", "")

	if err != nil {
		return nil, err
	}

	u, err := url.Parse(urlConf.(string))

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

	timeout, err := conf.Get("request.timeout.ms", 10000)

	return &restService{
		url:     u,
		headers: headers,
		Client: &http.Client{
			Transport: transport,
			Timeout:   time.Duration(timeout.(int)) * time.Millisecond,
		},
	}, nil
}

// encodeBasicAuth adds a basic http authentication header to the provided header
func encodeBasicAuth(userinfo string) string {
	return base64.StdEncoding.EncodeToString([]byte(userinfo))
}

// configureTLS populates tlsConf
func configureTLS(conf *ConfigMap, tlsConf *tls.Config) error {
	certFile, err := conf.Get("ssl.certificate.location", "")
	if err != nil {
		return err
	}
	keyFile, err := conf.Get("ssl.key.location", "")
	if err != nil {
		return err
	}
	caFile, err := conf.Get("ssl.ca.location", "")
	if err != nil {
		return err
	}
	unsafe, err := conf.Get("ssl.disable.endpoint.verification", false)
	if err != nil {
		return err
	}

	if certFile != "" {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(certFile.(string), keyFile.(string))
		if err != nil {
			return err
		}
		tlsConf.Certificates = []tls.Certificate{cert}
	}

	if caFile != "" {
		if unsafe.(bool) {
			log.Println("WARN: endpoint verification is currently disabled. " +
				"This feature should be configured for development purposes only")
		}
		var caCert []byte
		caCert, err = ioutil.ReadFile(caFile.(string))
		if err != nil {
			return err
		}
		tlsConf.RootCAs.AppendCertsFromPEM(caCert)
		if err != nil {
			return err
		}
	}

	tlsConf.BuildNameToCertificate()

	return err
}

// configureTransport returns a new Transport for use by the Confluent Schema Registry REST client
func configureTransport(conf *ConfigMap) (*http.Transport, error) {

	// Exposed for testing purposes only. In production properly formed certificates should be used
	// https://tools.ietf.org/html/rfc2818#section-3
	tlsConfig := &tls.Config{}
	if err := configureTLS(conf, tlsConfig); err != nil {
		return nil, err
	}

	timeout, err := conf.Get("connection.timout.ms", 10000)
	if err != nil {
		return nil, err
	}

	return &http.Transport{
		Dial: (&net.Dialer{
			Timeout: time.Duration(timeout.(int)) *
				time.Millisecond,
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
func configureSASLAuth(conf *ConfigMap, header http.Header) error {
	mech, err := conf.Get("sasl.mechanism", "GSSAPI")

	if err != nil || strings.ToUpper(mech.(string)) == "GSSAPI" {
		return fmt.Errorf("SASL_INHERIT support PLAIN and SCRAM SASL mechanisms only")
	}

	user, err := conf.Get("sasl.username", "")
	if err != nil {
		return err
	}

	pass, err := conf.Get("sasl.password", "")
	if err != nil {
		return err
	}

	if user.(string) == "" || pass.(string) == "" {
		return fmt.Errorf("SASL_INHERIT requires both sasl.username and sasl.password be set")
	}

	header.Add("Authorization", fmt.Sprintf("Basic %s", encodeBasicAuth(fmt.Sprintf("%s:%s", user.(string), pass.(string)))))
	return nil
}

// configureUSERINFOAuth copies basic.auth.user.info
func configureUSERINFOAuth(conf *ConfigMap, header http.Header) error {
	auth, err := conf.Get("basic.auth.user.info", "")

	if err != nil {
		return err
	}

	if auth.(string) == "" {
		return fmt.Errorf("USER_INFO source configured without basic.auth.user.info ")
	}

	header.Add("Authorization", fmt.Sprintf("Basic %s", encodeBasicAuth(auth.(string))))
	return nil

}

// newAuthHeader returns a base64 encoded userinfo string identified on the configured credentials source
func newAuthHeader(service *url.URL, conf *ConfigMap) (http.Header, error) {
	// Remove userinfo from url regardless of source to avoid confusion/conflicts
	defer func() {
		service.User = nil
	}()

	source, err := conf.Get("basic.auth.credentials.source", "URL")
	if err != nil {
		return nil, err
	}

	header := http.Header{}

	switch strings.ToUpper(source.(string)) {
	case "URL":
		err = configureURLAuth(service, header)
	case "SASL_INHERIT":
		err = configureSASLAuth(conf, header)
	case "USER_INFO":
		err = configureUSERINFOAuth(conf, header)
	default:
		err = fmt.Errorf("unrecognized value for basic.auth.credentials.source %s", source.(string))
	}
	return header, err
}

// handleRequest sends a HTTP(S) request to the Schema Registry, placing results into the response object
func (rs *restService) handleRequest(request *api, response interface{}) error {
	endpoint, err := rs.url.Parse(fmt.Sprintf(base+request.endpoint, request.arguments...))
	if err != nil {
		return err
	}

	outbuf, err := json.Marshal(request.body)
	if err != nil {
		return err
	}

	req := &http.Request{
		Method: request.method,
		URL:    endpoint,
		Body:   ioutil.NopCloser(bytes.NewBuffer(outbuf)),
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
