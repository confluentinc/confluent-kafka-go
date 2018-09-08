package kafka

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
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
	base          = ".."
	schemas       = "/schemas/ids/%d"
	subject       = "/subjects"
	subjects      = subject + "/%s"
	version       = subjects + "/versions"
	versions      = version + "/%v"
	compatibility = "/compatibility" + versions
	config        = "/config"
	override      = config + "/%s"
)

/* REST API request */
type api struct {
	method    string
	endpoint  string
	arguments []interface{}
	body      interface{}
}

/* Constructs new API request object */
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
*		- 40402 - Version not found
*		- 40403 - Schema not found
*	422: Invalid {resource}
*		- 42201 - Invalid Schema
*		- 42202 - Invalid Version
*	500: Internal Server Error (something broke between SR and Kafka)
*		- 50001 - Error in backend(kafka)
*		- 50002 - Operation timed out
*		- 50003 - Error forwarding request to SR leader
 */

/* Schema Registry HTTP Error response */
type httpError struct {
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

type RestService struct {
	url     *url.URL
	headers http.Header
	*http.Client
}

/* Instantiates a new RestService client for contacting the Remote Confluent Schema Registry */
func NewRestService(conf ConfigMap) (restService *RestService, err error) {
	var url *url.URL
	if url, err = url.Parse(conf.GetString("url", "")); err != nil {
		log.Printf("Failed to parse schema registry url %s\n", err)
		return nil, err
	}

	headers := http.Header{
		"Content-Type": []string{"application/vnd.schemaregistry.v1+json"},
	}

	_, err = configureAuth(url, conf)
	if err != nil {
		return nil, err
	}

	//if auth != "" {
	//	headers.Add("Authorization", "Basic "+auth)
	//}

	if err != nil {
		return nil, err
	}

	transport, err := configureTransport(conf)
	if err != nil {
		return nil, err
	}

	return &RestService{
		url:     url,
		headers: headers,
		Client: &http.Client{
			Transport: transport,
			Timeout:   time.Duration(conf.GetInt("request.timeout.ms", 30000)) * time.Millisecond,
		},
	}, nil
}

// encodeBasicAuth adds a basic http authentication header to the provided header
func encodeBasicAuth(userinfo string) string {
	return base64.StdEncoding.EncodeToString([]byte(userinfo))
}

/* Configure transport with properties provided in kafka.ConfigMap */
func configureTransport(conf ConfigMap) (*http.Transport, error) {
	certFile := conf.GetString("ssl.certificate.location", "")
	keyFile := conf.GetString("ssl.key.location", "")
	caFile := conf.GetString("ssl.ca.location", "")
	/*
	 * Exposed for testing purposes only. In production properly formed certificates should be used
	 * https://tools.ietf.org/html/rfc2818#section-3
	 */
	unsafe := conf.GetBool("ssl.disable.endpoint.verification", false)

	tlsConfig := &tls.Config{InsecureSkipVerify: unsafe}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if caFile != "" {
		if unsafe {
			log.Println("WARN: endpoint verification is currently disabled. " +
				"This feature should be configured for development purposes only")
		}
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}

		caCertPool.AppendCertsFromPEM(caCert)
	}

	tlsConfig.BuildNameToCertificate()

	return &http.Transport{
		Dial: (&net.Dialer{
			Timeout: time.Duration(conf.GetInt("connection.timeout.ms", 10000)) *
				time.Millisecond,
		}).Dial,
		TLSClientConfig: tlsConfig,
	}, nil
}

/* configureAuth returns a base64 encoded userinfo string identified on the configured credentials source */
func configureAuth(service *url.URL, conf ConfigMap) (auth string, err error) {
	// Remove userinfo from url regardless of source to avoid confusion/conflicts
	defer func() {
		service.User = nil
	}()

	switch source := strings.ToUpper(conf.GetString("basic.auth.credentials.source", "url")); source {
	case "URL":
		return encodeBasicAuth(service.User.String()), nil
	case "SASL_INHERIT":
		if strings.ToUpper(conf.GetString("sasl.mechanism", "GSSAPI")) == "GSSAPI" {
			return "", fmt.Errorf("SASL_INHERIT support PLAIN and SCRAM SASL mechanisms only")
		}

		user := conf.GetString("sasl.username", "")
		pass := conf.GetString("sasl.password", "")

		if user != "" && pass != "" {
			return encodeBasicAuth(user + ":" + pass), nil
		}

		err = fmt.Errorf("SASL_INHERIT requires both sasl.username and sasl.password be set")
	case "USER_INFO":
		if auth := conf.GetString("basic.auth.user.info", ""); auth != "" {
			return encodeBasicAuth(auth), nil
		}
		err = fmt.Errorf("USER_INFO source configured without basic.auth.user.info ")
	default:
		err = fmt.Errorf("unsupported credentials source %s", source)
	}
	return "", err
}

/* handleRequest ends a HTTP(S) request to the Schema Registry, placing results into the response object */
func (rs *RestService) handleRequest(request *api, response interface{}) error {
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

	var failure httpError
	if err := json.NewDecoder(resp.Body).Decode(&failure); err != nil {
		return err
	}

	return fmt.Errorf("request failed: Code: %d, Reason: %s", failure.Code, failure.Message)
}
