package serdes

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
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
func newAPI(method string, endpoint string, body interface{}, arguments ...interface{}) *api {
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
	url *url.URL
	*http.Client
}

/* Instantiates a new RestService client for contacting the Remote Confluent Schema Registry */
func NewRestService(conf *kafka.ConfigMap) (restService *RestService, err error) {
	var baseURL *url.URL
	if baseURL, err = url.Parse(conf.GetString("schema.registry.url", "not configured")); err != nil {
		log.Printf("Failed to parse schema.registry.url %s\n", err)
		return nil, err
	}

	configureBasicAuth(conf)
	transport, err := configureTransport(conf)
	if err != nil {
		return nil, err
	}

	return &RestService{
		url: baseURL,
		Client: &http.Client{
			Transport: transport,
			Timeout:   time.Duration(conf.GetInt("request.timeout.ms", 30000)) * time.Millisecond,
		},
	}, nil
}

/* Configure transport with properties provided in kafka.ConfigMap */
func configureTransport(conf *kafka.ConfigMap) (*http.Transport, error) {
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
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
	}

	tlsConfig.BuildNameToCertificate()

	return &http.Transport{
		Dial: (&net.Dialer{
			Timeout: time.Duration(conf.GetInt("schema.registry.connection.timeout.ms", 10000)) *
				time.Millisecond,
		}).Dial,
		TLSClientConfig: tlsConfig,
	}, nil
}

/* Configure basic http auth using the configured source */
func configureBasicAuth(conf *kafka.ConfigMap) (userinfo *url.Userinfo, err error) {
	switch source := conf.GetString("basic.auth.credentials.source", "url"); source {
	case "url":
		u, err := url.Parse(conf.GetString("schema.registry.url", "none"))
		if err != nil {
			return nil, err
		}
		userinfo = u.User
	case "sasl_inherit":
		userinfo = url.UserPassword(
			conf.GetString("sasl.username", "none"),
			conf.GetString("sasl.password", "none"))
	case "userinfo":
		userinfo = url.User(conf.GetString("basic.auth.user.info", "none"))
	default:
		return nil, fmt.Errorf("unkown credentials source %s", source)
	}
	return userinfo, nil
}

/* Send HTTP(S) request to Schema Registry, unmarshal results into response object */
func (rs *RestService) handleRequest(request *api, response interface{}) error {
	u, err := rs.url.Parse(fmt.Sprintf(base+request.endpoint, request.arguments...))
	if err != nil {
		return err
	}

	outbuf, err := json.Marshal(request.body)
	if err != nil {
		return err
	}

	r := &http.Request{
		Method: request.method,
		URL:    u,
		Body:   ioutil.NopCloser(bytes.NewBuffer(outbuf)),
	}

	resp, err := rs.Do(r)
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
