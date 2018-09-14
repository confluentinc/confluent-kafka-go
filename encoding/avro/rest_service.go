package avro

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
	"github.com/confluentinc/confluent-kafka-go/kafka"
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

type restService struct {
	url     *url.URL
	headers http.Header
	*http.Client
}

/* Instantiates a new restService client for contacting the Remote Confluent Schema Registry */
func newRestService(conf kafka.ConfigMap) (*restService, error) {
	urlConf, err := conf.Extract("url", "")

	if err != nil {
		return nil, err
	}

	u, err := url.Parse(urlConf.(string))

	if err != nil {
		return nil, err
	}

	headers , err := configureAuth(u, conf)
	if err != nil {
		return nil, err
	}

	headers.Add("Content-Type", "application/vnd.schemaregistry.v1+json")
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	transport, err := configureTransport(conf)
	if err != nil {
		return nil, err
	}

	timeout, err := conf.Extract("request.timeout.ms", 10000)

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

func configureTLS(conf kafka.ConfigMap, tlsConf *tls.Config) (error) {
	certFile, err := conf.Extract("ssl.certificate.location", "")
	if err != nil {
		return err
	}
	keyFile, err  := conf.Extract("ssl.key.location", "")
	if err != nil {
		return err
	}
	caFile, err := conf.Extract("ssl.ca.location", "")
	if err != nil {
		return err
	}
	unsafe, err := conf.Extract("ssl.disable.endpoint.verification", false)
	if err != nil{
		return err
	}

	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile.(string), keyFile.(string))
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
		caCert, err := ioutil.ReadFile(caFile.(string))
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

/* Configure transport with properties provided in kafka.kafka.kafka.ConfigMAp */
func configureTransport(conf kafka.ConfigMap) (*http.Transport, error) {
	/*
	 * Exposed for testing purposes only. In production properly formed certificates should be used
	 * https://tools.ietf.org/html/rfc2818#section-3
	 */
	tlsConfig := &tls.Config{}
	if err := configureTLS(conf, tlsConfig); err != nil {
		return nil, err
	}

	timeout, err := conf.Extract("connection.timout.ms", 10000)
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

/* configureURLAuth copies the url userinfo into a basic http auth authorization header */
func configureURLAuth(service *url.URL, header http.Header) (error) {
	header.Add("Authorization",  fmt.Sprintf("Basic %s", encodeBasicAuth(service.User.String())))
	return nil
}

/* configureSASLAuth copies the sasl username and password into a http basic authorization header */
func configureSASLAuth(conf kafka.ConfigMap, header http.Header) (error) {
	mech, err := conf.Extract("sasl.mechanism", "GSSAPI")

	if err != nil || strings.ToUpper(mech.(string)) == "GSSAPI" {
		return fmt.Errorf("SASL_INHERIT support PLAIN and SCRAM SASL mechanisms only")
	}

	user, err := conf.Extract("sasl.username", "")
	if err != nil {
		return err
	}

	pass, err := conf.Extract("sasl.password", "")
	if err != nil {
		return err
	}

	if user.(string) == "" || pass.(string) == "" {
		return fmt.Errorf("SASL_INHERIT requires both sasl.username and sasl.password be set")
	}

	header.Add("Authorization",  fmt.Sprintf("Basic %s", encodeBasicAuth(fmt.Sprintf("%s:%s", user.(string), pass.(string)))))
	return nil
}

/* configureUSERINFOAuth copies basic.auth.user.info */
func configureUSERINFOAuth(conf kafka.ConfigMap, header http.Header) (error) {
	auth, err := conf.Extract("basic.auth.user.info", "")

	if err != nil {
		return err
	}

	if auth.(string) == "" {
		return fmt.Errorf("USER_INFO source configured without basic.auth.user.info ")
	}

	header.Add("Authorization",  fmt.Sprintf("Basic %s", encodeBasicAuth(auth.(string))))
	return nil

}

/* configureAuth returns a base64 encoded userinfo string identified on the configured credentials source */
func configureAuth(service *url.URL, conf kafka.ConfigMap) (http.Header, error) {
	// Remove userinfo from url regardless of source to avoid confusion/conflicts
	defer func() {
		service.User = nil
	}()

	source, err := conf.Extract("basic.auth.credentials.source", "URL")
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

/* handleRequest ends a HTTP(S) request to the Schema Registry, placing results into the response object */
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

	var failure httpError
	if err := json.NewDecoder(resp.Body).Decode(&failure); err != nil {
		return err
	}

	return fmt.Errorf("request failed: Code: %d, Reason: %s", failure.Code, failure.Message)
}
