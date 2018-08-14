package kafka

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

/* Schema Registry API endpoints
*
* ====Schemas====
* Fetch string: schema(escaped) identified by the input id.
* -GET /schemas/ids/{int: id} returns: JSON blob: schema; raises: 404[03], 500[01]
*
* ====Subjects====
* Fetch JSON array str:subject of all registered subjects
* -GET /subjects returns: JSON array string: subjects; raises: 500[01]
* Fetch JSON array int:versions
* GET /subjects/{string: subject}/versions returns: JSON array of int: versions; raises: 404[01], 500[01]
*
* GET /subjects/{string: subject}/versions/{int|string('latest'): version} returns: JSON blob *schemaMetadata*; raises: 404[01, 02], 422[02], 500[01]
* GET /subjects/{string: subject}/versions/{int|string('latest'): version}/schema returns : JSON blob: schema(unescaped); raises: 404, 422, 500[01, 02, 03]
*
* Delete subject and it's associated subject configuration override
* -DELETE /subjects/{string: subject}) returns: JSON array int: version; raises: 404[01], 500[01]
* Delete subject version
* -DELETE /subjects/{string: subject}/versions/{int|str('latest'): version} returns int: deleted version id; raises: 404[01, 02]
*
* Register new schema under subject
* -POST /subjects/{string: subject}/versions returns JSON blob ; raises: 409, 422[01], 500[01, 02, 03]
* Return SchemaMetadata for the subject version (if any) associated with the schema in the request body
* -POST /subjects/{string: subject} returns JSON *schemaMetadata*; raises: 404[01, 03]
*
* ====Comparability====
* Test schema (http body) against configured comparability for subject version
* -POST /compatibility/subjects/{string: subject}/versions/{int:string('latest'): version} returns: JSON bool:is_compatible; raises: 404[01,02], 422[01,02], 500[01]
*
* ====Config====
* Returns global configuration
* -GET /config  returns: JSON string:comparability; raises: 500[01]
* Update global SR config
* -PUT /config returns: JSON string:compatibility; raises: 422[03], 500[01, 03]
* Update subject level override
* -PUT /config/{string: subject} returns: JSON string:compatibility; raises: 422[03], 500[01,03]
* Returns compatibility level of subject
* GET /config/(string: subject) returns: JSON string:compatibility; raises: 404, 500[01]

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

const (
	BASE          = ".."
	SCHEMAS       = "/schemas/ids/%d"
	SUBJECT       = "/subjects"
	SUBJECTS      = SUBJECT + "/%s"
	VERSION       = SUBJECTS + "/versions"
	VERSIONS      = VERSION + "/%v"
	COMPATIBILITY = "/compatibility" + VERSIONS
	CONFIG        = "/config"
	OVERRIDE      = CONFIG + "/%s"
)

/* Compatibility ENUM */
type Compatibility int

const (
	NONE = iota
	BACKWARD
	FORWARD
	FULL
)

var compatibilityENUM = []string{
	"none",
	"backward",
	"forward",
	"full",
}

/* ==== API types ==== */
type httpError struct {
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

type version int
type versions []int
type subjects []string

/* GET uses compatibilityLevel, POST uses compatibility */
type config struct {
	CompatibilityUpdate string `json:"compatibility,omitempty"`
	Compatibility       string `json:"compatibilityLevel,omitempty"`
}

type compatibilityConfig struct {
	Compatible bool `json:"is_compatible,omitempty"`
}

type SchemaMetadata struct {
	Id      int    `json:"id,omitempty"`
	Version int    `json:"version,omitempty"`
	Schema  string `json:"schema,omitempty"`
}

/* ==== HTTP Client and helper function ==== */
type api struct {
	method    string
	endpoint  string
	arguments []interface{}
	body      interface{}
}

func newAPIRequest(method string, endpoint string, body interface{}, arguments ...interface{}) *api {
	return &api{
		method:    method,
		endpoint:  endpoint,
		arguments: arguments,
		body:      body,
	}
}

func configureTransport(conf *ConfigMap) (*http.Transport, error) {
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

/* https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClient.java */
type SchemaRegistryClient interface {
	Register(subject string, schema string) (id int, err error)
	GetById(id int) (schema string, err error)
	GetBySubjectAndId(subject string, id int) (schema string, err error)
	GetLatestSchemaMetadata(subject string) (SchemaMetadata, error)
	GetSchemaMetadata(subject string, version int) (SchemaMetadata, error)
	GetVersion(subject string, schema string) (version int, err error)
	GetAllVersions(subject string) (versions, error)
	TestCompatibility(subject string, version int, schema string) (compatible bool, err error)
	UpdateCompatibility(subject string, update Compatibility) (compatibility string, err error)
	GetCompatibility(subject string) (compatibility string, err error)
	GetAllSubjects() (subjects, error)
	GetId(subject string, schema string) (id int, err error)
	DeleteSubject(subject string) (versions, error)
	DeleteSchemaVersion(subject string, version int) (deletes version, err error)
	GetDefaultCompatibility() (compatibility string, err error)
	UpdateDefaultCompatibility(update Compatibility) (compatibility string, err error)
}

type client struct {
	sync.Mutex
	url            *url.URL
	httpClient     *http.Client
	schemeById     *lru.Cache
	idByScheme     *lru.Cache
	indexBySubject map[string]*lru.Cache
}

/* Lock already held by the logic adding a new element to the LRU */
func (h *client) onEvict(key lru.Key, value interface{}) {
		switch key.(type){
		case string:
			h.idByScheme.Remove(key.(string))
			h.schemeById.Remove(value.(int))
		case int:
			h.idByScheme.Remove(value.(string))
			h.schemeById.Remove(key.(int))
		}
}

func NewCachedSchemaRegistryClient(conf *ConfigMap) (CachedSchemaRegistryClient SchemaRegistryClient, err error) {
	var baseURL *url.URL
	if baseURL, err = url.Parse(conf.GetString("schema.registry.url", "http://localhost:8082")); err != nil {
		log.Printf("Failed to parse schema.registry.url %s\n", err)
		return nil, err
	}

	transport, err := configureTransport(conf)

	if err != nil {
		return nil, err
	}

	schemaCache := lru.New(conf.GetInt("schema.registry.max.cached.schemas", 1000))
	idCache := lru.New(conf.GetInt("schema.registry.max.cached.schemas", 1000))

	handle := &client{
		url: baseURL,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   time.Duration(conf.GetInt("request.timeout.ms", 30000)) * time.Millisecond,
		},
		idByScheme:     schemaCache,
		schemeById:     idCache,
		indexBySubject: make(map[string]*lru.Cache),
	}

	schemaCache.OnEvicted = handle.onEvict
	idCache.OnEvicted = handle.onEvict

	return handle, nil
}

func (h *client) handleRequest(request *api, response interface{}) error {
	u, err := h.url.Parse(fmt.Sprintf(BASE+request.endpoint, request.arguments...))
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

	resp, err := h.httpClient.Do(r)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode < 400 {
		json.NewDecoder(resp.Body).Decode(response)
		return nil
	}

	var failure httpError
	json.NewDecoder(resp.Body).Decode(&failure)

	return errors.New(fmt.Sprintf("Request failed: Code: %d, Reason: %s", failure.Code, failure.Message))
}

func (h *client) Register(subject string, schema string) (id int, err error) {
	h.Lock()
	if cache, ok := h.indexBySubject[subject]; ok {
		if id, ok := cache.Get(schema); ok {
			h.Unlock()
			return id.(int), nil
		}
	} else {
		h.indexBySubject[subject] = h.schemeById
	}
	h.Unlock()

	result := &SchemaMetadata{
		Schema: schema,
	}
	err = h.handleRequest(newAPIRequest("POST", VERSION, &result, subject), &result)

	h.Lock()
	h.indexBySubject[subject].Add(schema, result.Id)
	h.Unlock()

	return result.Id, err
}

func (h *client) GetById(id int) (schema string, err error) {
	h.Lock()
	if schema, ok := h.idByScheme.Get(id); ok {
		h.Unlock()
		return schema.(string), err
	}
	h.Unlock()

	var result SchemaMetadata
	err = h.handleRequest(newAPIRequest("GET", SCHEMAS, nil, id), &result)

	h.Lock()
	h.idByScheme.Add(result.Id, result.Schema)
	h.Unlock()

	return result.Schema, err
}

func (h *client) GetAllVersions(subject string) (results versions, err error) {
	var result versions
	err = h.handleRequest(newAPIRequest("GET", VERSION, nil, subject), &result)

	return result, err
}

func (h *client) GetBySubjectAndId(subject string, id int) (schema string, err error) {
	var result SchemaMetadata
	err = h.handleRequest(newAPIRequest("GET", VERSION, nil, subject, id), &result)

	return result.Schema, err
}

func (h *client) GetLatestSchemaMetadata(subject string) (schemaMetadata SchemaMetadata, err error) {
	var result SchemaMetadata
	err = h.handleRequest(newAPIRequest("GET", VERSIONS, nil, subject, "latest"), &result)

	return result, err
}

func (h *client) GetSchemaMetadata(subject string, id int) (schemaMetadata SchemaMetadata, err error) {
	var result SchemaMetadata
	err = h.handleRequest(newAPIRequest("GET", VERSIONS, nil, subject, id), &result)

	return result, err
}

func (h *client) GetVersion(subject string, schema string) (id int, err error) {
	result := &SchemaMetadata{
		Schema: schema,
	}
	err = h.handleRequest(newAPIRequest("POST", SUBJECTS, &result, subject), &result)

	return result.Version, err
}

func (h *client) TestCompatibility(subject string, version int, schema string) (ok bool, err error) {
	var result compatibilityConfig
	candidate := &SchemaMetadata{
		Schema: schema,
	}

	err = h.handleRequest(newAPIRequest("POST", COMPATIBILITY, &candidate, subject, version), &result)

	return result.Compatible, err
}

func (h *client) UpdateCompatibility(subject string, update Compatibility) (compatibility string, err error) {
	result := &config{
		CompatibilityUpdate: compatibilityENUM[update],
	}
	err = h.handleRequest(newAPIRequest("PUT", OVERRIDE, &result, subject), &result)

	return result.CompatibilityUpdate, err
}

func (h *client) GetCompatibility(subject string) (compatibility string, err error) {
	var result config
	err = h.handleRequest(newAPIRequest("GET", OVERRIDE, nil, subject), &result)

	return result.Compatibility, err
}

func (h *client) GetAllSubjects() (subjects, error) {
	var result subjects
	err := h.handleRequest(newAPIRequest("GET", SUBJECT, nil), &result)

	return result, err
}

func (h *client) GetId(subject string, schema string) (id int, err error) {
	h.Lock()
	if cache, ok := h.indexBySubject[subject]; ok {
		if id, ok := cache.Get(schema); ok {
			h.Unlock()
			return id.(int), nil
		}
	} else {
		h.indexBySubject[subject] = h.schemeById
	}
	h.Unlock()

	result := &SchemaMetadata{
		Schema: schema,
	}
	err = h.handleRequest(newAPIRequest("POST", SUBJECTS, &result, subject), &result)

	h.Lock()
	h.indexBySubject[subject].Add(schema, result.Id)
	h.Unlock()

	return result.Id, err
}

func (h *client) DeleteSubject(subject string) (deleted versions, err error) {
	var result versions
	err = h.handleRequest(newAPIRequest("DELETE", SUBJECTS, nil, subject), &result)

	return result, err
}

func (h *client) DeleteSchemaVersion(subject string, delete int) (deleted version, err error) {
	var result version
	err = h.handleRequest(newAPIRequest("DELETE", VERSIONS, nil, subject, delete), &result)

	return result, err

}

func (h *client) UpdateDefaultCompatibility(update Compatibility) (compatibility string, err error) {
	result := &config{
		CompatibilityUpdate: compatibilityENUM[update],
	}
	err = h.handleRequest(newAPIRequest("PUT", CONFIG, &result), &result)

	return result.CompatibilityUpdate, err
}

func (h *client) GetDefaultCompatibility() (compatibility string, err error) {
	var result config
	err = h.handleRequest(newAPIRequest("GET", CONFIG, nil), &result)

	return result.Compatibility, err
}
