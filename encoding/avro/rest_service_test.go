package avro

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"testing"
	"net/http"
	"encoding/json"
	"sync"
	"sync/atomic"
	"net"
)

type mockListener struct {
	port string
	mux *http.ServeMux
	listener net.Listener
}

var authTests = []struct {
	input    kafka.ConfigMap
	expected string
}{
	{kafka.ConfigMap{
		"basic.auth.credentials.source": "url",
		"url": "https://user_url:password_url@schema_registry1:8083"},
		"user_url:password_url"},
	{kafka.ConfigMap{
		"basic.auth.credentials.source": "user_info",
		"url": "https://user_url:password_url@schema_registry1:8083",
		"basic.auth.user.info": "user_user_info:password_user_info"},
		"user_user_info:password_user_info"},
	{kafka.ConfigMap{
		"basic.auth.credentials.source": "sasl_inherit",
		"url":            "https://user_url:password_url@schema_registry1:8083",
		"sasl.username":  "user_sasl_inherit",
		"sasl.password":  "password_sasl_inherit",
		"sasl.mechanism": "PLAIN"},
		"user_sasl_inherit:password_sasl_inherit"},
}

func serveSubjects(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(struct {
		ID int64 `json:"id"`
	}{1})
}

func serveSchemas(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(struct {
		Schema string `json:"schema"`
	}{`"{\"type\": \"string\"}"`})
}

func verifiableHandler(counter *int32, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request){
		atomic.AddInt32(counter, 1)
		handler(w, r)
	}
}

func newMockRegistry(port string, t *testing.T) (*mockListener, error) {
	maybeFail = initFailFunc(t)

	l, err := net.Listen("tcp", port)
	if err != nil {
		return nil, err
	}
	m := &mockListener{
		port: port,
		mux: http.NewServeMux(),
		listener: l,
	}

	return m, nil
}

func (m *mockListener) Start() {
	go func(){http.Serve(m.listener,m.mux)}()
}

func (m *mockListener) AddEndpoint(endpoint string, handler http.HandlerFunc) {
	m.mux.HandleFunc(endpoint, handler)
}

func (m *mockListener) Stop() {
	m.listener.Close()
}

// Ensure request fencing prevents redundant in-flight requests
func TestClient_RequestFencing(t *testing.T) {
	maybeFail = initFailFunc(t)

	var subjectReponses, schemaResponses int32

	sr, err := newMockRegistry(":5218", t)
	maybeFail("start mock service", err)

	// request fencing should ensure only the first request is tendered by the SR
	sr.AddEndpoint("/schemas/", verifiableHandler(&schemaResponses, serveSchemas))
	sr.AddEndpoint("/subjects/", verifiableHandler(&subjectReponses, serveSubjects))
	sr.Start()
	defer sr.Stop()

	schema, _ := ParseFile(schemaTests[0].source)

	client, _ := NewCachedSchemaRegistryClient(kafka.ConfigMap{
		"url": "http://localhost:5218",
	})

	// Coerce errors
	deadClient, _ := NewCachedSchemaRegistryClient(kafka.ConfigMap{
		"url": "http://localhost:5219",
	})

	wg := sync.WaitGroup{}
	for i := 0; i <= 1000; i++ {
		wg.Add(1)
		go func(i int) {
			deadClient.Register("registrant", schema)
			client.GetByID(1)
			client.Register("registrant", schema)
			deadClient.GetByID(1)
			wg.Done()
		}(i)
	}
	wg.Wait()

	maybeFail("", expect(schemaResponses, int32(1)), expect(subjectReponses, int32(1)))
}

func init() {
	if !testconfRead() {
		log.Fatal("FATAL: Missing testconf.json")
	}
}