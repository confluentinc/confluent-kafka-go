package avro

import (
	"encoding/base64"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"net/url"
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

var schemaTests = []struct {
	source         string
	load           func(string) string
	testSerializer func(record Schema) interface{}
	verifySerializer func(...interface{})
}{
	{source: "./schemas/advanced.avsc",
		testSerializer: testAdvanced,
		//verifySerialization: ,
	},
	//{source: "./schemas/complex.avsc",
	//	handle: testComplex,
	//},
	//{source: "./schemas/union.avsc",
	//	handle: testGeneric3,
	//},
	{source: "./schemas/Null.avsc",
		testSerializer: testNull,
		verifySerializer: verifyPrimitive,
	},
	{source: "./schemas/Bool.avsc",
		testSerializer: testBool,
		verifySerializer: verifyPrimitive,
	},
	{source: "./schemas/Int.avsc",
		testSerializer: testInt,
		verifySerializer: verifyPrimitive,
	},
	{source: "./schemas/Long.avsc",
		testSerializer: testLong,
		verifySerializer: verifyPrimitive,

	},
	{source: "./schemas/Float.avsc",
		testSerializer: testFloat,
		verifySerializer: verifyPrimitive,
	},
	{source: "./schemas/Double.avsc",
		testSerializer: testDouble,
		verifySerializer: verifyPrimitive,
	},
	{source: "./schemas/String.avsc",
		testSerializer: testString,
		verifySerializer: verifyPrimitive,
	},
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

func verifiableHanlder(counter *int32, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request){
		atomic.AddInt32(counter, 1)
		handler(w, r)
	}
}

func NewMockRegistry(port string, t *testing.T) (*mockListener, error) {
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

func TestClient_BasicAuth(t *testing.T) {
	for _, test := range authTests {
		urlConf, _ := test.input.Extract("url", "")

		u, _ := url.Parse(urlConf.(string))

		ui, err := configureAuth(u, test.input)
		if err != nil {
			t.Fatal(err)
		}
		if ui["Authorization"][0] != fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(test.expected))) {
			actual, _ := base64.StdEncoding.DecodeString(ui["Authorization"][0])
			t.Logf("FAILED: Got %s expected %s", actual, test.expected)
			t.Fail()
		}
	}
}

// Ensure request fencing prevents redundant in-flight requests
func TestClient_RequestFencing(t *testing.T) {
	maybeFail = initFailFunc(t)

	var subjectReponses, schemaResponses int32

	sr, err := NewMockRegistry(":5218", t)
	maybeFail("start mock service", err)

	// request fencing should ensure only the first request is tendered by the SR
	sr.AddEndpoint("/schemas/", verifiableHanlder(&schemaResponses, serveSchemas))
	sr.AddEndpoint("/subjects/", verifiableHanlder(&subjectReponses, serveSubjects))
	sr.Start()
	defer sr.Stop()

	schema, _ := Load(schemaTests[0].source)
	schemaString := schema.String()

	client, _ := NewCachedSchemaRegistryClient(kafka.ConfigMap{
		"url": "http://localhost:5218",
	})

	// Coerce errors
	deadClient, _ := NewCachedSchemaRegistryClient(kafka.ConfigMap{
		"url": "http://localhost:5219",
	})

	wg := sync.WaitGroup{}
	for i := 0; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			deadClient.Register("registrant", &schemaString)
			client.GetByID(1)
			client.Register("registrant", &schemaString)
			deadClient.GetByID(1)
			wg.Done()
		}(i)
	}
	wg.Wait()

	maybeFail("", expect(schemaResponses, int32(1)), expect(subjectReponses, int32(1)))
}

func testAdvanced(schema Schema) interface{} {
	datum := NewGenericRecord(schema)
	datum.Set("value", int32(3))
	datum.Set("union", int32(1234))

	subRecords := make([]AvroRecord, 2)

	subRecord0 := NewGenericRecord(datum.Schema())
	subRecord0.Set("stringValue", "Hello")
	subRecord0.Set("intValue", int32(1))
	subRecord0.Set("fruits", "apple")
	subRecords[0] = subRecord0

	subRecord1 := NewGenericRecord(datum.Schema())
	subRecord1.Set("stringValue", "World")
	subRecord1.Set("intValue", int32(2))
	subRecord1.Set("fruits", "pear")
	subRecords[1] = subRecord1

	datum.Set("rec", subRecords)
	return datum
}

func testNull(_ Schema) interface{} {
	return nil
}

func testBool(_ Schema) interface{} {
	return true
}

func testInt(_ Schema) interface{} {
	return int32(1)
}

func testLong(_ Schema) interface{} {
	return int64(2)
}

func testFloat(_ Schema) interface{} {
	return float32(3.4)
}

func testDouble(_ Schema) interface{} {
	return float64(5.67)
}

func testString(_ Schema) interface{} {
	return "this is a test of the emergency primitive string broadcast system"
}

// verify unserialized[0] primitive against deserialized[1] primitive
func verifyPrimitive(args ...interface{}) {
	maybeFail("primitive deserialization", expect(args[0], args[1]))
}

func TestSerializer_Serialize(t *testing.T) {
	maybeFail = initFailFunc(t)

	conf := kafka.ConfigMap{
		"url": testconf.getObject("schema-registry-client").getString("url"),
	}

	as := NewAvroSerializer()
	as.Configure(conf, true)

	ds := NewAvroDeserializer()
	ds.Configure(conf, true)

	for idx, elm := range schemaTests {
		topic := fmt.Sprintf("schema%d", idx)
		schema, _ := Load(elm.source)

		record := elm.testSerializer(schema)

		serialized, err := as.Serialize(&topic, record)
		maybeFail(elm.source, err)

		deserialized, err  := ds.Deserialize(&topic, serialized)
		maybeFail(elm.source, err)

		if elm.verifySerializer != nil {
			elm.verifySerializer(record, deserialized)
		}
	}
}

func init() {
	if !testconfRead() {
		log.Fatal("FATAL: Missing testconf.json")
	}
}
