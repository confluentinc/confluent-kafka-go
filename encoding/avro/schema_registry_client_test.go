package avro

import (
"fmt"
"sort"
"testing"
"github.com/confluentinc/confluent-kafka-go/kafka"
"strconv"
	"log"
)

var schemaTests = []struct {
	source         string
	load           func(string) string
	testSerializer func(record Schema) interface{}
	verifySerializer func(...interface{})
}{
	{source: "./schemas/advanced.avsc",
		//testSerializer: testAdvanced,
		//verifySerialization: ,
	},
	//{source: "./schemas/complex.avsc",
	//	handle: testComplex,
	//},
	//{source: "./schemas/union.avsc",
	//	handle: testGeneric3,
	//},
	{source: "./schemas/Null.avsc",
		//testSerializer: testNull,
		//verifySerializer: verifyPrimitive,
	},
	{source: "./schemas/Bool.avsc",
		//testSerializer: testBool,
		//verifySerializer: verifyPrimitive,
	},
	{source: "./schemas/Int.avsc",
		//testSerializer: testInt,
		//verifySerializer: verifyPrimitive,
	},
	{source: "./schemas/Long.avsc",
		//testSerializer: testLong,
		//verifySerializer: verifyPrimitive,

	},
	{source: "./schemas/Float.avsc",
		//testSerializer: testFloat,
		//verifySerializer: verifyPrimitive,
	},
	{source: "./schemas/Double.avsc",
		//testSerializer: testDouble,
		//verifySerializer: verifyPrimitive,
	},
	{source: "./schemas/String.avsc",
		//testSerializer: testString,
		//verifySerializer: verifyPrimitive,
	},
}

func testClient_Register(subject string, schema Schema) (id int) {
	id, err := srClient.Register(subject, schema)
	maybeFail(subject, err)

	return id
}

func testClient_GetByID(id int) Schema {
	schema, err := srClient.GetByID(id)
	maybeFail(strconv.Itoa(id), err)
	return schema
}

func testClient_GetID(subject string, schema Schema, expected int) int {
	actual, err := srClient.GetID(subject, schema)
	maybeFail(subject, err, expect(actual.ID, expected))
	return actual.Version
}

func testClient_GetLatestSchemaMetadata(subject string) {
	_, err := srClient.GetLatestSchemaMetadata(subject)
	maybeFail(subject, err)
}

func testClient_GetSchemaMetadata(subject string, versionID int, expected string) {
	actual, err := srClient.GetSchemaMetadata(subject, versionID)
	// avoid nil pointer dereference
	maybeFail(subject, err)
	maybeFail(subject, expect(expected, actual.Schema.String()))
}

func testClient_GetVersion(subject string, schema Schema, expected int) {
	actual, err := srClient.GetVersion(subject, schema)
	maybeFail(subject, err, expect(expected, actual))
}

func testClient_GetAllVersions(subject string, expected []int) {
	actual, err := srClient.GetAllVersions(subject)
	sort.Ints(actual)
	sort.Ints(expected)
	maybeFail(subject, err, expect(actual, expected))
}

func testClient_GetCompatibility(subject string, expected Compatibility) {
	actual, err := srClient.GetCompatibility(subject)
	maybeFail(subject, err, expect(actual, expected))
}

func testClient_UpdateCompatibility(subject string, update Compatibility, expected Compatibility) {
	actual, err := srClient.UpdateCompatibility(subject, update)
	maybeFail(subject, err, expect(actual, expected))
}

func testClient_GetDefaultCompatibility(expected Compatibility) {
	actual, err := srClient.GetDefaultCompatibility()
	maybeFail("Default Compatibility", err, expect(actual, expected))
}

func testClient_UpdateDefaultCompatibility(update Compatibility, expected Compatibility) {
	actual, err := srClient.UpdateDefaultCompatibility(update)
	maybeFail("Default Compatibility", err, expect(actual, expected))
}

func testClient_GetAllSubjects(expected []string) {
	actual, err := srClient.GetAllSubjects()
	sort.Strings(actual)
	sort.Strings(expected)
	maybeFail("All Subjects", err, expect(actual, expected))
}

func testClient_DeleteSubject(subject string, expected []int) {
	actual, err := srClient.DeleteSubject(subject)
	sort.Ints(actual)
	sort.Ints(expected)
	maybeFail(subject, err, expect(actual, expected))
}

func testClient_DeleteSubjectVersion(subject string, version int, expected int) {
	actual, err := srClient.DeleteSubjectVersion(subject, version)
	maybeFail(subject, err, expect(actual, expected))
}

func testClient_TestCompatibility(subject string, version int, schema Schema, expected bool) {
	actual, err := srClient.TestCompatibility(subject, version, schema)
	maybeFail(subject, err, expect(actual, expected))
}

func TestClient(t *testing.T) {
	maybeFail = initFailFunc(t)

	conf := kafka.ConfigMap{
		"url": testconf.getString("SchemaRegistryURL"),
	}

	var err error
	srClient, err = NewCachedSchemaRegistryClient(conf)
	maybeFail("schema registry client instantiation ", err)

	var subjects []string
	var version, lastVersion int
	for idx, elm := range schemaTests {
		subject := fmt.Sprintf("schema%d-key", idx)
		schema, _ := ParseFile(elm.source)

		id := testClient_Register(subject, schema)
		lastVersion = version
		version = testClient_GetID(subject, schema, id)

		// The schema registry will return a normalized Avro Schema so we can't directly compare the two
		// To work around this we retrieve a normalized schema from the Schema registry first for comparison
		normalized := testClient_GetByID(id)
		testClient_GetSchemaMetadata(subject, version, normalized.String())
		testClient_GetLatestSchemaMetadata(subject)

		testClient_UpdateCompatibility(subject, FORWARD, FORWARD)
		testClient_GetCompatibility(subject, FORWARD)
		testClient_TestCompatibility(subject, version, schema, true)

		testClient_UpdateDefaultCompatibility(NONE, NONE)
		testClient_GetDefaultCompatibility(NONE)

		testClient_GetAllVersions(subject, []int{version})
		testClient_GetVersion(subject, schema, version)

		subjects = append(subjects, subject)
	}

	testClient_DeleteSubject(subjects[len(subjects)-1], []int{version})
	testClient_DeleteSubjectVersion(subjects[len(subjects)-2], lastVersion, lastVersion)
	testClient_GetAllSubjects(subjects[:len(subjects)-2])
}

func init() {
	if !testconfRead() {
		log.Fatal("FATAL: Missing testconf.json")
	}
}
