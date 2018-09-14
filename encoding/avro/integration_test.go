package avro

import (
	"fmt"
	"sort"
	"testing"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strconv"
)


func testClient_Register(subject string, schema *string) (id int) {
	id, err := srClient.Register(subject, schema)
	maybeFail(subject, err)

	return id
}

func testClient_GetByID(id int) *string {
	schema, err := srClient.GetByID(id)
	maybeFail(strconv.Itoa(id), err)
	return schema
}

func testClient_GetID(subject string, schema *string, expected int) int {
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
	maybeFail(subject, expect(expected, *actual.Schema))
}

func testClient_GetVersion(subject string, schema *string, expected int) {
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

func testClient_TestCompatibility(subject string, version int, schema *string, expected bool) {
	actual, err := srClient.TestCompatibility(subject, version, schema)
	maybeFail(subject, err, expect(actual, expected))
}

func TestClient(t *testing.T) {
	maybeFail = initFailFunc(t)

	conf := kafka.ConfigMap{
		"url": testconf.getObject("schema-registry-client").getString("url"),
	}

	var err error
	srClient, err = NewCachedSchemaRegistryClient(conf)
	maybeFail("schema registry client instantiation ", err)

	var subjects []string
	var version, lastVersion int
	for idx, elm := range schemaTests {
		subject := fmt.Sprintf("schema%d-key", idx)
		schema, _ := Load(elm.source)

		schemaString := schema.String()
		id := testClient_Register(subject, &schemaString)
		lastVersion = version
		version = testClient_GetID(subject, &schemaString, id)

		// The schema registry will return a normalized Avro Schema so we can't directly compare the two
		// To work around this we retrieve a normalized schema from the Schema registry first for comparison
		normalized := testClient_GetByID(id)
		testClient_GetSchemaMetadata(subject, version, *normalized)
		testClient_GetLatestSchemaMetadata(subject)

		testClient_UpdateCompatibility(subject, FORWARD, FORWARD)
		testClient_GetCompatibility(subject, FORWARD)
		testClient_TestCompatibility(subject, version, &schemaString, true)

		testClient_UpdateDefaultCompatibility(NONE, NONE)
		testClient_GetDefaultCompatibility(NONE)

		testClient_GetAllVersions(subject, []int{version})
		testClient_GetVersion(subject, &schemaString, version)

		subjects = append(subjects, subject)
	}

	testClient_DeleteSubject(subjects[len(subjects)-1], []int{version})
	testClient_DeleteSubjectVersion(subjects[len(subjects)-2], lastVersion, lastVersion)
	testClient_GetAllSubjects(subjects[:len(subjects)-2])
}

