package schemaregistry

import (
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"testing"
)

var schemaTests = []string{
	"./test/avro/advanced.avsc",
	"./test/avro/complex.avsc",
	"./test/avro/union.avsc",
	"./test/avro/Null.avsc",
	"./test/avro/Bool.avsc",
	"./test/avro/Int.avsc",
	"./test/avro/Long.avsc",
	"./test/avro/Float.avsc",
	"./test/avro/Double.avsc",
	"./test/avro/String.avsc",
}

func testRegister(subject string, schema SchemaInfo) (id int) {
	id, err := srClient.Register(subject, schema, false)
	maybeFail(subject, err)
	return id
}

func testGetBySubjectAndID(subject string, id int) SchemaInfo {
	schema, err := srClient.GetBySubjectAndID(subject, id)
	maybeFail(strconv.Itoa(id), err)
	return schema
}

func testGetID(subject string, schema SchemaInfo, expected int) int {
	actual, err := srClient.GetID(subject, schema, false)
	maybeFail(subject, err, expect(actual, expected))
	return actual
}

func testGetLatestSchemaMetadata(subject string) {
	_, err := srClient.GetLatestSchemaMetadata(subject)
	maybeFail(subject, err)
}

func testGetSchemaMetadata(subject string, versionID int, expected string) {
	actual, err := srClient.GetSchemaMetadata(subject, versionID)
	// avoid nil pointer dereference
	maybeFail(subject, err)
	maybeFail(subject, expect(expected, actual.Schema))
}

func testGetVersion(subject string, schema SchemaInfo) (version int) {
	actual, err := srClient.GetVersion(subject, schema, false)
	maybeFail(subject, err)
	return actual
}

func testGetAllVersions(subject string, expected []int) {
	actual, err := srClient.GetAllVersions(subject)
	sort.Ints(actual)
	sort.Ints(expected)
	maybeFail(subject, err, expect(actual, expected))
}

func testGetCompatibility(subject string, expected Compatibility) {
	actual, err := srClient.GetCompatibility(subject)
	maybeFail(subject, err, expect(actual, expected))
}

func testUpdateCompatibility(subject string, update Compatibility, expected Compatibility) {
	actual, err := srClient.UpdateCompatibility(subject, update)
	maybeFail(subject, err, expect(actual, expected))
}

func testGetDefaultCompatibility(expected Compatibility) {
	actual, err := srClient.GetDefaultCompatibility()
	maybeFail("Default Compatibility", err, expect(actual, expected))
}

func testUpdateDefaultCompatibility(update Compatibility, expected Compatibility) {
	actual, err := srClient.UpdateDefaultCompatibility(update)
	maybeFail("Default Compatibility", err, expect(actual, expected))
}

func testGetAllSubjects(expected []string) {
	actual, err := srClient.GetAllSubjects()
	sort.Strings(actual)
	sort.Strings(expected)
	maybeFail("All Subjects", err, expect(actual, expected))
}

func testDeleteSubject(subject string, expected []int) {
	actual, err := srClient.DeleteSubject(subject)
	sort.Ints(actual)
	sort.Ints(expected)
	maybeFail(subject, err, expect(actual, expected))
}

func testDeleteSubjectVersion(subject string, version int, expected int) {
	actual, err := srClient.DeleteSubjectVersion(subject, version)
	maybeFail(subject, err, expect(actual, expected))
}

func testTestCompatibility(subject string, version int, schema SchemaInfo, expected bool) {
	actual, err := srClient.TestCompatibility(subject, version, schema)
	maybeFail(subject, err, expect(actual, expected))
}

func TestClient(t *testing.T) {
	maybeFail = initFailFunc(t)

	url := testconf.getString("SchemaRegistryURL")
	if url == "" {
		url = "mock://"
	}
	conf := ConfigMap{
		SchemaRegistryURL: url,
	}

	var err error
	srClient, err = NewClient(&conf)
	maybeFail("schema registry client instantiation ", err)

	var subjects []string
	var version, lastVersion int
	for idx, schemaTest := range schemaTests {
		subject := fmt.Sprintf("schema%d-key", idx)
		buff, err := ioutil.ReadFile(schemaTest)
		if err != nil {
			panic(err)
		}
		schema := SchemaInfo{
			Schema: string(buff),
		}

		id := testRegister(subject, schema)
		lastVersion = version
		version = testGetVersion(subject, schema)

		// The schema registry will return a normalized Avro Schema so we can't directly compare the two
		// To work around this we retrieve a normalized schema from the Schema registry first for comparison
		normalized := testGetBySubjectAndID(subject, id)
		testGetSchemaMetadata(subject, version, normalized.Schema)
		testGetLatestSchemaMetadata(subject)

		testUpdateCompatibility(subject, Forward, Forward)
		testGetCompatibility(subject, Forward)

		testUpdateDefaultCompatibility(None, None)
		testGetDefaultCompatibility(None)

		testGetAllVersions(subject, []int{version})

		subjects = append(subjects, subject)
	}

	testDeleteSubject(subjects[len(subjects)-1], []int{version})
	testDeleteSubjectVersion(subjects[len(subjects)-2], lastVersion, lastVersion)
	testGetAllSubjects(subjects[:len(subjects)-2])
}

func init() {
	if !testconfRead() {
		log.Print("WARN: Missing testconf.json, using mock client")
	}
}
