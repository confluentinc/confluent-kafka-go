/**
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package schemaregistry

import (
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"testing"
)

var schemaTests = [][]string{
	{"./test/avro/complex.avsc"},
	{"./test/avro/union.avsc"},
	{"./test/avro/null.avsc"},
	{"./test/avro/bool.avsc"},
	{"./test/avro/int.avsc"},
	{"./test/avro/long.avsc"},
	{"./test/avro/float.avsc"},
	{"./test/avro/double.avsc"},
	{"./test/avro/advanced.avsc", "./test/avro/advanced-2.avsc"},
	{"./test/avro/string.avsc"},
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

func testGetBySubjectAndIDNotFound(subject string, id int) {
	_, err := srClient.GetBySubjectAndID(subject, id)
	if err == nil {
		maybeFail("testGetBySubjectAndIDNotFound", fmt.Errorf("Expected error, found nil"))
	}
}

func testGetID(subject string, schema SchemaInfo, expected int) int {
	actual, err := srClient.GetID(subject, schema, false)
	maybeFail(subject, err, expect(actual, expected))
	return actual
}

func testGetIDNotFound(subject string, schema SchemaInfo) {
	_, err := srClient.GetID(subject, schema, false)
	if err == nil {
		maybeFail("testGetIDNotFound", fmt.Errorf("Expected error, found nil"))
	}
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

func testGetVersionNotFound(subject string, schema SchemaInfo) {
	_, err := srClient.GetVersion(subject, schema, false)
	if err == nil {
		maybeFail("testGetVersionNotFound", fmt.Errorf("Expected error, found nil"))
	}
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

func testDeleteSubject(subject string, permanent bool, expected []int, ids []int, schemas []SchemaInfo) {
	actual, err := srClient.DeleteSubject(subject, permanent)
	sort.Ints(actual)
	sort.Ints(expected)
	maybeFail(subject, err, expect(actual, expected))
	for i := range expected {
		if permanent {
			testGetBySubjectAndIDNotFound(subject, ids[i])
		} else {
			testGetBySubjectAndID(subject, ids[i])
		}
		testGetIDNotFound(subject, schemas[i])
		testGetVersionNotFound(subject, schemas[i])
	}
}

func testDeleteSubjectVersion(subject string, permanent bool, version int, expected int, id int, schema SchemaInfo) {
	actual, err := srClient.DeleteSubjectVersion(subject, version, permanent)
	maybeFail(subject, err, expect(actual, expected))
	if permanent {
		testGetBySubjectAndIDNotFound(subject, id)
	} else {
		testGetBySubjectAndID(subject, id)
	}
	testGetIDNotFound(subject, schema)
	testGetVersionNotFound(subject, schema)
}

func testTestCompatibility(subject string, version int, schema SchemaInfo, expected bool) {
	actual, err := srClient.TestCompatibility(subject, version, schema)
	maybeFail(subject, err, expect(actual, expected))
}

func testRemainingVersions(subjects []string, schemas [][]SchemaInfo, ids [][]int, versions [][]int) {
	for i := range subjects {
		for j := range schemas[i] {
			testGetID(subjects[i], schemas[i][j], ids[i][j])
			foundVersion := testGetVersion(subjects[i], schemas[i][j])
			maybeFail("testRemainingVersions", expect(foundVersion, versions[i][j]))
		}
	}
}

func TestClient(t *testing.T) {
	maybeFail = initFailFunc(t)

	url := testconf.getString("SchemaRegistryURL")
	if url == "" {
		url = "mock://"
	}
	conf := NewConfig(url)

	var err error
	srClient, err = NewClient(conf)
	maybeFail("schema registry client instantiation ", err)

	var subjects = make([]string, len(schemaTests))
	var ids = make([][]int, len(schemaTests))
	var versions = make([][]int, len(schemaTests))
	var schemas = make([][]SchemaInfo, len(schemaTests))
	var version int

	for idx, schemaTestVersions := range schemaTests {
		var currentVersions = make([]int, 0)
		subject := fmt.Sprintf("schema%d-key", idx)
		srClient.DeleteSubject(subject, false)
		srClient.DeleteSubject(subject, true)
		subjects[idx] = subject
		for _, schemaTest := range schemaTestVersions {
			buff, err := ioutil.ReadFile(schemaTest)
			if err != nil {
				panic(err)
			}
			schema := SchemaInfo{
				Schema: string(buff),
			}

			id := testRegister(subject, schema)
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

			currentVersions = append(currentVersions, version)
			testGetAllVersions(subject, currentVersions)

			ids[idx] = append(ids[idx], id)
			versions[idx] = append(versions[idx], version)
			schemas[idx] = append(schemas[idx], schema)
		}
	}

	lastSubject := len(subjects) - 1
	secondToLastSubject := len(subjects) - 2
	testDeleteSubject(subjects[lastSubject], false, versions[lastSubject], ids[lastSubject], schemas[lastSubject])
	testDeleteSubjectVersion(subjects[secondToLastSubject], false, versions[secondToLastSubject][0], versions[secondToLastSubject][0], ids[secondToLastSubject][0], schemas[secondToLastSubject][0])
	// Second to last subject now has only one version
	initialVersionsSecondToLastSubject := versions[secondToLastSubject][0:]
	initialSchemasSecondToLastSubject := schemas[secondToLastSubject][0:]
	initialIdsSecondToLastSubject := ids[secondToLastSubject][0:]
	versions[secondToLastSubject] = versions[secondToLastSubject][1:]
	schemas[secondToLastSubject] = schemas[secondToLastSubject][1:]
	ids[secondToLastSubject] = ids[secondToLastSubject][1:]
	// Only last subject has been removed completely
	testGetAllSubjects(subjects[:lastSubject])
	remainingSubjects := subjects[:lastSubject]
	testRemainingVersions(remainingSubjects, schemas, ids, versions)
	// Cleanup subjects
	for i := range remainingSubjects {
		testDeleteSubject(remainingSubjects[i], false, versions[i], ids[i], schemas[i])
		if i == secondToLastSubject {
			testDeleteSubject(remainingSubjects[i], true, initialVersionsSecondToLastSubject, initialIdsSecondToLastSubject, initialSchemasSecondToLastSubject)
		} else {
			testDeleteSubject(remainingSubjects[i], true, versions[i], ids[i], schemas[i])
		}
	}
}

func init() {
	if !testconfRead() {
		log.Print("WARN: Missing testconf.json, using mock client")
	}
}
