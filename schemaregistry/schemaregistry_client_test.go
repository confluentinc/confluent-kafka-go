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
	"strings"
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

func testGetAllContexts(expected []string) {
	actual, err := srClient.GetAllContexts()
	sort.Strings(actual)
	sort.Strings(expected)
	maybeFail("All Contexts", err, expect(actual, expected))
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

func testGetSubjectsAndVersionsByID(id int, ids [][]int, subjects []string, versions [][]int) {
	expected := make([]SubjectAndVersion, 0)
	for subjectIdx, subject := range subjects {
		for idIdx, sID := range ids[subjectIdx] {
			if sID == id {
				expected = append(expected, SubjectAndVersion{
					Subject: subject,
					Version: versions[subjectIdx][idIdx],
				})
			}
		}
	}

	actual, err := srClient.GetSubjectsAndVersionsByID(id)
	sort.Slice(actual, func(i, j int) bool {
		return actual[i].Subject < actual[j].Subject
	})
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].Subject < expected[j].Subject
	})
	maybeFail("testGetSubjectsAndVersionsByID", err, expect(actual, expected))
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

func testGetLatestWithMetadata(subject string, filename string, expectedMetadata Metadata) {
	actual, err := srClient.GetLatestWithMetadata(subject, map[string]string{"fileName": filename}, false)
	// avoid nil pointer dereference
	maybeFail(subject, err)
	maybeFail(subject, expect(expectedMetadata, *actual.Metadata))
}

func testGetSchemaMetadata(subject string, versionID int, expectedSchema string, expectedMetadata Metadata) {
	actual, err := srClient.GetSchemaMetadata(subject, versionID)
	// avoid nil pointer dereference
	maybeFail(subject, err)
	maybeFail(subject, expect(expectedSchema, actual.Schema))
	maybeFail(subject, expect(expectedMetadata, *actual.Metadata))
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
		_, _ = srClient.DeleteSubject(subject, false)
		_, _ = srClient.DeleteSubject(subject, true)
		subjects[idx] = subject
		for _, schemaTest := range schemaTestVersions {
			buff, err := ioutil.ReadFile(schemaTest)
			if err != nil {
				panic(err)
			}
			metadata := Metadata{
				Properties: map[string]string{
					"fileName": schemaTest,
				},
			}
			schema := SchemaInfo{
				Schema:   string(buff),
				Metadata: &metadata,
			}

			id := testRegister(subject, schema)
			version = testGetVersion(subject, schema)

			// The schema registry will return a normalized Avro Schema so we can't directly compare the two
			// To work around this we retrieve a normalized schema from the Schema registry first for comparison
			normalized := testGetBySubjectAndID(subject, id)
			testGetSchemaMetadata(subject, version, normalized.Schema, metadata)
			testGetLatestSchemaMetadata(subject)
			testGetLatestWithMetadata(subject, schemaTest, metadata)

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

	testGetAllContexts([]string{"."})
	lastSubject := len(subjects) - 1
	secondToLastSubject := len(subjects) - 2
	testGetSubjectsAndVersionsByID(ids[lastSubject][0], ids, subjects, versions)
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

type resource struct {
	ResourceName      string
	ResourceNamespace string
	ResourceID        string
	ResourceType      string
}

func generateAssociationCreateRequest(resource resource, associationCreateInfo ...AssociationCreateInfo) (result AssociationCreateRequest) {
	associationCreateRequest := AssociationCreateRequest{
		ResourceName:      resource.ResourceName,
		ResourceNamespace: resource.ResourceNamespace,
		ResourceID:        resource.ResourceID,
		ResourceType:      resource.ResourceType,
	}
	var associations []AssociationCreateInfo
	for _, associationCreateInfo := range associationCreateInfo {
		associations = append(associations, associationCreateInfo)
	}
	associationCreateRequest.Associations = associations
	return associationCreateRequest
}

func TestAssociations(t *testing.T) {
	maybeFail = initFailFunc(t)

	// Use mock client for testing
	conf := NewConfig("mock://")

	t.Run("TestAssociationCreateRequestValidationLogic", func(t *testing.T) {
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		// Pre-create subjects used for testing
		_, err = client.Register("testKey", SchemaInfo{Schema: "{\"type\": \"string\"}"}, true)
		maybeFail("Register schema", err)
		_, err = client.Register("testValue", SchemaInfo{Schema: "{\"type\": \"string\"}"}, true)
		maybeFail("Register schema", err)

		createInfo1 := AssociationCreateInfo{Subject: "testKey", AssociationType: "key"}
		createInfo2 := AssociationCreateInfo{Subject: "testValue", AssociationType: "value"}

		// Invalid requests
		invalidRequests := []AssociationCreateRequest{}
		// No resource name
		invalidRequests = append(invalidRequests, AssociationCreateRequest{ResourceNamespace: "lkc1", ResourceID: "test-id", ResourceType: "topic", Associations: []AssociationCreateInfo{createInfo1, createInfo2}})
		// No resource namespace
		invalidRequests = append(invalidRequests, AssociationCreateRequest{ResourceName: "test", ResourceID: "test-id", ResourceType: "topic", Associations: []AssociationCreateInfo{createInfo1, createInfo2}})
		// No resource id
		invalidRequests = append(invalidRequests, AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceType: "topic", Associations: []AssociationCreateInfo{createInfo1, createInfo2}})
		// No associations
		invalidRequests = append(invalidRequests, AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceID: "test-id", ResourceType: "topic"})
		// No subject name in AssociationCreateInfo
		invalidRequests = append(invalidRequests, AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceID: "test-id", ResourceType: "topic",
			Associations: []AssociationCreateInfo{{AssociationType: "value"}}})
		// Unsupported ResourceType
		invalidRequests = append(invalidRequests, AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceID: "test-id", ResourceType: "topic2", Associations: []AssociationCreateInfo{createInfo1, createInfo2}})
		// Unsupported AssociationType
		invalidRequests = append(invalidRequests, AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceID: "test-id", ResourceType: "topic", Associations: []AssociationCreateInfo{{Subject: "testValue", AssociationType: "value2"}}})
		// Duplicate AssociationType in the request
		invalidRequests = append(invalidRequests, AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceID: "test-id", ResourceType: "topic",
			Associations: []AssociationCreateInfo{{Subject: "testKey", AssociationType: "value"}, {Subject: "testValue", AssociationType: "value"}}})
		// Weak association with frozen to be true
		invalidRequests = append(invalidRequests, AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceID: "test-id", ResourceType: "topic", Associations: []AssociationCreateInfo{{Subject: "testValue", Lifecycle: "weak", Frozen: true}}})

		for _, invalidRequest := range invalidRequests {
			_, err := client.CreateAssociation(invalidRequest)
			maybeFail("CreateAssociation with invalid request", expect(err != nil, true))
		}

		// Minimum valid request
		createRequest := AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceID: "test-id", Associations: []AssociationCreateInfo{{Subject: "testValue"}}}
		createResponse, err := client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation with invalid response", err,
			expect(createResponse.ResourceName, createResponse.ResourceName),
			expect(createResponse.ResourceNamespace, createResponse.ResourceNamespace),
			expect(createResponse.ResourceID, createResponse.ResourceID),
			expect(createResponse.ResourceType, "topic"),
			expect(len(createResponse.Associations), 1),
			expect(createResponse.Associations[0].Subject, createResponse.Associations[0].Subject),
			expect(createResponse.Associations[0].AssociationType, "value"),
			expect(createResponse.Associations[0].Lifecycle, STRONG),
			expect(createResponse.Associations[0].Frozen, false),
			expect(createResponse.Associations[0].Schema == nil, true))
	})

	t.Run("TestCreateOneAssociationInCreateRequest", func(t *testing.T) {
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		// Pre-create subjects used for testing
		testValueSubject := "testValue"
		schemaInfo := SchemaInfo{
			Schema: "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\",\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}",
		}
		_, err = client.Register(testValueSubject, schemaInfo, true)
		maybeFail("Register schema", err)

		// Make an association with an existing subject without new schema
		createRequest := AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceID: "test-id",
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: testValueSubject}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation", err)

		// Create association request is idempotent. Re-issue the same create request should succeed.
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation for idempotency", err)

		// Re-issue the same request with different association property (except schema) will error out.
		createRequest = AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceID: "test-id",
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: testValueSubject, Lifecycle: WEAK}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("Existing association gets modified ", expect(err != nil, true))

		// Make an association with an existing subject with new schema
		updatedSchemaInfo := SchemaInfo{
			Schema: "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\",\"fields\":[{\"type\":\"string\",\"name\":\"id\"}, {\"type\":\"string\",\"name\":\"id2\"}]}",
		}
		createRequest = AssociationCreateRequest{ResourceName: "test", ResourceNamespace: "lkc1", ResourceID: "test-id",
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: testValueSubject, Schema: &updatedSchemaInfo}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation with updated schema", err)

		// Make an association with a new subject without new schema. Test should fail.
		testValueSubject = "testValue2"
		createRequest = AssociationCreateRequest{ResourceName: "test2", ResourceNamespace: "lkc1", ResourceID: "test-id2",
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: testValueSubject}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation with new subject and schema", expect(err != nil, true))

		// Make an association with a new subject with new schema
		testValueSubject = "testValue2"
		createRequest = AssociationCreateRequest{ResourceName: "test2", ResourceNamespace: "lkc1", ResourceID: "test-id2",
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: testValueSubject, Schema: &updatedSchemaInfo}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation with new subject and schema", err)
	})

	t.Run("TestCreateMultipleAssociationsInCreateRequest", func(t *testing.T) {
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		// Both associations using existing subjects
		keySubject, valueSubject := "test1Key", "test1Value"
		resourceName, resourceID := "test1", "test1-id"
		schemaInfo := SchemaInfo{
			Schema: "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\",\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}",
		}
		// Pre-create subjects
		_, err = client.Register(keySubject, schemaInfo, true)
		maybeFail("Register schema for keySubject", err)
		_, err = client.Register(valueSubject, schemaInfo, true)
		maybeFail("Register schema for valueSubject", err)

		createRequest := AssociationCreateRequest{ResourceName: resourceName, ResourceNamespace: "lkc1", ResourceID: resourceID,
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: keySubject, AssociationType: "key"}, AssociationCreateInfo{Subject: valueSubject, AssociationType: "value"}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation", err)

		// One using existing subject, one creating new subject
		keySubject, valueSubject = "test2Key", "test2Value"
		resourceName, resourceID = "test2", "test2-id"
		_, err = client.Register(keySubject, schemaInfo, true)
		maybeFail("Register schema for keySubject", err)

		createRequest = AssociationCreateRequest{ResourceName: resourceName, ResourceNamespace: "lkc1", ResourceID: resourceID,
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: keySubject, AssociationType: "key"},
				AssociationCreateInfo{Subject: valueSubject, AssociationType: "value", Schema: &schemaInfo}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation", err)

		// Both creating new subjects
		keySubject, valueSubject = "test3Key", "test3Value"
		resourceName, resourceID = "test3", "test3-id"
		createRequest = AssociationCreateRequest{ResourceName: resourceName, ResourceNamespace: "lkc1", ResourceID: resourceID,
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: keySubject, AssociationType: "key", Schema: &schemaInfo},
				AssociationCreateInfo{Subject: valueSubject, AssociationType: "value", Schema: &schemaInfo}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation", err)
	})

	// Successful case, subject exists, one association without schema info
	t.Run("TestCreateStrongAndWeakAssociationsForTheSameSubject", func(t *testing.T) {
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		// resources for testing
		resourceFoo := resource{ResourceName: "foo", ResourceNamespace: "lkc1", ResourceID: "id-foo", ResourceType: "topic"}
		resourceBar := resource{ResourceName: "bar", ResourceNamespace: "lkc1", ResourceID: "id-bar", ResourceType: "topic"}
		var fooValueSubject = "fooValue"
		value := "value"

		// Pre-create subject used by testing
		schemaInfo := SchemaInfo{
			Schema: "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\",\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}",
		}
		_, err = client.Register(fooValueSubject, schemaInfo, true)
		maybeFail("Failed to register subject", err)

		// Same subject. Foo association is strong, Bar is strong. The second should fail.
		fooValueAssociationRequest := generateAssociationCreateRequest(resourceFoo, AssociationCreateInfo{Subject: fooValueSubject, AssociationType: value, Lifecycle: STRONG, Frozen: false})
		_, err = client.CreateAssociation(fooValueAssociationRequest)
		maybeFail("CreateAssociation", err)
		result, err := client.GetAssociationsByResourceID(resourceFoo.ResourceID, "", nil, "", 0, -1)
		maybeFail("GetAssociationsByResourceID", err, expect(len(result), 1), expect(result[0].GUID != "", true))

		barValueAssociationRequest := generateAssociationCreateRequest(resourceBar, AssociationCreateInfo{Subject: fooValueSubject, AssociationType: value, Lifecycle: STRONG})
		_, err = client.CreateAssociation(barValueAssociationRequest)
		maybeFail("CreateAssociation", expect(err != nil, true))

		// Foo association is strong, Bar is weak. The second should fail.
		barValueAssociationRequest = generateAssociationCreateRequest(resourceBar, AssociationCreateInfo{Subject: fooValueSubject, AssociationType: value, Lifecycle: WEAK})
		_, err = client.CreateAssociation(barValueAssociationRequest)
		maybeFail("CreateAssociation", expect(err != nil, true))

		// Foo association is weak, Bar is strong. The second should fail.
		// Delete Bar association without deleting the subject.
		err = client.DeleteAssociations(fooValueAssociationRequest.ResourceID, "", nil, false)
		maybeFail("DeleteAssociations without cascade", err)
		associations, err := client.GetAssociationsByResourceID(resourceFoo.ResourceID, "", nil, "", 0, -1)
		maybeFail("GetAssociationsByResourceID", err, expect(len(associations), 0))
		metadata, err := client.GetLatestSchemaMetadata(fooValueSubject)
		maybeFail("GetLatestSchemaMetadata ", err, expect(metadata.ID > 0, true))
		// Foo weak association
		fooValueAssociationRequest = generateAssociationCreateRequest(resourceFoo, AssociationCreateInfo{Subject: fooValueSubject, AssociationType: value, Lifecycle: WEAK, Frozen: false})
		_, err = client.CreateAssociation(fooValueAssociationRequest)
		maybeFail("CreateAssociation", err)
		result, err = client.GetAssociationsByResourceID(resourceFoo.ResourceID, "", nil, "", 0, -1)
		maybeFail("GetAssociationsByResourceID", err, expect(len(result), 1), expect(result[0].GUID != "", true))

		barValueAssociationRequest = generateAssociationCreateRequest(resourceBar, AssociationCreateInfo{Subject: fooValueSubject, AssociationType: value, Lifecycle: STRONG})
		_, err = client.CreateAssociation(barValueAssociationRequest)
		maybeFail("CreateAssociation", expect(err != nil, true))

		// Foo association is weak, Bar is weak. The second should succeed.
		barValueAssociationRequest = generateAssociationCreateRequest(resourceBar, AssociationCreateInfo{Subject: fooValueSubject, AssociationType: value, Lifecycle: WEAK})
		_, err = client.CreateAssociation(barValueAssociationRequest)
		maybeFail("CreateAssociation", err)
		result, err = client.GetAssociationsByResourceID(resourceBar.ResourceID, "", nil, "", 0, -1)
		maybeFail("GetAssociationsByResourceID", err, expect(len(result), 1), expect(result[0].GUID != "", true))
		result, err = client.GetAssociationsBySubject(fooValueSubject, "", nil, "", 0, -1)
		maybeFail("GetAssociationsBySubject", err, expect(len(result), 2))
	})

	// Successful case, subjects exist, two associations without schema info
	// Successful case, new subjects, one association with schema info
	// Successful case, new subjects, two associations with schema info
	// Failed case, new subjects, one association without schema info
	// Failed case, subject exists, one association with schema info, update failed
	// Failed case, one new subject, one existing subject. two associations. update failed but new subject was created.

	t.Run("TestGetAssociationsWithFilters", func(t *testing.T) {
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		// Both associations using existing subjects
		keySubject, valueSubject := "test1Key", "test1Value"
		resourceName, resourceID := "test1", "test1-id"
		schemaInfo := SchemaInfo{
			Schema: "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\",\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}",
		}

		createRequest := AssociationCreateRequest{ResourceName: resourceName, ResourceNamespace: "lkc1", ResourceID: resourceID,
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: keySubject, Schema: &schemaInfo, Lifecycle: STRONG, AssociationType: "key"},
				AssociationCreateInfo{Subject: valueSubject, Schema: &schemaInfo, Lifecycle: WEAK, AssociationType: "value"}}}

		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation with new subject and schema", err)

		// lifecycle should be upper case WEAK or STRONG
		associations, err := client.GetAssociationsBySubject(keySubject, "", []string{"key", "value"}, "weak", 0, -1)
		maybeFail("GetAssociationsBySubject", expect(err != nil, true))

		associations, err = client.GetAssociationsBySubject(keySubject, "", []string{"key", "value"}, "WEAK", 0, -1)
		maybeFail("GetAssociationsBySubject", err, expect(len(associations), 0))

		associations, err = client.GetAssociationsBySubject(keySubject, "", []string{"key", "value"}, "STRONG", 0, -1)
		maybeFail("GetAssociationsBySubject", err, expect(len(associations), 1))

		associations, err = client.GetAssociationsByResourceID(resourceID, "", []string{"key", "value"}, "", 0, -1)
		maybeFail("GetAssociationsBySubject", err, expect(len(associations), 2))
	})

	t.Run("TestDeleteAssociation", func(t *testing.T) {
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		// Create one strong key association, and one weak value association
		keySubject, valueSubject := "test1Key", "test1Value"
		resourceName, resourceID := "test1", "test1-id"
		schemaInfo := SchemaInfo{
			Schema: "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\",\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}",
		}

		createRequest := AssociationCreateRequest{ResourceName: resourceName, ResourceNamespace: "lkc1", ResourceID: resourceID,
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: keySubject, Schema: &schemaInfo, Lifecycle: STRONG, AssociationType: "key"},
				AssociationCreateInfo{Subject: valueSubject, Schema: &schemaInfo, Lifecycle: WEAK, AssociationType: "value"}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation with new subject and schema", err)

		// With cascade delete
		err = client.DeleteAssociations(resourceID, "", nil, true)
		maybeFail("DeleteAssociation", err)
		keySchemaMetadata, err := client.GetLatestSchemaMetadata(keySubject)
		maybeFail("GetLatestSchemaMetadata for key", expect(err != nil, true), expect(strings.Contains(strings.ToLower(err.Error()), "not found"), true))
		valueSchemaMetadata, err := client.GetLatestSchemaMetadata(valueSubject)
		maybeFail("GetLatestSchemaMetadata for value", err, expect(valueSchemaMetadata.ID > 0, true))
		associations, err := client.GetAssociationsByResourceID(resourceID, "", nil, "", 0, -1)
		maybeFail("GetAssociationsByResourceID", err, expect(associations == nil || len(associations) == 0, true))

		// Without cascade delete
		keySubject, valueSubject = "test2Key", "test2Value"
		resourceName, resourceID = "test2", "test2-id"
		createRequest = AssociationCreateRequest{ResourceName: resourceName, ResourceNamespace: "lkc1", ResourceID: resourceID,
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: keySubject, Schema: &schemaInfo, Lifecycle: STRONG, AssociationType: "key"},
				AssociationCreateInfo{Subject: valueSubject, Schema: &schemaInfo, Lifecycle: WEAK, AssociationType: "value"}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation with new subject and schema", err)

		err = client.DeleteAssociations(resourceID, "", nil, false)
		maybeFail("DeleteAssociation", err)
		keySchemaMetadata, err = client.GetLatestSchemaMetadata(keySubject)
		maybeFail("GetLatestSchemaMetadata for key", err, expect(keySchemaMetadata.ID > 0, true))
		valueSchemaMetadata, err = client.GetLatestSchemaMetadata(valueSubject)
		maybeFail("GetLatestSchemaMetadata for value", err, expect(valueSchemaMetadata.ID > 0, true))
		_, err = client.GetAssociationsByResourceID(resourceID, "", nil, "", 0, -1)
		maybeFail("GetAssociationsByResourceID", err, expect(associations == nil || len(associations) == 0, true))

		// Delete a non-existing association. Should return nothing.
		err = client.DeleteAssociations(resourceID, "", nil, false)
		maybeFail("DeleteAssociation for non-existing association", err)

		// Delete a frozen association with cascade = false. Should return error.
		keySubject, valueSubject = "test3Key", "test3Value"
		resourceName, resourceID = "test3", "test3-id"
		createRequest = AssociationCreateRequest{ResourceName: resourceName, ResourceNamespace: "lkc1", ResourceID: resourceID,
			Associations: []AssociationCreateInfo{AssociationCreateInfo{Subject: keySubject, Schema: &schemaInfo, Lifecycle: STRONG, AssociationType: "key", Frozen: true},
				AssociationCreateInfo{Subject: valueSubject, Schema: &schemaInfo, Lifecycle: WEAK, AssociationType: "value"}}}
		_, err = client.CreateAssociation(createRequest)
		maybeFail("CreateAssociation with new subject and schema", err)
		err = client.DeleteAssociations(resourceID, "", nil, false)
		maybeFail("DeleteAssociation", expect(err != nil, true))
		// Setting cascade = true will only delete subject with strong association; subject with weak association will not be deleted.
		err = client.DeleteAssociations(resourceID, "", nil, true)
		maybeFail("DeleteAssociation", err)
		_, err = client.GetAllVersions(keySubject) // keySubject should not exist
		maybeFail("GetAllVersions for keySubject", expect(err != nil, true))
		_, err = client.GetAllVersions(valueSubject) // valueSubject should exist
		maybeFail("GetAllVersions for valueSubject", err)
	})
}

func init() {
	if !testconfRead() {
		log.Print("WARN: Missing testconf.json, using mock client")
	}
}
