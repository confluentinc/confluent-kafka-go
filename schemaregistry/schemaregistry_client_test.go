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
	resourceName      string
	resourceNamespace string
	resourceID        string
	resourceType      string
}

func generateAssociationCreateRequest(resource resource, associationCreateInfo ...AssociationCreateOrUpdateInfo) (result AssociationCreateOrUpdateRequest) {
	AssociationCreateOrUpdateRequest := AssociationCreateOrUpdateRequest{
		ResourceName:      resource.resourceName,
		ResourceNamespace: resource.resourceNamespace,
		ResourceID:        resource.resourceID,
		ResourceType:      resource.resourceType,
	}
	var associations []AssociationCreateOrUpdateInfo
	for _, associationCreateInfo := range associationCreateInfo {
		associations = append(associations, associationCreateInfo)
	}
	AssociationCreateOrUpdateRequest.Associations = associations
	return AssociationCreateOrUpdateRequest
}

// Associations tests
const (
	simpleStringSchema = `{"type":"string"}`
	simpleAvroSchema   = `{"namespace":"basicavro","type":"record","name":"Payment","fields":[{"type":"string","name":"id"}]}`
	evolvedAvroSchema  = `{"namespace":"basicavro","type":"record","name":"Payment","fields":[{"type":"string","name":"id"},{"type":"string","name":"id2"}]}`
	topic              = "topic"
	key                = "key"
	value              = "value"
)

var (
	defaultResourceName      = "test"
	defaultResourceNamespace = "lkc1"
	defaultResourceID        = "test-id"
	defaultKeySubject        = "testKey"
	defaultValueSubject      = "testValue"
)

type AssociationRequestBuilder struct {
	resourceName         string
	resourceNamespace    string
	resourceID           string
	resourceType         string
	associations         []AssociationCreateOrUpdateInfo
	keyAssociation       AssociationCreateOrUpdateInfo
	valueAssociation     AssociationCreateOrUpdateInfo
	keyAssocInitCalled   bool
	valueAssocInitCalled bool
}

func (builder *AssociationRequestBuilder) resource(resourceName, resourceNamespace, resourceID, resourceType string) *AssociationRequestBuilder {
	builder.resourceName = resourceName
	builder.resourceNamespace = resourceNamespace
	builder.resourceID = resourceID
	builder.resourceType = resourceType
	return builder
}

func (builder *AssociationRequestBuilder) defaultResource() *AssociationRequestBuilder {
	return builder.resource(defaultResourceName,
		defaultResourceNamespace,
		defaultResourceID,
		defaultResourceType)
}

func (builder *AssociationRequestBuilder) initKeyAssociation() {
	builder.keyAssociation = AssociationCreateOrUpdateInfo{AssociationType: key}
	builder.keyAssocInitCalled = true
}

func (builder *AssociationRequestBuilder) initValueAssociation() {
	builder.valueAssociation = AssociationCreateOrUpdateInfo{AssociationType: value}
	builder.valueAssocInitCalled = true
}

func (builder *AssociationRequestBuilder) keySubject(keySubject string) *AssociationRequestBuilder {
	if !builder.keyAssocInitCalled {
		builder.initKeyAssociation()
	}
	builder.keyAssociation.Subject = keySubject
	return builder
}

func (builder *AssociationRequestBuilder) keySchema(keySchema string) *AssociationRequestBuilder {
	if !builder.keyAssocInitCalled {
		builder.initKeyAssociation()
	}
	builder.keyAssociation.Schema = &SchemaInfo{Schema: keySchema}
	return builder
}

func (builder *AssociationRequestBuilder) keyLifecycle(keyLifecyclePolicy string) *AssociationRequestBuilder {
	if !builder.keyAssocInitCalled {
		builder.initKeyAssociation()
	}
	builder.keyAssociation.Lifecycle = LifecyclePolicy(keyLifecyclePolicy)
	return builder
}

func (builder *AssociationRequestBuilder) valueSubject(valueSubject string) *AssociationRequestBuilder {
	if !builder.valueAssocInitCalled {
		builder.initValueAssociation()
	}
	builder.valueAssociation.Subject = valueSubject
	return builder
}

func (builder *AssociationRequestBuilder) valueSchema(valueSchema string) *AssociationRequestBuilder {
	if !builder.valueAssocInitCalled {
		builder.initValueAssociation()
	}
	builder.valueAssociation.Schema = &SchemaInfo{Schema: valueSchema}
	return builder
}

func (builder *AssociationRequestBuilder) valueLifecycle(valueLifecyclePolicy string) *AssociationRequestBuilder {
	if !builder.valueAssocInitCalled {
		builder.initValueAssociation()
	}
	builder.valueAssociation.Lifecycle = LifecyclePolicy(valueLifecyclePolicy)
	return builder
}

func (builder *AssociationRequestBuilder) valueFrozen(isFrozen bool) *AssociationRequestBuilder {
	if !builder.valueAssocInitCalled {
		builder.initValueAssociation()
	}
	builder.valueAssociation.Frozen = isFrozen
	return builder
}

func (builder *AssociationRequestBuilder) association(subject, associationType, lifecyclePolicy string,
	frozen bool, schema string, normalize bool) *AssociationRequestBuilder {
	var schemaInfo *SchemaInfo
	if schema != "" {
		schemaInfo = &SchemaInfo{Schema: schema}
	}

	info := AssociationCreateOrUpdateInfo{
		Subject:         subject,
		AssociationType: associationType,
		Lifecycle:       LifecyclePolicy(lifecyclePolicy),
		Frozen:          frozen,
		Schema:          schemaInfo,
		Normalize:       normalize,
	}

	builder.associations = append(builder.associations, info)
	return builder
}

func (builder *AssociationRequestBuilder) build() AssociationCreateOrUpdateRequest {
	if builder.keyAssocInitCalled {
		builder.associations = append(builder.associations, builder.keyAssociation)
	}
	if builder.valueAssocInitCalled {
		builder.associations = append(builder.associations, builder.valueAssociation)
	}

	return AssociationCreateOrUpdateRequest{
		ResourceName:      builder.resourceName,
		ResourceNamespace: builder.resourceNamespace,
		ResourceID:        builder.resourceID,
		ResourceType:      builder.resourceType,
		Associations:      builder.associations,
	}
}

type associationCreator func(request AssociationCreateOrUpdateRequest) (AssociationResponse, error)

func registerTestAvroSchemaInSchemaRegistry(client Client, subject string, schema string, normalize bool) {
	_, err := client.Register(subject, SchemaInfo{Schema: schema}, normalize)
	maybeFail("Register schema", err)
}

func testInvalidAssociationCreateRequestHelper(client Client, creator associationCreator) {
	registerTestAvroSchemaInSchemaRegistry(client, defaultKeySubject, simpleAvroSchema, true)
	registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, simpleAvroSchema, true)

	validKeyAssocInfo1 := AssociationCreateOrUpdateInfo{Subject: defaultKeySubject, AssociationType: key}
	validValueAssocInfo1 := AssociationCreateOrUpdateInfo{Subject: defaultValueSubject, AssociationType: value}

	invalidRequests := []AssociationCreateOrUpdateRequest{
		// No resource name
		{
			ResourceName:      "",
			ResourceNamespace: defaultResourceNamespace,
			ResourceID:        defaultResourceID,
			ResourceType:      defaultResourceType,
			Associations:      []AssociationCreateOrUpdateInfo{validKeyAssocInfo1, validValueAssocInfo1},
		},
		// No resource namespace
		{
			ResourceName:      defaultResourceName,
			ResourceNamespace: "",
			ResourceID:        defaultResourceID,
			ResourceType:      defaultResourceType,
			Associations:      []AssociationCreateOrUpdateInfo{validKeyAssocInfo1, validValueAssocInfo1},
		},
		// No resource id
		{
			ResourceName:      defaultResourceName,
			ResourceNamespace: defaultResourceNamespace,
			ResourceID:        "",
			ResourceType:      topic,
			Associations:      []AssociationCreateOrUpdateInfo{validKeyAssocInfo1, validValueAssocInfo1},
		},
		// No associations
		{
			ResourceName:      defaultResourceName,
			ResourceNamespace: defaultResourceNamespace,
			ResourceID:        defaultResourceID,
			ResourceType:      topic,
			Associations:      nil,
		},
		// Duplicate association types
		{
			ResourceName:      defaultResourceName,
			ResourceNamespace: defaultResourceNamespace,
			ResourceID:        "",
			ResourceType:      topic,
			Associations:      []AssociationCreateOrUpdateInfo{validKeyAssocInfo1, validKeyAssocInfo1},
		},
		// No subject name in AssociationCreateOrUpdateInfo
		{
			ResourceName:      defaultResourceName,
			ResourceNamespace: defaultResourceNamespace,
			ResourceID:        defaultResourceID,
			ResourceType:      topic,
			Associations: []AssociationCreateOrUpdateInfo{
				{
					Subject:         "",
					AssociationType: value,
				},
			},
		},
		// Unsupported ResourceType
		{
			ResourceName:      defaultResourceName,
			ResourceNamespace: defaultResourceNamespace,
			ResourceID:        defaultResourceID,
			ResourceType:      "topic2",
			Associations:      []AssociationCreateOrUpdateInfo{validKeyAssocInfo1, validValueAssocInfo1},
		},
		// Unsupported AssociationType
		{
			ResourceName:      defaultResourceName,
			ResourceNamespace: defaultResourceNamespace,
			ResourceID:        defaultResourceID,
			ResourceType:      topic,
			Associations: []AssociationCreateOrUpdateInfo{
				{
					Subject:         defaultValueSubject,
					AssociationType: "value2",
				},
			},
		},
		// Weak association with frozen to be true
		{
			ResourceName:      defaultResourceName,
			ResourceNamespace: defaultResourceNamespace,
			ResourceID:        defaultResourceID,
			ResourceType:      topic,
			Associations: []AssociationCreateOrUpdateInfo{
				{
					Subject:         defaultValueSubject,
					AssociationType: value,
					Lifecycle:       WEAK,
					Frozen:          true,
				},
			},
		},
	}
	for _, invalidRequest := range invalidRequests {
		_, err := creator(invalidRequest)
		maybeFail("CreateAssociation with invalid request", expect(err != nil, true))
	}
}

func TestInvalidAssociationCreateRequest(t *testing.T) {
	// Use mock client for testing
	conf := NewConfig("mock://")

	t.Run("createEndpoint", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testInvalidAssociationCreateRequestHelper(client, client.CreateAssociation)
	})

	// upsertEndpoint is createOrUpdate endpoint
	t.Run("upsertEndpoint", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testInvalidAssociationCreateRequestHelper(client, client.CreateOrUpdateAssociation)
	})
}

func testMinimumValidCreateAssociationRequestHelper(client Client, associationCreator associationCreator) {
	// Pre-create value subject for testing
	registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, simpleStringSchema, true)

	createRequest := (&AssociationRequestBuilder{}).
		resource(defaultResourceName, defaultResourceNamespace, defaultResourceID, "").
		association(defaultValueSubject, "", "", false, "", false).
		build()

	createResponse, err := associationCreator(createRequest)
	maybeFail("Create association", err)

	// Assertions
	maybeFail("CreateAssociation with invalid response", err,
		expect(createResponse.ResourceName, createResponse.ResourceName),
		expect(createResponse.ResourceNamespace, createResponse.ResourceNamespace),
		expect(createResponse.ResourceID, createResponse.ResourceID),
		expect(createResponse.ResourceType, "topic"),
		expect(len(createResponse.Associations), 1),
		expect(createResponse.Associations[0].Subject, createResponse.Associations[0].Subject),
		expect(createResponse.Associations[0].AssociationType, "value"),
		expect(createResponse.Associations[0].Lifecycle, WEAK),
		expect(createResponse.Associations[0].Frozen, false),
		expect(createResponse.Associations[0].Schema == nil, true))
}

func TestMinimumValidCreateAssociationRequest(t *testing.T) {
	// Use mock client for testing
	conf := NewConfig("mock://")

	t.Run("createEndpoint", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testMinimumValidCreateAssociationRequestHelper(client, client.CreateAssociation)
	})

	// upsertEndpoint is createOrUpdate endpoint
	t.Run("upsertEndpoint", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testMinimumValidCreateAssociationRequestHelper(client, client.CreateOrUpdateAssociation)
	})
}

func testCreateOneAssociationHelper(client Client, associationCreator associationCreator) {
	// Pre-create subjects
	registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, simpleAvroSchema, true)

	// Create a new value association using an existing subject.
	createRequest := (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		build()

	_, err := associationCreator(createRequest)
	maybeFail("AssociationCreateOrUpdateRequest should succeed", err)

	// Create association request is idempotent. Re-issue the same create request should succeed.
	_, err = associationCreator(createRequest)
	maybeFail("AssociationCreateOrUpdateRequest should succeed (idempotency)", err)

	// After the second request, the subject and resource should still have just one association.
	associations, err := client.GetAssociationsBySubject(defaultValueSubject, "", nil, "", 0, -1)
	maybeFail("GetAssociationsBySubject should succeed", err)

	if associations == nil {
		maybeFail("Associations should not be nil", fmt.Errorf("associations is nil"))
	}

	if len(associations) != 1 {
		maybeFail("Should have exactly 1 association",
			fmt.Errorf("expected 1 association, got %d", len(associations)))
	}

	// Create a key association using a new subject without schema should fail.
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		keySubject(defaultKeySubject).
		build()

	_, err = associationCreator(createRequest)
	if err == nil {
		maybeFail("Expected error - new subject without schema",
			fmt.Errorf("expected error but got nil"))
	}

	// Create a key association using a new subject with a schema should succeed.
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		keySubject(defaultKeySubject).
		keySchema(evolvedAvroSchema).
		build()

	_, err = associationCreator(createRequest)
	maybeFail("AssociationCreateOrUpdateRequest should succeed", err)
}

func TestCreateOneAssociation(t *testing.T) {
	// Use mock client for testing
	conf := NewConfig("mock://")

	t.Run("createEndpoint", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateOneAssociationHelper(client, client.CreateAssociation)
	})

	// upsertEndpoint is createOrUpdate endpoint
	t.Run("upsertEndpoint", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateOneAssociationHelper(client, client.CreateOrUpdateAssociation)
	})
}

func TestUpdateOneAssociationViaCreateEndpoint(t *testing.T) {
	maybeFail = initFailFunc(t)

	// Use mock client for testing
	conf := NewConfig("mock://")
	client, err := NewClient(conf)
	maybeFail("schema registry client instantiation", err)

	// Pre-create subjects
	registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, simpleAvroSchema, true)

	// Create a new value association using an existing subject.
	createRequest := (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		build()

	_, err = client.CreateAssociation(createRequest)
	maybeFail("CreateAssociation should succeed", err)

	// Re-issue the same request with different association property (lifecycle) should error out.
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		valueLifecycle("STRONG").
		build()

	_, err = client.CreateAssociation(createRequest)
	maybeFail("Create association with different property should fail",
		expect(err != nil, true))

	// Create an existing value association with updated schema should also error out.
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		valueSchema(evolvedAvroSchema).
		build()

	_, err = client.CreateAssociation(createRequest)
	maybeFail("Create association with different schema should fail",
		expect(err != nil, true))
}

func TestUpdateOneAssociationViaUpsertEndpoint(t *testing.T) {
	maybeFail = initFailFunc(t)

	// The upsert endpoint is CreateOrUpdateAssociation endpoint.
	// Use mock client for testing
	conf := NewConfig("mock://")
	client, err := NewClient(conf)
	maybeFail("schema registry client instantiation", err)

	// Pre-create subjects
	registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, simpleAvroSchema, true)

	// Create a new value association using an existing subject.
	// final state: strong, non-frozen
	createRequest := (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		valueLifecycle("STRONG").
		build()

	_, err = client.CreateOrUpdateAssociation(createRequest)
	maybeFail("CreateOrUpdateAssociation should succeed", err)

	// Change strong, not frozen lifecycle to weak, non-frozen lifecycle should succeed.
	// final state: weak, non-frozen
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		valueLifecycle("WEAK").
		build()

	_, err = client.CreateOrUpdateAssociation(createRequest)
	maybeFail("Update association to weak should succeed", err)

	// Update schema should succeed.
	// final state: weak, non-frozen
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		valueSchema(evolvedAvroSchema).
		build()

	_, err = client.CreateOrUpdateAssociation(createRequest)
	maybeFail("Update association with different schema should succeed", err)

	// Change weak, non-frozen lifecycle to weak, frozen lifecycle should fail.
	// final state: weak, non-frozen
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		valueFrozen(true).
		build()

	_, err = client.CreateOrUpdateAssociation(createRequest)
	maybeFail("Update association to weak frozen should fail",
		expect(err != nil, true))

	// Change to strong, not frozen should succeed.
	// final state: strong, non-frozen
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		valueLifecycle("STRONG").
		build()

	_, err = client.CreateOrUpdateAssociation(createRequest)
	maybeFail("Update association back to strong should succeed", err)

	// Change to frozen should succeed.
	// final state: strong, frozen
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		valueFrozen(true).
		build()

	_, err = client.CreateOrUpdateAssociation(createRequest)
	maybeFail("Update association to strong frozen should succeed", err)

	// Change to non-frozen should fail.
	// final state: strong, frozen
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		valueFrozen(false).
		build()

	_, err = client.CreateOrUpdateAssociation(createRequest)
	maybeFail("Update association back to strong non-frozen should fail",
		expect(err != nil, true))

	// Change to weak should fail.
	// final state: strong, frozen
	createRequest = (&AssociationRequestBuilder{}).
		defaultResource().
		valueSubject(defaultValueSubject).
		valueLifecycle("WEAK").
		build()

	_, err = client.CreateOrUpdateAssociation(createRequest)
	maybeFail("Update association back to weak when frozen should fail",
		expect(err != nil, true))
}

func testCreateAssociationsHelperBothExistingSubjects(client Client, associationCreator associationCreator) {
	// Pre-create subjects
	registerTestAvroSchemaInSchemaRegistry(client, defaultKeySubject, simpleAvroSchema, true)
	registerTestAvroSchemaInSchemaRegistry(client, defaultValueSubject, simpleAvroSchema, true)

	// Scenario 1: Both associations using existing subjects
	createRequest := (&AssociationRequestBuilder{}).
		defaultResource().
		keySubject(defaultKeySubject).
		valueSubject(defaultValueSubject).
		build()

	_, err := associationCreator(createRequest)
	maybeFail("CreateAssociation with both existing subjects should succeed", err)
}

func testCreateAssociationsHelperOneExistingOneNewSubject(client Client, associationCreator associationCreator) {
	// Scenario 2: One using existing subject, one creating new subject
	registerTestAvroSchemaInSchemaRegistry(client, defaultKeySubject, simpleAvroSchema, true)

	createRequest := (&AssociationRequestBuilder{}).
		defaultResource().
		keySubject(defaultKeySubject).
		valueSubject(defaultValueSubject).
		valueSchema(simpleAvroSchema).
		build()

	_, err := associationCreator(createRequest)
	maybeFail("CreateAssociation with one existing and one new subject should succeed", err)
}

func testCreateAssociationsHelperBothNewSubjects(associationCreator associationCreator) {
	// Scenario 3: Both creating new subjects
	createRequest := (&AssociationRequestBuilder{}).
		defaultResource().
		keySubject(defaultKeySubject).
		keySchema(simpleAvroSchema).
		valueSubject(defaultValueSubject).
		valueSchema(simpleAvroSchema).
		build()

	_, err := associationCreator(createRequest)
	maybeFail("CreateAssociation with both new subjects should succeed", err)
}

func TestCreateMultipleAssociations(t *testing.T) {
	// Use mock client for testing
	conf := NewConfig("mock://")

	t.Run("createEndpointBothExist", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsHelperBothExistingSubjects(client, client.CreateAssociation)
	})
	t.Run("createEndpointOneExistOneNew", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsHelperOneExistingOneNewSubject(client, client.CreateAssociation)
	})
	t.Run("testCreateAssociationsHelperBothNewSubjects", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsHelperBothNewSubjects(client.CreateAssociation)
	})

	// upsertEndpoint is createOrUpdate endpoint
	t.Run("upsertEndpointBothExist", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsHelperBothExistingSubjects(client, client.CreateOrUpdateAssociation)
	})
	t.Run("upsertEndpointOneExistOneNew", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsHelperBothExistingSubjects(client, client.CreateOrUpdateAssociation)
	})
	t.Run("upsertEndpointBothNew", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsHelperBothNewSubjects(client.CreateOrUpdateAssociation)
	})
}

func testCreateAssociationsForOneSubjectHelper(resourceFoo resource, resourceFooLifecycle LifecyclePolicy,
	resourceBar resource, resourceBarLifecycle LifecyclePolicy, subject string, client Client, associationCreator associationCreator) {
	registerTestAvroSchemaInSchemaRegistry(client, subject, simpleAvroSchema, true)

	fooRequest := (&AssociationRequestBuilder{}).
		resource(resourceFoo.resourceName, resourceFoo.resourceNamespace, resourceFoo.resourceID, resourceFoo.resourceType).
		valueSubject(subject).
		valueLifecycle(string(resourceFooLifecycle)).
		build()

	_, err := associationCreator(fooRequest)
	// Create the second time shouldn't have impact on the result. Test idempotency.
	_, err = associationCreator(fooRequest)
	maybeFail("CreateAssociation for Foo should succeed", err)

	result, err := client.GetAssociationsByResourceID(resourceFoo.resourceID, "", nil, "", 0, -1)
	maybeFail("GetAssociationsByResourceId should succeed", err)

	maybeFail("Should have 1 association", expect(len(result), 1))
	maybeFail("Guid should not be empty", expect(result[0].GUID != "", true))

	barRequest := (&AssociationRequestBuilder{}).
		resource(resourceBar.resourceName, resourceBar.resourceNamespace, resourceBar.resourceID, resourceBar.resourceType).
		valueSubject(subject).
		valueLifecycle(string(resourceBarLifecycle)).
		build()

	_, err = client.CreateOrUpdateAssociation(barRequest)

	if resourceFooLifecycle == WEAK && resourceBarLifecycle == WEAK {
		// The only case where the bar creation will succeed.
		maybeFail("CreateAssociation for Bar with WEAK should succeed", err)

		result, err = client.GetAssociationsByResourceID(resourceBar.resourceID, "", nil, "", 0, -1)
		maybeFail("GetAssociationsByResourceId for Bar should succeed", err)
		maybeFail("Should have 1 association", expect(len(result), 1))
		maybeFail("Guid should not be empty", expect(result[0].GUID != "", true))

		// Verify subject has 2 associations
		result, err = client.GetAssociationsBySubject(subject, "", nil, "", 0, -1)
		maybeFail("GetAssociationsBySubject should succeed", err)
		maybeFail("Subject should have 2 associations", expect(len(result), 2))
	} else {
		// All other lifecycle combinations should fail.
		maybeFail("Cannot create bar association ", expect(err != nil, true))
	}
}

func TestCreateStrongAndWeakAssociationsForTheSameSubject(t *testing.T) {
	// Use mock client for testing
	conf := NewConfig("mock://")
	resourceFoo := resource{
		resourceName:      "foo",
		resourceNamespace: defaultResourceNamespace,
		resourceID:        "id-foo",
		resourceType:      topic,
	}
	resourceBar := resource{
		resourceName:      "bar",
		resourceNamespace: defaultResourceNamespace,
		resourceID:        "id-bar",
		resourceType:      topic,
	}
	subject := "subjectFoo"

	t.Run("createEndpoint_STRONG_STRONG", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsForOneSubjectHelper(resourceFoo, STRONG, resourceBar, STRONG, subject, client, client.CreateAssociation)
	})
	t.Run("createEndpoint_STRONG_WEAK", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsForOneSubjectHelper(resourceFoo, STRONG, resourceBar, WEAK, subject, client, client.CreateAssociation)
	})
	t.Run("createEndpoint_WEAK_STRONG", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsForOneSubjectHelper(resourceFoo, WEAK, resourceBar, STRONG, subject, client, client.CreateAssociation)
	})
	t.Run("createEndpoint_WEAK_WEAK", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsForOneSubjectHelper(resourceFoo, WEAK, resourceBar, WEAK, subject, client, client.CreateAssociation)
	})

	// upsertEndpoint is createOrUpdate endpoint
	t.Run("upsertEndpoint", func(t *testing.T) {
		maybeFail = initFailFunc(t)
		client, err := NewClient(conf)
		maybeFail("schema registry client instantiation", err)

		testCreateAssociationsForOneSubjectHelper(resourceFoo, WEAK, resourceBar, WEAK, subject, client, client.CreateOrUpdateAssociation)
	})
}

func TestGetAssociationsWithFilters(t *testing.T) {
	maybeFail = initFailFunc(t)

	// Use mock client for testing
	conf := NewConfig("mock://")
	client, err := NewClient(conf)
	maybeFail("schema registry client instantiation", err)

	// Setup
	// Create associations: key=STRONG, value=WEAK
	createRequest := (&AssociationRequestBuilder{}).
		defaultResource().
		keySubject(defaultKeySubject).
		keySchema(simpleAvroSchema).
		keyLifecycle(string(STRONG)).
		valueSubject(defaultValueSubject).
		valueSchema(simpleAvroSchema).
		valueLifecycle(string(WEAK)).
		build()

	_, err = client.CreateOrUpdateAssociation(createRequest)
	maybeFail("CreateOrUpdateAssociation should succeed", err)

	// Query by subject with lifecycle filter "weak" - should return error
	_, err = client.GetAssociationsBySubject(
		defaultKeySubject, "", []string{key, value}, "weak", 0, -1)
	maybeFail("GetAssociationsBySubject with lower case lifecycle should fail",
		expect(err != nil, true))

	// Query by subject with lifecycle filter "WEAK" - should return 0
	associations, err := client.GetAssociationsBySubject(
		defaultKeySubject, "", []string{key, value}, "WEAK", 0, -1)
	maybeFail("GetAssociationsBySubject should succeed", err)
	maybeFail("Should return 0 associations",
		expect(len(associations), 0))

	// Query by subject with lifecycle filter "STRONG" - should return 1
	associations, err = client.GetAssociationsBySubject(
		defaultKeySubject, "", []string{key, value}, "STRONG", 0, -1)
	maybeFail("GetAssociationsBySubject should succeed", err)
	maybeFail("Should return 1 association",
		expect(len(associations), 1))

	// Query by resourceID without lifecycle filter - should return 2
	associations, err = client.GetAssociationsByResourceID(
		defaultResourceID, "", []string{key, value}, "", 0, -1)
	maybeFail("GetAssociationsByResourceId should succeed", err)
	maybeFail("Should return 2 associations",
		expect(len(associations), 2))

	// Query by resourceName with a wrong resource name - should return 0
	associations, err = client.GetAssociationsByResourceName(
		"WrongResourceName", "", "", []string{key, value}, "", 0, -1)
	maybeFail("GetAssociationsByResourceName should succeed", err)
	maybeFail("Should return 0 associations",
		expect(len(associations), 0))

	// Query by resourceName without lifecycle filter - should return 2
	associations, err = client.GetAssociationsByResourceName(
		defaultResourceName, "", "", []string{key, value}, "", 0, -1)
	maybeFail("GetAssociationsByResourceName should succeed", err)
	maybeFail("Should return 2 associations",
		expect(len(associations), 2))
}

func TestDeleteAssociation(t *testing.T) {
	maybeFail = initFailFunc(t)

	// Use mock client for testing
	conf := NewConfig("mock://")
	client, err := NewClient(conf)
	maybeFail("schema registry client instantiation", err)

	// Create one strong key association, and one weak value association
	keySubject, valueSubject := "test1Key", "test1Value"
	resourceName, resourceID, resourceNamespace := "test1", "test1-id", "lkc1"
	schemaInfo := SchemaInfo{
		Schema: "{\"namespace\":\"basicavro\",\"type\":\"record\",\"name\":\"Payment\",\"fields\":[{\"type\":\"string\",\"name\":\"id\"}]}",
	}

	createRequest := AssociationCreateOrUpdateRequest{ResourceName: resourceName, ResourceNamespace: resourceNamespace, ResourceID: resourceID,
		Associations: []AssociationCreateOrUpdateInfo{AssociationCreateOrUpdateInfo{Subject: keySubject, Schema: &schemaInfo, Lifecycle: STRONG, AssociationType: "key"},
			AssociationCreateOrUpdateInfo{Subject: valueSubject, Schema: &schemaInfo, Lifecycle: WEAK, AssociationType: "value"}}}
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
	associations, err = client.GetAssociationsByResourceName(resourceName, resourceNamespace, "", nil, "", 0, -1)
	maybeFail("GetAssociationsByResourceName", err, expect(associations == nil || len(associations) == 0, true))

	// Without cascade delete
	keySubject, valueSubject = "test2Key", "test2Value"
	resourceName, resourceID = "test2", "test2-id"
	createRequest = AssociationCreateOrUpdateRequest{ResourceName: resourceName, ResourceNamespace: resourceNamespace, ResourceID: resourceID,
		Associations: []AssociationCreateOrUpdateInfo{AssociationCreateOrUpdateInfo{Subject: keySubject, Schema: &schemaInfo, Lifecycle: STRONG, AssociationType: "key"},
			AssociationCreateOrUpdateInfo{Subject: valueSubject, Schema: &schemaInfo, Lifecycle: WEAK, AssociationType: "value"}}}
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
	_, err = client.GetAssociationsByResourceName(resourceName, "lkc1", "", nil, "", 0, -1)
	maybeFail("GetAssociationsByResourceName", err, expect(associations == nil || len(associations) == 0, true))

	// Delete a non-existing association. Should return nothing.
	err = client.DeleteAssociations(resourceID, "", nil, false)
	maybeFail("DeleteAssociation for non-existing association", err)

	// Delete a frozen association with cascade = false. Should return error.
	keySubject, valueSubject = "test3Key", "test3Value"
	resourceName, resourceID = "test3", "test3-id"
	createRequest = AssociationCreateOrUpdateRequest{ResourceName: resourceName, ResourceNamespace: "lkc1", ResourceID: resourceID,
		Associations: []AssociationCreateOrUpdateInfo{AssociationCreateOrUpdateInfo{Subject: keySubject, Schema: &schemaInfo, Lifecycle: STRONG, AssociationType: "key", Frozen: true},
			AssociationCreateOrUpdateInfo{Subject: valueSubject, Schema: &schemaInfo, Lifecycle: WEAK, AssociationType: "value"}}}
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
}

func init() {
	if !testconfRead() {
		log.Print("WARN: Missing testconf.json, using mock client")
	}
}
