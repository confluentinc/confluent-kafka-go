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
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/internal"
)

const noSubject = ""
const defaultResourceType = "topic"
const defaultAssociationType = "value"
const defaultLifecyclePolicy = STRONG

var validResourceTypesAndAssociationTypesMap = map[string][]string{
	defaultResourceType: {"key", defaultAssociationType},
}

type counter struct {
	count int
}

func (c *counter) currentValue() int {
	return c.count
}

func (c *counter) increment() int {
	c.count++
	return c.count
}

type versionCacheEntry struct {
	version     int
	softDeleted bool
}

type infoCacheEntry struct {
	info        *SchemaInfo
	softDeleted bool
}

type metadataCacheEntry struct {
	metadata    *SchemaMetadata
	softDeleted bool
}

type resourceAndAssociationType struct {
	resourceId        string
	resourceName      string
	resourceNamespace string
	resourceType      string
	associationType   string
}

/* HTTP(S) Schema Registry Client and schema caches */
type mockclient struct {
	sync.Mutex
	config                               *Config
	url                                  *url.URL
	infoToSchemaCache                    map[subjectJSON]metadataCacheEntry
	infoToSchemaCacheLock                sync.RWMutex
	idToSchemaCache                      map[subjectID]infoCacheEntry
	idToSchemaCacheLock                  sync.RWMutex
	guidToSchemaCache                    map[string]infoCacheEntry
	guidToSchemaCacheLock                sync.RWMutex
	schemaToVersionCache                 map[subjectJSON]versionCacheEntry
	schemaToVersionCacheLock             sync.RWMutex
	configCache                          map[string]ServerConfig
	configCacheLock                      sync.RWMutex
	subjectToAssocCache                  map[string][]*Association
	subjectToAssocCacheLock              sync.RWMutex
	resourceAndAssocTypeToAssocCache     map[resourceAndAssociationType]*Association
	resourceAndAssocTypeToAssocCacheLock sync.RWMutex
	resourceIdToAssocCache               map[string][]*Association
	resourceIdToAssocCacheLock           sync.RWMutex
	counter                              counter
}

var _ Client = new(mockclient)

// Fetch all contexts used
// Returns a string slice containing contexts
func (c *mockclient) GetAllContexts() ([]string, error) {
	return []string{"."}, nil
}

// Config returns the client config
func (c *mockclient) Config() *Config {
	return c.config
}

// Register registers Schema aliased with subject
func (c *mockclient) Register(subject string, schema SchemaInfo, normalize bool) (id int, err error) {
	metadata, err := c.RegisterFullResponse(subject, schema, normalize)
	if err != nil {
		return -1, err
	}
	return metadata.ID, err
}

// RegisterFullResponse registers Schema aliased with subject
func (c *mockclient) RegisterFullResponse(subject string, schema SchemaInfo, normalize bool) (result SchemaMetadata, err error) {
	schemaJSON, err := schema.MarshalJSON()
	if err != nil {
		return SchemaMetadata{
			ID: -1,
		}, err
	}
	cacheKey := subjectJSON{
		subject: subject,
		json:    string(schemaJSON),
	}
	c.infoToSchemaCacheLock.RLock()
	cacheEntryVal, ok := c.infoToSchemaCache[cacheKey]
	if cacheEntryVal.softDeleted {
		ok = false
	}
	c.infoToSchemaCacheLock.RUnlock()
	if ok {
		return *cacheEntryVal.metadata, nil
	}

	id, guid, err := c.getIDFromRegistry(subject, schema)
	if err != nil {
		return SchemaMetadata{
			ID: -1,
		}, err
	}
	result = SchemaMetadata{
		SchemaInfo: schema,
		ID:         id,
		GUID:       guid,
	}
	c.infoToSchemaCacheLock.Lock()
	c.infoToSchemaCache[cacheKey] = metadataCacheEntry{&result, false}
	c.infoToSchemaCacheLock.Unlock()
	return result, nil
}

func (c *mockclient) getIDFromRegistry(subject string, schema SchemaInfo) (int, string, error) {
	var id = -1
	c.idToSchemaCacheLock.RLock()
	for key, value := range c.idToSchemaCache {
		if key.subject == subject && schemasEqual(*value.info, schema) {
			id = key.id
			break
		}
	}
	c.idToSchemaCacheLock.RUnlock()
	var guid string
	c.guidToSchemaCacheLock.RLock()
	for key, value := range c.guidToSchemaCache {
		if schemasEqual(*value.info, schema) {
			guid = key
			break
		}
	}
	c.guidToSchemaCacheLock.RUnlock()
	err := c.generateVersion(subject, schema)
	if err != nil {
		return -1, "", err
	}
	if id < 0 {
		id = c.counter.increment()
		idCacheKey := subjectID{
			subject: subject,
			id:      id,
		}
		c.idToSchemaCacheLock.Lock()
		c.idToSchemaCache[idCacheKey] = infoCacheEntry{&schema, false}
		c.idToSchemaCacheLock.Unlock()

		guid = uuid.New().String()
		c.guidToSchemaCacheLock.Lock()
		c.guidToSchemaCache[guid] = infoCacheEntry{&schema, false}
		c.guidToSchemaCacheLock.Unlock()
	}
	return id, guid, nil
}

func (c *mockclient) generateVersion(subject string, schema SchemaInfo) error {
	versions := c.allVersions(subject)
	var newVersion int
	if len(versions) == 0 {
		newVersion = 1
	} else {
		newVersion = versions[len(versions)-1] + 1
	}
	schemaJSON, err := schema.MarshalJSON()
	if err != nil {
		return err
	}
	cacheKey := subjectJSON{
		subject: subject,
		json:    string(schemaJSON),
	}
	c.schemaToVersionCacheLock.Lock()
	c.schemaToVersionCache[cacheKey] = versionCacheEntry{newVersion, false}
	c.schemaToVersionCacheLock.Unlock()
	return nil
}

// GetBySubjectAndID returns the schema identified by id
// Returns Schema object on success
func (c *mockclient) GetBySubjectAndID(subject string, id int) (schema SchemaInfo, err error) {
	cacheKey := subjectID{
		subject: subject,
		id:      id,
	}
	c.idToSchemaCacheLock.RLock()
	cacheEntryValue, ok := c.idToSchemaCache[cacheKey]
	c.idToSchemaCacheLock.RUnlock()
	if ok {
		return *cacheEntryValue.info, nil
	}
	posErr := url.Error{
		Op:  "GET",
		URL: c.url.String() + fmt.Sprintf(internal.SchemasBySubject, id, url.QueryEscape(subject)),
		Err: errors.New("Subject Not Found"),
	}
	return SchemaInfo{}, &posErr
}

// GetByGUID returns the schema identified by guid
// Returns Schema object on success
func (c *mockclient) GetByGUID(guid string) (schema SchemaInfo, err error) {
	c.guidToSchemaCacheLock.RLock()
	cacheEntryValue, ok := c.guidToSchemaCache[guid]
	c.guidToSchemaCacheLock.RUnlock()
	if ok {
		return *cacheEntryValue.info, nil
	}
	posErr := url.Error{
		Op:  "GET",
		URL: c.url.String() + fmt.Sprintf(internal.SchemasByGUID, guid),
		Err: errors.New("Schema Not Found"),
	}
	return SchemaInfo{}, &posErr
}

func (c *mockclient) GetSubjectsAndVersionsByID(id int) (subjectsAndVersions []SubjectAndVersion, err error) {
	subjectsAndVersions = make([]SubjectAndVersion, 0)

	c.infoToSchemaCacheLock.RLock()
	c.schemaToVersionCacheLock.RLock()

	for key, value := range c.infoToSchemaCache {
		if !value.softDeleted && value.metadata.ID == id {
			var schemaJSON []byte
			schemaJSON, err = value.metadata.SchemaInfo.MarshalJSON()
			if err != nil {
				return
			}

			versionCacheKey := subjectJSON{
				subject: key.subject,
				json:    string(schemaJSON),
			}

			versionEntry, ok := c.schemaToVersionCache[versionCacheKey]
			if !ok {
				err = fmt.Errorf("entry in version cache not found")
				return
			}

			subjectsAndVersions = append(subjectsAndVersions, SubjectAndVersion{
				Subject: key.subject,
				Version: versionEntry.version,
			})
		}
	}

	c.schemaToVersionCacheLock.RUnlock()
	c.infoToSchemaCacheLock.RUnlock()

	if len(subjectsAndVersions) == 0 {
		err = &url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(internal.SubjectsAndVersionsByID, id),
			Err: errors.New("schema ID not found"),
		}
	}

	sort.Slice(subjectsAndVersions, func(i, j int) bool {
		return subjectsAndVersions[i].Subject < subjectsAndVersions[j].Subject
	})
	return
}

// GetID checks if a schema has been registered with the subject. Returns ID if the registration can be found
func (c *mockclient) GetID(subject string, schema SchemaInfo, normalize bool) (id int, err error) {
	metadata, err := c.GetIDFullResponse(subject, schema, normalize)
	if err != nil {
		return -1, err
	}
	return metadata.ID, err
}

// GetIDFullResponse checks if a schema has been registered with the subject. Returns ID if the registration can be found
func (c *mockclient) GetIDFullResponse(subject string, schema SchemaInfo, normalize bool) (result SchemaMetadata, err error) {
	schemaJSON, err := schema.MarshalJSON()
	if err != nil {
		return SchemaMetadata{
			ID: -1,
		}, err
	}
	cacheKey := subjectJSON{
		subject: subject,
		json:    string(schemaJSON),
	}
	c.infoToSchemaCacheLock.RLock()
	cacheEntryVal, ok := c.infoToSchemaCache[cacheKey]
	if cacheEntryVal.softDeleted {
		ok = false
	}
	c.infoToSchemaCacheLock.RUnlock()
	if ok {
		return *cacheEntryVal.metadata, nil
	}

	posErr := url.Error{
		Op:  "GET",
		URL: c.url.String() + fmt.Sprintf(internal.Subjects, url.PathEscape(subject)),
		Err: errors.New("Subject Not found"),
	}
	return SchemaMetadata{
		ID: -1,
	}, &posErr
}

// GetLatestSchemaMetadata fetches latest version registered with the provided subject
// Returns SchemaMetadata object
func (c *mockclient) GetLatestSchemaMetadata(subject string) (result SchemaMetadata, err error) {
	version := c.latestVersion(subject)
	if version < 0 {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(internal.Versions, url.PathEscape(subject), "latest"),
			Err: errors.New("Subject Not found"),
		}
		return SchemaMetadata{}, &posErr
	}
	return c.GetSchemaMetadata(subject, version)
}

// GetSchemaMetadata fetches the requested subject schema identified by version
// Returns SchemaMetadata object
func (c *mockclient) GetSchemaMetadata(subject string, version int) (result SchemaMetadata, err error) {
	return c.GetSchemaMetadataIncludeDeleted(subject, version, false)
}

// GetSchemaMetadataIncludeDeleted fetches the requested subject schema identified by version and deleted flag
// Returns SchemaMetadata object
func (c *mockclient) GetSchemaMetadataIncludeDeleted(subject string, version int, deleted bool) (result SchemaMetadata, err error) {
	var json string
	c.schemaToVersionCacheLock.RLock()
	for key, value := range c.schemaToVersionCache {
		if key.subject == subject && value.version == version && (!value.softDeleted || deleted) {
			json = key.json
			break
		}
	}
	c.schemaToVersionCacheLock.RUnlock()
	if json == "" {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(internal.Versions, url.PathEscape(subject), version),
			Err: errors.New("Subject Not found"),
		}
		return SchemaMetadata{}, &posErr
	}

	var info SchemaInfo
	err = info.UnmarshalJSON([]byte(json))
	if err != nil {
		return SchemaMetadata{}, err
	}
	var id = -1
	c.idToSchemaCacheLock.RLock()
	for key, value := range c.idToSchemaCache {
		if key.subject == subject && schemasEqual(*value.info, info) && (!value.softDeleted || deleted) {
			id = key.id
			break
		}
	}
	c.idToSchemaCacheLock.RUnlock()
	if id == -1 {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(internal.Versions, url.PathEscape(subject), version),
			Err: errors.New("Subject Not found"),
		}
		return SchemaMetadata{}, &posErr
	}
	var guid string
	c.guidToSchemaCacheLock.RLock()
	for key, value := range c.guidToSchemaCache {
		if schemasEqual(*value.info, info) && (!value.softDeleted || deleted) {
			guid = key
			break
		}
	}
	c.guidToSchemaCacheLock.RUnlock()
	if guid == "" {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(internal.Versions, url.PathEscape(subject), version),
			Err: errors.New("Subject Not found"),
		}
		return SchemaMetadata{}, &posErr
	}
	return SchemaMetadata{
		SchemaInfo: info,

		ID:      id,
		GUID:    guid,
		Subject: subject,
		Version: version,
	}, nil
}

// GetLatestWithMetadata fetches the latest subject schema with the given metadata
// Returns SchemaMetadata object
func (c *mockclient) GetLatestWithMetadata(subject string, metadata map[string]string, deleted bool) (result SchemaMetadata, err error) {
	sb := strings.Builder{}
	for key, value := range metadata {
		_, _ = sb.WriteString("&key=")
		_, _ = sb.WriteString(key)
		_, _ = sb.WriteString("&value=")
		_, _ = sb.WriteString(value)
	}
	metadataStr := sb.String()
	var results []SchemaMetadata
	c.schemaToVersionCacheLock.RLock()
	for key, value := range c.schemaToVersionCache {
		if key.subject == subject && (!value.softDeleted || deleted) {
			var info SchemaInfo
			err = info.UnmarshalJSON([]byte(key.json))
			if err != nil {
				return SchemaMetadata{}, err
			}
			if info.Metadata != nil && isSubset(metadata, info.Metadata.Properties) {
				results = append(results, SchemaMetadata{
					SchemaInfo: info,
					Subject:    subject,
					Version:    value.version,
				})
			}
		}
	}
	result.Version = 0
	for _, schema := range results {
		if schema.Version > result.Version {
			result = schema
		}
	}
	c.schemaToVersionCacheLock.RUnlock()
	if result.Version <= 0 {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(internal.LatestWithMetadata, url.PathEscape(subject), deleted, metadataStr),
			Err: errors.New("Subject Not found"),
		}
		return SchemaMetadata{}, &posErr
	}

	result.ID = -1
	c.idToSchemaCacheLock.RLock()
	for key, value := range c.idToSchemaCache {
		if key.subject == subject && schemasEqual(*value.info, result.SchemaInfo) && (!value.softDeleted || deleted) {
			result.ID = key.id
			break
		}
	}
	c.idToSchemaCacheLock.RUnlock()
	if result.ID < 0 {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(internal.LatestWithMetadata, url.PathEscape(subject), deleted, metadataStr),
			Err: errors.New("Subject Not found"),
		}
		return SchemaMetadata{}, &posErr
	}
	return result, nil
}

func isSubset(containee map[string]string, container map[string]string) bool {
	for key, value := range containee {
		if container[key] != value {
			return false
		}
	}
	return true
}

// GetAllVersions fetches a list of all version numbers associated with the provided subject registration
// Returns integer slice on success
func (c *mockclient) GetAllVersions(subject string) (results []int, err error) {
	results = c.allVersions(subject)
	if len(results) == 0 {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(internal.Version, url.PathEscape(subject)),
			Err: errors.New("Subject Not Found"),
		}
		return nil, &posErr
	}
	return results, err
}

func (c *mockclient) allVersions(subject string) (results []int) {
	versions := make([]int, 0)
	c.schemaToVersionCacheLock.RLock()
	for key, value := range c.schemaToVersionCache {
		if key.subject == subject && !value.softDeleted {
			versions = append(versions, value.version)
		}
	}
	c.schemaToVersionCacheLock.RUnlock()
	sort.Ints(versions)
	return versions
}

func (c *mockclient) latestVersion(subject string) int {
	versions := c.allVersions(subject)
	if len(versions) == 0 {
		return -1
	}
	return versions[len(versions)-1]
}

func (c *mockclient) deleteVersion(key subjectJSON, version int, permanent bool) {
	if permanent {
		delete(c.schemaToVersionCache, key)
	} else {
		c.schemaToVersionCache[key] = versionCacheEntry{version, true}
	}
}

func (c *mockclient) deleteInfo(key subjectID, info *SchemaInfo, permanent bool) {
	if permanent {
		delete(c.idToSchemaCache, key)
	} else {
		c.idToSchemaCache[key] = infoCacheEntry{info, true}
	}
}

func (c *mockclient) deleteMetadata(key subjectJSON, metadata *SchemaMetadata, permanent bool) {
	if permanent {
		delete(c.infoToSchemaCache, key)
	} else {
		c.infoToSchemaCache[key] = metadataCacheEntry{metadata, true}
	}
}

// GetVersion finds the Subject SchemaMetadata associated with the provided schema
// Returns integer SchemaMetadata number
func (c *mockclient) GetVersion(subject string, schema SchemaInfo, normalize bool) (int, error) {
	return c.GetVersionIncludeDeleted(subject, schema, normalize, false)
}

// GetVersionIncludeDeleted finds the Subject SchemaMetadata associated with the schema and deleted flag
// Returns integer SchemaMetadata number
func (c *mockclient) GetVersionIncludeDeleted(subject string, schema SchemaInfo, normalize bool, deleted bool) (int, error) {
	schemaJSON, err := schema.MarshalJSON()
	if err != nil {
		return -1, err
	}
	cacheKey := subjectJSON{
		subject: subject,
		json:    string(schemaJSON),
	}
	c.schemaToVersionCacheLock.RLock()
	cacheEntryVal, ok := c.schemaToVersionCache[cacheKey]
	if cacheEntryVal.softDeleted {
		ok = false
	}
	c.schemaToVersionCacheLock.RUnlock()
	if ok {
		return cacheEntryVal.version, nil
	}
	posErr := url.Error{
		Op:  "GET",
		URL: c.url.String() + fmt.Sprintf(internal.Subjects, url.PathEscape(subject)),
		Err: errors.New("Subject Not Found"),
	}
	return -1, &posErr
}

// Fetch all Subjects registered with the schema Registry
// Returns a string slice containing all registered subjects
func (c *mockclient) GetAllSubjects() ([]string, error) {
	subjects := make([]string, 0)
	c.schemaToVersionCacheLock.RLock()
	for key, value := range c.schemaToVersionCache {
		if !value.softDeleted {
			subjects = append(subjects, key.subject)
		}
	}
	c.schemaToVersionCacheLock.RUnlock()
	sort.Strings(subjects)
	return subjects, nil
}

// Deletes provided Subject from registry
// Returns integer slice of versions removed by delete
func (c *mockclient) DeleteSubject(subject string, permanent bool) (deleted []int, err error) {
	// Check if subject has associations. If so, abort the deletion operation.
	associations, err := c.GetAssociationsBySubject(subject, "", nil, "", 0, -1)
	if err != nil {
		return nil, err
	}
	if len(associations) != 0 {
		posErr := url.Error{
			Op:  "DELETE",
			URL: c.url.String() + fmt.Sprintf(internal.SubjectsDelete, subject, permanent),
			Err: errors.New("Subject has associations. Deletion aborted."),
		}
		return nil, &posErr
	}

	c.infoToSchemaCacheLock.Lock()
	for key, value := range c.infoToSchemaCache {
		if key.subject == subject && (!value.softDeleted || permanent) {
			c.deleteMetadata(key, value.metadata, permanent)
		}
	}
	c.infoToSchemaCacheLock.Unlock()
	c.schemaToVersionCacheLock.Lock()
	for key, value := range c.schemaToVersionCache {
		if key.subject == subject && (!value.softDeleted || permanent) {
			c.deleteVersion(key, value.version, permanent)
			deleted = append(deleted, value.version)
		}
	}
	c.schemaToVersionCacheLock.Unlock()
	c.configCacheLock.Lock()
	delete(c.configCache, subject)
	c.configCacheLock.Unlock()
	if permanent {
		c.idToSchemaCacheLock.Lock()
		for key, value := range c.idToSchemaCache {
			if key.subject == subject && (!value.softDeleted || permanent) {
				c.deleteInfo(key, value.info, permanent)
			}
		}
		c.idToSchemaCacheLock.Unlock()
	}
	return deleted, nil
}

// DeleteSubjectVersion removes the version identified by delete from the subject's registration
// Returns integer id for the deleted version
func (c *mockclient) DeleteSubjectVersion(subject string, version int, permanent bool) (deleted int, err error) {
	c.schemaToVersionCacheLock.Lock()
	for key, value := range c.schemaToVersionCache {
		if key.subject == subject && value.version == version {
			c.deleteVersion(key, value.version, permanent)
			schemaJSON := key.json
			cacheKeySchema := subjectJSON{
				subject: subject,
				json:    schemaJSON,
			}
			c.infoToSchemaCacheLock.Lock()
			infoSchemaEntryVal, ok := c.infoToSchemaCache[cacheKeySchema]
			if ok {
				c.deleteMetadata(key, infoSchemaEntryVal.metadata, permanent)
			}
			c.infoToSchemaCacheLock.Unlock()
			if permanent && ok {
				cacheKeyID := subjectID{
					subject: subject,
					id:      infoSchemaEntryVal.metadata.ID,
				}
				c.idToSchemaCacheLock.Lock()
				idSchemaEntryVal, ok := c.idToSchemaCache[cacheKeyID]
				if ok {
					c.deleteInfo(cacheKeyID, idSchemaEntryVal.info, permanent)
				}
				c.idToSchemaCacheLock.Unlock()
			}
		}
	}
	c.schemaToVersionCacheLock.Unlock()
	return version, nil
}

// TestSubjectCompatibility verifies schema against all schemas in the subject
// Returns true if the schema is compatible, false otherwise
func (c *mockclient) TestSubjectCompatibility(subject string, schema SchemaInfo) (ok bool, err error) {
	return false, errors.New("unsupported operation")
}

// TestCompatibility verifies schema against the subject's compatibility policy
// Returns true if the schema is compatible, false otherwise
func (c *mockclient) TestCompatibility(subject string, version int, schema SchemaInfo) (ok bool, err error) {
	return false, errors.New("unsupported operation")
}

// Fetch compatibility level currently configured for provided subject
// Returns compatibility level string upon success
func (c *mockclient) GetCompatibility(subject string) (compatibility Compatibility, err error) {
	c.configCacheLock.RLock()
	result, ok := c.configCache[subject]
	c.configCacheLock.RUnlock()
	if !ok {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(internal.SubjectConfig, url.PathEscape(subject)),
			Err: errors.New("Subject Not Found"),
		}
		return compatibility, &posErr
	}
	return result.CompatibilityLevel, nil
}

// UpdateCompatibility updates subject's compatibility level
// Returns new compatibility level string upon success
func (c *mockclient) UpdateCompatibility(subject string, update Compatibility) (compatibility Compatibility, err error) {
	c.configCacheLock.Lock()
	c.configCache[subject] = ServerConfig{
		CompatibilityLevel: update,
	}
	c.configCacheLock.Unlock()
	return update, nil
}

// GetDefaultCompatibility fetches the global(default) compatibility level
// Returns global(default) compatibility level
func (c *mockclient) GetDefaultCompatibility() (compatibility Compatibility, err error) {
	c.configCacheLock.RLock()
	result, ok := c.configCache[noSubject]
	c.configCacheLock.RUnlock()
	if !ok {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprint(internal.Config),
			Err: errors.New("Subject Not Found"),
		}
		return compatibility, &posErr
	}
	return result.CompatibilityLevel, nil
}

// UpdateDefaultCompatibility updates the global(default) compatibility level
// Returns new string compatibility level
func (c *mockclient) UpdateDefaultCompatibility(update Compatibility) (compatibility Compatibility, err error) {
	c.configCacheLock.Lock()
	c.configCache[noSubject] = ServerConfig{
		CompatibilityLevel: update,
	}
	c.configCacheLock.Unlock()
	return update, nil
}

// Fetch config currently configured for provided subject
// Returns config string upon success
func (c *mockclient) GetConfig(subject string, defaultToGlobal bool) (result ServerConfig, err error) {
	c.configCacheLock.RLock()
	result, ok := c.configCache[subject]
	c.configCacheLock.RUnlock()
	if !ok {
		if !defaultToGlobal {
			posErr := url.Error{
				Op:  "GET",
				URL: c.url.String() + fmt.Sprintf(internal.SubjectConfigDefault, url.PathEscape(subject), defaultToGlobal),
				Err: errors.New("Subject Not Found"),
			}
			return result, &posErr
		}
		return c.GetDefaultConfig()
	}
	return result, nil
}

// UpdateCompatibility updates subject's config
// Returns new config string upon success
func (c *mockclient) UpdateConfig(subject string, update ServerConfig) (result ServerConfig, err error) {
	c.configCacheLock.Lock()
	c.configCache[subject] = update
	c.configCacheLock.Unlock()
	return update, nil
}

// GetDefaultCompatibility fetches the global(default) config
// Returns global(default) config
func (c *mockclient) GetDefaultConfig() (result ServerConfig, err error) {
	c.configCacheLock.RLock()
	result, ok := c.configCache[noSubject]
	c.configCacheLock.RUnlock()
	if !ok {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprint(internal.Config),
			Err: errors.New("Subject Not Found"),
		}
		return result, &posErr
	}
	return result, nil
}

// UpdateDefaultCompatibility updates the global(default) config
// Returns new string config
func (c *mockclient) UpdateDefaultConfig(update ServerConfig) (result ServerConfig, err error) {
	c.configCacheLock.Lock()
	c.configCache[noSubject] = update
	c.configCacheLock.Unlock()
	return update, nil
}

// ClearLatestCaches clears caches of latest versions
func (c *mockclient) ClearLatestCaches() error {
	return nil
}

// ClearCaches clears all caches
func (c *mockclient) ClearCaches() error {
	return nil
}

// Close closes the client
func (c *mockclient) Close() error {
	return nil
}

func validateResourceTypeAndAssociationType(resourceType string, associationType string) error {
	// Look up in the map to see if the resource type is supported,
	// and if the association type is supported for that resource type.
	validAssociationTypes, exists := validResourceTypesAndAssociationTypesMap[resourceType]
	if !exists {
		return fmt.Errorf("Unsupported resource type %s.", resourceType)
	}
	for _, validAssociationType := range validAssociationTypes {
		if validAssociationType == associationType {
			return nil
		}
	}
	return fmt.Errorf("Unsupported association type %s for resource type %s.", associationType, resourceType)
}

func (c *mockclient) validateAssociationCreateRequest(request *AssociationCreateRequest) error {
	posErr := url.Error{
		Op:  "POST",
		URL: c.url.String() + fmt.Sprintf(internal.Associations),
	}

	if request.ResourceName == "" || request.ResourceNamespace == "" || request.ResourceID == "" || request.Associations == nil {
		posErr.Err = errors.New("resourceName, resourceNamespace, resourceID and associations are required.")
		return &posErr
	}
	if request.ResourceType == "" {
		request.ResourceType = defaultResourceType
	}
	for _, associationCreateInfo := range request.Associations {
		if associationCreateInfo.Subject == "" {
			posErr.Err = errors.New("subject is required for each association.")
			return &posErr
		}
		if associationCreateInfo.AssociationType == "" {
			associationCreateInfo.AssociationType = defaultAssociationType
		}
		err := validateResourceTypeAndAssociationType(request.ResourceType, associationCreateInfo.AssociationType)
		if err != nil {
			posErr.Err = err
			return &posErr
		}
		if associationCreateInfo.Lifecycle == "" {
			associationCreateInfo.Lifecycle = defaultLifecyclePolicy
		}
		if !associationCreateInfo.Lifecycle.IsValid() {
			posErr.Err = fmt.Errorf("Invalid lifecycle %s. Valid lifecycle inputs are: %s, %s", associationCreateInfo.Lifecycle, STRONG, WEAK)
			return &posErr
		}
	}
	return nil
}

func (c *mockclient) validateResourceCanAcceptNewAssociationType(request *AssociationCreateRequest) error {
	var exists bool
	for _, newAssociation := range request.Associations {
		resourceAndAssociationType := resourceAndAssociationType{
			resourceId:        request.ResourceID,
			resourceName:      request.ResourceName,
			resourceNamespace: request.ResourceNamespace,
			resourceType:      request.ResourceType,
			associationType:   newAssociation.AssociationType,
		}
		c.resourceAndAssocTypeToAssocCacheLock.RLock()
		_, exists = c.resourceAndAssocTypeToAssocCache[resourceAndAssociationType]
		c.resourceAndAssocTypeToAssocCacheLock.RUnlock()
		if exists {
			posErr := url.Error{
				Op:  "POST",
				URL: c.url.String() + fmt.Sprintf(internal.Associations),
				Err: fmt.Errorf("associationType %s already exists for resource and associationType %s", newAssociation.AssociationType, resourceAndAssociationType),
			}
			return &posErr
		}
	}

	return nil
}

func (c *mockclient) validateSubjectsCanAcceptNewAssociations(request *AssociationCreateRequest) error {
	posErr := url.Error{
		Op:  "POST",
		URL: c.url.String() + fmt.Sprintf(internal.Associations),
	}

	var existingAssociations []*Association
	var exists bool
	for _, newAssociation := range request.Associations {
		err := func() error {
			subject := newAssociation.Subject
			schemaInfo := newAssociation.Schema
			if schemaInfo == nil {
				// subject has to exist
				latestVersion := c.latestVersion(subject)
				if latestVersion == -1 {
					return fmt.Errorf("subject %s doesn't exist.", subject)
				}
			}
			c.subjectToAssocCacheLock.RLock()
			existingAssociations, exists = c.subjectToAssocCache[subject]
			defer c.subjectToAssocCacheLock.RUnlock()
			// subject has no associations, can create new association
			if !exists {
				return nil
			}
			// subject has associations, new association can't be strong
			if newAssociation.Lifecycle == STRONG {
				return fmt.Errorf("subject %s already has associations; can't create a new strong association with this subject.",
					subject)
			}
			// new association is weak, and the existing association must be weak too.
			if existingAssociations[0].Lifecycle == STRONG {
				return fmt.Errorf("subject %s already has a strong association; can't create a new association with this subject.",
					subject)
			}
			// new association is weak, and the existing associations are weak. Can create a new association
			return nil
		}()
		if err != nil {
			posErr.Err = err
			return &posErr
		}
	}
	return nil
}

func (c *mockclient) removeAssociationFromSlice(associationSlice []*Association, associationToRemove *Association) []*Association {
	for i, association := range associationSlice {
		if *association == *associationToRemove {
			return append(associationSlice[:i], associationSlice[i+1:]...)
		}
	}
	return associationSlice
}

func (c *mockclient) removeAssociationFromMap(associationsMap map[string][]*Association, key string, associationToRemove *Association) {
	slice := associationsMap[key]
	newSlice := c.removeAssociationFromSlice(slice, associationToRemove)
	if len(newSlice) == 0 {
		delete(associationsMap, key)
	} else {
		associationsMap[key] = newSlice
	}
}

func (c *mockclient) createAllAssociationsInRequest(request AssociationCreateRequest, index int) error {
	if index == len(request.Associations) {
		return nil
	}
	associationInRequest := request.Associations[index]
	resourceAndAssociationType := resourceAndAssociationType{
		resourceId:        request.ResourceID,
		resourceName:      request.ResourceName,
		resourceNamespace: request.ResourceNamespace,
		resourceType:      request.ResourceType,
		associationType:   associationInRequest.AssociationType,
	}
	newAssociation := Association{
		Subject:           associationInRequest.Subject,
		ResourceName:      request.ResourceName,
		ResourceNamespace: request.ResourceNamespace,
		ResourceID:        request.ResourceID,
		ResourceType:      request.ResourceType,
		AssociationType:   associationInRequest.AssociationType,
		Lifecycle:         associationInRequest.Lifecycle,
		Frozen:            associationInRequest.Frozen,
	}
	// resourceAndAssociationType either doesn't have any association
	// or the existing association matches the one in request. In this case, we just need to post the new schema
	existingAssociation, existingAssociationExists := c.resourceAndAssocTypeToAssocCache[resourceAndAssociationType]
	if existingAssociationExists {
		if matches := existingAssociation.equalsWithoutGUID(&newAssociation); !matches {
			return fmt.Errorf("Trying to modify an existing association for resource and association type %s.", resourceAndAssociationType)
		}
		if associationInRequest.Schema != nil {
			// register the subject and schema
			_, err := c.Register(associationInRequest.Subject, *associationInRequest.Schema, associationInRequest.Normalize)
			if err != nil {
				return err
			}
		}
		// Finished processing this request; Go to the next one. Nothing to roll back if the following request fails.
		return c.createAllAssociationsInRequest(request, index+1)
	}
	// subject can accept new associations
	existingSubjectAssociations, exists := c.subjectToAssocCache[associationInRequest.Subject]
	if exists {
		if existingSubjectAssociations[0].Lifecycle == STRONG {
			return fmt.Errorf("subject %s already has a strong association; can't create a new association with this subject.", associationInRequest.Subject)
		} else if associationInRequest.Lifecycle == STRONG {
			return fmt.Errorf("subject %s already has associations; can't create a new strong association with this subject.", associationInRequest.Subject)
		}
	}
	// subject has to exist
	if associationInRequest.Schema == nil {
		latestVersion := c.latestVersion(associationInRequest.Subject)
		if latestVersion == -1 {
			return fmt.Errorf("New subject schema %s doesn't exist.", associationInRequest.Subject)
		}
	} else {
		// Register the subject and schema. If the next request fails, the schema will not be rolled back.
		_, err := c.Register(associationInRequest.Subject, *associationInRequest.Schema, associationInRequest.Normalize)
		if err != nil {
			return err
		}
	}
	// update all caches
	newAssociation.GUID = uuid.New().String()
	c.resourceAndAssocTypeToAssocCache[resourceAndAssociationType] = &newAssociation
	c.subjectToAssocCache[associationInRequest.Subject] = append(c.subjectToAssocCache[associationInRequest.Subject], &newAssociation)
	c.resourceIdToAssocCache[request.ResourceID] = append(c.resourceIdToAssocCache[request.ResourceID], &newAssociation)

	err := c.createAllAssociationsInRequest(request, index+1)
	// roll back to the previous state if error occurs
	if err != nil {
		delete(c.resourceAndAssocTypeToAssocCache, resourceAndAssociationType)
		c.removeAssociationFromMap(c.subjectToAssocCache, associationInRequest.Subject, &newAssociation)
		c.removeAssociationFromMap(c.resourceIdToAssocCache, request.ResourceID, &newAssociation)
	}
	return nil
}

func (c *mockclient) CreateAssociation(request AssociationCreateRequest) (result AssociationResponse, err error) {
	// Validations
	/*
		1. For resource:
		Required: ResourceName, ResourceNamespace, ResourceID.
		Optional: ResourceType (default topic).
		2. For associations:
		Required: Subject.
		Optional: AssociationType(default value), LifecyclePolicy (default strong), Frozen (default false)
	*/
	err = c.validateAssociationCreateRequest(&request)
	if err != nil {
		return result, err
	}

	c.resourceAndAssocTypeToAssocCacheLock.Lock()
	c.subjectToAssocCacheLock.Lock()
	c.resourceIdToAssocCacheLock.Lock()
	// recursively process all the request. Roll back to the previous state if error occurs.
	err = c.createAllAssociationsInRequest(request, 0)
	c.resourceIdToAssocCacheLock.Unlock()
	c.subjectToAssocCacheLock.Unlock()
	c.resourceAndAssocTypeToAssocCacheLock.Unlock()

	if err != nil {
		return result, err
	}
	result = AssociationResponse{
		ResourceName:      request.ResourceName,
		ResourceNamespace: request.ResourceNamespace,
		ResourceID:        request.ResourceID,
		ResourceType:      request.ResourceType,
	}
	var associationsInResponse []AssociationInfo
	for _, associationInRequest := range request.Associations {
		associationInfo := AssociationInfo{
			Subject:         associationInRequest.Subject,
			AssociationType: associationInRequest.AssociationType,
			Lifecycle:       associationInRequest.Lifecycle,
			Frozen:          associationInRequest.Frozen,
			Schema:          associationInRequest.Schema,
			Normalize:       associationInRequest.Normalize,
		}
		associationsInResponse = append(associationsInResponse, associationInfo)
	}
	result.Associations = associationsInResponse
	return result, nil
}

func (c *mockclient) applyFilter(associations []*Association, resourceType string, associationTypes []string,
	lifecycle string, offset int, limit int) (result []Association, err error) {
	filtered := make([]Association, 0)
	for _, association := range associations {
		// Filter by resource type if provided
		if resourceType != "" && association.ResourceType != resourceType {
			continue
		}
		// Filter by association types if provided
		if len(associationTypes) > 0 {
			found := false
			for _, at := range associationTypes {
				if association.AssociationType == at {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		// Filter by lifecycle if provided
		if lifecycle != "" && string(association.Lifecycle) != lifecycle {
			continue
		}
		filtered = append(filtered, *association)
	}

	// Apply pagination
	start := offset
	if start > len(filtered) {
		start = len(filtered)
	}
	end := start + limit
	if limit <= 0 || end > len(filtered) {
		end = len(filtered)
	}
	return filtered[start:end], nil
}

// GetAssociationsBySubject retrieves associations by subject
func (c *mockclient) GetAssociationsBySubject(subject string, resourceType string, associationTypes []string,
	lifecycle string, offset int, limit int) (result []Association, err error) {
	if subject == "" {
		return result, errors.New("subject is required")
	}
	c.subjectToAssocCacheLock.RLock()
	defer c.subjectToAssocCacheLock.RUnlock()
	associations, exists := c.subjectToAssocCache[subject]
	if !exists || len(associations) == 0 {
		return result, nil
	}
	return c.applyFilter(associations, resourceType, associationTypes, lifecycle, offset, limit)
}

// GetAssociationsByResourceID retrieves associations by resource ID
func (c *mockclient) GetAssociationsByResourceID(resourceID string, resourceType string, associationTypes []string,
	lifecycle string, offset int, limit int) (result []Association, err error) {
	if resourceID == "" {
		return result, errors.New("resourceID is required.")
	}
	c.resourceIdToAssocCacheLock.RLock()
	defer c.resourceIdToAssocCacheLock.RUnlock()
	associations, exists := c.resourceIdToAssocCache[resourceID]
	if !exists || len(associations) == 0 {
		return result, nil
	}
	return c.applyFilter(associations, resourceType, associationTypes, lifecycle, offset, limit)
}

// DeleteAssociations deletes associations for a resource
func (c *mockclient) DeleteAssociations(resourceID string, resourceType string, associationTypes []string,
	cascadeLifecycle bool) error {
	c.resourceIdToAssocCacheLock.Lock()
	associations, exists := c.resourceIdToAssocCache[resourceID]
	if !exists || len(associations) == 0 {
		// If such resourceId not found, do nothing and not reporting error
		return nil
	}
	associationsToDelete, err := c.applyFilter(associations, resourceType, associationTypes, "", 0, -1)
	if err != nil {
		return err
	}
	if len(associationsToDelete) == 0 {
		return nil
	}

	// delete associationsToDelete from resourceIdToAssocCache
	for _, associationToDelete := range associationsToDelete {
		c.removeAssociationFromMap(c.resourceIdToAssocCache, resourceID, &associationToDelete)
	}

	// delete associationsToDelete from resourceAndAssocTypeToAssocCache
	c.resourceAndAssocTypeToAssocCacheLock.Lock()
	for _, associationToDelete := range associationsToDelete {
		resourceAndAssociationType := resourceAndAssociationType{
			resourceId:        associationToDelete.ResourceID,
			resourceName:      associationToDelete.ResourceName,
			resourceNamespace: associationToDelete.ResourceNamespace,
			resourceType:      associationToDelete.ResourceType,
			associationType:   associationToDelete.AssociationType,
		}
		delete(c.resourceAndAssocTypeToAssocCache, resourceAndAssociationType)
	}
	c.resourceAndAssocTypeToAssocCacheLock.Unlock()

	// delete associationsToDelete from subjectToAssocCache
	c.subjectToAssocCacheLock.Lock()
	for _, associationToDelete := range associationsToDelete {
		c.removeAssociationFromMap(c.subjectToAssocCache, associationToDelete.Subject, &associationToDelete)
	}
	c.subjectToAssocCacheLock.Unlock()

	c.resourceIdToAssocCacheLock.Unlock()

	// delete subjects if cascade is true and lifecycle is strong
	var errs []string
	for _, associationToDelete := range associationsToDelete {
		if cascadeLifecycle && associationToDelete.Lifecycle == STRONG {
			// delete subject permanently
			_, err = c.DeleteSubject(associationToDelete.Subject, true)
			if err != nil {
				errs = append(errs, fmt.Sprintf("subject: %s, err: %s", associationToDelete.Subject, err))
			}
		}
	}
	if len(errs) > 0 {
		posErr := url.Error{
			Op:  "DELETE",
			URL: c.url.String() + fmt.Sprintf(internal.AssociationsDeleteByResource, resourceID),
			Err: fmt.Errorf(strings.Join(errs, "\n")),
		}
		return &posErr
	}
	return nil
}

func schemasEqual(info1 SchemaInfo, info2 SchemaInfo) bool {
	refs1 := info1.References
	if refs1 == nil {
		refs1 = make([]Reference, 0)
	}
	refs2 := info2.References
	if refs2 == nil {
		refs2 = make([]Reference, 0)
	}
	return info1.Schema == info2.Schema &&
		info1.SchemaType == info2.SchemaType &&
		reflect.DeepEqual(refs1, refs2) &&
		reflect.DeepEqual(info1.Metadata, info2.Metadata) &&
		reflect.DeepEqual(info1.RuleSet, info2.RuleSet)
}
