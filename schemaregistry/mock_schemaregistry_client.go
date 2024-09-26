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

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/internal"
)

const noSubject = ""

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

/* HTTP(S) Schema Registry Client and schema caches */
type mockclient struct {
	sync.Mutex
	config                   *Config
	url                      *url.URL
	infoToSchemaCache        map[subjectJSON]metadataCacheEntry
	infoToSchemaCacheLock    sync.RWMutex
	idToSchemaCache          map[subjectID]infoCacheEntry
	idToSchemaCacheLock      sync.RWMutex
	schemaToVersionCache     map[subjectJSON]versionCacheEntry
	schemaToVersionCacheLock sync.RWMutex
	configCache              map[string]ServerConfig
	configCacheLock          sync.RWMutex
	counter                  counter
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

	id, err := c.getIDFromRegistry(subject, schema)
	if err != nil {
		return SchemaMetadata{
			ID: -1,
		}, err
	}
	result = SchemaMetadata{
		SchemaInfo: schema,
		ID:         id,
	}
	c.infoToSchemaCacheLock.Lock()
	c.infoToSchemaCache[cacheKey] = metadataCacheEntry{&result, false}
	c.infoToSchemaCacheLock.Unlock()
	return result, nil
}

func (c *mockclient) getIDFromRegistry(subject string, schema SchemaInfo) (int, error) {
	var id = -1
	c.idToSchemaCacheLock.RLock()
	for key, value := range c.idToSchemaCache {
		if key.subject == subject && schemasEqual(*value.info, schema) {
			id = key.id
			break
		}
	}
	c.idToSchemaCacheLock.RUnlock()
	err := c.generateVersion(subject, schema)
	if err != nil {
		return -1, err
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
	}
	return id, nil
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
	schemaJSON, err := schema.MarshalJSON()
	if err != nil {
		return -1, err
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
		return cacheEntryVal.metadata.ID, nil
	}

	posErr := url.Error{
		Op:  "GET",
		URL: c.url.String() + fmt.Sprintf(internal.Subjects, url.PathEscape(subject)),
		Err: errors.New("Subject Not found"),
	}
	return -1, &posErr
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
	return SchemaMetadata{
		SchemaInfo: info,

		ID:      id,
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
