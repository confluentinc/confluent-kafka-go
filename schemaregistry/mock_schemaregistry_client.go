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
	"sync"
)

const noSubject = ""

type counter struct {
	count int
}

func (c counter) currentValue() int {
	return c.count
}

func (c counter) increment() int {
	c.count++
	return c.count
}

type versionCacheEntry struct {
	version     int
	softDeleted bool
}

type idCacheEntry struct {
	id          int
	softDeleted bool
}

/* HTTP(S) Schema Registry Client and schema caches */
type mockclient struct {
	sync.Mutex
	url                      *url.URL
	schemaToIdCache          map[subjectJSON]idCacheEntry
	schemaToIdCacheLock      sync.RWMutex
	idToSchemaCache          map[subjectID]*SchemaInfo
	idToSchemaCacheLock      sync.RWMutex
	schemaToVersionCache     map[subjectJSON]versionCacheEntry
	schemaToVersionCacheLock sync.RWMutex
	compatibilityCache       map[string]Compatibility
	compatibilityCacheLock   sync.RWMutex
	counter                  counter
}

var _ Client = new(mockclient)

// Register registers Schema aliased with subject
func (c *mockclient) Register(subject string, schema SchemaInfo, normalize bool) (id int, err error) {
	schemaJSON, err := schema.MarshalJSON()
	if err != nil {
		return -1, err
	}
	cacheKey := subjectJSON{
		subject: subject,
		json:    string(schemaJSON),
	}
	c.schemaToIdCacheLock.RLock()
	idCacheEntryVal, ok := c.schemaToIdCache[cacheKey]
	if idCacheEntryVal.softDeleted {
		ok = false
	}
	c.schemaToIdCacheLock.RUnlock()
	if ok {
		return idCacheEntryVal.id, nil
	}

	id, err = c.getIDFromRegistry(subject, schema)
	if err != nil {
		return -1, err
	}
	c.schemaToIdCacheLock.Lock()
	c.schemaToIdCache[cacheKey] = idCacheEntry{id, false}
	c.schemaToIdCacheLock.Unlock()
	return id, nil
}

func (c *mockclient) getIDFromRegistry(subject string, schema SchemaInfo) (int, error) {
	var id = -1
	c.idToSchemaCacheLock.RLock()
	for key, value := range c.idToSchemaCache {
		if key.subject == subject && schemasEqual(*value, schema) {
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
		c.idToSchemaCache[idCacheKey] = &schema
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
	info, ok := c.idToSchemaCache[cacheKey]
	c.idToSchemaCacheLock.RUnlock()
	if ok {
		return *info, nil
	}
	posErr := url.Error{
		Op:  "GET",
		URL: c.url.String() + fmt.Sprintf(schemasBySubject, id, url.QueryEscape(subject)),
		Err: errors.New("Subject Not Found"),
	}
	return SchemaInfo{}, &posErr
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
	c.schemaToIdCacheLock.RLock()
	idCacheEntryVal, ok := c.schemaToIdCache[cacheKey]
	if idCacheEntryVal.softDeleted {
		ok = false
	}
	c.schemaToIdCacheLock.RUnlock()
	if ok {
		return idCacheEntryVal.id, nil
	}

	posErr := url.Error{
		Op:  "GET",
		URL: c.url.String() + fmt.Sprintf(subjects, url.PathEscape(subject)),
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
			URL: c.url.String() + fmt.Sprintf(versions, url.PathEscape(subject), "latest"),
			Err: errors.New("Subject Not found"),
		}
		return SchemaMetadata{}, &posErr
	}
	return c.GetSchemaMetadata(subject, version)
}

// GetSchemaMetadata fetches the requested subject schema identified by version
// Returns SchemaMetadata object
func (c *mockclient) GetSchemaMetadata(subject string, version int) (result SchemaMetadata, err error) {
	var json string
	c.schemaToVersionCacheLock.RLock()
	for key, value := range c.schemaToVersionCache {
		if key.subject == subject && value.version == version && !value.softDeleted {
			json = key.json
			break
		}
	}
	c.schemaToVersionCacheLock.RUnlock()
	if json == "" {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(versions, url.PathEscape(subject), version),
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
		if key.subject == subject && schemasEqual(*value, info) {
			id = key.id
			break
		}
	}
	c.idToSchemaCacheLock.RUnlock()
	if id == -1 {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(versions, url.PathEscape(subject), version),
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

// GetAllVersions fetches a list of all version numbers associated with the provided subject registration
// Returns integer slice on success
func (c *mockclient) GetAllVersions(subject string) (results []int, err error) {
	results = c.allVersions(subject)
	if len(results) == 0 {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(version, url.PathEscape(subject)),
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

func (c *mockclient) deleteID(key subjectJSON, id int, permanent bool) {
	if permanent {
		delete(c.schemaToIdCache, key)
	} else {
		c.schemaToIdCache[key] = idCacheEntry{id, true}
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
	versionCacheEntryVal, ok := c.schemaToVersionCache[cacheKey]
	if versionCacheEntryVal.softDeleted {
		ok = false
	}
	c.schemaToVersionCacheLock.RUnlock()
	if ok {
		return versionCacheEntryVal.version, nil
	}
	posErr := url.Error{
		Op:  "GET",
		URL: c.url.String() + fmt.Sprintf(subjects, url.PathEscape(subject)),
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
	c.schemaToIdCacheLock.Lock()
	for key, value := range c.schemaToIdCache {
		if key.subject == subject && (!value.softDeleted || permanent) {
			c.deleteID(key, value.id, permanent)
		}
	}
	c.schemaToIdCacheLock.Unlock()
	c.schemaToVersionCacheLock.Lock()
	for key, value := range c.schemaToVersionCache {
		if key.subject == subject && (!value.softDeleted || permanent) {
			c.deleteVersion(key, value.version, permanent)
			deleted = append(deleted, value.version)
		}
	}
	c.schemaToVersionCacheLock.Unlock()
	c.compatibilityCacheLock.Lock()
	delete(c.compatibilityCache, subject)
	c.compatibilityCacheLock.Unlock()
	if permanent {
		c.idToSchemaCacheLock.Lock()
		for key := range c.idToSchemaCache {
			if key.subject == subject {
				delete(c.idToSchemaCache, key)
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
				json:    string(schemaJSON),
			}
			c.schemaToIdCacheLock.Lock()
			idSchemaEntryVal, ok := c.schemaToIdCache[cacheKeySchema]
			if ok {
				c.deleteID(key, idSchemaEntryVal.id, permanent)
			}
			c.schemaToIdCacheLock.Unlock()
			if permanent && ok {
				c.idToSchemaCacheLock.Lock()
				cacheKeyID := subjectID{
					subject: subject,
					id:      idSchemaEntryVal.id,
				}
				delete(c.idToSchemaCache, cacheKeyID)
				c.idToSchemaCacheLock.Unlock()
			}
		}
	}
	c.schemaToVersionCacheLock.Unlock()
	return version, nil
}

// Fetch compatibility level currently configured for provided subject
// Returns compatibility level string upon success
func (c *mockclient) GetCompatibility(subject string) (compatibility Compatibility, err error) {
	c.compatibilityCacheLock.RLock()
	compatibility, ok := c.compatibilityCache[subject]
	c.compatibilityCacheLock.RUnlock()
	if !ok {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(subjectConfig, url.PathEscape(subject)),
			Err: errors.New("Subject Not Found"),
		}
		return compatibility, &posErr
	}
	return compatibility, nil
}

// UpdateCompatibility updates subject's compatibility level
// Returns new compatibility level string upon success
func (c *mockclient) UpdateCompatibility(subject string, update Compatibility) (compatibility Compatibility, err error) {
	c.compatibilityCacheLock.Lock()
	c.compatibilityCache[subject] = update
	c.compatibilityCacheLock.Unlock()
	return update, nil
}

// TestCompatibility verifies schema against the subject's compatibility policy
// Returns true if the schema is compatible, false otherwise
func (c *mockclient) TestCompatibility(subject string, version int, schema SchemaInfo) (ok bool, err error) {
	return false, errors.New("unsupported operaiton")
}

// GetDefaultCompatibility fetches the global(default) compatibility level
// Returns global(default) compatibility level
func (c *mockclient) GetDefaultCompatibility() (compatibility Compatibility, err error) {
	c.compatibilityCacheLock.RLock()
	compatibility, ok := c.compatibilityCache[noSubject]
	c.compatibilityCacheLock.RUnlock()
	if !ok {
		posErr := url.Error{
			Op:  "GET",
			URL: c.url.String() + fmt.Sprintf(config),
			Err: errors.New("Subject Not Found"),
		}
		return compatibility, &posErr
	}
	return compatibility, nil
}

// UpdateDefaultCompatibility updates the global(default) compatibility level level
// Returns new string compatibility level
func (c *mockclient) UpdateDefaultCompatibility(update Compatibility) (compatibility Compatibility, err error) {
	c.compatibilityCacheLock.Lock()
	c.compatibilityCache[noSubject] = update
	c.compatibilityCacheLock.Unlock()
	return update, nil
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
		reflect.DeepEqual(refs1, refs2)
}
