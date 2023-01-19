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
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/cache"
)

/* Schema Registry API endpoints
*
* ====Schemas====
* Fetch string: schema(escaped) identified by the input id.
* -GET /schemas/ids/{int: id} returns: JSON blob: schema; raises: 404[03], 500[01]
*
* ====Subjects====
* Fetch JSON array str:subject of all registered subjects
* -GET /subjects returns: JSON array string: subjects; raises: 500[01]
* Fetch JSON array int:versions
* GET /subjects/{string: subject}/versions returns: JSON array of int: versions; raises: 404[01], 500[01]
*
* GET /subjects/{string: subject}/versions/{int|string('latest'): version} returns: JSON blob *schemaMetadata*; raises: 404[01, 02], 422[02], 500[01]
* GET /subjects/{string: subject}/versions/{int|string('latest'): version}/schema returns : JSON blob: schema(unescaped); raises: 404, 422, 500[01, 02, 03]
*
* Delete subject and it's associated subject configuration subjectConfig
* -DELETE /subjects/{string: subject}) returns: JSON array int: version; raises: 404[01], 500[01]
* Delete subject version
* -DELETE /subjects/{string: subject}/versions/{int|str('latest'): version} returns int: deleted version id; raises: 404[01, 02]
*
* Register new schema under subject
* -POST /subjects/{string: subject}/versions returns JSON blob ; raises: 409, 422[01], 500[01, 02, 03]
* Return SchemaMetadata for the subject version (if any) associated with the schema in the request body
* -POST /subjects/{string: subject} returns JSON *schemaMetadata*; raises: 404[01, 03]
*
* ====Compatibility====
* Test schema (http body) against configured comparability for subject version
* -POST /compatibility/subjects/{string: subject}/versions/{int:string('latest'): version} returns: JSON bool:is_compatible; raises: 404[01,02], 422[01,02], 500[01]
*
* ====SerializerConfig====
* Returns global configuration
* -GET /config  returns: JSON string:comparability; raises: 500[01]
* Update global SR config
* -PUT /config returns: JSON string:compatibility; raises: 422[03], 500[01, 03]
* Update subject level subjectConfig
* -PUT /config/{string: subject} returns: JSON string:compatibility; raises: 422[03], 500[01,03]
* Returns compatibility level of subject
* GET /config/(string: subject) returns: JSON string:compatibility; raises: 404, 500[01]
 */

// Reference represents a schema reference
type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// SchemaInfo represents basic schema information
type SchemaInfo struct {
	Schema     string      `json:"schema,omitempty"`
	SchemaType string      `json:"schemaType,omitempty"`
	References []Reference `json:"references,omitempty"`
}

// MarshalJSON implements the json.Marshaler interface
func (sd *SchemaInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Schema     string      `json:"schema,omitempty"`
		SchemaType string      `json:"schemaType,omitempty"`
		References []Reference `json:"references,omitempty"`
	}{
		sd.Schema,
		sd.SchemaType,
		sd.References,
	})
}

// UnmarshalJSON implements the json.Unmarshaller interface
func (sd *SchemaInfo) UnmarshalJSON(b []byte) error {
	var err error
	var tmp struct {
		Schema     string      `json:"schema,omitempty"`
		SchemaType string      `json:"schemaType,omitempty"`
		References []Reference `json:"references,omitempty"`
	}

	err = json.Unmarshal(b, &tmp)

	sd.Schema = tmp.Schema
	sd.SchemaType = tmp.SchemaType
	sd.References = tmp.References

	return err
}

// SchemaMetadata represents schema metadata
type SchemaMetadata struct {
	SchemaInfo
	ID      int    `json:"id,omitempty"`
	Subject string `json:"subject,omitempty"`
	Version int    `json:"version,omitempty"`
}

// MarshalJSON implements the json.Marshaler interface
func (sd *SchemaMetadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Schema     string      `json:"schema,omitempty"`
		SchemaType string      `json:"schemaType,omitempty"`
		References []Reference `json:"references,omitempty"`
		ID         int         `json:"id,omitempty"`
		Subject    string      `json:"subject,omitempty"`
		Version    int         `json:"version,omitempty"`
	}{
		sd.Schema,
		sd.SchemaType,
		sd.References,
		sd.ID,
		sd.Subject,
		sd.Version,
	})
}

// UnmarshalJSON implements the json.Unmarshaller interface
func (sd *SchemaMetadata) UnmarshalJSON(b []byte) error {
	var err error
	var tmp struct {
		Schema     string      `json:"schema,omitempty"`
		SchemaType string      `json:"schemaType,omitempty"`
		References []Reference `json:"references,omitempty"`
		ID         int         `json:"id,omitempty"`
		Subject    string      `json:"subject,omitempty"`
		Version    int         `json:"version,omitempty"`
	}

	err = json.Unmarshal(b, &tmp)

	sd.Schema = tmp.Schema
	sd.SchemaType = tmp.SchemaType
	sd.References = tmp.References
	sd.ID = tmp.ID
	sd.Subject = tmp.Subject
	sd.Version = tmp.Version

	return err
}

type subjectJSON struct {
	subject string
	json    string
}

type subjectID struct {
	subject string
	id      int
}

/* HTTP(S) Schema Registry Client and schema caches */
type client struct {
	sync.Mutex
	restService      *restService
	schemaCache      cache.Cache
	schemaCacheLock  sync.RWMutex
	idCache          cache.Cache
	idCacheLock      sync.RWMutex
	versionCache     cache.Cache
	versionCacheLock sync.RWMutex
}

var _ Client = new(client)

// Client is an interface for clients interacting with the Confluent Schema Registry.
// The Schema Registry's REST interface is further explained in Confluent's Schema Registry API documentation
// https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClient.java
type Client interface {
	Register(subject string, schema SchemaInfo, normalize bool) (id int, err error)
	GetBySubjectAndID(subject string, id int) (schema SchemaInfo, err error)
	GetID(subject string, schema SchemaInfo, normalize bool) (id int, err error)
	GetLatestSchemaMetadata(subject string) (SchemaMetadata, error)
	GetSchemaMetadata(subject string, version int) (SchemaMetadata, error)
	GetAllVersions(subject string) ([]int, error)
	GetVersion(subject string, schema SchemaInfo, normalize bool) (version int, err error)
	GetAllSubjects() ([]string, error)
	DeleteSubject(subject string, permanent bool) ([]int, error)
	DeleteSubjectVersion(subject string, version int, permanent bool) (deletes int, err error)
	GetCompatibility(subject string) (compatibility Compatibility, err error)
	UpdateCompatibility(subject string, update Compatibility) (compatibility Compatibility, err error)
	TestCompatibility(subject string, version int, schema SchemaInfo) (compatible bool, err error)
	GetDefaultCompatibility() (compatibility Compatibility, err error)
	UpdateDefaultCompatibility(update Compatibility) (compatibility Compatibility, err error)
}

// NewClient returns a Client implementation
func NewClient(conf *Config) (Client, error) {

	urlConf := conf.SchemaRegistryURL
	if strings.HasPrefix(urlConf, "mock://") {
		url, err := url.Parse(urlConf)
		if err != nil {
			return nil, err
		}
		mock := &mockclient{
			url:                url,
			schemaCache:        make(map[subjectJSON]idCacheEntry),
			idCache:            make(map[subjectID]*SchemaInfo),
			versionCache:       make(map[subjectJSON]versionCacheEntry),
			compatibilityCache: make(map[string]Compatibility),
		}
		return mock, nil
	}

	restService, err := newRestService(conf)
	if err != nil {
		return nil, err
	}

	var schemaCache cache.Cache
	var idCache cache.Cache
	var versionCache cache.Cache
	if conf.CacheCapacity != 0 {
		schemaCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
		idCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
		versionCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
	} else {
		schemaCache = cache.NewMapCache()
		idCache = cache.NewMapCache()
		versionCache = cache.NewMapCache()
	}
	handle := &client{
		restService:  restService,
		schemaCache:  schemaCache,
		idCache:      idCache,
		versionCache: versionCache,
	}
	return handle, nil
}

// Register registers Schema aliased with subject
func (c *client) Register(subject string, schema SchemaInfo, normalize bool) (id int, err error) {
	schemaJSON, err := schema.MarshalJSON()
	if err != nil {
		return -1, err
	}
	cacheKey := subjectJSON{
		subject: subject,
		json:    string(schemaJSON),
	}
	c.schemaCacheLock.RLock()
	idValue, ok := c.schemaCache.Get(cacheKey)
	c.schemaCacheLock.RUnlock()
	if ok {
		return idValue.(int), nil
	}

	metadata := SchemaMetadata{
		SchemaInfo: schema,
	}
	c.schemaCacheLock.Lock()
	// another goroutine could have already put it in cache
	idValue, ok = c.schemaCache.Get(cacheKey)
	if !ok {
		err = c.restService.handleRequest(newRequest("POST", versionNormalize, &metadata, url.PathEscape(subject), normalize), &metadata)
		if err == nil {
			c.schemaCache.Put(cacheKey, metadata.ID)
		} else {
			metadata.ID = -1
		}
	} else {
		metadata.ID = idValue.(int)
	}
	c.schemaCacheLock.Unlock()
	return metadata.ID, err
}

// GetBySubjectAndID returns the schema identified by id
// Returns Schema object on success
func (c *client) GetBySubjectAndID(subject string, id int) (schema SchemaInfo, err error) {
	cacheKey := subjectID{
		subject: subject,
		id:      id,
	}
	c.idCacheLock.RLock()
	infoValue, ok := c.idCache.Get(cacheKey)
	c.idCacheLock.RUnlock()
	if ok {
		return *infoValue.(*SchemaInfo), nil
	}

	metadata := SchemaMetadata{}
	newInfo := &SchemaInfo{}
	c.idCacheLock.Lock()
	// another goroutine could have already put it in cache
	infoValue, ok = c.idCache.Get(cacheKey)
	if !ok {
		if len(subject) > 0 {
			err = c.restService.handleRequest(newRequest("GET", schemasBySubject, nil, id, url.QueryEscape(subject)), &metadata)
		} else {
			err = c.restService.handleRequest(newRequest("GET", schemas, nil, id), &metadata)
		}
		if err == nil {
			newInfo = &SchemaInfo{
				Schema:     metadata.Schema,
				SchemaType: metadata.SchemaType,
				References: metadata.References,
			}
			c.idCache.Put(cacheKey, newInfo)
		}
	} else {
		newInfo = infoValue.(*SchemaInfo)
	}
	c.idCacheLock.Unlock()
	return *newInfo, err
}

// GetID checks if a schema has been registered with the subject. Returns ID if the registration can be found
func (c *client) GetID(subject string, schema SchemaInfo, normalize bool) (id int, err error) {
	schemaJSON, err := schema.MarshalJSON()
	if err != nil {
		return -1, err
	}
	cacheKey := subjectJSON{
		subject: subject,
		json:    string(schemaJSON),
	}
	c.schemaCacheLock.RLock()
	idValue, ok := c.schemaCache.Get(cacheKey)
	c.schemaCacheLock.RUnlock()
	if ok {
		return idValue.(int), nil
	}

	metadata := SchemaMetadata{
		SchemaInfo: schema,
	}
	c.schemaCacheLock.Lock()
	// another goroutine could have already put it in cache
	idValue, ok = c.schemaCache.Get(cacheKey)
	if !ok {
		err = c.restService.handleRequest(newRequest("POST", subjectsNormalize, &metadata, url.PathEscape(subject), normalize), &metadata)
		if err == nil {
			c.schemaCache.Put(cacheKey, metadata.ID)
		} else {
			metadata.ID = -1
		}
	} else {
		metadata.ID = idValue.(int)
	}
	c.schemaCacheLock.Unlock()
	return metadata.ID, err
}

// GetLatestSchemaMetadata fetches latest version registered with the provided subject
// Returns SchemaMetadata object
func (c *client) GetLatestSchemaMetadata(subject string) (result SchemaMetadata, err error) {
	err = c.restService.handleRequest(newRequest("GET", versions, nil, url.PathEscape(subject), "latest"), &result)

	return result, err
}

// GetSchemaMetadata fetches the requested subject schema identified by version
// Returns SchemaMetadata object
func (c *client) GetSchemaMetadata(subject string, version int) (result SchemaMetadata, err error) {
	err = c.restService.handleRequest(newRequest("GET", versions, nil, url.PathEscape(subject), version), &result)

	return result, err
}

// GetAllVersions fetches a list of all version numbers associated with the provided subject registration
// Returns integer slice on success
func (c *client) GetAllVersions(subject string) (results []int, err error) {
	var result []int
	err = c.restService.handleRequest(newRequest("GET", version, nil, url.PathEscape(subject)), &result)

	return result, err
}

// GetVersion finds the Subject SchemaMetadata associated with the provided schema
// Returns integer SchemaMetadata number
func (c *client) GetVersion(subject string, schema SchemaInfo, normalize bool) (version int, err error) {
	schemaJSON, err := schema.MarshalJSON()
	if err != nil {
		return -1, err
	}
	cacheKey := subjectJSON{
		subject: subject,
		json:    string(schemaJSON),
	}
	c.versionCacheLock.RLock()
	versionValue, ok := c.versionCache.Get(cacheKey)
	c.versionCacheLock.RUnlock()
	if ok {
		return versionValue.(int), nil
	}

	metadata := SchemaMetadata{
		SchemaInfo: schema,
	}
	c.versionCacheLock.Lock()
	// another goroutine could have already put it in cache
	versionValue, ok = c.versionCache.Get(cacheKey)
	if !ok {
		err = c.restService.handleRequest(newRequest("POST", subjectsNormalize, &metadata, url.PathEscape(subject), normalize), &metadata)
		if err == nil {
			c.versionCache.Put(cacheKey, metadata.Version)
		} else {
			metadata.Version = -1
		}
	} else {
		metadata.Version = versionValue.(int)
	}
	c.versionCacheLock.Unlock()
	return metadata.Version, err
}

// Fetch all Subjects registered with the schema Registry
// Returns a string slice containing all registered subjects
func (c *client) GetAllSubjects() ([]string, error) {
	var result []string
	err := c.restService.handleRequest(newRequest("GET", subject, nil), &result)

	return result, err
}

// Deletes provided Subject from registry
// Returns integer slice of versions removed by delete
func (c *client) DeleteSubject(subject string, permanent bool) (deleted []int, err error) {
	c.schemaCacheLock.Lock()
	for keyValue := range c.schemaCache.ToMap() {
		key := keyValue.(subjectJSON)
		if key.subject == subject {
			c.schemaCache.Delete(key)
		}
	}
	c.schemaCacheLock.Unlock()
	c.versionCacheLock.Lock()
	for keyValue := range c.versionCache.ToMap() {
		key := keyValue.(subjectJSON)
		if key.subject == subject {
			c.versionCache.Delete(key)
		}
	}
	c.versionCacheLock.Unlock()
	c.idCacheLock.Lock()
	for keyValue := range c.idCache.ToMap() {
		key := keyValue.(subjectID)
		if key.subject == subject {
			c.idCache.Delete(key)
		}
	}
	c.idCacheLock.Unlock()
	var result []int
	err = c.restService.handleRequest(newRequest("DELETE", subjectsDelete, nil, url.PathEscape(subject), permanent), &result)
	return result, err
}

// DeleteSubjectVersion removes the version identified by delete from the subject's registration
// Returns integer id for the deleted version
func (c *client) DeleteSubjectVersion(subject string, version int, permanent bool) (deleted int, err error) {
	c.versionCacheLock.Lock()
	for keyValue, value := range c.versionCache.ToMap() {
		key := keyValue.(subjectJSON)
		if key.subject == subject && value == version {
			c.versionCache.Delete(key)
			schemaJSON := key.json
			cacheKeySchema := subjectJSON{
				subject: subject,
				json:    string(schemaJSON),
			}
			c.schemaCacheLock.Lock()
			idValue, ok := c.schemaCache.Get(cacheKeySchema)
			if ok {
				c.schemaCache.Delete(cacheKeySchema)
			}
			c.schemaCacheLock.Unlock()
			if ok {
				id := idValue.(int)
				c.idCacheLock.Lock()
				cacheKeyID := subjectID{
					subject: subject,
					id:      id,
				}
				c.idCache.Delete(cacheKeyID)
				c.idCacheLock.Unlock()
			}
		}
	}
	c.versionCacheLock.Unlock()
	var result int
	err = c.restService.handleRequest(newRequest("DELETE", versionsDelete, nil, url.PathEscape(subject), version, permanent), &result)
	return result, err

}

// Compatibility options
type Compatibility int

const (
	_ = iota
	// None is no compatibility
	None
	// Backward compatibility
	Backward
	// Forward compatibility
	Forward
	// Full compatibility
	Full
	// BackwardTransitive compatibility
	BackwardTransitive
	// ForwardTransitive compatibility
	ForwardTransitive
	// FullTransitive compatibility
	FullTransitive
)

var compatibilityEnum = []string{
	"",
	"NONE",
	"BACKWARD",
	"FORWARD",
	"FULL",
	"BACKWARD_TRANSITIVE",
	"FORWARD_TRANSITIVE",
	"FULL_TRANSITIVE",
}

/* NOTE: GET uses compatibilityLevel, POST uses compatibility */
type compatibilityLevel struct {
	CompatibilityUpdate Compatibility `json:"compatibility,omitempty"`
	Compatibility       Compatibility `json:"compatibilityLevel,omitempty"`
}

// MarshalJSON implements json.Marshaler
func (c Compatibility) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

// UnmarshalJSON implements json.Unmarshaler
func (c *Compatibility) UnmarshalJSON(b []byte) error {
	val := string(b[1 : len(b)-1])
	return c.ParseString(val)
}

type compatibilityValue struct {
	Compatible bool `json:"is_compatible,omitempty"`
}

func (c Compatibility) String() string {
	return compatibilityEnum[c]
}

// ParseString returns a Compatibility for the given string
func (c *Compatibility) ParseString(val string) error {
	for idx, elm := range compatibilityEnum {
		if elm == val {
			*c = Compatibility(idx)
			return nil
		}
	}

	return fmt.Errorf("failed to unmarshal Compatibility")
}

// Fetch compatibility level currently configured for provided subject
// Returns compatibility level string upon success
func (c *client) GetCompatibility(subject string) (compatibility Compatibility, err error) {
	var result compatibilityLevel
	err = c.restService.handleRequest(newRequest("GET", subjectConfig, nil, url.PathEscape(subject)), &result)

	return result.Compatibility, err
}

// UpdateCompatibility updates subject's compatibility level
// Returns new compatibility level string upon success
func (c *client) UpdateCompatibility(subject string, update Compatibility) (compatibility Compatibility, err error) {
	result := compatibilityLevel{
		CompatibilityUpdate: update,
	}
	err = c.restService.handleRequest(newRequest("PUT", subjectConfig, &result, url.PathEscape(subject)), &result)

	return result.CompatibilityUpdate, err
}

// TestCompatibility verifies schema against the subject's compatibility policy
// Returns true if the schema is compatible, false otherwise
func (c *client) TestCompatibility(subject string, version int, schema SchemaInfo) (ok bool, err error) {
	var result compatibilityValue
	candidate := SchemaMetadata{
		SchemaInfo: schema,
	}

	err = c.restService.handleRequest(newRequest("POST", compatibility, &candidate, url.PathEscape(subject), version), &result)

	return result.Compatible, err
}

// GetDefaultCompatibility fetches the global(default) compatibility level
// Returns global(default) compatibility level
func (c *client) GetDefaultCompatibility() (compatibility Compatibility, err error) {
	var result compatibilityLevel
	err = c.restService.handleRequest(newRequest("GET", config, nil), &result)

	return result.Compatibility, err
}

// UpdateDefaultCompatibility updates the global(default) compatibility level level
// Returns new string compatibility level
func (c *client) UpdateDefaultCompatibility(update Compatibility) (compatibility Compatibility, err error) {
	result := compatibilityLevel{
		CompatibilityUpdate: update,
	}
	err = c.restService.handleRequest(newRequest("PUT", config, &result), &result)

	return result.CompatibilityUpdate, err
}
