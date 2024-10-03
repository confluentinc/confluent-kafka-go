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
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/cache"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/internal"
)

/* Schema Registry API endpoints
*
* ====Contexts====
* Fetch JSON array str:context of all contexts
* -GET /contexts returns: JSON array string: contexts; raises: 500[01]
*
* ====Schemas====
* Fetch string: schema(escaped) identified by the input id.
* -GET /schemas/ids/{int: id} returns: JSON blob: schema; raises: 404[03], 500[01]
* Fetch string: JSON array (subject, version) of schemas identified by ID.
* -GET /schemas/ids/{int: id}/versions returns: JSON array; raises: 404[03], 500[01]
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

// Rule represents a data contract rule
type Rule struct {
	// Rule name
	Name string `json:"name,omitempty"`
	// Rule doc
	Doc string `json:"doc,omitempty"`
	// Rule kind
	Kind string `json:"kind,omitempty"`
	// Rule mode
	Mode string `json:"mode,omitempty"`
	// Rule type
	Type string `json:"type,omitempty"`
	// The tags to which this rule applies
	Tags []string `json:"tags,omitempty"`
	// Optional params for the rule
	Params map[string]string `json:"params,omitempty"`
	// Rule expression
	Expr string `json:"expr,omitempty"`
	// Rule action on success
	OnSuccess string `json:"onSuccess,omitempty"`
	// Rule action on failure
	OnFailure string `json:"onFailure,omitempty"`
	// Whether the rule is disabled
	Disabled bool `json:"disabled,omitempty"`
}

// RuleMode represents the rule mode
type RuleMode = int

const (
	// Upgrade denotes upgrade mode
	Upgrade = 1
	// Downgrade denotes downgrade mode
	Downgrade = 2
	// UpDown denotes upgrade/downgrade mode
	UpDown = 3
	// Write denotes write mode
	Write = 4
	// Read denotes read mode
	Read = 5
	// WriteRead denotes write/read mode
	WriteRead = 6
)

var modes = map[string]RuleMode{
	"UPGRADE":   Upgrade,
	"DOWNGRADE": Downgrade,
	"UPDOWN":    UpDown,
	"WRITE":     Write,
	"READ":      Read,
	"WRITEREAD": WriteRead,
}

// ParseMode parses the given rule mode
func ParseMode(mode string) (RuleMode, bool) {
	c, ok := modes[strings.ToUpper(mode)]
	return c, ok
}

// RuleSet represents a data contract rule set
type RuleSet struct {
	MigrationRules []Rule `json:"migrationRules,omitempty"`
	DomainRules    []Rule `json:"domainRules,omitempty"`
}

// HasRules checks if the ruleset has rules for the given mode
func (r *RuleSet) HasRules(mode RuleMode) bool {
	switch mode {
	case Upgrade, Downgrade:
		return r.hasRules(r.MigrationRules, func(ruleMode RuleMode) bool {
			return ruleMode == mode || ruleMode == UpDown
		})
	case UpDown:
		return r.hasRules(r.MigrationRules, func(ruleMode RuleMode) bool {
			return ruleMode == mode
		})
	case Write, Read:
		return r.hasRules(r.DomainRules, func(ruleMode RuleMode) bool {
			return ruleMode == mode || ruleMode == WriteRead
		})
	case WriteRead:
		return r.hasRules(r.DomainRules, func(ruleMode RuleMode) bool {
			return ruleMode == mode
		})
	}
	return false
}

func (r *RuleSet) hasRules(rules []Rule, filter func(RuleMode) bool) bool {
	for _, rule := range rules {
		ruleMode, ok := ParseMode(rule.Mode)
		if ok && filter(ruleMode) {
			return true
		}
	}
	return false
}

// Metadata represents user-defined metadata
type Metadata struct {
	Tags       map[string][]string `json:"tags,omitempty"`
	Properties map[string]string   `json:"properties,omitempty"`
	Sensitive  []string            `json:"sensitive,omitempty"`
}

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
	Metadata   *Metadata   `json:"metadata,omitempty"`
	RuleSet    *RuleSet    `json:"ruleSet,omitempty"`
}

// SubjectAndVersion represents a pair of subject and version
type SubjectAndVersion struct {
	Subject string `json:"subject,omitempty"`
	Version int    `json:"version,omitempty"`
}

// MarshalJSON implements the json.Marshaler interface
func (sd *SchemaInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Schema     string      `json:"schema,omitempty"`
		SchemaType string      `json:"schemaType,omitempty"`
		References []Reference `json:"references,omitempty"`
		Metadata   *Metadata   `json:"metadata,omitempty"`
		RuleSet    *RuleSet    `json:"ruleSet,omitempty"`
	}{
		sd.Schema,
		sd.SchemaType,
		sd.References,
		sd.Metadata,
		sd.RuleSet,
	})
}

// UnmarshalJSON implements the json.Unmarshaller interface
func (sd *SchemaInfo) UnmarshalJSON(b []byte) error {
	var err error
	var tmp struct {
		Schema     string      `json:"schema,omitempty"`
		SchemaType string      `json:"schemaType,omitempty"`
		References []Reference `json:"references,omitempty"`
		Metadata   *Metadata   `json:"metadata,omitempty"`
		RuleSet    *RuleSet    `json:"ruleSet,omitempty"`
	}

	err = json.Unmarshal(b, &tmp)

	sd.Schema = tmp.Schema
	sd.SchemaType = tmp.SchemaType
	sd.References = tmp.References
	sd.Metadata = tmp.Metadata
	sd.RuleSet = tmp.RuleSet

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
		Metadata   *Metadata   `json:"metadata,omitempty"`
		RuleSet    *RuleSet    `json:"ruleSet,omitempty"`
		ID         int         `json:"id,omitempty"`
		Subject    string      `json:"subject,omitempty"`
		Version    int         `json:"version,omitempty"`
	}{
		sd.Schema,
		sd.SchemaType,
		sd.References,
		sd.Metadata,
		sd.RuleSet,
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
		Metadata   *Metadata   `json:"metadata,omitempty"`
		RuleSet    *RuleSet    `json:"ruleSet,omitempty"`
		ID         int         `json:"id,omitempty"`
		Subject    string      `json:"subject,omitempty"`
		Version    int         `json:"version,omitempty"`
	}

	err = json.Unmarshal(b, &tmp)

	sd.Schema = tmp.Schema
	sd.SchemaType = tmp.SchemaType
	sd.References = tmp.References
	sd.Metadata = tmp.Metadata
	sd.RuleSet = tmp.RuleSet
	sd.ID = tmp.ID
	sd.Subject = tmp.Subject
	sd.Version = tmp.Version

	return err
}

// ServerConfig represents config params for Schema Registry
/* NOTE: GET uses compatibilityLevel, POST uses compatibility */
type ServerConfig struct {
	Alias               string        `json:"alias,omitempty"`
	Normalize           bool          `json:"normalize,omitempty"`
	CompatibilityUpdate Compatibility `json:"compatibility,omitempty"`
	CompatibilityLevel  Compatibility `json:"compatibilityLevel,omitempty"`
	CompatibilityGroup  string        `json:"compatibilityGroup,omitempty"`
	DefaultMetadata     *Metadata     `json:"defaultMetadata,omitempty"`
	OverrideMetadata    *Metadata     `json:"overrideMetadata,omitempty"`
	DefaultRuleSet      *RuleSet      `json:"defaultRuleSet,omitempty"`
	OverrideRuleSet     *RuleSet      `json:"overrideRuleSet,omitempty"`
}

type subjectJSON struct {
	subject string
	json    string
}

type subjectID struct {
	subject string
	id      int
}

type subjectVersion struct {
	subject string
	version int
	deleted bool
}

type subjectMetadata struct {
	subject  string
	metadata string
	deleted  bool
}

/* HTTP(S) Schema Registry Client and schema caches */
type client struct {
	sync.Mutex
	config                    *Config
	restService               *internal.RestService
	infoToSchemaCache         cache.Cache
	infoToSchemaCacheLock     sync.RWMutex
	idToSchemaInfoCache       cache.Cache
	idToSchemaInfoCacheLock   sync.RWMutex
	schemaToVersionCache      cache.Cache
	schemaToVersionCacheLock  sync.RWMutex
	versionToSchemaCache      cache.Cache
	versionToSchemaCacheLock  sync.RWMutex
	latestToSchemaCache       cache.Cache
	latestToSchemaCacheLock   sync.RWMutex
	metadataToSchemaCache     cache.Cache
	metadataToSchemaCacheLock sync.RWMutex
	evictor                   *evictor
}

var _ Client = new(client)

// Client is an interface for clients interacting with the Confluent Schema Registry.
// The Schema Registry's REST interface is further explained in Confluent's Schema Registry API documentation
// https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClient.java
type Client interface {
	Config() *Config
	GetAllContexts() ([]string, error)
	Register(subject string, schema SchemaInfo, normalize bool) (id int, err error)
	RegisterFullResponse(subject string, schema SchemaInfo, normalize bool) (result SchemaMetadata, err error)
	GetBySubjectAndID(subject string, id int) (schema SchemaInfo, err error)
	GetSubjectsAndVersionsByID(id int) (subjectAndVersion []SubjectAndVersion, err error)
	GetID(subject string, schema SchemaInfo, normalize bool) (id int, err error)
	GetLatestSchemaMetadata(subject string) (SchemaMetadata, error)
	GetSchemaMetadata(subject string, version int) (SchemaMetadata, error)
	GetSchemaMetadataIncludeDeleted(subject string, version int, deleted bool) (SchemaMetadata, error)
	GetLatestWithMetadata(subject string, metadata map[string]string, deleted bool) (SchemaMetadata, error)
	GetAllVersions(subject string) ([]int, error)
	GetVersion(subject string, schema SchemaInfo, normalize bool) (version int, err error)
	GetAllSubjects() ([]string, error)
	DeleteSubject(subject string, permanent bool) ([]int, error)
	DeleteSubjectVersion(subject string, version int, permanent bool) (deletes int, err error)
	TestSubjectCompatibility(subject string, schema SchemaInfo) (compatible bool, err error)
	TestCompatibility(subject string, version int, schema SchemaInfo) (compatible bool, err error)
	GetCompatibility(subject string) (compatibility Compatibility, err error)
	UpdateCompatibility(subject string, update Compatibility) (compatibility Compatibility, err error)
	GetDefaultCompatibility() (compatibility Compatibility, err error)
	UpdateDefaultCompatibility(update Compatibility) (compatibility Compatibility, err error)
	GetConfig(subject string, defaultToGlobal bool) (result ServerConfig, err error)
	UpdateConfig(subject string, update ServerConfig) (result ServerConfig, err error)
	GetDefaultConfig() (result ServerConfig, err error)
	UpdateDefaultConfig(update ServerConfig) (result ServerConfig, err error)
	ClearLatestCaches() error
	ClearCaches() error
	Close() error
}

// NewClient returns a Client implementation
func NewClient(conf *Config) (Client, error) {

	urlConf := conf.SchemaRegistryURL
	// for testing
	if strings.HasPrefix(urlConf, "mock://") {
		url, err := url.Parse(urlConf)
		if err != nil {
			return nil, err
		}
		mock := &mockclient{
			config:               conf,
			url:                  url,
			infoToSchemaCache:    make(map[subjectJSON]metadataCacheEntry),
			idToSchemaCache:      make(map[subjectID]infoCacheEntry),
			schemaToVersionCache: make(map[subjectJSON]versionCacheEntry),
			configCache:          make(map[string]ServerConfig),
		}
		return mock, nil
	}

	restService, err := internal.NewRestService(&conf.ClientConfig)
	if err != nil {
		return nil, err
	}

	var schemaToIDCache cache.Cache
	var idToSchemaCache cache.Cache
	var schemaToVersionCache cache.Cache
	var versionToSchemaCache cache.Cache
	var latestToSchemaCache cache.Cache
	var metadataToSchemaCache cache.Cache
	if conf.CacheCapacity != 0 {
		schemaToIDCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
		idToSchemaCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
		schemaToVersionCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
		versionToSchemaCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
		latestToSchemaCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
		metadataToSchemaCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
	} else {
		schemaToIDCache = cache.NewMapCache()
		idToSchemaCache = cache.NewMapCache()
		schemaToVersionCache = cache.NewMapCache()
		versionToSchemaCache = cache.NewMapCache()
		latestToSchemaCache = cache.NewMapCache()
		metadataToSchemaCache = cache.NewMapCache()
	}
	handle := &client{
		config:                conf,
		restService:           restService,
		infoToSchemaCache:     schemaToIDCache,
		idToSchemaInfoCache:   idToSchemaCache,
		schemaToVersionCache:  schemaToVersionCache,
		versionToSchemaCache:  versionToSchemaCache,
		latestToSchemaCache:   latestToSchemaCache,
		metadataToSchemaCache: metadataToSchemaCache,
	}
	if conf.CacheLatestTTLSecs > 0 {
		runEvictor(handle, time.Duration(conf.CacheLatestTTLSecs)*time.Second)
		runtime.SetFinalizer(handle, stopEvictor)
	}
	return handle, nil
}

// Returns a string slice containing all available contexts
func (c *client) GetAllContexts() ([]string, error) {
	var result []string
	err := c.restService.HandleRequest(internal.NewRequest("GET", internal.Contexts, nil), &result)

	return result, err
}

// Config returns the client config
func (c *client) Config() *Config {
	return c.config
}

// Register registers Schema aliased with subject
func (c *client) Register(subject string, schema SchemaInfo, normalize bool) (id int, err error) {
	metadata, err := c.RegisterFullResponse(subject, schema, normalize)
	if err != nil {
		return -1, err
	}
	return metadata.ID, err
}

// RegisterFullResponse registers Schema aliased with subject
func (c *client) RegisterFullResponse(subject string, schema SchemaInfo, normalize bool) (result SchemaMetadata, err error) {
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
	metadataValue, ok := c.infoToSchemaCache.Get(cacheKey)
	c.infoToSchemaCacheLock.RUnlock()
	if ok {
		return *metadataValue.(*SchemaMetadata), nil
	}

	input := SchemaMetadata{
		SchemaInfo: schema,
	}
	c.infoToSchemaCacheLock.Lock()
	// another goroutine could have already put it in cache
	metadataValue, ok = c.infoToSchemaCache.Get(cacheKey)
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("POST", internal.VersionNormalize, &input, url.PathEscape(subject), normalize), &result)
		if err == nil {
			c.infoToSchemaCache.Put(cacheKey, &result)
		} else {
			result = SchemaMetadata{
				ID: -1,
			}
		}
	} else {
		result = *metadataValue.(*SchemaMetadata)
	}
	c.infoToSchemaCacheLock.Unlock()
	return result, err
}

// GetBySubjectAndID returns the schema identified by id
// Returns Schema object on success
func (c *client) GetBySubjectAndID(subject string, id int) (schema SchemaInfo, err error) {
	cacheKey := subjectID{
		subject: subject,
		id:      id,
	}
	c.idToSchemaInfoCacheLock.RLock()
	infoValue, ok := c.idToSchemaInfoCache.Get(cacheKey)
	c.idToSchemaInfoCacheLock.RUnlock()
	if ok {
		return *infoValue.(*SchemaInfo), nil
	}

	metadata := SchemaMetadata{}
	newInfo := &SchemaInfo{}
	c.idToSchemaInfoCacheLock.Lock()
	// another goroutine could have already put it in cache
	infoValue, ok = c.idToSchemaInfoCache.Get(cacheKey)
	if !ok {
		if len(subject) > 0 {
			err = c.restService.HandleRequest(internal.NewRequest("GET", internal.SchemasBySubject, nil, id, url.QueryEscape(subject)), &metadata)
		} else {
			err = c.restService.HandleRequest(internal.NewRequest("GET", internal.Schemas, nil, id), &metadata)
		}
		if err == nil {
			newInfo = &metadata.SchemaInfo
			c.idToSchemaInfoCache.Put(cacheKey, newInfo)
		}
	} else {
		newInfo = infoValue.(*SchemaInfo)
	}
	c.idToSchemaInfoCacheLock.Unlock()
	return *newInfo, err
}

// GetSubjectsAndVersionsByID returns the subject-version pairs for a given ID.
// Returns SubjectAndVersion object on success.
// This method cannot not use caching to increase performance.
func (c *client) GetSubjectsAndVersionsByID(id int) (subbjectsAndVersions []SubjectAndVersion, err error) {
	err = c.restService.HandleRequest(internal.NewRequest("GET", internal.SubjectsAndVersionsByID, nil, id), &subbjectsAndVersions)
	return
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
	c.infoToSchemaCacheLock.RLock()
	metadataValue, ok := c.infoToSchemaCache.Get(cacheKey)
	c.infoToSchemaCacheLock.RUnlock()
	if ok {
		md := *metadataValue.(*SchemaMetadata)
		return md.ID, nil
	}

	metadata := SchemaMetadata{
		SchemaInfo: schema,
	}
	c.infoToSchemaCacheLock.Lock()
	// another goroutine could have already put it in cache
	metadataValue, ok = c.infoToSchemaCache.Get(cacheKey)
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("POST", internal.SubjectsNormalize, &metadata, url.PathEscape(subject), normalize), &metadata)
		if err == nil {
			c.infoToSchemaCache.Put(cacheKey, &metadata)
		} else {
			metadata.ID = -1
		}
	} else {
		md := *metadataValue.(*SchemaMetadata)
		metadata.ID = md.ID
	}
	c.infoToSchemaCacheLock.Unlock()
	return metadata.ID, err
}

// GetLatestSchemaMetadata fetches latest version registered with the provided subject
// Returns SchemaMetadata object
func (c *client) GetLatestSchemaMetadata(subject string) (result SchemaMetadata, err error) {
	c.latestToSchemaCacheLock.RLock()
	metadataValue, ok := c.latestToSchemaCache.Get(subject)
	c.latestToSchemaCacheLock.RUnlock()
	if ok {
		return *metadataValue.(*SchemaMetadata), nil
	}

	c.latestToSchemaCacheLock.Lock()
	// another goroutine could have already put it in cache
	metadataValue, ok = c.latestToSchemaCache.Get(subject)
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("GET", internal.Versions, nil, url.PathEscape(subject), "latest"), &result)
		if err == nil {
			c.latestToSchemaCache.Put(subject, &result)
		}
	} else {
		result = *metadataValue.(*SchemaMetadata)
	}
	c.latestToSchemaCacheLock.Unlock()
	return result, err
}

// GetSchemaMetadata fetches the requested subject schema identified by version
// Returns SchemaMetadata object
func (c *client) GetSchemaMetadata(subject string, version int) (result SchemaMetadata, err error) {
	return c.GetSchemaMetadataIncludeDeleted(subject, version, false)
}

// GetSchemaMetadataIncludeDeleted fetches the requested subject schema identified by version and deleted flag
// Returns SchemaMetadata object
func (c *client) GetSchemaMetadataIncludeDeleted(subject string, version int, deleted bool) (result SchemaMetadata, err error) {
	cacheKey := subjectVersion{
		subject: subject,
		version: version,
		deleted: deleted,
	}
	c.versionToSchemaCacheLock.RLock()
	metadataValue, ok := c.versionToSchemaCache.Get(cacheKey)
	c.versionToSchemaCacheLock.RUnlock()
	if ok {
		return *metadataValue.(*SchemaMetadata), nil
	}

	c.versionToSchemaCacheLock.Lock()
	// another goroutine could have already put it in cache
	metadataValue, ok = c.versionToSchemaCache.Get(cacheKey)
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("GET", internal.VersionsIncludeDeleted, nil, url.PathEscape(subject), version, deleted), &result)
		if err == nil {
			c.versionToSchemaCache.Put(cacheKey, &result)
		}
	} else {
		result = *metadataValue.(*SchemaMetadata)
	}
	c.versionToSchemaCacheLock.Unlock()
	return result, err
}

// GetLatestWithMetadata fetches the latest subject schema with the given metadata
// Returns SchemaMetadata object
func (c *client) GetLatestWithMetadata(subject string, metadata map[string]string, deleted bool) (result SchemaMetadata, err error) {
	b, _ := json.Marshal(metadata)
	metadataStr := string(b)
	cacheKey := subjectMetadata{
		subject:  subject,
		metadata: metadataStr,
		deleted:  deleted,
	}
	c.metadataToSchemaCacheLock.RLock()
	metadataValue, ok := c.metadataToSchemaCache.Get(cacheKey)
	c.metadataToSchemaCacheLock.RUnlock()
	if ok {
		return *metadataValue.(*SchemaMetadata), nil
	}

	c.metadataToSchemaCacheLock.Lock()
	// another goroutine could have already put it in cache
	metadataValue, ok = c.metadataToSchemaCache.Get(cacheKey)
	sb := strings.Builder{}
	for key, value := range metadata {
		_, _ = sb.WriteString("&key=")
		_, _ = sb.WriteString(key)
		_, _ = sb.WriteString("&value=")
		_, _ = sb.WriteString(value)
	}
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("GET", internal.LatestWithMetadata, nil, url.PathEscape(subject), deleted, sb.String()), &result)
		if err == nil {
			c.metadataToSchemaCache.Put(cacheKey, &result)
		}
	} else {
		result = *metadataValue.(*SchemaMetadata)
	}
	c.metadataToSchemaCacheLock.Unlock()
	return result, err
}

// GetAllVersions fetches a list of all version numbers associated with the provided subject registration
// Returns integer slice on success
func (c *client) GetAllVersions(subject string) (results []int, err error) {
	var result []int
	err = c.restService.HandleRequest(internal.NewRequest("GET", internal.Version, nil, url.PathEscape(subject)), &result)

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
	c.schemaToVersionCacheLock.RLock()
	versionValue, ok := c.schemaToVersionCache.Get(cacheKey)
	c.schemaToVersionCacheLock.RUnlock()
	if ok {
		return versionValue.(int), nil
	}

	metadata := SchemaMetadata{
		SchemaInfo: schema,
	}
	c.schemaToVersionCacheLock.Lock()
	// another goroutine could have already put it in cache
	versionValue, ok = c.schemaToVersionCache.Get(cacheKey)
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("POST", internal.SubjectsNormalize, &metadata, url.PathEscape(subject), normalize), &metadata)
		if err == nil {
			c.schemaToVersionCache.Put(cacheKey, metadata.Version)
		} else {
			metadata.Version = -1
		}
	} else {
		metadata.Version = versionValue.(int)
	}
	c.schemaToVersionCacheLock.Unlock()
	return metadata.Version, err
}

// Fetch all Subjects registered with the schema Registry
// Returns a string slice containing all registered subjects
func (c *client) GetAllSubjects() ([]string, error) {
	var result []string
	err := c.restService.HandleRequest(internal.NewRequest("GET", internal.Subject, nil), &result)

	return result, err
}

// Deletes provided Subject from registry
// Returns integer slice of versions removed by delete
func (c *client) DeleteSubject(subject string, permanent bool) (deleted []int, err error) {
	c.infoToSchemaCacheLock.Lock()
	for keyValue := range c.infoToSchemaCache.ToMap() {
		key := keyValue.(subjectJSON)
		if key.subject == subject {
			c.infoToSchemaCache.Delete(key)
		}
	}
	c.infoToSchemaCacheLock.Unlock()
	c.schemaToVersionCacheLock.Lock()
	for keyValue := range c.schemaToVersionCache.ToMap() {
		key := keyValue.(subjectJSON)
		if key.subject == subject {
			c.schemaToVersionCache.Delete(key)
		}
	}
	c.schemaToVersionCacheLock.Unlock()
	c.versionToSchemaCacheLock.Lock()
	for keyValue := range c.versionToSchemaCache.ToMap() {
		key := keyValue.(subjectVersion)
		if key.subject == subject {
			c.versionToSchemaCache.Delete(key)
		}
	}
	c.versionToSchemaCacheLock.Unlock()
	c.idToSchemaInfoCacheLock.Lock()
	for keyValue := range c.idToSchemaInfoCache.ToMap() {
		key := keyValue.(subjectID)
		if key.subject == subject {
			c.idToSchemaInfoCache.Delete(key)
		}
	}
	c.idToSchemaInfoCacheLock.Unlock()
	var result []int
	err = c.restService.HandleRequest(internal.NewRequest("DELETE", internal.SubjectsDelete, nil, url.PathEscape(subject), permanent), &result)
	return result, err
}

// DeleteSubjectVersion removes the version identified by delete from the subject's registration
// Returns integer id for the deleted version
func (c *client) DeleteSubjectVersion(subject string, version int, permanent bool) (deleted int, err error) {
	c.schemaToVersionCacheLock.Lock()
	for keyValue, value := range c.schemaToVersionCache.ToMap() {
		key := keyValue.(subjectJSON)
		if key.subject == subject && value == version {
			c.schemaToVersionCache.Delete(key)
			schemaJSON := key.json
			cacheKeySchema := subjectJSON{
				subject: subject,
				json:    schemaJSON,
			}
			c.infoToSchemaCacheLock.Lock()
			metadataValue, ok := c.infoToSchemaCache.Get(cacheKeySchema)
			if ok {
				c.infoToSchemaCache.Delete(cacheKeySchema)
			}
			c.infoToSchemaCacheLock.Unlock()
			if ok {
				md := *metadataValue.(*SchemaMetadata)
				c.idToSchemaInfoCacheLock.Lock()
				cacheKeyID := subjectID{
					subject: subject,
					id:      md.ID,
				}
				c.idToSchemaInfoCache.Delete(cacheKeyID)
				c.idToSchemaInfoCacheLock.Unlock()
			}
		}
	}
	c.schemaToVersionCacheLock.Unlock()
	c.versionToSchemaCacheLock.Lock()
	cacheKey := subjectVersion{
		subject: subject,
		version: version,
	}
	c.versionToSchemaCache.Delete(cacheKey)
	c.versionToSchemaCacheLock.Unlock()
	var result int
	err = c.restService.HandleRequest(internal.NewRequest("DELETE", internal.VersionsDelete, nil, url.PathEscape(subject), version, permanent), &result)
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
func (c *Compatibility) MarshalJSON() ([]byte, error) {
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

func (c *Compatibility) String() string {
	return compatibilityEnum[*c]
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

// TestSubjectCompatibility verifies schema against all schemas in the subject
// Returns true if the schema is compatible, false otherwise
func (c *client) TestSubjectCompatibility(subject string, schema SchemaInfo) (ok bool, err error) {
	var result compatibilityValue
	candidate := SchemaMetadata{
		SchemaInfo: schema,
	}

	err = c.restService.HandleRequest(internal.NewRequest("POST", internal.SubjectCompatibility, &candidate, url.PathEscape(subject)), &result)

	return result.Compatible, err
}

// TestCompatibility verifies schema against the subject's compatibility policy
// Returns true if the schema is compatible, false otherwise
func (c *client) TestCompatibility(subject string, version int, schema SchemaInfo) (ok bool, err error) {
	var result compatibilityValue
	candidate := SchemaMetadata{
		SchemaInfo: schema,
	}

	err = c.restService.HandleRequest(internal.NewRequest("POST", internal.Compatibility, &candidate, url.PathEscape(subject), version), &result)

	return result.Compatible, err
}

// Fetch compatibility level currently configured for provided subject
// Returns compatibility level string upon success
func (c *client) GetCompatibility(subject string) (compatibility Compatibility, err error) {
	var result compatibilityLevel
	err = c.restService.HandleRequest(internal.NewRequest("GET", internal.SubjectConfig, nil, url.PathEscape(subject)), &result)

	return result.Compatibility, err
}

// UpdateCompatibility updates subject's compatibility level
// Returns new compatibility level string upon success
func (c *client) UpdateCompatibility(subject string, update Compatibility) (compatibility Compatibility, err error) {
	result := compatibilityLevel{
		CompatibilityUpdate: update,
	}
	err = c.restService.HandleRequest(internal.NewRequest("PUT", internal.SubjectConfig, &result, url.PathEscape(subject)), &result)

	return result.CompatibilityUpdate, err
}

// GetDefaultCompatibility fetches the global(default) compatibility level
// Returns global(default) compatibility level
func (c *client) GetDefaultCompatibility() (compatibility Compatibility, err error) {
	var result compatibilityLevel
	err = c.restService.HandleRequest(internal.NewRequest("GET", internal.Config, nil), &result)

	return result.Compatibility, err
}

// UpdateDefaultCompatibility updates the global(default) compatibility level
// Returns new string compatibility level
func (c *client) UpdateDefaultCompatibility(update Compatibility) (compatibility Compatibility, err error) {
	result := compatibilityLevel{
		CompatibilityUpdate: update,
	}
	err = c.restService.HandleRequest(internal.NewRequest("PUT", internal.Config, &result), &result)

	return result.CompatibilityUpdate, err
}

// Fetch config currently configured for provided subject
// Returns config upon success
func (c *client) GetConfig(subject string, defaultToGlobal bool) (result ServerConfig, err error) {
	err = c.restService.HandleRequest(internal.NewRequest("GET", internal.SubjectConfigDefault, nil, url.PathEscape(subject), defaultToGlobal), &result)

	return result, err
}

// UpdateConfig updates subject's config
// Returns new config string upon success
func (c *client) UpdateConfig(subject string, update ServerConfig) (result ServerConfig, err error) {
	err = c.restService.HandleRequest(internal.NewRequest("PUT", internal.SubjectConfig, &update, url.PathEscape(subject)), &result)

	return result, err
}

// GetDefaultCompatibility fetches the global(default) config
// Returns global(default) config
func (c *client) GetDefaultConfig() (result ServerConfig, err error) {
	err = c.restService.HandleRequest(internal.NewRequest("GET", internal.Config, nil), &result)

	return result, err
}

// UpdateDefaultCompatibility updates the global(default) config
// Returns new string config
func (c *client) UpdateDefaultConfig(update ServerConfig) (result ServerConfig, err error) {
	err = c.restService.HandleRequest(internal.NewRequest("PUT", internal.Config, &update), &result)

	return result, err
}

// ClearLatestCaches clears caches of latest versions
func (c *client) ClearLatestCaches() error {
	c.latestToSchemaCacheLock.Lock()
	c.latestToSchemaCache.Clear()
	c.latestToSchemaCacheLock.Unlock()
	c.metadataToSchemaCacheLock.Lock()
	c.metadataToSchemaCache.Clear()
	c.metadataToSchemaCacheLock.Unlock()
	return nil
}

// ClearCaches clears all caches
func (c *client) ClearCaches() error {
	c.infoToSchemaCacheLock.Lock()
	c.infoToSchemaCache.Clear()
	c.infoToSchemaCacheLock.Unlock()
	c.idToSchemaInfoCacheLock.Lock()
	c.idToSchemaInfoCache.Clear()
	c.idToSchemaInfoCacheLock.Unlock()
	c.schemaToVersionCacheLock.Lock()
	c.schemaToVersionCache.Clear()
	c.schemaToVersionCacheLock.Unlock()
	c.versionToSchemaCacheLock.Lock()
	c.versionToSchemaCache.Clear()
	c.versionToSchemaCacheLock.Unlock()
	c.latestToSchemaCacheLock.Lock()
	c.latestToSchemaCache.Clear()
	c.latestToSchemaCacheLock.Unlock()
	c.metadataToSchemaCacheLock.Lock()
	c.metadataToSchemaCache.Clear()
	c.metadataToSchemaCacheLock.Unlock()
	return nil
}

// Close closes the client
func (c *client) Close() error {
	c.ClearCaches()
	return nil
}

type evictor struct {
	Interval time.Duration
	stop     chan bool
}

func (e *evictor) Run(c cache.Cache) {
	ticker := time.NewTicker(e.Interval)
	for {
		select {
		case <-ticker.C:
			c.Clear()
		case <-e.stop:
			ticker.Stop()
			return
		}
	}
}

func stopEvictor(c *client) {
	c.evictor.stop <- true
}

func runEvictor(c *client, ci time.Duration) {
	e := &evictor{
		Interval: ci,
		stop:     make(chan bool),
	}
	c.evictor = e
	go e.Run(c.latestToSchemaCache)
}
