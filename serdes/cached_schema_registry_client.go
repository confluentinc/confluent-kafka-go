package serdes

import (
	"log"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/groupcache/lru"
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
* Delete subject and it's associated subject configuration override
* -DELETE /subjects/{string: subject}) returns: JSON array int: version; raises: 404[01], 500[01]
* Delete subject version
* -DELETE /subjects/{string: subject}/versions/{int|str('latest'): version} returns int: deleted version id; raises: 404[01, 02]
*
* Register new schema under subject
* -POST /subjects/{string: subject}/versions returns JSON blob ; raises: 409, 422[01], 500[01, 02, 03]
* Return SchemaMetadata for the subject version (if any) associated with the schema in the request body
* -POST /subjects/{string: subject} returns JSON *schemaMetadata*; raises: 404[01, 03]
*
* ====Comparability====
* Test schema (http body) against configured comparability for subject version
* -POST /compatibility/subjects/{string: subject}/versions/{int:string('latest'): version} returns: JSON bool:is_compatible; raises: 404[01,02], 422[01,02], 500[01]
*
* ====Config====
* Returns global configuration
* -GET /config  returns: JSON string:comparability; raises: 500[01]
* Update global SR config
* -PUT /config returns: JSON string:compatibility; raises: 422[03], 500[01, 03]
* Update subject level override
* -PUT /config/{string: subject} returns: JSON string:compatibility; raises: 422[03], 500[01,03]
* Returns compatibility level of subject
* GET /config/(string: subject) returns: JSON string:compatibility; raises: 404, 500[01]
 */

type Schema *string
type Subject string

/* LRU cache of Schemas */
type schemaCache struct {
	*lru.Cache
}

// Return Schema if available in the LRU
// If no Schema matches its added with an initialized Cache entry
func (c schemaCache) get(schema Schema) *subjectRegistry {
	if sd, ok := c.Get(schema); ok {
		return sd.(*subjectRegistry)
	}

	s := &subjectRegistry{
		subjects: make(map[string]*cacheEntry),
	}

	s.Schema = schema

	c.Add(schema, s)
	return s
}

/* Schema and its integer id */
type schemaDescriptor struct {
	ID     int    `json:"id,omitempty"`
	Schema Schema `json:"schema,omitempty"`
}

/* Cache entry state */
type cacheEntry struct {
	cond  *sync.Cond `json:"-"`
	err   error
	state int
}

/* Subject Version and its associated Schema */
type Version struct {
	Version int `json:"version,omitempty"`
	schemaDescriptor
}

/* Subject->Schema registration table */
type subjectRegistry struct {
	subjects map[string]*cacheEntry `json:"-"`
	schemaDescriptor
}

/* Index->Schema registry */
type idRegistry struct {
	*cacheEntry
	schemaDescriptor
}

/* HTTP(S) Schema Registry Client and schema caches */
type client struct {
	sync.Mutex
	restService *RestService
	schemaByID  *lru.Cache
	idBySchema  *schemaCache
}

/* https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClient.java */
type SchemaRegistryClient interface {
	Register(subject Subject, schema Schema) (id int, err error)
	GetByID(id int) (schema Schema, err error)
	GetID(subject Subject, schema Schema) (id int, err error)
	GetLatestSchemaMetadata(subject Subject) (Version, error)
	GetSchemaMetadata(subject Subject, version int) (Version, error)
	GetVersion(subject Subject, schema Schema) (version int, err error)
	GetAllVersions(subject Subject) ([]int, error)
	GetCompatibility(subject Subject) (compatibility string, err error)
	GetAllSubjects() ([]string, error)
	DeleteSubject(subject Subject) ([]int, error)
	DeleteSchemaVersion(subject Subject, version int) (deletes int, err error)
	TestCompatibility(subject Subject, version int, schema Schema) (compatible bool, err error)
	UpdateCompatibility(subject Subject, update Compatibility) (compatibility string, err error)
	GetDefaultCompatibility() (compatibility string, err error)
	UpdateDefaultCompatibility(update Compatibility) (compatibility string, err error)
}

/* Returns concrete SchemaRegistry Client to be used by Avro SERDES */
func NewCachedSchemaRegistryClient(conf *kafka.ConfigMap) (SchemaRegistryClient *client, err error) {
	restService, err := NewRestService(conf)

	if err != nil {
		return nil, err
	}

	handle := &client{
		restService: restService,
		idBySchema:  &schemaCache{lru.New(conf.GetInt("schema.registry.max.cached.schemas", 1000))},
		schemaByID:  lru.New(conf.GetInt("schema.registry.max.cached.schemas", 1000)),
	}
	handle.idBySchema.OnEvicted = OnEvict
	return handle, nil
}

/* Callback executed on cache entry eviction */
func OnEvict(key lru.Key, value interface{}) {
	log.Printf("Evicting entry %+v from cache\n", key)
}

// Register new subject with the provided schema.
// Returns the Schema's integer id after registration succeeds
func (c *client) Register(subject Subject, schema Schema) (id int, err error) {
	c.Lock()
	sd := c.idBySchema.get(schema)

	var s *cacheEntry
	if s = sd.subjects[string(subject)]; s != nil {
		for s.state == 0 {
			log.Printf("Parking redundant registration for %s while request is in flight.\n", subject)
			s.cond.Wait()
		}
		c.Unlock()
		log.Printf("Subject %s registration served from local cache\n", subject)
		return sd.ID, s.err
	}

	s = &cacheEntry{
		cond: sync.NewCond(c),
	}

	sd.subjects[string(subject)] = s
	c.Unlock()

	err = c.restService.handleRequest(newAPI("POST", version, &sd, subject), &sd)
	log.Printf("Registered subject %s with schema %d with remote registry\n", subject, sd.ID)

	c.Lock()
	if err != nil {
		s.err = err
	}
	c.idBySchema.Add(schema, sd)
	s.state = sd.ID
	s.cond.Broadcast()
	c.Unlock()
	return sd.ID, err
}

// Fetch schema identified by input id.
// Returns Schema object on success
func (c *client) GetByID(id int) (schema Schema, err error) {
	c.Lock()
	if entry, ok := c.schemaByID.Get(id); ok {
		entry := entry.(*idRegistry)
		for entry.Schema == nil {
			log.Printf("Parking redundant fetch for schema %d while request is in flight.\n", id)
			entry.cond.Wait()
		}
		c.Unlock()
		log.Printf("Returning schema %d from local cache\n", id)
		return entry.Schema, entry.err
	}

	entry := &idRegistry{
		cacheEntry: &cacheEntry{
			cond: sync.NewCond(c),
		},
	}

	c.schemaByID.Add(id, entry)
	c.Unlock()

	log.Printf("Retrieving schema %d from remote registry\n", id)
	err = c.restService.handleRequest(newAPI("GET", schemas, nil, id), &entry)

	c.Lock()
	entry.cond.Broadcast()
	c.schemaByID.Add(id, entry)
	c.Unlock()

	return entry.Schema, err
}

// Fetch schema ID associated with Subject and Schema
// Returns Schema integer ID
func (c *client) GetID(subject Subject, schema Schema) (id int, err error) {
	result := &schemaDescriptor{}
	result.Schema = schema

	err = c.restService.handleRequest(newAPI("POST", subjects, &result, subject), &result)

	return result.ID, err
}

// Find Subject Version associated with the provided schema
// Returns integer Version number
func (c *client) GetVersion(subject Subject, schema Schema) (id int, err error) {
	result := &Version{}
	result.Schema = schema

	err = c.restService.handleRequest(newAPI("POST", subjects, &result, subject), &result)

	return result.Version, err
}

// Fetches a list of all version numbers associated with the provided subject registration
// Returns integer slice on success
func (c *client) GetAllVersions(subject Subject) (results []int, err error) {
	var result []int
	err = c.restService.handleRequest(newAPI("GET", version, nil, subject), &result)

	return result, err
}

// Fetches latest version registered with the provided subject
// Returns Version object
func (c *client) GetLatestSchemaMetadata(subject Subject) (schemaMetadata Version, err error) {
	var result Version
	err = c.restService.handleRequest(newAPI("GET", versions, nil, subject, "latest"), &result)

	return result, err
}

// Fetch requested version registered with provided subject
// Returns Version object
func (c *client) GetSchemaMetadata(subject Subject, version int) (schemaMetadata Version, err error) {
	var result Version
	err = c.restService.handleRequest(newAPI("GET", versions, nil, subject, version), &result)

	return result, err
}

// Fetch all Subjects registered with the schema Registry
// Returns a string slice containing all registered subjects
func (c *client) GetAllSubjects() ([]string, error) {
	var result []string
	err := c.restService.handleRequest(newAPI("GET", subject, nil), &result)

	return result, err
}

// Deletes provided Subject from registry
// Returns integer slice of versions removed by delete
func (c *client) DeleteSubject(subject Subject) (deleted []int, err error) {
	var result []int
	err = c.restService.handleRequest(newAPI("DELETE", subjects, nil, subject), &result)

	return result, err
}

// Delete version of provided subject
// Returns integer id for the deleted version
func (c *client) DeleteSchemaVersion(subject Subject, delete int) (deleted int, err error) {
	var result int
	err = c.restService.handleRequest(newAPI("DELETE", versions, nil, subject, delete), &result)

	return result, err

}

/* Compatibility ENUM */
type Compatibility int

const (
	NONE = iota
	BACKWARD
	FORWARD
	FULL
)

var compatibilityENUM = []string{
	"none",
	"backward",
	"forward",
	"full",
}

/* GET uses compatibilityLevel, POST uses compatibility */
type compatibilityLevel struct {
	CompatibilityUpdate string `json:"compatibility,omitempty"`
	Compatibility       string `json:"compatibilityLevel,omitempty"`
}

type compatibilityValue struct {
	Compatible bool `json:"is_compatible,omitempty"`
}

// Tests Subject registration with Schema against configured compatibility version
// Returns true if the schema is compatible, false otherwise
func (c *client) TestCompatibility(subject Subject, version int, schema Schema) (ok bool, err error) {
	var result compatibilityValue
	candidate := &Version{}
	candidate.Schema = schema

	err = c.restService.handleRequest(newAPI("POST", compatibility, &candidate, subject, version), &result)

	return result.Compatible, err
}

// Update Subject compatibility level
// Returns new compatibility level string upon success
func (c *client) UpdateCompatibility(subject Subject, update Compatibility) (compatibility string, err error) {
	result := &compatibilityLevel{
		CompatibilityUpdate: compatibilityENUM[update],
	}
	err = c.restService.handleRequest(newAPI("PUT", override, &result, subject), &result)

	return result.CompatibilityUpdate, err
}

// Fetch compatibility level currently configured for provided subject
// Returns compatibility level string upon success
func (c *client) GetCompatibility(subject Subject) (compatibility string, err error) {
	var result compatibilityLevel
	err = c.restService.handleRequest(newAPI("GET", override, nil, subject), &result)

	return result.Compatibility, err
}

// Updates global(default) compatibility configuration
// Returns new string compatibility level
func (c *client) UpdateDefaultCompatibility(update Compatibility) (compatibility string, err error) {
	result := &compatibilityLevel{
		CompatibilityUpdate: compatibilityENUM[update],
	}
	err = c.restService.handleRequest(newAPI("PUT", config, &result), &result)

	return result.CompatibilityUpdate, err
}

// Fetches global(default) compatibility level
// Returns global(default) compatibility level
func (c *client) GetDefaultCompatibility() (compatibility string, err error) {
	var result compatibilityLevel
	err = c.restService.handleRequest(newAPI("GET", config, nil), &result)

	return result.Compatibility, err
}
