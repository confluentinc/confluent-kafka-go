/**
 * Copyright 2024 Confluent Inc.
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

package deks

import (
	"encoding/base64"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/cache"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/internal"
	"net/url"
	"strings"
	"sync"
)

/* DEK Registry API endpoints
*
* ====KEKs====
* Returns kek
* -GET /dek-registry/v1/keks/{string: name} returns: JSON blob: kek; raises: 404[70], 422[71]
* Register new kek
* -POST /dek-registry/v1/keks/{string: name} returns JSON blob ; raises: 409[71, 72], 422[71]
*
* ====DEKs====
* Returns dek
* -GET /dek-registry/v1/keks/{string: name}/deks/{string: subject} returns: JSON blob: dek; raises: 404[70], 422[71], 500[70]
* Returns versioned dek
* -GET /dek-registry/v1/keks/{string: name}/deks/{string: subject}/versions/{version} returns: JSON blob: dek; raises: 404[70], 422[02, 71], 500[70]
* Register new dek
* -POST /dek-registry/v1/keks/{string: name}/deks returns JSON blob ; raises: 409[71, 72], 422[71], 500[70]
*
 */

// Kek represents a Key Encryption Key
type Kek struct {
	// Name
	Name string `json:"name,omitempty"`
	// KmsType
	KmsType string `json:"kmsType,omitempty"`
	// KmsKeyID
	KmsKeyID string `json:"kmsKeyId,omitempty"`
	// KmsProps
	KmsProps map[string]string `json:"kmsProps,omitempty"`
	// Doc
	Doc string `json:"doc,omitempty"`
	// Shared
	Shared bool `json:"shared,omitempty"`
	// Ts
	Ts int64 `json:"ts,omitempty"`
	// Deleted
	Deleted bool `json:"deleted,omitempty"`
}

// CreateKekRequest represents a request to create a kek
type CreateKekRequest struct {
	// Name
	Name string `json:"name,omitempty"`
	// KmsType
	KmsType string `json:"kmsType,omitempty"`
	// KmsKeyID
	KmsKeyID string `json:"kmsKeyId,omitempty"`
	// KmsProps
	KmsProps map[string]string `json:"kmsProps,omitempty"`
	// Doc
	Doc string `json:"doc,omitempty"`
	// Shared
	Shared bool `json:"shared,omitempty"`
}

// Dek represents a Data Encryption Key
type Dek struct {
	// KekName
	KekName string `json:"kekName,omitempty"`
	// Subject
	Subject string `json:"subject,omitempty"`
	// Version
	Version int `json:"version,omitempty"`
	// Algorithm
	Algorithm string `json:"algorithm,omitempty"`
	// EncryptedKeyMaterial
	EncryptedKeyMaterial string `json:"encryptedKeyMaterial,omitempty"`
	// EncryptedKeyMaterialBytes
	EncryptedKeyMaterialBytes []byte `json:"-"`
	// KeyMaterial
	KeyMaterial string `json:"keyMaterial,omitempty"`
	// KeyMaterialBytes
	KeyMaterialBytes []byte `json:"-"`
	// Ts
	Ts int64 `json:"ts,omitempty"`
	// Deleted
	Deleted bool `json:"deleted,omitempty"`
}

// GetEncryptedKeyMaterialBytes returns the EncryptedKeyMaterialBytes
func (d *Dek) GetEncryptedKeyMaterialBytes() ([]byte, error) {
	if d.EncryptedKeyMaterial == "" {
		return nil, nil
	}
	if d.EncryptedKeyMaterialBytes == nil {
		bytes, err := base64.StdEncoding.DecodeString(d.EncryptedKeyMaterial)
		if err != nil {
			return nil, err
		}
		d.EncryptedKeyMaterialBytes = bytes
	}
	return d.EncryptedKeyMaterialBytes, nil
}

// GetKeyMaterialBytes returns the KeyMaterialBytes
func (d *Dek) GetKeyMaterialBytes() ([]byte, error) {
	if d.KeyMaterial == "" {
		return nil, nil
	}
	if d.KeyMaterialBytes == nil {
		bytes, err := base64.StdEncoding.DecodeString(d.KeyMaterial)
		if err != nil {
			return nil, err
		}
		d.KeyMaterialBytes = bytes
	}
	return d.KeyMaterialBytes, nil
}

// SetKeyMaterial sets the KeyMaterial using the given bytes
func (d *Dek) SetKeyMaterial(keyMaterialBytes []byte) {
	if keyMaterialBytes != nil {
		str := base64.StdEncoding.EncodeToString(keyMaterialBytes)
		d.KeyMaterial = str
	} else {
		d.KeyMaterial = ""
	}
}

// CreateDekRequest represents a request to create a dek
type CreateDekRequest struct {
	// Subject
	Subject string `json:"subject,omitempty"`
	// Version
	Version int `json:"version,omitempty"`
	// Algorithm
	Algorithm string `json:"algorithm,omitempty"`
	// EncryptedKeyMaterial
	EncryptedKeyMaterial string `json:"encryptedKeyMaterial,omitempty"`
}

// KekID represents a Key Encryption Key ID
type KekID struct {
	// Name
	Name string
	// Deleted
	Deleted bool
}

// DekID represents a Key Encryption Key ID
type DekID struct {
	// KekName
	KekName string
	// Subject
	Subject string
	// Version
	Version int
	// Algorithm
	Algorithm string
	// Deleted
	Deleted bool
}

/* HTTP(S) Schema Registry Client and schema caches */
type client struct {
	sync.Mutex
	config       *schemaregistry.Config
	restService  *internal.RestService
	kekCache     cache.Cache
	kekCacheLock sync.RWMutex
	dekCache     cache.Cache
	dekCacheLock sync.RWMutex
}

var _ Client = new(client)

// Client is an interface for clients interacting with the Confluent DEK Registry.
// The DEK Registry's REST interface is further explained in Confluent's Schema Registry API documentation
// https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClient.java
type Client interface {
	Config() *schemaregistry.Config
	RegisterKek(name string, kmsType string, kmsKeyID string, kmsProps map[string]string, doc string, shared bool) (kek Kek, err error)
	GetKek(name string, deleted bool) (kek Kek, err error)
	RegisterDek(kekName string, subject string, algorithm string, encryptedKeyMaterial string) (dek Dek, err error)
	GetDek(kekName string, subject string, algorithm string, deleted bool) (dek Dek, err error)
	RegisterDekVersion(kekName string, subject string, version int, algorithm string, encryptedKeyMaterial string) (dek Dek, err error)
	GetDekVersion(kekName string, subject string, version int, algorithm string, deleted bool) (dek Dek, err error)
	Close() error
}

// NewClient returns a Client implementation
func NewClient(conf *schemaregistry.Config) (Client, error) {

	urlConf := conf.SchemaRegistryURL
	// for testing
	if strings.HasPrefix(urlConf, "mock://") {
		u, err := url.Parse(urlConf)
		if err != nil {
			return nil, err
		}
		mock := &mockclient{
			config: conf,
			url:    u,
		}
		return mock, nil
	}

	restService, err := internal.NewRestService(&conf.ClientConfig)
	if err != nil {
		return nil, err
	}

	var kekCache cache.Cache
	var dekCache cache.Cache
	if conf.CacheCapacity != 0 {
		kekCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
		dekCache, err = cache.NewLRUCache(conf.CacheCapacity)
		if err != nil {
			return nil, err
		}
	} else {
		kekCache = cache.NewMapCache()
		dekCache = cache.NewMapCache()
	}
	handle := &client{
		config:      conf,
		restService: restService,
		kekCache:    kekCache,
		dekCache:    dekCache,
	}
	return handle, nil
}

// Config returns the client config
func (c *client) Config() *schemaregistry.Config {
	return c.config
}

// RegisterKek registers kek
func (c *client) RegisterKek(name string, kmsType string, kmsKeyID string, kmsProps map[string]string, doc string, shared bool) (kek Kek, err error) {
	cacheKey := KekID{
		Name:    name,
		Deleted: false,
	}
	c.kekCacheLock.RLock()
	cacheValue, ok := c.kekCache.Get(cacheKey)
	c.kekCacheLock.RUnlock()
	if ok {
		return *cacheValue.(*Kek), nil
	}

	input := CreateKekRequest{
		Name:     name,
		KmsType:  kmsType,
		KmsKeyID: kmsKeyID,
		KmsProps: kmsProps,
		Doc:      doc,
		Shared:   shared,
	}
	c.kekCacheLock.Lock()
	// another goroutine could have already put it in cache
	cacheValue, ok = c.kekCache.Get(cacheKey)
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("POST", internal.Keks, &input), &kek)
		if err == nil {
			c.kekCache.Put(cacheKey, &kek)
		} else {
			kek = Kek{}
		}
	} else {
		kek = *cacheValue.(*Kek)
	}
	c.kekCacheLock.Unlock()
	return kek, err
}

// GetKek returns the kek identified by name
// Returns kek object on success
func (c *client) GetKek(name string, deleted bool) (kek Kek, err error) {
	cacheKey := KekID{
		Name:    name,
		Deleted: deleted,
	}
	c.kekCacheLock.RLock()
	cacheValue, ok := c.kekCache.Get(cacheKey)
	c.kekCacheLock.RUnlock()
	if ok {
		return *cacheValue.(*Kek), nil
	}

	c.kekCacheLock.Lock()
	// another goroutine could have already put it in cache
	cacheValue, ok = c.kekCache.Get(cacheKey)
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("GET", internal.KekByName, nil, url.QueryEscape(name), deleted), &kek)
		if err == nil {
			c.kekCache.Put(cacheKey, &kek)
		}
	} else {
		kek = *cacheValue.(*Kek)
	}
	c.kekCacheLock.Unlock()
	return kek, err
}

// RegisterDek registers dek
func (c *client) RegisterDek(kekName string, subject string, algorithm string, encryptedKeyMaterial string) (dek Dek, err error) {
	return c.RegisterDekVersion(kekName, subject, 1, algorithm, encryptedKeyMaterial)
}

// GetDek returns the dek
// Returns dek object on success
func (c *client) GetDek(kekName string, subject string, algorithm string, deleted bool) (dek Dek, err error) {
	cacheKey := DekID{
		KekName:   kekName,
		Subject:   subject,
		Version:   1,
		Algorithm: algorithm,
		Deleted:   deleted,
	}
	c.dekCacheLock.RLock()
	cacheValue, ok := c.dekCache.Get(cacheKey)
	c.dekCacheLock.RUnlock()
	if ok {
		return *cacheValue.(*Dek), nil
	}

	c.dekCacheLock.Lock()
	// another goroutine could have already put it in cache
	cacheValue, ok = c.dekCache.Get(cacheKey)
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("GET", internal.DeksBySubject, nil, url.QueryEscape(kekName), url.QueryEscape(subject), deleted), &dek)
		if err == nil {
			c.dekCache.Put(cacheKey, &dek)
		}
	} else {
		dek = *cacheValue.(*Dek)
	}
	c.dekCacheLock.Unlock()
	return dek, err
}

// RegisterDekVersion registers versioned dek
func (c *client) RegisterDekVersion(kekName string, subject string, version int, algorithm string, encryptedKeyMaterial string) (dek Dek, err error) {
	cacheKey := DekID{
		KekName:   kekName,
		Subject:   subject,
		Version:   version,
		Algorithm: algorithm,
		Deleted:   false,
	}
	c.dekCacheLock.RLock()
	cacheValue, ok := c.dekCache.Get(cacheKey)
	c.dekCacheLock.RUnlock()
	if ok {
		return *cacheValue.(*Dek), nil
	}

	input := CreateDekRequest{
		Subject:              subject,
		Version:              version,
		Algorithm:            algorithm,
		EncryptedKeyMaterial: encryptedKeyMaterial,
	}
	c.dekCacheLock.Lock()
	// another goroutine could have already put it in cache
	cacheValue, ok = c.dekCache.Get(cacheKey)
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("POST", internal.Deks, &input, url.QueryEscape(kekName)), &dek)
		if err == nil {
			c.dekCache.Put(cacheKey, &dek)
		} else {
			dek = Dek{}
		}
		// Ensure latest dek is invalidated, such as in case of conflict (409)
		c.dekCache.Delete(DekID{KekName: kekName, Subject: subject, Version: -1, Algorithm: algorithm, Deleted: false})
		c.dekCache.Delete(DekID{KekName: kekName, Subject: subject, Version: -1, Algorithm: algorithm, Deleted: true})
	} else {
		dek = *cacheValue.(*Dek)
	}
	c.dekCacheLock.Unlock()
	return dek, err
}

// GetDekVersion returns the versioned dek
// Returns dek object on success
func (c *client) GetDekVersion(kekName string, subject string, version int, algorithm string, deleted bool) (dek Dek, err error) {
	cacheKey := DekID{
		KekName:   kekName,
		Subject:   subject,
		Version:   version,
		Algorithm: algorithm,
		Deleted:   deleted,
	}
	c.dekCacheLock.RLock()
	cacheValue, ok := c.dekCache.Get(cacheKey)
	c.dekCacheLock.RUnlock()
	if ok {
		return *cacheValue.(*Dek), nil
	}

	c.dekCacheLock.Lock()
	// another goroutine could have already put it in cache
	cacheValue, ok = c.dekCache.Get(cacheKey)
	if !ok {
		err = c.restService.HandleRequest(internal.NewRequest("GET", internal.DeksByVersion, nil, url.QueryEscape(kekName), url.QueryEscape(subject), version, deleted), &dek)
		if err == nil {
			c.dekCache.Put(cacheKey, &dek)
		}
	} else {
		dek = *cacheValue.(*Dek)
	}
	c.dekCacheLock.Unlock()
	return dek, err
}

// Close closes the client
func (c *client) Close() error {
	return nil
}
