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

package encryption

import (
	"fmt"
	"strings"
	"sync"
)

var (
	kmsDriversMu sync.RWMutex
	kmsDrivers   = []KMSDriver{}
)

// RegisterKMSDriver is used to register a new KMS driver.
func RegisterKMSDriver(kmsDriver KMSDriver) {
	kmsDriversMu.Lock()
	defer kmsDriversMu.Unlock()
	kmsDrivers = append(kmsDrivers, kmsDriver)
}

// GetKMSDriver fetches a KMSDriver by a given URI.
func GetKMSDriver(keyURI string) (KMSDriver, error) {
	kmsDriversMu.RLock()
	defer kmsDriversMu.RUnlock()
	for _, kmsDriver := range kmsDrivers {
		if supported(kmsDriver, keyURI) {
			return kmsDriver, nil
		}
	}
	return nil, fmt.Errorf("KMS driver supporting %s not found", keyURI)
}

func supported(kmsDriver KMSDriver, keyURL string) bool {
	return strings.HasPrefix(strings.ToLower(keyURL), kmsDriver.GetKeyURLPrefix())
}
