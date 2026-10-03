/**
 * Copyright 2026 Confluent Inc.
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

package internal

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rest"
)

type failoverResponseBody struct {
	*strings.Reader
	closes int
}

func (b *failoverResponseBody) Close() error {
	b.closes++
	return nil
}

type failoverTransport func(*http.Request) (*http.Response, error)

func (f failoverTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestHandleRequest_ClosesResponseBodyOnFailover(t *testing.T) {
	for _, tt := range []struct {
		name       string
		statuses   []int
		maxRetries int
		wantStatus int
		wantCalls  int
	}{
		{"success after failover", []int{503, 200}, 1, 0, 3},
		{"no content after failover", []int{503, 204}, 1, 0, 3},
		{"final retriable error", []int{503, 502}, 2, 502, 6},
		{"final permanent error", []int{503, 404}, 1, 404, 3},
		{"final network error", []int{503, 0}, 1, -1, 4},
		{"several failovers", []int{503, 502, 200}, 2, 0, 7},
		{"single URL", []int{503}, 2, 503, 3},
		{"success without failover", []int{200, 503}, 1, 0, 1},
		{"permanent error without failover", []int{404, 200}, 1, 404, 1},
		{"retry limit without failover", []int{503, 200}, 0, 503, 1},
		{"retry limit after failover", []int{503, 502, 200}, 1, 502, 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			urls := make([]string, len(tt.statuses))
			statuses := make(map[string]int, len(tt.statuses))
			for i, status := range tt.statuses {
				host := fmt.Sprintf("registry-%d.invalid", i)
				urls[i] = "http://" + host
				statuses[host] = status
			}

			var bodies []*failoverResponseBody
			calls := 0
			lastStatus := 0
			lastHost := ""
			networkError := errors.New("connection refused")
			transport := failoverTransport(func(request *http.Request) (*http.Response, error) {
				calls++
				if len(bodies) > 0 {
					previous := bodies[len(bodies)-1]
					if previous.closes != 1 || previous.Len() != 0 {
						t.Errorf("Previous response body was not drained and closed before request %d: closes=%d, unread=%d", calls, previous.closes, previous.Len())
					}
				}
				lastHost = request.URL.Host
				lastStatus = statuses[lastHost]
				if lastStatus == 0 {
					return nil, networkError
				}
				payload := `{"id":1}`
				if lastStatus == http.StatusNoContent {
					payload = ""
				} else if !isSuccess(lastStatus) {
					payload = fmt.Sprintf(`{"error_code":%d,"message":%q}`, lastStatus*100+1, lastHost)
				}
				body := &failoverResponseBody{Reader: strings.NewReader(payload)}
				bodies = append(bodies, body)
				return &http.Response{StatusCode: lastStatus, Body: body, Header: make(http.Header)}, nil
			})
			rs, err := NewRestService(&ClientConfig{
				SchemaRegistryURL: strings.Join(urls, ","),
				MaxRetries:        tt.maxRetries,
				RetriesWaitMs:     1,
				RetriesMaxWaitMs:  2,
				HTTPClient:        &http.Client{Transport: transport},
			})
			if err != nil {
				t.Fatalf("Failed to create RestService: %v", err)
			}

			var response map[string]interface{}
			err = rs.HandleRequest(NewRequest("GET", "/subjects", nil), &response)
			switch {
			case tt.wantStatus == -1:
				if !errors.Is(err, networkError) {
					t.Errorf("Expected network error, got %v", err)
				}
			case tt.wantStatus > 0:
				var restErr *rest.Error
				if !errors.As(err, &restErr) {
					t.Errorf("Expected a *rest.Error, got %v", err)
				} else if restErr.Status != tt.wantStatus || restErr.Code != tt.wantStatus*100+1 || restErr.Message != lastHost {
					t.Errorf("Expected final error response from %s with status %d, got %+v", lastHost, tt.wantStatus, restErr)
				}
			default:
				if err != nil {
					t.Errorf("Expected success, got %v", err)
				} else if lastStatus != http.StatusNoContent && response["id"] != float64(1) {
					t.Errorf("Expected response id 1, got %v", response)
				}
			}
			if calls != tt.wantCalls {
				t.Errorf("Expected %d requests, got %d", tt.wantCalls, calls)
			}
			for i, body := range bodies {
				if body.closes != 1 {
					t.Errorf("Response body %d was closed %d times, expected once", i, body.closes)
				}
			}
		})
	}
}
