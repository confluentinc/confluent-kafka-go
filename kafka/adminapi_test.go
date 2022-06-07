/**
 * Copyright 2018 Confluent Inc.
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

package kafka

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestAdminAPIWithDefaultValue tests CreateTopics with default
// NumPartitions and ReplicationFactor values
func TestAdminAPIWithDefaultValue(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	topic := "testWithDefaultValue"

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers}
	if err := conf.updateFromTestconf(); err != nil {
		t.Fatalf("Failed to update test configuration: %v\n", err)
	}

	expDuration, err := time.ParseDuration("30s")
	if err != nil {
		t.Fatalf("Failed to Parse Duration: %s", err)
	}

	adminClient, err := NewAdminClient(&conf)
	if err != nil {
		t.Fatalf("Failed to create AdminClient %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err := adminClient.CreateTopics(
		ctx,
		[]TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     -1,
				ReplicationFactor: -1,
			},
		})
	if err != nil {
		adminClient.Close()
		t.Fatalf("Failed to create topics %v\n", err)
	}
	t.Logf("Succeed to create topic %v\n", res)

	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err = adminClient.DeleteTopics(ctx, []string{topic})
	if err != nil {
		adminClient.Close()
		t.Fatalf("Failed to delete topic %v, err: %v", topic, err)
	}
	t.Logf("Succeed to delete topic %v\n", res)

	adminClient.Close()
}

func testAdminAPIsCreateAcls(what string, a *AdminClient, t *testing.T) {
	var res []CreateAclResult
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	var expDuration time.Duration
	var expDurationLonger time.Duration
	var invalidTests []AclBindings

	checkFail := func(res []CreateAclResult, err error) {
		if res != nil || err == nil {
			t.Fatalf("Expected CreateAcls to fail, but got result: %v, err: %v", res, err)
		}
	}

	aclBindings := AclBindings{
		{
			Type:                ResourceTopic,
			Name:                "mytopic",
			ResourcePatternType: ResourcePatternTypeLiteral,
			Principal:           "User:myuser",
			Host:                "*",
			Operation:           AclOperationAll,
			PermissionType:      AclPermissionTypeAllow,
		},
	}

	copyAclBindings := func() AclBindings {
		return append(AclBindings{}, aclBindings...)
	}

	t.Logf("AdminClient API - ACLs testing on %s: %s", a, what)
	expDuration, err = time.ParseDuration("0.1s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	// nil aclBindings
	res, err = a.CreateAcls(ctx, nil)
	checkFail(res, err)
	if err.Error() != "Expected non-nil slice of AclBinding structs" {
		t.Fatalf("Expected a different error than \"%v\"", err.Error())
	}

	// empty aclBindings
	res, err = a.CreateAcls(ctx, AclBindings{})
	checkFail(res, err)
	if err.Error() != "Expected non-empty slice of AclBinding structs" {
		t.Fatalf("Expected a different error than \"%v\"", err.Error())
	}

	// Correct input, fail with timeout
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()

	res, err = a.CreateAcls(ctx, aclBindings)
	checkFail(res, err)
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
	}

	// request timeout comes before context deadline
	expDurationLonger, err = time.ParseDuration("0.2s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDurationLonger)
	defer cancel()

	res, err = a.CreateAcls(ctx, aclBindings, SetAdminRequestTimeout(expDuration))
	checkFail(res, err)
	if err.Error() != "Failed while waiting for controller: Local: Timed out" {
		t.Fatalf("Expected a different error than \"%v\"", err.Error())
	}

	// Invalid ACL bindings
	invalidTests = []AclBindings{copyAclBindings(), copyAclBindings()}
	invalidTests[0][0].Type = ResourceUnknown
	invalidTests[1][0].Type = ResourceAny
	for _, invalidAclBindings := range invalidTests {
		res, err = a.CreateAcls(ctx, invalidAclBindings)
		checkFail(res, err)
		if !strings.HasSuffix(err.Error(), ": Invalid resource type") {
			t.Fatalf("Expected a different error than \"%v\"", err.Error())
		}
	}

	suffixes := []string{
		": Invalid resource pattern type",
		": Invalid resource pattern type",
		": Invalid resource pattern type",
		": Invalid operation",
		": Invalid operation",
		": Invalid permission type",
		": Invalid permission type",
		": Invalid resource name",
		": Invalid principal",
		": Invalid host",
	}
	nInvalidTests := len(suffixes)
	invalidTests = make([]AclBindings, nInvalidTests)
	for i := 0; i < nInvalidTests; i++ {
		invalidTests[i] = copyAclBindings()
	}
	invalidTests[0][0].ResourcePatternType = ResourcePatternTypeUnknown
	invalidTests[1][0].ResourcePatternType = ResourcePatternTypeMatch
	invalidTests[2][0].ResourcePatternType = ResourcePatternTypeAny
	invalidTests[3][0].Operation = AclOperationUnknown
	invalidTests[4][0].Operation = AclOperationAny
	invalidTests[5][0].PermissionType = AclPermissionTypeUnknown
	invalidTests[6][0].PermissionType = AclPermissionTypeAny
	invalidTests[7][0].Name = ""
	invalidTests[8][0].Principal = ""
	invalidTests[9][0].Host = ""

	for i, invalidAclBindings := range invalidTests {
		res, err = a.CreateAcls(ctx, invalidAclBindings)
		checkFail(res, err)
		if !strings.HasSuffix(err.Error(), suffixes[i]) {
			t.Fatalf("Expected a different error than \"%v\"", err.Error())
		}
	}
}

func testAdminAPIsDescribeAcls(what string, a *AdminClient, t *testing.T) {
	var res *DescribeAclsResult
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	var expDuration time.Duration
	var expDurationLonger time.Duration

	checkFail := func(res *DescribeAclsResult, err error) {
		if res != nil || err == nil {
			t.Fatalf("Expected DescribeAcls to fail, but got result: %v, err: %v", res, err)
		}
	}

	aclBindingsFilter := AclBindingFilter{
		Type:                ResourceTopic,
		ResourcePatternType: ResourcePatternTypeLiteral,
		Operation:           AclOperationAll,
		PermissionType:      AclPermissionTypeAllow,
	}

	t.Logf("AdminClient API - ACLs testing on %s: %s", a, what)
	expDuration, err = time.ParseDuration("0.1s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Correct input, fail with timeout
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()

	res, err = a.DescribeAcls(ctx, aclBindingsFilter)
	checkFail(res, err)
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
	}

	// request timeout comes before context deadline
	expDurationLonger, err = time.ParseDuration("0.2s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDurationLonger)
	defer cancel()

	res, err = a.DescribeAcls(ctx, aclBindingsFilter, SetAdminRequestTimeout(expDuration))
	checkFail(res, err)
	if err.Error() != "Failed while waiting for controller: Local: Timed out" {
		t.Fatalf("Expected a different error than \"%v\"", err.Error())
	}

	// Invalid ACL binding filters
	suffixes := []string{
		": Invalid resource pattern type",
		": Invalid operation",
		": Invalid permission type",
	}
	nInvalidTests := len(suffixes)
	invalidTests := make(AclBindingFilters, nInvalidTests)
	for i := 0; i < nInvalidTests; i++ {
		invalidTests[i] = aclBindingsFilter
	}
	invalidTests[0].ResourcePatternType = ResourcePatternTypeUnknown
	invalidTests[1].Operation = AclOperationUnknown
	invalidTests[2].PermissionType = AclPermissionTypeUnknown

	for i, invalidAclBindingFilter := range invalidTests {
		res, err = a.DescribeAcls(ctx, invalidAclBindingFilter)
		checkFail(res, err)
		if !strings.HasSuffix(err.Error(), suffixes[i]) {
			t.Fatalf("Expected a different error than \"%v\"", err.Error())
		}
	}

	// ACL binding filters are valid with empty strings,
	// matching any value
	validTests := [3]AclBindingFilter{}
	for i := 0; i < len(validTests); i++ {
		validTests[i] = aclBindingsFilter
	}
	validTests[0].Name = ""
	validTests[1].Principal = ""
	validTests[2].Host = ""

	for _, validAclBindingFilter := range validTests {
		res, err = a.DescribeAcls(ctx, validAclBindingFilter)
		checkFail(res, err)
		if ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
		}
	}
}

func testAdminAPIsDeleteAcls(what string, a *AdminClient, t *testing.T) {
	var res []DeleteAclsResult
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	var expDuration time.Duration
	var expDurationLonger time.Duration

	checkFail := func(res []DeleteAclsResult, err error) {
		if res != nil || err == nil {
			t.Fatalf("Expected DeleteAcl to fail, but got result: %v, err: %v", res, err)
		}
	}

	aclBindingsFilters := AclBindingFilters{
		{
			Type:                ResourceTopic,
			ResourcePatternType: ResourcePatternTypeLiteral,
			Operation:           AclOperationAll,
			PermissionType:      AclPermissionTypeAllow,
		},
	}

	copyAclBindingFilters := func() AclBindingFilters {
		return append(AclBindingFilters{}, aclBindingsFilters...)
	}

	t.Logf("AdminClient API - ACLs testing on %s: %s", a, what)
	expDuration, err = time.ParseDuration("0.1s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	// nil aclBindingFilters
	res, err = a.DeleteAcls(ctx, nil)
	checkFail(res, err)
	if err.Error() != "Expected non-nil slice of AclBindingFilter structs" {
		t.Fatalf("Expected a different error than \"%v\"", err.Error())
	}

	// empty aclBindingFilters
	res, err = a.DeleteAcls(ctx, AclBindingFilters{})
	checkFail(res, err)
	if err.Error() != "Expected non-empty slice of AclBindingFilter structs" {
		t.Fatalf("Expected a different error than \"%v\"", err.Error())
	}

	// Correct input, fail with timeout
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()

	res, err = a.DeleteAcls(ctx, aclBindingsFilters)
	checkFail(res, err)
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
	}

	// request timeout comes before context deadline
	expDurationLonger, err = time.ParseDuration("0.2s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDurationLonger)
	defer cancel()

	res, err = a.DeleteAcls(ctx, aclBindingsFilters, SetAdminRequestTimeout(expDuration))
	checkFail(res, err)
	if err.Error() != "Failed while waiting for controller: Local: Timed out" {
		t.Fatalf("Expected a different error than \"%v\"", err.Error())
	}

	// Invalid ACL binding filters
	suffixes := []string{
		": Invalid resource pattern type",
		": Invalid operation",
		": Invalid permission type",
	}
	nInvalidTests := len(suffixes)
	invalidTests := make([]AclBindingFilters, nInvalidTests)
	for i := 0; i < nInvalidTests; i++ {
		invalidTests[i] = copyAclBindingFilters()
	}
	invalidTests[0][0].ResourcePatternType = ResourcePatternTypeUnknown
	invalidTests[1][0].Operation = AclOperationUnknown
	invalidTests[2][0].PermissionType = AclPermissionTypeUnknown

	for i, invalidAclBindingFilters := range invalidTests {
		res, err = a.DeleteAcls(ctx, invalidAclBindingFilters)
		checkFail(res, err)
		if !strings.HasSuffix(err.Error(), suffixes[i]) {
			t.Fatalf("Expected a different error than \"%v\"", err.Error())
		}
	}

	// ACL binding filters are valid with empty strings,
	// matching any value
	validTests := [3]AclBindingFilters{}
	for i := 0; i < len(validTests); i++ {
		validTests[i] = copyAclBindingFilters()
	}
	validTests[0][0].Name = ""
	validTests[1][0].Principal = ""
	validTests[2][0].Host = ""

	for _, validAclBindingFilters := range validTests {
		res, err = a.DeleteAcls(ctx, validAclBindingFilters)
		checkFail(res, err)
		if ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
		}
	}
}

func testAdminAPIs(what string, a *AdminClient, t *testing.T) {
	t.Logf("AdminClient API testing on %s: %s", a, what)

	expDuration, err := time.ParseDuration("0.1s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	confStrings := map[string]string{
		"some.topic.config":  "unchecked",
		"these.are.verified": "on the broker",
		"and.this.is":        "just",
		"a":                  "unit test"}

	// Correct input, fail with timeout
	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err := a.CreateTopics(
		ctx,
		[]TopicSpecification{
			{
				Topic:             "mytopic",
				NumPartitions:     7,
				ReplicationFactor: 3,
			},
			{
				Topic:         "mytopic2",
				NumPartitions: 2,
				ReplicaAssignment: [][]int32{
					[]int32{1, 2, 3},
					[]int32{3, 2, 1},
				},
			},
			{
				Topic:             "mytopic3",
				NumPartitions:     10000,
				ReplicationFactor: 90,
				Config:            confStrings,
			},
		})
	if res != nil || err == nil {
		t.Fatalf("Expected CreateTopics to fail, but got result: %v, err: %v", res, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
	}

	// Incorrect input, fail with ErrInvalidArg
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err = a.CreateTopics(
		ctx,
		[]TopicSpecification{
			{
				// Must not specify both ReplicationFactor and ReplicaAssignment
				Topic:             "mytopic",
				NumPartitions:     2,
				ReplicationFactor: 3,
				ReplicaAssignment: [][]int32{
					[]int32{1, 2, 3},
					[]int32{3, 2, 1},
				},
			},
		})
	if res != nil || err == nil {
		t.Fatalf("Expected CreateTopics to fail, but got result: %v, err: %v", res, err)
	}
	if ctx.Err() != nil {
		t.Fatalf("Did not expect context to fail: %v", ctx.Err())
	}
	if err.(Error).Code() != ErrInvalidArg {
		t.Fatalf("Expected ErrInvalidArg, not %v", err)
	}

	// Incorrect input, fail with ErrInvalidArg
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err = a.CreateTopics(
		ctx,
		[]TopicSpecification{
			{
				// ReplicaAssignment must be same length as Numpartitions
				Topic:         "mytopic",
				NumPartitions: 7,
				ReplicaAssignment: [][]int32{
					[]int32{1, 2, 3},
					[]int32{3, 2, 1},
				},
			},
		})
	if res != nil || err == nil {
		t.Fatalf("Expected CreateTopics to fail, but got result: %v, err: %v", res, err)
	}
	if ctx.Err() != nil {
		t.Fatalf("Did not expect context to fail: %v", ctx.Err())
	}
	if err.(Error).Code() != ErrInvalidArg {
		t.Fatalf("Expected ErrInvalidArg, not %v", err)
	}

	// Correct input, using options
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err = a.CreateTopics(
		ctx,
		[]TopicSpecification{
			{
				Topic:         "mytopic4",
				NumPartitions: 9,
				ReplicaAssignment: [][]int32{
					[]int32{1},
					[]int32{2},
					[]int32{3},
					[]int32{4},
					[]int32{1},
					[]int32{2},
					[]int32{3},
					[]int32{4},
					[]int32{1},
				},
				Config: map[string]string{
					"some.topic.config":  "unchecked",
					"these.are.verified": "on the broker",
					"and.this.is":        "just",
					"a":                  "unit test",
				},
			},
		},
		SetAdminValidateOnly(false))
	if res != nil || err == nil {
		t.Fatalf("Expected CreateTopics to fail, but got result: %v, err: %v", res, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}

	//
	// Remaining APIs
	// Timeout code is identical for all APIs, no need to test
	// them for each API.
	//

	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err = a.CreatePartitions(
		ctx,
		[]PartitionsSpecification{
			{
				Topic:      "topic",
				IncreaseTo: 19,
				ReplicaAssignment: [][]int32{
					[]int32{3234522},
					[]int32{99999},
				},
			},
			{
				Topic:      "topic2",
				IncreaseTo: 2,
				ReplicaAssignment: [][]int32{
					[]int32{99999},
				},
			},
		})
	if res != nil || err == nil {
		t.Fatalf("Expected CreatePartitions to fail, but got result: %v, err: %v", res, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err = a.DeleteTopics(
		ctx,
		[]string{"topic1", "topic2"})
	if res != nil || err == nil {
		t.Fatalf("Expected DeleteTopics to fail, but got result: %v, err: %v", res, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v for error %v", ctx.Err(), err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	cres, err := a.AlterConfigs(
		ctx,
		[]ConfigResource{{Type: ResourceTopic, Name: "topic"}})
	if cres != nil || err == nil {
		t.Fatalf("Expected AlterConfigs to fail, but got result: %v, err: %v", cres, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	cres, err = a.DescribeConfigs(
		ctx,
		[]ConfigResource{{Type: ResourceTopic, Name: "topic"}})
	if cres != nil || err == nil {
		t.Fatalf("Expected DescribeConfigs to fail, but got result: %v, err: %v", cres, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	clusterID, err := a.ClusterID(ctx)
	if err == nil {
		t.Fatalf("Expected ClusterID to fail, but got result: %v", clusterID)
	}
	if ctx.Err() != context.DeadlineExceeded || err != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	controllerID, err := a.ControllerID(ctx)
	if err == nil {
		t.Fatalf("Expected ControllerID to fail, but got result: %v", controllerID)
	}
	if ctx.Err() != context.DeadlineExceeded || err != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}

	testAdminAPIsCreateAcls(what, a, t)
	testAdminAPIsDescribeAcls(what, a, t)
	testAdminAPIsDeleteAcls(what, a, t)
}

// TestAdminAPIs dry-tests most Admin APIs, no broker is needed.
func TestAdminAPIs(t *testing.T) {

	a, err := NewAdminClient(&ConfigMap{})
	if err != nil {
		t.Fatalf("%s", err)
	}

	testAdminAPIs("Non-derived, no config", a, t)
	a.Close()

	a, err = NewAdminClient(&ConfigMap{"retries": 1234})
	if err != nil {
		t.Fatalf("%s", err)
	}

	testAdminAPIs("Non-derived, config", a, t)
	a.Close()

	// Test derived clients
	c, err := NewConsumer(&ConfigMap{"group.id": "test"})
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer c.Close()

	a, err = NewAdminClientFromConsumer(c)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if !strings.Contains(a.String(), c.String()) {
		t.Fatalf("Expected derived client %s to have similar name to parent %s", a, c)
	}

	testAdminAPIs("Derived from consumer", a, t)
	a.Close()

	a, err = NewAdminClientFromConsumer(c)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if !strings.Contains(a.String(), c.String()) {
		t.Fatalf("Expected derived client %s to have similar name to parent %s", a, c)
	}

	testAdminAPIs("Derived from same consumer", a, t)
	a.Close()

	p, err := NewProducer(&ConfigMap{})
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer p.Close()

	a, err = NewAdminClientFromProducer(p)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if !strings.Contains(a.String(), p.String()) {
		t.Fatalf("Expected derived client %s to have similar name to parent %s", a, p)
	}

	testAdminAPIs("Derived from Producer", a, t)
	a.Close()

	a, err = NewAdminClientFromProducer(p)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if !strings.Contains(a.String(), p.String()) {
		t.Fatalf("Expected derived client %s to have similar name to parent %s", a, p)
	}

	testAdminAPIs("Derived from same Producer", a, t)
	a.Close()
}
