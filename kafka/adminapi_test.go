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

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers}
	if err := conf.updateFromTestconf(); err != nil {
		t.Fatalf("Failed to update test configuration: %v\n", err)
	}

	adminClient, err := NewAdminClient(&conf)
	if err != nil {
		t.Fatalf("Failed to create AdminClient %v", err)
	}
	testAdminAPIWithDefaultValue(t, adminClient, nil)

	// adminClient will be closed at the end of above function. So we don't need to close explicitly
	// to test on closed adminClient.
	testAdminAPIWithDefaultValue(t, adminClient, getOperationNotAllowedErrorForClosedClient())

}

func testAdminAPIWithDefaultValue(t *testing.T, a *AdminClient, errCheck error) {
	topic := "testWithDefaultValue"
	expDuration, err := time.ParseDuration("30s")
	if err != nil {
		t.Fatalf("Failed to Parse Duration: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err := a.CreateTopics(
		ctx,
		[]TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     -1,
				ReplicationFactor: -1,
			},
		})
	if err != errCheck {
		t.Errorf("CreateTopics() should have thrown err : %s, but got %s", errCheck, err)
		if !a.IsClosed() {
			a.Close()
			t.Fatalf("Failed to create topics %v\n", err)
		}
	}
	if !a.IsClosed() {
		t.Logf("Succeed to create topic %v\n", res)
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err = a.DeleteTopics(ctx, []string{topic})
	if err != errCheck {
		t.Errorf("DeleteTopics() should have thrown err : %s, but got %s", errCheck, err)
		if !a.IsClosed() {
			a.Close()
			t.Fatalf("Failed to delete topics %v\n", err)
		}
	}
	if !a.IsClosed() {
		t.Logf("Succeed to delete topic %v\n", res)
	}

	a.Close()
}

func testAdminAPIsCreateACLs(what string, a *AdminClient, t *testing.T) {
	var res []CreateACLResult
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	var expDuration time.Duration
	var expDurationLonger time.Duration
	var expError string
	var invalidTests []ACLBindings

	checkFail := func(res []CreateACLResult, err error) {
		if res != nil || err == nil {
			t.Fatalf("Expected CreateACLs to fail, but got result: %v, err: %v", res, err)
		}
	}

	testACLBindings := ACLBindings{
		{
			Type:                ResourceTopic,
			Name:                "mytopic",
			ResourcePatternType: ResourcePatternTypeLiteral,
			Principal:           "User:myuser",
			Host:                "*",
			Operation:           ACLOperationAll,
			PermissionType:      ACLPermissionTypeAllow,
		},
	}

	copyACLBindings := func() ACLBindings {
		return append(ACLBindings{}, testACLBindings...)
	}

	t.Logf("AdminClient API - ACLs testing on %s: %s", a, what)
	expDuration, err = time.ParseDuration("0.5s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	// nil aclBindings
	res, err = a.CreateACLs(ctx, nil)
	checkFail(res, err)
	expError = "Expected non-nil slice of ACLBinding structs"
	if err.Error() != expError {
		t.Fatalf("Expected error \"%s\", received: \"%v\"", expError, err.Error())
	}

	// empty aclBindings
	res, err = a.CreateACLs(ctx, ACLBindings{})
	checkFail(res, err)
	expError = "Expected non-empty slice of ACLBinding structs"
	if err.Error() != expError {
		t.Fatalf("Expected error \"%s\", received: \"%v\"", expError, err.Error())
	}

	// Correct input, fail with timeout
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()

	res, err = a.CreateACLs(ctx, testACLBindings)
	checkFail(res, err)
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
	}

	// request timeout comes before context deadline
	expDurationLonger, err = time.ParseDuration("1s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDurationLonger)
	defer cancel()

	res, err = a.CreateACLs(ctx, testACLBindings, SetAdminRequestTimeout(expDuration))
	checkFail(res, err)
	expError = "Failed while waiting for controller: Local: Timed out"
	if err.Error() != expError {
		t.Fatalf("Expected error \"%s\", received: \"%v\"", expError, err.Error())
	}

	// Invalid ACL bindings
	invalidTests = []ACLBindings{copyACLBindings(), copyACLBindings()}
	invalidTests[0][0].Type = ResourceUnknown
	invalidTests[1][0].Type = ResourceAny
	expError = ": Invalid resource type"
	for _, invalidACLBindings := range invalidTests {
		res, err = a.CreateACLs(ctx, invalidACLBindings)
		checkFail(res, err)
		if !strings.HasSuffix(err.Error(), expError) {
			t.Fatalf("Expected an error ending with \"%s\", received: \"%s\"", expError, err.Error())
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
	invalidTests = make([]ACLBindings, nInvalidTests)
	for i := 0; i < nInvalidTests; i++ {
		invalidTests[i] = copyACLBindings()
	}
	invalidTests[0][0].ResourcePatternType = ResourcePatternTypeUnknown
	invalidTests[1][0].ResourcePatternType = ResourcePatternTypeMatch
	invalidTests[2][0].ResourcePatternType = ResourcePatternTypeAny
	invalidTests[3][0].Operation = ACLOperationUnknown
	invalidTests[4][0].Operation = ACLOperationAny
	invalidTests[5][0].PermissionType = ACLPermissionTypeUnknown
	invalidTests[6][0].PermissionType = ACLPermissionTypeAny
	invalidTests[7][0].Name = ""
	invalidTests[8][0].Principal = ""
	invalidTests[9][0].Host = ""

	for i, invalidACLBindings := range invalidTests {
		res, err = a.CreateACLs(ctx, invalidACLBindings)
		checkFail(res, err)
		if !strings.HasSuffix(err.Error(), suffixes[i]) {
			t.Fatalf("Expected an error ending with \"%s\", received: \"%s\"", suffixes[i], err.Error())
		}
	}
}

func testAdminAPIsDescribeACLs(what string, a *AdminClient, t *testing.T) {
	var res *DescribeACLsResult
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	var expDuration time.Duration
	var expDurationLonger time.Duration
	var expError string

	checkFail := func(res *DescribeACLsResult, err error) {
		if res != nil || err == nil {
			t.Fatalf("Expected DescribeACLs to fail, but got result: %v, err: %v", res, err)
		}
	}

	aclBindingsFilter := ACLBindingFilter{
		Type:                ResourceTopic,
		ResourcePatternType: ResourcePatternTypeLiteral,
		Operation:           ACLOperationAll,
		PermissionType:      ACLPermissionTypeAllow,
	}

	t.Logf("AdminClient API - ACLs testing on %s: %s", a, what)
	expDuration, err = time.ParseDuration("0.5s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Correct input, fail with timeout
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()

	res, err = a.DescribeACLs(ctx, aclBindingsFilter)
	checkFail(res, err)
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
	}

	// request timeout comes before context deadline
	expDurationLonger, err = time.ParseDuration("1s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDurationLonger)
	defer cancel()

	res, err = a.DescribeACLs(ctx, aclBindingsFilter, SetAdminRequestTimeout(expDuration))
	checkFail(res, err)
	expError = "Failed while waiting for controller: Local: Timed out"
	if err.Error() != expError {
		t.Fatalf("Expected error \"%s\", received: \"%v\"", expError, err.Error())
	}

	// Invalid ACL binding filters
	suffixes := []string{
		": Invalid resource pattern type",
		": Invalid operation",
		": Invalid permission type",
	}
	nInvalidTests := len(suffixes)
	invalidTests := make(ACLBindingFilters, nInvalidTests)
	for i := 0; i < nInvalidTests; i++ {
		invalidTests[i] = aclBindingsFilter
	}
	invalidTests[0].ResourcePatternType = ResourcePatternTypeUnknown
	invalidTests[1].Operation = ACLOperationUnknown
	invalidTests[2].PermissionType = ACLPermissionTypeUnknown

	for i, invalidACLBindingFilter := range invalidTests {
		res, err = a.DescribeACLs(ctx, invalidACLBindingFilter)
		checkFail(res, err)
		if !strings.HasSuffix(err.Error(), suffixes[i]) {
			t.Fatalf("Expected an error ending with \"%s\", received: \"%s\"", suffixes[i], err.Error())
		}
	}

	// ACL binding filters are valid with empty strings,
	// matching any value
	validTests := [3]ACLBindingFilter{}
	for i := 0; i < len(validTests); i++ {
		validTests[i] = aclBindingsFilter
	}
	validTests[0].Name = ""
	validTests[1].Principal = ""
	validTests[2].Host = ""

	for _, validACLBindingFilter := range validTests {
		res, err = a.DescribeACLs(ctx, validACLBindingFilter)
		checkFail(res, err)
		if ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
		}
	}
}

func testAdminAPIsDeleteACLs(what string, a *AdminClient, t *testing.T) {
	var res []DeleteACLsResult
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	var expDuration time.Duration
	var expDurationLonger time.Duration
	var expError string

	checkFail := func(res []DeleteACLsResult, err error) {
		if res != nil || err == nil {
			t.Fatalf("Expected DeleteACL to fail, but got result: %v, err: %v", res, err)
		}
	}

	aclBindingsFilters := ACLBindingFilters{
		{
			Type:                ResourceTopic,
			ResourcePatternType: ResourcePatternTypeLiteral,
			Operation:           ACLOperationAll,
			PermissionType:      ACLPermissionTypeAllow,
		},
	}

	copyACLBindingFilters := func() ACLBindingFilters {
		return append(ACLBindingFilters{}, aclBindingsFilters...)
	}

	t.Logf("AdminClient API - ACLs testing on %s: %s", a, what)
	expDuration, err = time.ParseDuration("0.5s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	// nil aclBindingFilters
	res, err = a.DeleteACLs(ctx, nil)
	checkFail(res, err)
	expError = "Expected non-nil slice of ACLBindingFilter structs"
	if err.Error() != expError {
		t.Fatalf("Expected error \"%s\", received: \"%v\"", expError, err.Error())
	}

	// empty aclBindingFilters
	res, err = a.DeleteACLs(ctx, ACLBindingFilters{})
	checkFail(res, err)
	expError = "Expected non-empty slice of ACLBindingFilter structs"
	if err.Error() != expError {
		t.Fatalf("Expected error \"%s\", received: \"%v\"", expError, err.Error())
	}

	// Correct input, fail with timeout
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()

	res, err = a.DeleteACLs(ctx, aclBindingsFilters)
	checkFail(res, err)
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
	}

	// request timeout comes before context deadline
	expDurationLonger, err = time.ParseDuration("1s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDurationLonger)
	defer cancel()

	res, err = a.DeleteACLs(ctx, aclBindingsFilters, SetAdminRequestTimeout(expDuration))
	checkFail(res, err)
	expError = "Failed while waiting for controller: Local: Timed out"
	if err.Error() != expError {
		t.Fatalf("Expected error \"%s\", received: \"%v\"", expError, err.Error())
	}

	// Invalid ACL binding filters
	suffixes := []string{
		": Invalid resource pattern type",
		": Invalid operation",
		": Invalid permission type",
	}
	nInvalidTests := len(suffixes)
	invalidTests := make([]ACLBindingFilters, nInvalidTests)
	for i := 0; i < nInvalidTests; i++ {
		invalidTests[i] = copyACLBindingFilters()
	}
	invalidTests[0][0].ResourcePatternType = ResourcePatternTypeUnknown
	invalidTests[1][0].Operation = ACLOperationUnknown
	invalidTests[2][0].PermissionType = ACLPermissionTypeUnknown

	for i, invalidACLBindingFilters := range invalidTests {
		res, err = a.DeleteACLs(ctx, invalidACLBindingFilters)
		checkFail(res, err)
		if !strings.HasSuffix(err.Error(), suffixes[i]) {
			t.Fatalf("Expected an error ending with \"%s\", received: \"%s\"", suffixes[i], err.Error())
		}
	}

	// ACL binding filters are valid with empty strings,
	// matching any value
	validTests := [3]ACLBindingFilters{}
	for i := 0; i < len(validTests); i++ {
		validTests[i] = copyACLBindingFilters()
	}
	validTests[0][0].Name = ""
	validTests[1][0].Principal = ""
	validTests[2][0].Host = ""

	for _, validACLBindingFilters := range validTests {
		res, err = a.DeleteACLs(ctx, validACLBindingFilters)
		checkFail(res, err)
		if ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
		}
	}
}

func testAdminAPIsListConsumerGroups(
	what string, a *AdminClient, expDuration time.Duration, t *testing.T) {
	state, err := ConsumerGroupStateFromString("Stable")
	if err != nil || state != ConsumerGroupStateStable {
		t.Fatalf("Expected ConsumerGroupStateFromString to work for Stable state")
	}

	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	listres, err := a.ListConsumerGroups(
		ctx, SetAdminRequestTimeout(time.Second),
		SetAdminMatchConsumerGroupStates([]ConsumerGroupState{state}))
	if err == nil {
		t.Fatalf("Expected ListConsumerGroups to fail, but got result: %v, err: %v",
			listres, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}
}

func testAdminAPIsDescribeConsumerGroups(
	what string, a *AdminClient, expDuration time.Duration, t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	descres, err := a.DescribeConsumerGroups(
		ctx, nil, SetAdminRequestTimeout(time.Second))
	if descres.ConsumerGroupDescriptions != nil || err == nil {
		t.Fatalf("Expected DescribeConsumerGroups to fail, but got result: %v, err: %v",
			descres, err)
	}
	if err.(Error).Code() != ErrInvalidArg {
		t.Fatalf("Expected ErrInvalidArg with empty groups list, but got %s", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	descres, err = a.DescribeConsumerGroups(
		ctx, []string{"test"}, SetAdminRequestTimeout(time.Second))
	if descres.ConsumerGroupDescriptions != nil || err == nil {
		t.Fatalf("Expected DescribeConsumerGroups to fail, but got result: %v, err: %v",
			descres, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %s %v", err.(Error).Code(), ctx.Err())
	}
}

func testAdminAPIsDescribeTopics(
	what string, a *AdminClient, expDuration time.Duration, t *testing.T) {
	requestTimeout := SetAdminRequestTimeout(time.Second)
	for _, options := range [][]DescribeTopicsAdminOption{
		{},
		{requestTimeout},
		{requestTimeout, SetAdminOptionIncludeAuthorizedOperations(true)},
		{SetAdminOptionIncludeAuthorizedOperations(false)},
	} {

		// nil slice gives error
		ctx, cancel := context.WithTimeout(context.Background(), expDuration)
		defer cancel()
		descres, err := a.DescribeTopics(
			ctx, NewTopicCollectionOfTopicNames(nil), options...)
		if descres.TopicDescriptions != nil || err == nil {
			t.Fatalf("Expected DescribeTopics to fail, but got result: %v, err: %v",
				descres, err)
		}
		if err.(Error).Code() != ErrInvalidArg {
			t.Fatalf("Expected ErrInvalidArg with nil slice, but got %s", err)
		}

		// Empty slice returns empty TopicDescription slice
		ctx, cancel = context.WithTimeout(context.Background(), expDuration)
		defer cancel()
		descres, err = a.DescribeTopics(
			ctx, NewTopicCollectionOfTopicNames([]string{}), options...)
		if descres.TopicDescriptions == nil || err != nil {
			t.Fatalf("Expected DescribeTopics to succeed, but got result: %v, err: %v",
				descres, err)
		}
		if len(descres.TopicDescriptions) > 0 {
			t.Fatalf("Expected an empty TopicDescription slice, but got %d elements",
				len(descres.TopicDescriptions))
		}

		// Empty topic names
		for _, topicCollection := range []TopicCollection{
			NewTopicCollectionOfTopicNames([]string{""}),
			NewTopicCollectionOfTopicNames([]string{"correct", ""}),
		} {
			ctx, cancel = context.WithTimeout(context.Background(), expDuration)
			defer cancel()
			descres, err = a.DescribeTopics(
				ctx, topicCollection,
				options...)
			if descres.TopicDescriptions != nil || err == nil {
				t.Fatalf("Expected DescribeTopics to fail, but got result: %v, err: %v",
					descres, err)
			}
			if err.(Error).Code() != ErrInvalidArg {
				t.Fatalf("Expected ErrInvalidArg, not %d %v", err.(Error).Code(), err.Error())
			}
		}

		// Normal call
		ctx, cancel = context.WithTimeout(context.Background(), expDuration)
		defer cancel()
		descres, err = a.DescribeTopics(
			ctx, NewTopicCollectionOfTopicNames([]string{"test"}),
			options...)
		if descres.TopicDescriptions != nil || err == nil {
			t.Fatalf("Expected DescribeTopics to fail, but got result: %v, err: %v",
				descres, err)
		}
		if ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("Expected DeadlineExceeded, not %s %v", err.(Error).Code(), ctx.Err())
		}
	}
}

func testAdminAPIsDescribeCluster(
	what string, a *AdminClient, expDuration time.Duration, t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	descres, err := a.DescribeCluster(
		ctx, SetAdminRequestTimeout(time.Second))
	if descres.Nodes != nil || err == nil {
		t.Fatalf("Expected DescribeTopics to fail, but got result: %v, err: %v",
			descres, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %s %v", err.(Error).Code(), ctx.Err())
	}
}

func testAdminAPIsDeleteConsumerGroups(
	what string, a *AdminClient, expDuration time.Duration, t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	dgres, err := a.DeleteConsumerGroups(ctx, []string{"group1"},
		SetAdminRequestTimeout(time.Second))
	if dgres.ConsumerGroupResults != nil || err == nil {
		t.Fatalf("Expected DeleteGroups to fail, but got result: %v, err: %v",
			dgres, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}
}

func testAdminAPIsListConsumerGroupOffsets(
	what string, a *AdminClient, expDuration time.Duration, t *testing.T) {
	topic := "topic"
	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	lres, err := a.ListConsumerGroupOffsets(
		ctx,
		[]ConsumerGroupTopicPartitions{
			{
				"test",
				[]TopicPartition{
					{
						Topic:     &topic,
						Partition: 0,
					},
				},
			},
		},
		SetAdminRequireStableOffsets(false))
	if lres.ConsumerGroupsTopicPartitions != nil || err == nil {
		t.Fatalf("Expected ListConsumerGroupOffsets to fail, but got result: %v, err: %v",
			lres, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}

	// Test with nil topic partitions list, denoting all topic partitions.
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	lres, err = a.ListConsumerGroupOffsets(
		ctx,
		[]ConsumerGroupTopicPartitions{
			{
				"test",
				nil,
			},
		},
		SetAdminRequireStableOffsets(false))
	if lres.ConsumerGroupsTopicPartitions != nil || err == nil {
		t.Fatalf("Expected ListConsumerGroupOffsets to fail, but got result: %v, err: %v",
			lres, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}

	// Test with empty topic partitions list - should error due to an invalid argument.
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	lres, err = a.ListConsumerGroupOffsets(
		ctx,
		[]ConsumerGroupTopicPartitions{
			{
				"test",
				[]TopicPartition{},
			},
		},
		SetAdminRequireStableOffsets(false))
	if lres.ConsumerGroupsTopicPartitions != nil || err == nil {
		t.Fatalf("Expected ListConsumerGroupOffsets to fail, but got result: %v, err: %v",
			lres, err)
	}
	if err.(Error).Code() != ErrInvalidArg {
		t.Fatalf("Expected ErrInvalidArg, not %v", err.(Error).Code())
	}
}

func testAdminAPIsAlterConsumerGroupOffsets(
	what string, a *AdminClient, expDuration time.Duration, t *testing.T) {
	topic := "topic"
	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	ares, err := a.AlterConsumerGroupOffsets(
		ctx,
		[]ConsumerGroupTopicPartitions{
			{
				"test",
				[]TopicPartition{
					{
						Topic:     &topic,
						Partition: 0,
						Offset:    10,
					},
				},
			},
		})
	if ares.ConsumerGroupsTopicPartitions != nil || err == nil {
		t.Fatalf("Expected AlterConsumerGroupOffsets to fail, but got result: %v, err: %v",
			ares, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}
}

func testAdminAPIsListOffsets(
	what string, a *AdminClient, expDuration time.Duration, t *testing.T) {
	topic := "test"
	invalidTopic := ""
	requestTimeout := SetAdminRequestTimeout(time.Second)

	// Invalid option value
	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	result, err := a.ListOffsets(
		ctx,
		map[TopicPartition]OffsetSpec{}, SetAdminRequestTimeout(-1))
	if result.ResultInfos != nil || err == nil {
		t.Fatalf("Expected ListOffsets to fail, but got result: %v, err: %v",
			result, err)
	}
	if err.(Error).Code() != ErrInvalidArg {
		t.Fatalf("Expected ErrInvalidArg, not %v", err)
	}

	for _, options := range [][]ListOffsetsAdminOption{
		{},
		{requestTimeout},
		{requestTimeout, SetAdminIsolationLevel(IsolationLevelReadUncommitted)},
		{SetAdminIsolationLevel(IsolationLevelReadCommitted)},
	} {
		// nil argument should fail, not being treated as empty
		ctx, cancel := context.WithTimeout(context.Background(), expDuration)
		defer cancel()
		result, err := a.ListOffsets(
			ctx,
			nil, options...)
		if result.ResultInfos != nil || err == nil {
			t.Fatalf("Expected ListOffsets to fail, but got result: %v, err: %v",
				result, err)
		}
		if err.(Error).Code() != ErrInvalidArg {
			t.Fatalf("Expected ErrInvalidArg, not %v", err)
		}

		// Empty map returns empty ResultInfos
		ctx, cancel = context.WithTimeout(context.Background(), expDuration)
		defer cancel()
		result, err = a.ListOffsets(
			ctx,
			map[TopicPartition]OffsetSpec{}, options...)
		if result.ResultInfos == nil || err != nil {
			t.Fatalf("Expected ListOffsets to succeed, but got result: %v, err: %v",
				result, err)
		}
		if len(result.ResultInfos) > 0 {
			t.Fatalf("Expected empty ResultInfos, not %v", result.ResultInfos)
		}

		// Invalid TopicPartition
		for _, topicPartitionOffsets := range []map[TopicPartition]OffsetSpec{
			{{Topic: &invalidTopic, Partition: 0}: EarliestOffsetSpec},
			{{Topic: &topic, Partition: -1}: EarliestOffsetSpec},
		} {
			ctx, cancel = context.WithTimeout(context.Background(), expDuration)
			defer cancel()
			result, err = a.ListOffsets(
				ctx,
				topicPartitionOffsets, options...)
			if result.ResultInfos != nil || err == nil {
				t.Fatalf("Expected ListOffsets to fail, but got result: %v, err: %v",
					result, err)
			}
			if err.(Error).Code() != ErrInvalidArg {
				t.Fatalf("Expected ErrInvalidArg, not %v", err)
			}
		}

		// Same partition with different offsets
		ctx, cancel = context.WithTimeout(context.Background(), expDuration)
		defer cancel()
		topicPartitionOffsets := map[TopicPartition]OffsetSpec{
			{Topic: &topic, Partition: 0, Offset: 10}: EarliestOffsetSpec,
			{Topic: &topic, Partition: 0, Offset: 20}: LatestOffsetSpec,
		}
		result, err = a.ListOffsets(
			ctx,
			topicPartitionOffsets, options...)
		if result.ResultInfos != nil || err == nil {
			t.Fatalf("Expected ListOffsets to fail, but got result: %v, err: %v",
				result, err)
		}
		if err.(Error).Code() != ErrInvalidArg {
			t.Fatalf("Expected ErrInvalidArg, not %v", err)
		}

		// Two different partitions
		ctx, cancel = context.WithTimeout(context.Background(), expDuration)
		defer cancel()
		topicPartitionOffsets = map[TopicPartition]OffsetSpec{
			{Topic: &topic, Partition: 0, Offset: 10}: EarliestOffsetSpec,
			{Topic: &topic, Partition: 1, Offset: 20}: EarliestOffsetSpec,
		}
		result, err = a.ListOffsets(
			ctx,
			topicPartitionOffsets, options...)
		if result.ResultInfos != nil || err == nil {
			t.Fatalf("Expected ListOffsets to fail, but got result: %v, err: %v",
				result, err)
		}
		if ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
		}

		// Single partition
		ctx, cancel = context.WithTimeout(context.Background(), expDuration)
		defer cancel()
		topicPartitionOffsets = map[TopicPartition]OffsetSpec{
			{Topic: &topic, Partition: 0}: EarliestOffsetSpec,
		}
		result, err = a.ListOffsets(
			ctx,
			topicPartitionOffsets, options...)
		if result.ResultInfos != nil || err == nil {
			t.Fatalf("Expected ListOffsets to fail, but got result: %v, err: %v",
				result, err)
		}
		if ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
		}
	}
}

func testAdminAPIsUserScramCredentials(what string, a *AdminClient, expDuration time.Duration, t *testing.T) {
	var users []string

	// With nil users
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, describeErr := a.DescribeUserScramCredentials(ctx, users)
	if describeErr == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected context deadline exceeded, got %s and %s\n",
			describeErr, ctx.Err())
	}

	// With one user
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	users = append(users, "non-existent")
	_, describeErr = a.DescribeUserScramCredentials(ctx, users)
	if describeErr == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected context deadline exceeded, got %s and %s\n",
			describeErr, ctx.Err())
	}

	// With duplicate users
	users = append(users, "non-existent")
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	users = append(users, "non-existent")
	_, describeErr = a.DescribeUserScramCredentials(ctx, users)
	if describeErr == nil || describeErr.(Error).Code() != ErrInvalidArg {
		t.Fatalf("Duplicate user should give an InvalidArgument error, got %s\n", describeErr)
	}

	var upsertions []UserScramCredentialUpsertion
	upsertions = append(upsertions,
		UserScramCredentialUpsertion{
			User: "non-existent",
			ScramCredentialInfo: ScramCredentialInfo{
				Mechanism: ScramMechanismSHA256, Iterations: 10000},
			Password: []byte("password"),
			Salt:     []byte("salt"),
		})
	var deletions []UserScramCredentialDeletion
	deletions = append(deletions, UserScramCredentialDeletion{
		User: "non-existent", Mechanism: ScramMechanismSHA256})

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, alterErr := a.AlterUserScramCredentials(ctx, upsertions, deletions)
	if alterErr == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected context deadline exceeded, got %s and %s\n",
			alterErr, ctx.Err())
	}

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, alterErr = a.AlterUserScramCredentials(ctx, upsertions, nil)
	if alterErr == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected context deadline exceeded, got %s and %s\n",
			alterErr, ctx.Err())
	}

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, alterErr = a.AlterUserScramCredentials(ctx, nil, deletions)
	if alterErr == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected context deadline exceeded, got %s and %s\n",
			alterErr, ctx.Err())
	}
}

func testAdminAPIsDeleteRecords(what string, a *AdminClient, expDuration time.Duration, t *testing.T) {
	topic := "test"
	partition := int32(0)
	offset := Offset(2)
	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	topicPartitionOffset := []TopicPartition{{Topic: &topic, Partition: partition, Offset: offset}}
	_, err := a.DeleteRecords(ctx, topicPartitionOffset , SetAdminRequestTimeout(time.Second))
	if err == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected context deadline exceeded, got %s and %s\n",
			err, ctx.Err())
	}

	// Invalid option value
	_, err = a.DeleteRecords(ctx, topicPartitionOffset, SetAdminRequestTimeout(-1))
	if err == nil || err.(Error).Code() != ErrInvalidArg {
		t.Fatalf("Expected ErrInvalidArg, not %v", err)
	}
	
	for _, options := range [][]DeleteRecordsAdminOption{
		{},
		{SetAdminRequestTimeout(time.Second)},
	} {
		// nil argument should fail, not being treated as empty
		ctx, cancel = context.WithTimeout(context.Background(), expDuration)
		defer cancel()
		result, err := a.DeleteRecords(ctx, nil, options...)
		if result.TopicPartitions != nil || err == nil {
			t.Fatalf("Expected DeleteRecords to fail, but got result: %v, err: %v",
				result, err)
		}
		if err.(Error).Code() != ErrInvalidArg {
			t.Fatalf("Expected ErrInvalidArg, not %v", err)
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
	icres, err := a.IncrementalAlterConfigs(
		ctx,
		[]ConfigResource{{Type: ResourceTopic, Name: "topic"}})
	if icres != nil || err == nil {
		t.Fatalf("Expected IncrementalAlterConfigs to fail, but got result: %v, err: %v", cres, err)
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

	testAdminAPIsCreateACLs(what, a, t)
	testAdminAPIsDescribeACLs(what, a, t)
	testAdminAPIsDeleteACLs(what, a, t)

	testAdminAPIsListConsumerGroups(what, a, expDuration, t)
	testAdminAPIsDescribeConsumerGroups(what, a, expDuration, t)
	testAdminAPIsDescribeTopics(what, a, expDuration, t)
	testAdminAPIsDescribeCluster(what, a, expDuration, t)
	testAdminAPIsDeleteConsumerGroups(what, a, expDuration, t)

	testAdminAPIsListConsumerGroupOffsets(what, a, expDuration, t)
	testAdminAPIsAlterConsumerGroupOffsets(what, a, expDuration, t)
	testAdminAPIsListOffsets(what, a, expDuration, t)

	testAdminAPIsUserScramCredentials(what, a, expDuration, t)
	testAdminAPIsDeleteRecords(what, a, expDuration, t)
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

	// Tests the SetSaslCredentials call to ensure that the API does not crash.
	a.SetSaslCredentials("username", "password")

	a.Close()
}
