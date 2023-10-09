// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigtableio

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestHashStringToInt(t *testing.T) {
	equalVal := "equal"

	firstHash := hashStringToInt(equalVal)
	secondHash := hashStringToInt(equalVal)

	if firstHash != secondHash {
		t.Errorf("hashStringToInt(\"%s\") should equal hashStringToInt(\"%s\")", equalVal, equalVal)
	}

	if hashStringToInt("helloWorld") == hashStringToInt("helloworld") {
		t.Error("hashStringToInt(\"helloWorld\") should not equal hashStringToInt(\"helloworld\")")
	}

	if hashStringToInt("saturnsmoon1") == hashStringToInt("saturnsmoon2") {
		t.Error("hashStringToInt(\"saturnsmoon1\") should not equal hashStringToInt(\"saturnsmoon2\")")
	}
}

func TestAddGroupKeyFnGroupKeyGiven(t *testing.T) {
	mutationWithGroupKey := NewMutation("rowKey").WithGroupKey("1")
	groupKey, _ := addGroupKeyFn(*mutationWithGroupKey)
	if groupKey == 1 {
		t.Error("addGroupKeyFn should hash groupKey values properly, but projected \"1\" -> 1")
	}
}

func TestAddGroupKeyFnNoGroupKeyGiven(t *testing.T) {
	mutationNoGroupKey := NewMutation("rowKey")
	groupKey, _ := addGroupKeyFn(*mutationNoGroupKey)
	if groupKey != 1 {
		t.Errorf("addGroupKeyFn should assign 1 as hash if no groupKey is given, but projected nil -> %d", groupKey)
	}
}

func TestMustBeBigtableioMutation(t *testing.T) {

	mutation := NewMutation("key")
	mutation.Set("family", "column", 0, []byte{})

	mutationWithGroupKey := NewMutation("key").WithGroupKey("groupKey")
	mutationWithGroupKey.Set("family", "column", 0, []byte{})

	passValues := []Mutation{
		{},
		{RowKey: "key"},
		{Ops: []Operation{{}}},
		*mutation,
		*mutationWithGroupKey,
	}

	for _, passValue := range passValues {
		passType := reflect.TypeOf(passValue)
		err := mustBeBigtableioMutation(passType)
		if err != nil {
			t.Errorf("input type %v should be considered a bigtableio.Mutation", passType)
		}
	}
}

func TestMustNotBeBigtableioMutation(t *testing.T) {
	failValues := []any{
		1,
		1.0,
		"strings must fail",
		errors.New("errors must fail"),
	}

	for _, failValue := range failValues {
		failType := reflect.TypeOf(reflect.ValueOf(failValue))
		err := mustBeBigtableioMutation(failType)
		if err == nil {
			t.Errorf("input type %v should not be considered a bigtableio.Mutation", failType)
		}
	}
}

func TestTryApplyBulk(t *testing.T) {
	err := tryApplyBulk(nil, nil)
	if err != nil {
		t.Error("tryApplyBulk should not return an error for inputs <nil, nil> but returned:\n", err)
	}

	err = tryApplyBulk(nil, errors.New("error"))
	if err == nil {
		t.Error("tryApplyBulk should return an error for inputs <nil, error>")
	}

	err = tryApplyBulk([]error{errors.New("error")}, nil)
	if err == nil {
		t.Error("tryApplyBulk should return an error for inputs <[]error, nil>")
	}
}

func TestValidateMutationSucceedsWhenZeroOps(t *testing.T) {
	validMutation := NewMutation("rowKey")

	err := validateMutation(*validMutation)
	if err != nil {
		t.Errorf("mutation (0 ops) should be valid, but was marked invalid: %s", err)
	}
}

func TestValidateMutationSucceedsWhenLessThanOrEqualHundredKOps(t *testing.T) {
	validMutation := NewMutation("rowKey")

	for i := 0; i < 100000; i++ {
		validMutation.Set("family", fmt.Sprint(i), bigtable.Now(), []byte{})
	}

	err := validateMutation(*validMutation)
	if err != nil {
		t.Errorf("mutation (100,000 ops) should be valid, but was marked invalid: %s", err)
	}
}

func TestValidateMutationFailsWhenGreaterThanHundredKOps(t *testing.T) {
	validMutation := NewMutation("rowKey")

	for i := 0; i < 100001; i++ {
		validMutation.Set("family", fmt.Sprint(i), bigtable.Now(), []byte{})
	}

	err := validateMutation(*validMutation)
	if err == nil {
		t.Error("mutation (100,001 ops) should be invalid, but was marked valid")
	}
}

// Examples:

func ExampleWriteBatch() {
	pipeline := beam.NewPipeline()
	s := pipeline.Root()

	//sample PBCollection<bigtableio.Mutation>
	bigtableioMutationCol := beam.CreateList(s, func() []Mutation {
		columnFamilyName := "stats_summary"
		timestamp := bigtable.Now()

		// var muts []bigtableio.Mutation
		var muts []Mutation

		deviceA := "tablet"
		rowKeyA := deviceA + "#a0b81f74#20190501"

		// bigtableio.NewMutation(rowKeyA).WithGroupKey(deviceA)
		mutA := NewMutation(rowKeyA).WithGroupKey(deviceA) // this groups bundles by device identifiers
		mutA.Set(columnFamilyName, "connected_wifi", timestamp, []byte("1"))
		mutA.Set(columnFamilyName, "os_build", timestamp, []byte("12155.0.0-rc1"))

		muts = append(muts, *mutA)

		deviceB := "phone"
		rowKeyB := deviceB + "#a0b81f74#20190502"

		mutB := NewMutation(rowKeyB).WithGroupKey(deviceB)
		mutB.Set(columnFamilyName, "connected_wifi", timestamp, []byte("1"))
		mutB.Set(columnFamilyName, "os_build", timestamp, []byte("12145.0.0-rc6"))

		muts = append(muts, *mutB)

		return muts
	}())

	// bigtableio.WriteBatch(...)
	WriteBatch(s, "project", "instanceId", "tableName", bigtableioMutationCol)
}
