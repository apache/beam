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

package datastoreio

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"google.golang.org/api/option"
)

// fake client type implements datastoreio.clientType
type fakeClient struct {
	runCounter   int
	closeCounter int
}

func (client *fakeClient) Run(context.Context, *datastore.Query) *datastore.Iterator {
	client.runCounter += 1
	// return an empty iterator
	return new(datastore.Iterator)
}

func (client *fakeClient) Close() error {
	client.closeCounter += 1
	return nil
}

// mock type for query
type Foo struct {
}

type Bar struct {
}

func Test_query(t *testing.T) {
	testCases := []struct {
		v           interface{}
		shard       int
		expectRun   int
		expectClose int
	}{
		// case 1: shard=1, without split query
		{Foo{}, 1, 1, 1},
		// case 2: shard=2 (>1), with split query
		{Bar{}, 2, 2, 2},
	}
	for _, tc := range testCases {
		// setup a fake newClient caller
		client := fakeClient{}
		newClient := func(ctx context.Context,
			projectID string,
			opts ...option.ClientOption) (clientType, error) {
			return &client, nil
		}

		itemType := reflect.TypeOf(tc.v)
		itemKey := runtime.RegisterType(itemType)

		p, s := beam.NewPipelineWithRoot()
		query(s, "project", "Item", tc.shard, itemType, itemKey, newClient)

		ptest.RunAndValidate(t, p)

		if got, want := client.runCounter, tc.expectRun; got != want {
			t.Errorf("got number of datastore.Client.Run call: %v, wanted %v",
				got, want)
		}
		if got, want := client.closeCounter, tc.expectClose; got != want {
			t.Errorf("got number of datastore.Client.Close call: %v, wanted %v",
				got, want)
		}
	}
}

func Test_query_Bad(t *testing.T) {
	testCases := []struct {
		v            interface{}
		itemType     reflect.Type
		itemKey      string
		expectErrStr string
		newClientErr error
	}{
		// mismatch typeKey parameter
		{
			Foo{},
			reflect.TypeOf(Foo{}),
			"MismatchType",
			"No type registered MismatchType",
			nil,
		},
		// newClient caller returns error
		{
			Foo{},
			reflect.TypeOf(Foo{}),
			runtime.RegisterType(reflect.TypeOf(Foo{})),
			"fake client error",
			errors.New("fake client error"),
		},
	}
	for _, tc := range testCases {
		client := fakeClient{}
		newClient := func(ctx context.Context,
			projectID string,
			opts ...option.ClientOption) (clientType, error) {
			return &client, tc.newClientErr
		}

		p, s := beam.NewPipelineWithRoot()
		query(s, "project", "Item", 1, tc.itemType, tc.itemKey, newClient)
		err := ptest.Run(p)

		if got, want := err.Error(), tc.expectErrStr; !strings.Contains(got, want) {
			t.Errorf("got error: %v\nwanted error: %v", got, want)
		}
	}
}

func Test_splitQueryFn_Setup(t *testing.T) {
	s := splitQueryFn{"project", "kind", 1, nil}
	err := s.Setup()
	if nil != err {
		t.Errorf("failed to call Setup, got error: %v", err)
	}
	if nil == s.newClientFunc {
		t.Error("failed to setup newClientFunc.")
	}
}

func Test_queryFn_Setup(t *testing.T) {
	s := queryFn{"project", "kind", "type", nil}
	err := s.Setup()
	if nil != err {
		t.Errorf("failed to call Setup, got error: %v", err)
	}
	if nil == s.newClientFunc {
		t.Error("failed to setup newClientFunc.")
	}
}

func Test_keyLessThan(t *testing.T) {
	tsts := []struct {
		a      *datastore.Key
		b      *datastore.Key
		expect bool
		name   string
	}{
		{
			name:   "a<b",
			a:      datastore.NameKey("A", "a", nil),
			b:      datastore.NameKey("A", "b", nil),
			expect: true,
		},
		{
			name:   "b>a",
			a:      datastore.NameKey("A", "b", nil),
			b:      datastore.NameKey("A", "a", nil),
			expect: false,
		},
		{
			name:   "a=a",
			a:      datastore.NameKey("A", "a", nil),
			b:      datastore.NameKey("A", "a", nil),
			expect: false,
		},
		{
			name:   "a.a<a",
			a:      datastore.NameKey("A", "a", datastore.NameKey("A", "a", nil)),
			b:      datastore.NameKey("A", "a", nil),
			expect: true,
		},
		{
			name:   "a.a<a.b",
			a:      datastore.NameKey("A", "a", datastore.NameKey("A", "a", nil)),
			b:      datastore.NameKey("A", "a", datastore.NameKey("A", "b", nil)),
			expect: true,
		},
		{
			name:   "a.b>a.a",
			a:      datastore.NameKey("A", "a", datastore.NameKey("A", "b", nil)),
			b:      datastore.NameKey("A", "a", datastore.NameKey("A", "a", nil)),
			expect: false,
		},
		{
			name:   "a.a=a.a",
			a:      datastore.NameKey("A", "a", datastore.NameKey("A", "a", nil)),
			b:      datastore.NameKey("A", "a", datastore.NameKey("A", "a", nil)),
			expect: false,
		},
		{
			name:   "4dda<A",
			a:      datastore.NameKey("A", "4dda", nil),
			b:      datastore.NameKey("A", "A", nil),
			expect: true,
		},
	}
	for n := range tsts {
		index := n
		t.Run(tsts[index].name, func(t *testing.T) {
			got := keyLessThan(tsts[index].a, tsts[index].b)
			if tsts[index].expect != got {
				t.Fail()
			}
		})
	}
}

func Test_flatten(t *testing.T) {
	r := flatten(datastore.NameKey("A", "a", datastore.NameKey("B", "b", nil)))
	if !(r[0].Kind == "B" && r[0].Name == "b") {
		t.Errorf("Expected B.b in first position")
	}
	if !(r[1].Kind == "A" && r[1].Name == "a") {
		t.Errorf("Expected A.a in second position")
	}
}
