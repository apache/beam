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
	runFn   func()
	closeFn func()
}

func (client *fakeClient) Run(context.Context, *datastore.Query) *datastore.Iterator {
	client.runFn()
	// return an empty iterator
	return new(datastore.Iterator)
}

func (client *fakeClient) Close() error {
	client.closeFn()
	return nil
}

// mock type for query
type Foo struct {
}

type Bar struct {
}

func TestRead(t *testing.T) {
	runCounter := 0
	closeCounter := 0

	// setup a fake newClient caller
	originalClient := newClient
	newClient = func(ctx context.Context,
		projectID string,
		opts ...option.ClientOption) (clientType, error) {
		client := fakeClient{
			runFn: func() {
				runCounter += 1
			},
			closeFn: func() {
				closeCounter += 1
			},
		}
		return &client, nil
	}

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
		itemType := reflect.TypeOf(tc.v)
		itemKey := runtime.RegisterType(itemType)

		p, s := beam.NewPipelineWithRoot()
		Read(s, "project", "Item", tc.shard, itemType, itemKey)

		ptest.RunAndValidate(t, p)

		if runCounter != tc.expectRun {
			t.Errorf("got number of datastore.Client.Run call: %v, wanted %v",
				runCounter, tc.expectRun)
		}
		if runCounter != tc.expectRun {
			t.Errorf("got number of datastore.Client.Run call: %v, wanted %v",
				closeCounter, tc.expectClose)
		}

		// reset counter
		runCounter = 0
		closeCounter = 0
	}

	// tear down: recover original newClient caller
	newClient = originalClient
}

func TestRead_Bad(t *testing.T) {
	// setup a fake newClient caller
	originalClient := newClient
	newClient = func(ctx context.Context,
		projectID string,
		opts ...option.ClientOption) (clientType, error) {
		client := fakeClient{
			runFn:   func() {},
			closeFn: func() {},
		}
		return &client, nil
	}

	testCases := []struct {
		v            interface{}
		itemType     reflect.Type
		itemKey      string
		expectErrStr string
	}{
		// mismatch typeKey parameter
		{Foo{}, reflect.TypeOf(Foo{}), "MismatchType", "No type registered MismatchType"},
	}
	for _, tc := range testCases {
		p, s := beam.NewPipelineWithRoot()
		Read(s, "project", "Item", 1, tc.itemType, tc.itemKey)
		err := ptest.Run(p)

		if got, want := err.Error(), tc.expectErrStr; !strings.Contains(got, want) {
			t.Errorf("got error: %v\nwanted error: %v", got, want)
		}
	}

	// tear down: recover original newClient caller
	newClient = originalClient
}

func TestRead_ClientError(t *testing.T) {
	// setup a fake newClient caller that returns error
	originalClient := newClient
	expectErr := errors.New("fake client error")
	newClient = func(ctx context.Context,
		projectID string,
		opts ...option.ClientOption) (clientType, error) {
		client := fakeClient{
			runFn:   func() {},
			closeFn: func() {},
		}
		return &client, expectErr
	}
	itemType := reflect.TypeOf(Foo{})
	itemKey := runtime.RegisterType(itemType)

	p, s := beam.NewPipelineWithRoot()
	Read(s, "project", "Item", 1, itemType, itemKey)
	err := ptest.Run(p)
	if got, want := err.Error(), expectErr.Error(); !strings.Contains(got, want) {
		t.Errorf("got error: %v\nwanted error: %v", got, want)
	}

	// tear down: recover original newClient caller
	newClient = originalClient
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
