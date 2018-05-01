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
	"testing"

	"cloud.google.com/go/datastore"
)

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
			name:   "a.a<b",
			a:      datastore.NameKey("A", "a", nil),
			b:      datastore.NameKey("A", "a", datastore.NameKey("A", "b", nil)),
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
