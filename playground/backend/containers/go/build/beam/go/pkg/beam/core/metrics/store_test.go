// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package metrics

import (
	"reflect"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	store := newStore()

	m := make(map[Labels]any)
	e := &Extractor{
		SumInt64: func(l Labels, v int64) {
			m[l] = &counter{value: v}
		},
		DistributionInt64: func(l Labels, count, sum, min, max int64) {
			m[l] = &distribution{count: count, sum: sum, min: min, max: max}
		},
		GaugeInt64: func(l Labels, v int64, t time.Time) {
			m[l] = &gauge{v: v, t: t}
		},
		MsecsInt64: func(labels string, e *[4]ExecutionState) {},
	}

	now := time.Now()

	store.storeMetric("pid", newName("ns", "counter"), &counter{value: 1})
	store.storeMetric("pid", newName("ns", "distribution"), &distribution{count: 1, sum: 2, min: 3, max: 4})
	store.storeMetric("pid", newName("ns", "gauge"), &gauge{v: 1, t: now})

	// storing the same metric twice doesn't change anything
	store.storeMetric("pid", newName("ns", "counter"), &counter{value: 2})

	err := e.ExtractFrom(store)
	if err != nil {
		t.Fatalf("e.ExtractFrom(store) = %q, want nil", err)
	}

	expected := map[Labels]any{
		{transform: "pid", namespace: "ns", name: "counter"}:      &counter{value: 1},
		{transform: "pid", namespace: "ns", name: "distribution"}: &distribution{count: 1, sum: 2, min: 3, max: 4},
		{transform: "pid", namespace: "ns", name: "gauge"}:        &gauge{v: 1, t: now},
	}
	if !reflect.DeepEqual(m, expected) {
		t.Errorf("e.ExtractFrom(store) = %v, want %v", m, expected)
	}
}
