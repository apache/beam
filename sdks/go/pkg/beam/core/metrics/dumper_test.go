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

package metrics

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestDumperExtractor(t *testing.T) {
	var got []string
	printer := func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	}

	store := newStore()
	now := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	store.storeMetric("pid", newName("ns", "counter"), &counter{value: 1})
	store.storeMetric("pid", newName("ns", "distribution"), &distribution{count: 1, sum: 2, min: 3, max: 4})
	store.storeMetric("pid", newName("ns", "gauge"), &gauge{v: 1, t: now})

	expected := []string{
		"PTransformID: \"pid\"",
		"	ns.counter - value: 1",
		"	ns.distribution - count: 1 sum: 2 min: 3 max: 4",
		"	ns.gauge - Gauge time: 2019-01-01 00:00:00 +0000 UTC value: 1",
	}

	dumperExtractor(store, printer)
	if diff := cmp.Diff(expected, got); diff != "" {
		t.Errorf("dumperExtractor() got diff (-want +got): %v", diff)
	}
}
