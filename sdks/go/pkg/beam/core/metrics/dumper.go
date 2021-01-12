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
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

// DumpToLog is a debugging function that outputs all metrics available locally
// to beam.Log.
func DumpToLog(ctx context.Context) {
	store := GetStore(ctx)
	if store == nil {
		log.Errorf(ctx, "Unable to dump metrics: provided context doesn't contain metrics Store.")
		return
	}
	DumpToLogFromStore(ctx, store)
}

// DumpToLogFromStore dumps the metrics in the provided Store to beam.Log.
func DumpToLogFromStore(ctx context.Context, store *Store) {
	dumperExtractor(store, func(format string, args ...interface{}) {
		log.Errorf(ctx, format, args...)
	})
}

// DumpToOutFromContext is a debugging function that outputs all metrics
// available locally to std out,
// extracting the metric store from the context.
func DumpToOutFromContext(ctx context.Context) {
	store := GetStore(ctx)
	if store == nil {
		fmt.Printf("Unable to dump metrics: provided context doesn't contain metrics Store.")
		return
	}
	DumpToOutFromStore(store)
}

// DumpToOutFromStore is a debugging function that outputs all metrics
// available locally to std out directly from the store.
func DumpToOutFromStore(store *Store) {
	dumperExtractor(store, func(format string, args ...interface{}) {
		fmt.Printf(format+"\n", args...)
	})
}

func dumperExtractor(store *Store, p func(format string, args ...interface{})) {
	m := make(map[Labels]interface{})
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
	}
	e.ExtractFrom(store)
	dumpTo(m, p)
}

func dumpTo(store map[Labels]interface{}, p func(format string, args ...interface{})) {
	var ls []Labels
	for l := range store {
		ls = append(ls, l)
	}
	sort.Slice(ls, func(i, j int) bool {
		if ls[i].transform < ls[j].transform {
			return true
		}
		tEq := ls[i].transform == ls[j].transform
		if tEq && ls[i].namespace < ls[j].namespace {
			return true
		}
		nsEq := ls[i].namespace == ls[j].namespace
		if tEq && nsEq && ls[i].name < ls[j].name {
			return true
		}
		return false
	})
	curT := "probably-definitely-unset"
	for _, l := range ls {
		if l.transform != curT {
			curT = l.transform
			p("PTransformID: %q", curT)
		}
		m := store[l]
		p("\t%s - %s", name{namespace: l.namespace, name: l.name}, m)
	}
}
