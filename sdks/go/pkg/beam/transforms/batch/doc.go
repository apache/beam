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

// Package-level doc examples for batch. Kept in the package itself so
// `go doc` surfaces them without a separate test package and a broader
// module import graph.
//
// These examples only construct a pipeline to illustrate API shape; they
// do not run one.

package batch

import (
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

// Each input element is a (user, event) pair. After GroupIntoBatches,
// each batch holds up to 100 events for a single user, ready to be
// written to a BigQuery sink that accepts bulk inserts.
func ExampleGroupIntoBatches() {
	p := beam.NewPipeline()
	s := p.Root()

	// Build KV<string, string> PCollection via any source. The key
	// coder (string) is deterministic so state keying is safe.
	events := beam.CreateList(s, []string{"u1:login", "u1:click", "u2:login"})
	kvs := beam.ParDo(s, func(e string, emit func(string, string)) {
		for i, r := 0, []rune(e); i < len(r); i++ {
			if r[i] == ':' {
				emit(string(r[:i]), string(r[i+1:]))
				return
			}
		}
	}, events)

	batches := GroupIntoBatches(s, Params{BatchSize: 100}, kvs)

	// Downstream: process each per-user batch.
	_ = batches
	fmt.Println("pipeline constructed")

	// Output: pipeline constructed
}
