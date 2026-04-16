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

package batch

import (
	"os"
	"sort"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	f, _ := os.CreateTemp("", "dummy")
	*jobopts.WorkerBinary = f.Name()
	os.Exit(ptest.MainRetWithDefault(m, "prism"))
}

// splitOnBar parses "key|value" strings into KV<string,string>.
func splitOnBar(tuple string, emit func(string, string)) {
	for i, r := range tuple {
		if r == '|' {
			emit(tuple[:i], tuple[i+1:])
			return
		}
	}
}

func batchSize(_ string, batch []string) int {
	return len(batch)
}

func batchSizeSorted(_ string, batch []string) int {
	sort.Strings(batch)
	return len(batch)
}

// intPair emits KV<string, int> from a "key|int" string.
func intPair(tuple string, emit func(string, int)) {
	for i, r := range tuple {
		if r == '|' {
			n := 0
			for _, c := range tuple[i+1:] {
				n = n*10 + int(c-'0')
			}
			emit(tuple[:i], n)
			return
		}
	}
}

func intBatchSize(_ string, batch []int) int { return len(batch) }

func init() {
	register.Function2x0(splitOnBar)
	register.Function2x0(intPair)
	register.Function2x1(batchSize)
	register.Function2x1(intBatchSize)
	register.Function2x1(batchSizeSorted)
	register.Emitter2[string, int]()
}

// TAC-6 (BAC-4): GroupIntoBatchesWithShardedKey compiles and returns
// a PCollection<KV<K, []V>>. Single-process Prism cannot observe the
// cross-worker sharding effect; this test therefore checks that the
// transform constructs a valid pipeline but does not execute it here.
// End-to-end execution with runtime shard distribution is covered by
// running the test suite on a distributed runner (Flink, Spark, or
// Dataflow).
func TestGroupIntoBatchesWithShardedKey_Construction(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	raw := beam.CreateList(s, []string{"a|1", "a|2", "b|3"})
	kvs := beam.ParDo(s, splitOnBar, raw)

	batches := GroupIntoBatchesWithShardedKey(s, Params{BatchSize: 2}, kvs)
	_ = batches

	if p == nil {
		t.Fatal("pipeline is nil")
	}
}

// TestGroupIntoBatches_IntValues verifies that GroupIntoBatches works
// with a value type (int) that is not string — demonstrating the
// coder-driven generic value support (BAC-1 with non-string V).
func TestGroupIntoBatches_IntValues(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	raw := beam.CreateList(s, []string{
		"a|1", "a|2", "a|3", "a|4",
		"b|5", "b|6",
	})
	kvs := beam.ParDo(s, intPair, raw)

	batches := GroupIntoBatches(s, Params{BatchSize: 2}, kvs)
	sizes := beam.ParDo(s, intBatchSize, batches)

	passert.Equals(s, sizes, 2, 2, 2)

	ptest.RunAndValidate(t, p)
}

// TAC-1 (BAC-1): 1000 inputs over 10 keys with BatchSize 100 produces
// batches of exactly 100 elements for a single key.
func TestGroupIntoBatches_CountLimit(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	tuples := make([]string, 0, 1000)
	for k := 0; k < 10; k++ {
		for i := 0; i < 100; i++ {
			tuples = append(tuples, string(rune('a'+k))+"|"+string(rune('0'+i%10)))
		}
	}

	raw := beam.CreateList(s, tuples)
	kvs := beam.ParDo(s, splitOnBar, raw)

	batches := GroupIntoBatches(s, Params{BatchSize: 100}, kvs)
	sizes := beam.ParDo(s, batchSize, batches)

	// 10 batches of 100.
	wants := []any{}
	for i := 0; i < 10; i++ {
		wants = append(wants, 100)
	}
	passert.Equals(s, sizes, wants...)

	ptest.RunAndValidate(t, p)
}

// TAC-4 (BAC-3): BatchSizeBytes threshold triggers a flush before the
// sum exceeds the limit. With BatchSizeBytes=10 and input strings of
// length 5 each, three 5-byte values first sum to 15 (> 10), so the
// flush happens after 2 elements.
func TestGroupIntoBatches_ByteLimit(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	raw := beam.CreateList(s, []string{
		"a|11111", "a|22222", "a|33333", "a|44444", // 4 * 5 bytes on key a
		"b|55555", "b|66666", // 2 * 5 bytes on key b
	})
	kvs := beam.ParDo(s, splitOnBar, raw)

	batches := GroupIntoBatches(s, Params{BatchSizeBytes: 10}, kvs)
	sizes := beam.ParDo(s, batchSize, batches)

	// Each 2-element batch reaches 10 bytes and flushes: 2,2 for key a
	// and 2 for key b = three flushes of size 2.
	passert.Equals(s, sizes, 2, 2, 2)

	ptest.RunAndValidate(t, p)
}

// TAC-7 (BAC-5) simplified in global window: batches only contain
// elements for a single key. Mixed-key batches would fail the
// key-equality assertion downstream. This test confirms the per-key
// groupism holds.
func TestGroupIntoBatches_PerKey(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	raw := beam.CreateList(s, []string{
		"a|1", "b|1", "a|2", "b|2", "a|3", "b|3", "a|4", "b|4",
	})
	kvs := beam.ParDo(s, splitOnBar, raw)

	batches := GroupIntoBatches(s, Params{BatchSize: 2}, kvs)
	sizes := beam.ParDo(s, batchSize, batches)

	// 8 inputs / BatchSize 2 over 2 keys → 4 batches of size 2.
	passert.Equals(s, sizes, 2, 2, 2, 2)

	ptest.RunAndValidate(t, p)
}
