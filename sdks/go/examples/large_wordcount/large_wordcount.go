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

// large_wordcount is an example that demonstrates a more complex version
// of a wordcount pipeline. It uses a SplittableDoFn for reading the
// text files, then uses a map side input to build sorted shards.
//
// This example, large_wordcount, is the fourth in a series of five
// successively more detailed 'word count' examples. You may first want to
// take a look at minimal_wordcount and wordcount.
// Then look at debugging_worcount for some testing and validation concepts.
// After you've looked at this example, then see the windowed_wordcount pipeline,
// for introduction of additional concepts.
//
// Basic concepts, also in the minimal_wordcount and wordcount examples:
// Reading text files; counting a PCollection; executing a Pipeline both locally
// and using a selected runner; defining DoFns.
//
// Additional concepts, like testing DoFns.
//
// New Concepts:
//
//   1. Using a SplittableDoFn transform to read the IOs.
//   2. Using a Map Side Input to randomly access KV pairs from a sorted a keyspace.
//   3. Testing your Pipeline via passert, using Go testing tools.
//
// To change the runner, specify:
//
//     --runner=YOUR_SELECTED_RUNNER
//
// The input file defaults to a public data set containing the text of of King
// Lear, by William Shakespeare. You can override it and choose your own input
// with --input.
package main

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/harnessopts"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"

	// The imports here are for the side effect of runner registration.
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dot"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
)

// TODO(herohde) 10/16/2017: support metrics and log level cutoff.

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/*.txt", "File(s) to read.")
	output = flag.String("output", "", "Output file (required). Use @* or @N (eg. @5) to indicate dynamic, or fixed number of shards. No shard indicator means a single file.")
)

// Concept #1: a DoFn can also be a struct with methods for setup/teardown and
// element/bundle processing. It also allows configuration values to be made
// available at runtime.

func init() {
	beam.RegisterFunction(extractFn)
	beam.RegisterFunction(formatFn)
	// To be correctly serialized on non-direct runners, struct form DoFns must be
	// registered during initialization.
	beam.RegisterType(reflect.TypeOf((*makeMetakeys)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*metakey)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*pairWithMetaKey)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*writeTempFiles)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*renameFiles)(nil)).Elem())
}

// The below transforms are identical to the wordcount versions. If this was
// production code, common transforms would be placed in a separate package
// and shared directly rather than being copied.

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

// extractFn is a DoFn that emits the words in a given line.
func extractFn(line string, emit func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

// formatFn is a DoFn that formats a word and its count as a string.
func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

// CountWords is a composite transform that counts the words of an PCollection
// of lines. It expects a PCollection of type string and returns a PCollection
// of type KV<string,int>.
func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("CountWords")
	col := beam.ParDo(s, extractFn, lines)
	return stats.Count(s, col)
}

// makeMetakeys produces metakeys for each shard.
type makeMetakeys struct {
	Output  string // The format of output files.
	Dynamic int    // The number of elements for each dynamic shard. Default 10k. Ignored if the format doesn't contain `@*`.
}

// metakey serves the purpose of being a key for splitting up input
// into distinct shards.
type metakey struct {
	Low, High    string
	Shard, Total int
	TmpPrefix    int64
}

var outputRE = regexp.MustCompile(`(@\*|@\d+)`)

func makeTmpPrefix(v int64) string {
	return strconv.FormatInt(v, 36)
}

func (m *metakey) TmpFileName(output string) string {
	shard := fmt.Sprintf("%03d-%03d.%s", m.Shard, m.Total, makeTmpPrefix(m.TmpPrefix))
	return outputRE.ReplaceAllString(output, shard)
}

func (m *metakey) FinalFileName(output string) string {
	shard := fmt.Sprintf("%03d-%03d", m.Shard, m.Total)
	return outputRE.ReplaceAllString(output, shard)
}

func (fn *makeMetakeys) StartBundle(_ func(*string) bool, _ func(metakey)) {
	if fn.Dynamic <= 0 {
		fn.Dynamic = 10000
	}
}

func (fn *makeMetakeys) ProcessElement(ctx context.Context, _ []byte, iter func(*string) bool, emit func(metakey)) error {
	// Pull in and sort all the keys in memory.
	var v string
	var keys []string
	for iter(&v) {
		keys = append(keys, v)
	}
	sort.StringSlice(keys).Sort()

	// first, parse fn.Output for a shard.
	match := outputRE.FindString(fn.Output)
	r := offsetrange.Restriction{Start: 0, End: int64(len(keys)) - 1}
	var rs []offsetrange.Restriction
	switch match {
	case "": // No matches
		// Everything in a single file.
		rs = append(rs, r)
	case "@*": // Dynamic Sharding
		// Basic dynamic sharding, where each file will contain a fixed number of words.
		rs = r.SizedSplits(int64(fn.Dynamic))
	default: // @N Fixed Sharding
		// Fixed number of shards, where each shard will contain 1/Nth of the words.
		n, err := strconv.Atoi(match[1:])
		if err != nil {
			return fmt.Errorf("bad output format: Unable to extract shard count from %v: %v", fn.Output, err)
		}
		rs = r.EvenSplits(int64(n))
	}
	// Use the current time in unix as the temp prefix.
	// Since it's included with all metakeys, an int64 is preferable to strings for compactness.
	tmpPrefix := time.Now().Unix()
	// Log the identifier to assist with debugging.
	log.Infof(ctx, "makeMetakeys: temp file identifier %s used for output path %s", makeTmpPrefix(tmpPrefix), fn.Output)
	for s, ri := range rs {
		emit(metakey{
			Low:       keys[int(ri.Start)],
			High:      keys[int(ri.End)],
			Shard:     s,
			Total:     len(rs),
			TmpPrefix: tmpPrefix,
		})
	}
	return nil
}

// pairWithMetaKey processes each element, and re-emits them with the metakey.
// This associates them with each shard.
type pairWithMetaKey struct {
}

func (fn *pairWithMetaKey) ProcessElement(ctx context.Context, v string, iter func(*metakey) bool, emit func(metakey, string)) {
	// TODO move the read and sort into a start bundle if per is warranted.
	var mks []metakey
	var mk metakey
	for iter(&mk) {
		mks = append(mks, mk)
	}
	sort.Slice(mks, func(i, j int) bool {
		return mks[i].Shard < mks[j].Shard
	})

	n := len(mks)
	i := sort.Search(n, func(i int) bool {
		return v <= mks[i].High
	})

	emit(mks[i], v)
}

// writeTempFiles takes each metakey and it's grouped words (the original keys), and uses
// a map side input to lookup the original sum for each word.
//
// All words for the metakey is sorted in memory and written to a temporary file, outputing
// the temporary file name. Each metakey includes a temporary prefix used to distinguish
// a given attempt's set of files from each other, and the final successful files.
//
// A more robust implementation would write to the pipeline's temporary folder instead,
// but for this example, using the same output destination is sufficient.
type writeTempFiles struct {
	Output string

	fs filesystem.Interface
}

func (fn *writeTempFiles) StartBundle(ctx context.Context, _ func(string) func(*int) bool, _ func(string)) error {
	fs, err := filesystem.New(ctx, fn.Output)
	if err != nil {
		return err
	}
	fn.fs = fs
	return nil
}

func (fn *writeTempFiles) ProcessElement(ctx context.Context, k metakey, iter func(*string) bool, lookup func(string) func(*int) bool, emitFileName func(string)) error {
	// Pull in and sort all the keys in memory.
	var v string
	var keys []string
	for iter(&v) {
		keys = append(keys, v)
	}
	sort.StringSlice(keys).Sort()

	tmpFile := k.TmpFileName(fn.Output)
	wc, err := fn.fs.OpenWrite(ctx, tmpFile)
	if err != nil {
		return err
	}
	defer wc.Close()
	var count int
	for _, word := range keys {
		lookup(word)(&count) // Get the count for the word.
		fmt.Fprintf(wc, "%v: %d\n", word, count)
	}
	emitFileName(tmpFile)
	return nil
}

func (fn *writeTempFiles) FinishBundle(_ func(string) func(*int) bool, _ func(string)) {
	fn.fs.Close()
	fn.fs = nil
}

// renameFiles takes in files to rename as a side input so they can be moved/copied
// after successful file writes. Temporary files are removed as part of the rename.
//
// This implementation assumes temporary and final locations for files are on the
// same file system.
//
// A more robust implementation would move from the pipeline's temporary folder to
// the final output, or be able to move the files between different file systems.
type renameFiles struct {
	Output string

	fs filesystem.Interface
}

func (fn *renameFiles) StartBundle(ctx context.Context, _ func(*string) bool, _ func(string)) error {
	fs, err := filesystem.New(ctx, fn.Output)
	if err != nil {
		return err
	}
	fn.fs = fs
	return nil
}

func (fn *renameFiles) ProcessElement(ctx context.Context, k metakey, _ func(*string) bool, emit func(string)) error {
	// We don't read the side input for the temporary files, but it's critical
	// so that the rename step occurs only after all temporary files have been written.
	tmp := k.TmpFileName(fn.Output)
	final := k.FinalFileName(fn.Output)
	log.Infof(ctx, "renaming %v to %v", tmp, final)

	// Use the filesystem abstraction to perform the rename.
	if err := filesystem.Rename(ctx, fn.fs, tmp, final); err != nil {
		return err
	}

	// Rename's complete, so we emit the final file name, in case a downstream
	// consumer wishes to use them.
	emit(final)
	return nil
}

func (fn *renameFiles) FinishBundle(ctx context.Context, _ func(*string) bool, _ func(string)) error {
	fn.fs.Close()
	fn.fs = nil
	return nil
}

// SortAndShard takes in a PCollection<string,int> pairs and an output pattern,
// then produces a PCollection<string> with the output files.
// It demonstrates using a side input, a map side input and producing output.
func SortAndShard(s beam.Scope, in beam.PCollection, output string) beam.PCollection {
	s.Scope("SortAndShard")
	keys := beam.DropValue(s, in)
	// Use a side input to have a single worker sort and shard all the input. One metakey is produced per shard.
	// Requires that the keys fit in single worker memory.
	metakeys := beam.ParDo(s, &makeMetakeys{Output: output}, beam.Impulse(s), beam.SideInput{Input: keys})

	// Takes the metakeys, and pairs each key with it's metakey.
	rekeys := beam.ParDo(s, &pairWithMetaKey{}, keys, beam.SideInput{Input: metakeys})

	gbmeta := beam.GroupByKey(s, rekeys)

	// writeTempFiles produces temporary output files with the metakey.
	tmpFiles := beam.ParDo(s, &writeTempFiles{Output: output}, gbmeta, beam.SideInput{Input: in})

	// renameFiles takes the tmp files, and renames them to the final destination.
	// Using temporary names and then renaming is recommended to avoid conflicts on retries.
	return beam.ParDo(s, &renameFiles{Output: output}, metakeys, beam.SideInput{Input: tmpFiles})
}

// pipeline builds and executes the pipeline, returning a PCollection of strings
// representing the output files.
func Pipeline(s beam.Scope, input, output string) beam.PCollection {
	// Since this is the whole pipeline, we don't use a subscope here.
	lines := textio.ReadSdf(s, input)
	counted := CountWords(s, lines)
	return SortAndShard(s, counted, output)
}

func main() {
	flag.Parse()
	beam.Init()
	harnessopts.SideInputCacheCapacity(10000)

	// Concept #2: the beam logging package works both during pipeline
	// construction and at runtime. It should always be used.
	ctx := context.Background()
	if *output == "" {
		log.Exit(ctx, "No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()
	Pipeline(s, *input, *output)

	if _, err := beamx.RunWithMetrics(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
