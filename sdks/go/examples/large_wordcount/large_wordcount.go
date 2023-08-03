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
// After you've looked at this example, follow up with the windowed_wordcount
// pipeline, for introduction of additional concepts.
//
// Basic concepts, also in the minimal_wordcount and wordcount examples:
// Reading text files; counting a PCollection; executing a Pipeline both locally
// and using a selected runner; defining DoFns.
//
// New Concepts:
//
//  1. Using a SplittableDoFn transform to read the IOs.
//  2. Using a Map Side Input to access values for specific keys.
//  3. Testing your Pipeline via passert and metrics, using Go testing tools.
//
// This example will not be enumerating concepts, but will document them as they
// appear. There may be repetition from previous examples.
//
// To change the runner, specify:
//
//	--runner=YOUR_SELECTED_RUNNER
//
// The input file defaults to a public data set containing the text of King
// Lear, by William Shakespeare. You can override it and choose your own input
// with --input.
package main

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"

	// The imports here are for the side effect of runner registration.
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dot"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
)

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/*.txt", "File(s) to read.")
	output = flag.String("output", "", "Output file (required). Use @* or @N (eg. @5) to indicate dynamic, or fixed number of shards. No shard indicator means a single file.")
)

// Concept: DoFn and Type Registration
// All DoFns and user types used as PCollection elements must be registered with beam.

func init() {
	register.Function2x0(extractFn)
	register.Function2x1(formatFn)
	register.DoFn4x1[context.Context, []byte, func(*string) bool, func(metakey), error](&makeMetakeys{})

	register.DoFn4x0[context.Context, string, func(*metakey) bool, func(metakey, string)](&pairWithMetakey{})
	register.DoFn5x1[context.Context, metakey, func(*string) bool, func(string) func(*int) bool, func(string), error](&writeTempFiles{})
	register.DoFn4x1[context.Context, metakey, func(*string) bool, func(string), error](&renameFiles{})

	register.Emitter1[metakey]()
	register.Emitter2[metakey, string]()
	register.Iter1[*string]()
	register.Iter1[*metakey]()
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

// SortAndShard is defined earlier in the file, so it can provide an overview of this
// complex segment of pipeline. The DoFns that build it up follow.

// SortAndShard is a composite transform takes in a PCollection<string,int>
// and an output pattern. It returns a PCollection<string> with the output file paths.
// It demonstrates using a side input, a map side input and producing output.
func SortAndShard(s beam.Scope, in beam.PCollection, output string) beam.PCollection {
	s = s.Scope("SortAndShard")
	// For the sake of example, we drop the values from the keys.
	keys := beam.DropValue(s, in)

	// Concept: Impulse and Side Input to process on a single worker.
	// makeMetakeys is being started with an Impulse, and blocked from starting
	// until it's side input is ready. This will have all the work done for this
	// DoFn executed in a single bundle, on a single worker. This requires that
	// the values fit into memory for a single worker.

	// makeMetakeys divides the data into several shards as determined by the output pattern.
	// One metakey is produced per shard.
	metakeys := beam.ParDo(s, &makeMetakeys{Output: output}, beam.Impulse(s), beam.SideInput{Input: keys})

	// Takes the metakeys, and pairs each key with it's metakey.
	rekeys := beam.ParDo(s, &pairWithMetakey{}, keys, beam.SideInput{Input: metakeys})

	// Group all the newly paired values with their metakeys.
	// This forms the individual shards we will write to files.
	gbmeta := beam.GroupByKey(s, rekeys)

	// writeTempFiles produces temporary output files with the metakey.
	// Counts for each word are looked up in the map side input of the
	// original word + count pairs.
	tmpFiles := beam.ParDo(s, &writeTempFiles{Output: output}, gbmeta, beam.SideInput{Input: in})

	// renameFiles takes the tmp files, and renames them to the final destination.
	// Using temporary names and then renaming is recommended to avoid conflicts on retries,
	// if the original files fail to write.
	return beam.ParDo(s, &renameFiles{Output: output}, metakeys, beam.SideInput{Input: tmpFiles})
}

// metakey serves the purpose of being a key for splitting up input
// into distinct shards.
type metakey struct {
	Low, High    string
	Shard, Total int
	TmpInfix     int64
}

// outputRE is a regular expression representing the shard indicator: @* or @<shard count>
var outputRE = regexp.MustCompile(`(@\*|@\d+)`)

// makeTmpInfix converts a unix time into a compact string representation.
func makeTmpInfix(v int64) string {
	return strconv.FormatInt(v, 36)
}

// TmpFileName produces a temporary filename for this meta key, including an infix to
// group temporary files from the same run together.
func (m *metakey) TmpFileName(output string) string {
	shard := fmt.Sprintf("%03d-%03d.%s", m.Shard, m.Total, makeTmpInfix(m.TmpInfix))
	return outputRE.ReplaceAllString(output, shard)
}

// FinalFileName produces the final file name for this shard.
func (m *metakey) FinalFileName(output string) string {
	shard := fmt.Sprintf("%03d-%03d", m.Shard, m.Total)
	return outputRE.ReplaceAllString(output, shard)
}

// makeMetakeys produces metakeys for each shard.
type makeMetakeys struct {
	Output  string // The format of output files.
	Dynamic int    // The number of elements for each dynamic shard. Default 10k. Ignored if the format doesn't contain `@*`.

	keycount, metakeycount beam.Counter
}

func (fn *makeMetakeys) StartBundle(_ func(*string) bool, _ func(metakey)) {
	if fn.Dynamic <= 0 {
		fn.Dynamic = 10000
	}
	fn.keycount = beam.NewCounter("wordcount", "keycount")
	fn.metakeycount = beam.NewCounter("metakeys", "metakeycount")
}

func (fn *makeMetakeys) ProcessElement(ctx context.Context, _ []byte, iter func(*string) bool, emit func(metakey)) error {
	// Pull in and sort all the keys in memory.
	var v string
	var keys []string
	for iter(&v) {
		keys = append(keys, v)
	}
	sort.StringSlice(keys).Sort()

	// Increment for all the keys at once.
	fn.keycount.Inc(ctx, int64(len(keys)))

	// Code within DoFns can be arbitrarily complex,
	// and executes as ordinary code would.

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
	// Increment the number of expected shards.
	fn.metakeycount.Inc(ctx, int64(len(rs)))

	// Use the current time in unix as the temp infix.
	// Since it's included with all metakeys, an int64 is preferable to a string for compactness.
	tmpInfix := time.Now().Unix()

	// Log the identifier to assist with debugging.
	log.Infof(ctx, "makeMetakeys: temp file identifier %s used for output path %s", makeTmpInfix(tmpInfix), fn.Output)
	for s, ri := range rs {
		emit(metakey{
			Low:      keys[int(ri.Start)],
			High:     keys[int(ri.End)],
			Shard:    s,
			Total:    len(rs),
			TmpInfix: tmpInfix,
		})
	}
	return nil
}

// pairWithMetakey processes each element, and re-emits them with the metakey.
// This associates them with each shard of the final output.
type pairWithMetakey struct {
	mks []metakey
}

func (fn *pairWithMetakey) ProcessElement(ctx context.Context, v string, iter func(*metakey) bool, emit func(metakey, string)) {
	// Read in all the metakeys and sort on the first element.
	// Since this pipeline runs with the global window, the side input
	// will not change, so it can be cached in the DoFn.
	// This will only happen once per bundle.
	if fn.mks == nil {
		var mk metakey
		for iter(&mk) {
			fn.mks = append(fn.mks, mk)
		}
		sort.Slice(fn.mks, func(i, j int) bool {
			return fn.mks[i].Shard < fn.mks[j].Shard
		})
	}

	n := len(fn.mks)
	i := sort.Search(n, func(i int) bool {
		return v <= fn.mks[i].High
	})

	emit(fn.mks[i], v)
}

func (fn *pairWithMetakey) FinishBundle(_ func(*metakey) bool, _ func(metakey, string)) {
	fn.mks = nil // allow the metakeys to be garbage collected when the bundle is finished.
}

// writeTempFiles takes each metakey and it's grouped words (the original keys), and uses
// a map side input to lookup the original sum for each word.
//
// All words for the metakey are sorted in memory and written to a temporary file, outputing
// the temporary file name. Each metakey includes a temporary infix used to distinguish
// a given attempt's set of files from each other, and the final successful files.
//
// A more robust implementation would write to the pipeline's temporary folder instead,
// but for this example, using the same output destination is sufficient.
type writeTempFiles struct {
	Output string

	fs          filesystem.Interface
	countdistro beam.Distribution
}

func (fn *writeTempFiles) StartBundle(ctx context.Context, _ func(string) func(*int) bool, _ func(string)) error {
	fs, err := filesystem.New(ctx, fn.Output)
	if err != nil {
		return err
	}
	fn.fs = fs
	fn.countdistro = beam.NewDistribution("wordcount", "countdistro")
	return nil
}

func (fn *writeTempFiles) ProcessElement(ctx context.Context, k metakey, iter func(*string) bool, lookup func(string) func(*int) bool, emitFileName func(string)) error {
	// Pull in and sort all the keys for this shard.
	var v string
	var words []string
	for iter(&v) {
		words = append(words, v)
	}
	sort.StringSlice(words).Sort()

	tmpFile := k.TmpFileName(fn.Output)
	wc, err := fn.fs.OpenWrite(ctx, tmpFile)
	if err != nil {
		return err
	}
	defer wc.Close()
	for _, word := range words {
		var count int
		// Get the count for the word from the map side input.
		lookup(word)(&count)
		// Write the word and count to the file.
		fmt.Fprintf(wc, "%v: %d\n", word, count)
		// The count to a distribution for word counts.
		fn.countdistro.Update(ctx, int64(count))
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
	// consumer wishes to block on their readiness.
	emit(final)
	return nil
}

func (fn *renameFiles) FinishBundle(ctx context.Context, _ func(*string) bool, _ func(string)) error {
	fn.fs.Close()
	fn.fs = nil
	return nil
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
