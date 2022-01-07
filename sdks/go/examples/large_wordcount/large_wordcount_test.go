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
// limit, Shard: 0ations under the License.

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

// Concept: Testing

// TestMain calls ptest.Main in order to allow Go tests to call and
// execute beam pipelines against distributed runners.
func TestMain(m *testing.M) {
	ptest.Main(m)
}

var file1 string = `one two three four five
six seven eight nine ten`

var file2 string = `zero one two
 three four
five six seven 
eight nine`

func TestLargeWordcount_Loopback(t *testing.T) {
	// This call is not part of the example, but part of Beam testing infrastructure.
	integration.CheckFilters(t)

	// Force setting loopback to ensure file io is used for
	// all runners.
	oldEnvType := *jobopts.EnvironmentType
	*jobopts.EnvironmentType = "LOOPBACK"
	t.Cleanup(func() {
		*jobopts.EnvironmentType = oldEnvType
	})

	// Create a place to write out files for testing.
	dirPath := filepath.Join(os.TempDir(), "large_wordcount_test")
	t.Cleanup(func() {
		if err := os.RemoveAll(dirPath); err != nil {
			t.Fatalf("unable to remove @%v: %v", dirPath, err)
		}
	})
	const perms = 0777
	inputPath := filepath.Join(dirPath, "input")
	if err := os.MkdirAll(inputPath, perms); err != nil {
		t.Fatalf("unable to create %v: %v", inputPath, err)
	}
	outputDir := filepath.Join(dirPath, "output")
	if err := os.MkdirAll(outputDir, perms); err != nil {
		t.Fatalf("unable to create %v: %v", outputDir, err)
	}

	if err := os.WriteFile(filepath.Join(inputPath, "file1.txt"), []byte(file1), perms); err != nil {
		t.Fatalf("unable to write file1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(inputPath, "file2.txt"), []byte(file2), perms); err != nil {
		t.Fatalf("unable to write file2: %v", err)
	}

	// Testing a pipeline is as simple as running the pipeline.
	p, s := beam.NewPipelineWithRoot()

	finalFiles := Pipeline(s, filepath.Join(inputPath, "*.txt"), filepath.Join(outputDir, "wordcounts@2.txt"))
	passert.Equals(s, finalFiles, filepath.Join(outputDir, "wordcounts000-002.txt"), filepath.Join(outputDir, "wordcounts001-002.txt"))

	pr := ptest.RunAndValidate(t, p)

	qr := pr.Metrics().Query(func(mr beam.MetricResult) bool {
		return mr.Name() == "smallWords"
	})
	counter := metrics.CounterResult{}
	if len(qr.Counters()) != 0 {
		counter = qr.Counters()[0]
	}
	if counter.Result() != 0 {
		t.Errorf("Metrics().Query(by Name) failed. Got %d counters, Want %d counters", counter.Result(), 0)
	}

	qr = pr.Metrics().Query(func(mr beam.MetricResult) bool {
		return mr.Name() == "lineLenDistro"
	})
	distribution := metrics.DistributionResult{}
	if len(qr.Distributions()) != 0 {
		distribution = qr.Distributions()[0]
	}
	if distribution.Result().Count != 0 {
		t.Errorf("Metrics().Query(by Name) failed. Got %v distribution, Want %v distribution", distribution.Result(), 0)
	}
}

func TestPairWithMetakey(t *testing.T) {
	// This call is not part of the example, but part of Beam testing infrastructure.
	integration.CheckFilters(t)

	p, s := beam.NewPipelineWithRoot()
	keys := beam.Create(s, "a", "b", "c", "d", "e", "f", "g")

	low, mid, top := metakey{Low: "a", High: "b", Shard: 0}, metakey{Low: "c", High: "e", Shard: 1}, metakey{Low: "f", High: "g", Shard: 2}
	metakeys := beam.Create(s, low, mid, top)
	rekeys := beam.ParDo(s, &pairWithMetaKey{}, keys, beam.SideInput{Input: metakeys})
	passert.Equals(s, beam.DropKey(s, rekeys), keys)

	_ = ptest.RunAndValidate(t, p)
}

func removeTmpPrefix(mk metakey) metakey {
	mk.TmpPrefix = 0 // nil out the time component for comparisons
	return mk
}

func init() {
	beam.RegisterFunction(removeTmpPrefix)
}

func TestMetakeys(t *testing.T) {
	// This call is not part of the example, but part of Beam testing infrastructure.
	integration.CheckFilters(t)

	p, s := beam.NewPipelineWithRoot()
	keys := beam.Create(s, "a", "b", "c", "d", "e", "f", "g")

	// Use a side input to have a single worker sort and shard all the input. One metakey is produced per shard.
	// Requires that the keys fit in single worker memory.
	metakeys := beam.ParDo(s, &makeMetakeys{Output: "test@3.txt"}, beam.Impulse(s), beam.SideInput{Input: keys})

	low, mid, top := metakey{Low: "a", High: "c", Shard: 0, Total: 3}, metakey{Low: "c", High: "e", Shard: 1, Total: 3}, metakey{Low: "e", High: "g", Shard: 2, Total: 3}

	rekeys := beam.ParDo(s, &pairWithMetaKey{}, keys, beam.SideInput{Input: metakeys})
	passert.Equals(s, beam.DropKey(s, rekeys), keys)

	gotmetas := beam.ParDo(s, removeTmpPrefix, beam.DropValue(s, rekeys))

	passert.Equals(s, gotmetas, low, low, low, mid, mid, top, top)

	_ = ptest.RunAndValidate(t, p)
}
