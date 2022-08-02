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

	// Concept: Loopback mode.
	// Force setting loopback to allow for local file when testing
	// against local portable runners.
	// Loopback mode connects the runner back to the main program to
	// operate as the worker. This is useful for local development
	// and testing of pipelines.
	oldEnvType := *jobopts.EnvironmentType
	*jobopts.EnvironmentType = "LOOPBACK"
	t.Cleanup(func() {
		*jobopts.EnvironmentType = oldEnvType
	})

	// Create a place to write out files for testing & clean up post test completion.
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

	// Write out two test input files to read from.
	if err := os.WriteFile(filepath.Join(inputPath, "file1.txt"), []byte(file1), perms); err != nil {
		t.Fatalf("unable to write file1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(inputPath, "file2.txt"), []byte(file2), perms); err != nil {
		t.Fatalf("unable to write file2: %v", err)
	}

	// Testing a pipeline is as simple as configuring and running the pipeline.
	p, s := beam.NewPipelineWithRoot()
	finalFiles := Pipeline(s,
		filepath.Join(inputPath, "*.txt"),
		filepath.Join(outputDir, "wordcounts@2.txt"))

	// Concept: Using passert.Equals to validate PCollection output.
	// In this case, we validate that we have produced the expected final filepaths.
	// The passert package allows validation of properties of the pipeline in the pipeline itself.
	passert.Equals(s, finalFiles,
		filepath.Join(outputDir, "wordcounts000-002.txt"),
		filepath.Join(outputDir, "wordcounts001-002.txt"),
	)

	// Concept: ptest
	// ptest is a package with helpers intended to abstract out testing.
	// ptest allows you to execute the pipeline on a runner of your
	// choice with flags passed to the test execution.
	// By default, this runs against the Go direct runner.
	pr := ptest.RunAndValidate(t, p)

	// Concept: Using PipelineResults to query for metrics to ensure
	// expected running of specific DoFns.

	qr := pr.Metrics().Query(func(mr beam.MetricResult) bool {
		return mr.Name() == "keycount"
	})
	if got, want := len(qr.Counters()), 1; got != want {
		t.Fatalf("Metrics().Query(by Name = keycount) = %v counter, want %v", got, want)
	}
	c := qr.Counters()[0]
	if got, want := c.Result(), int64(11); got != want {
		t.Errorf("Metrics().Query(by Name) = %v, want %v", got, want)
	}

	qr = pr.Metrics().Query(func(mr beam.MetricResult) bool {
		return mr.Name() == "countdistro"
	})
	if got, want := len(qr.Distributions()), 1; got != want {
		t.Fatalf("Metrics().Query(by Name = countdistro) returned %d distribution, want %v", got, want)
	}
	d := qr.Distributions()[0]
	if got, want := d.Result(), (metrics.DistributionValue{Count: 11, Sum: 20, Min: 1, Max: 2}); got != want {
		t.Errorf("Metrics().Query(by Name = countdistro) failed. Got %+v distribution, Want %+v distribution", got, want)
	}
}
