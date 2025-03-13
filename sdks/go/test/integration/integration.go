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

// Package integration provides functionality that needs to be shared between all
// integration tests.
//
// Integration tests are implemented through Go's test framework, as test
// functions that create and execute pipelines using the ptest package. Tests
// should be placed in smaller sub-packages for organizational purposes and
// parallelism (tests are only run in parallel across different packages).
// Integration tests should always begin with a call to CheckFilters to ensure
// test filters can be applied, and each package containing integration tests
// should call ptest.Main in a TestMain function if it uses ptest.
//
// Running integration tests can be done with a go test call with any flags that
// are required by the test pipelines, such as --runner or --endpoint.
// Example:
//
//	go test -v ./sdks/go/test/integration/... --runner=portable --endpoint=localhost:8099
//
// Alternatively, tests can be executed by running the
// run_validatesrunner_tests.sh script, which also performs much of the
// environment setup, or by calling gradle commands in :sdks:go:test.
package integration

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"
	"time"

	// common runner flag.
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

// Filters for temporarily skipping integration tests. All filters are regex
// matchers that must match the full name of a test at the point where
// CheckFilters is called. Multiple tests can be skipped by using regex
// wildcards. (ex. "TestXLang_.*" filters all tests starting with TestXLang_)
//
// It is strongly recommended to include, TODOs, GitHub issues, or just comments
// describing why tests are being skipped.

// sickbay filters tests that fail due to Go SDK errors. These tests will not
// execute on any runners.
var sickbay = []string{}

// Runner-specific test filters, for features that are not yet supported on
// specific runners.

var directFilters = []string{
	// The direct runner does not yet support cross-language.
	"TestXLang.*",
	"TestKafkaIO.*",
	"TestBigQueryIO.*",
	"TestBigtableIO.*",
	"TestSpannerIO.*",
	"TestDebeziumIO_BasicRead",
	"TestJDBCIO_BasicReadWrite",
	"TestJDBCIO_PostgresReadWrite",
	"TestDataframe",
	// Triggers, Panes are not yet supported
	"TestTrigger.*",
	"TestPanes",
	// The direct runner does not support the TestStream primitive
	"TestTestStream.*",
	// (https://github.com/apache/beam/issues/21130): The direct runner does not support windowed side inputs
	"TestValidateWindowedSideInputs",
	// (https://github.com/apache/beam/issues/21130): The direct runner does not currently support multimap side inputs
	"TestParDoMultiMapSideInput",
	"TestLargeWordcount_Loopback",
	// The direct runner does not support self-checkpointing
	"TestCheckpointing",
	// The direct runner does not support pipeline drain for SDF.
	"TestDrain",
	// FhirIO currently only supports Dataflow runner
	"TestFhirIO.*",
	// OOMs currently only lead to heap dumps on Dataflow runner
	"TestOomParDo",
	// The direct runner does not support user state.
	"TestValueState",
	"TestValueStateWindowed",
	"TestValueStateClear",
	"TestBagState",
	"TestBagStateClear",
	"TestCombiningState",
	"TestMapState",
	"TestMapStateClear",
	"TestSetState",
	"TestSetStateClear",
	"TestTimers.*", // no timer support for the go direct runner.

	// no support for BundleFinalizer
	"TestParDoBundleFinalizer.*",
}

var portableFilters = []string{
	// The portable runner does not support the TestStream primitive
	"TestTestStream.*",
	// The trigger and pane tests uses TestStream
	"TestTrigger.*",
	"TestPanes",
	// TODO(https://github.com/apache/beam/issues/21058): Python portable runner times out on Kafka reads.
	"TestKafkaIO.*",
	// TODO(BEAM-13215): GCP IOs currently do not work in non-Dataflow portable runners.
	"TestBigQueryIO.*",
	"TestBigtableIO.*",
	"TestSpannerIO.*",
	// The portable runner does not support self-checkpointing
	"TestCheckpointing",
	// The portable runner does not support pipeline drain for SDF.
	"TestDrain",
	// FhirIO currently only supports Dataflow runner
	"TestFhirIO.*",
	// OOMs currently only lead to heap dumps on Dataflow runner
	"TestOomParDo",
	// The portable runner does not support user map states.
	"TestMapState",
	"TestMapStateClear",
	"TestSetState",
	"TestSetStateClear",

	// The portable runner does not uniquify timers. (data elements re-fired)
	"TestTimers.*",

	// no support for BundleFinalizer
	"TestParDoBundleFinalizer.*",
}

var prismFilters = []string{
	// The prism runner does not yet support Java's CoGBK.
	"TestXLang_CoGroupBy",
	// The trigger and pane tests uses TestStream
	"TestTrigger.*",
	"TestPanes",

	// TODO(https://github.com/apache/beam/issues/21058): Xlang ios don't yet work on prism.
	"TestKafkaIO.*",
	// TODO(BEAM-13215): GCP IOs currently do not work in non-Dataflow portable runners.
	"TestBigQueryIO.*",
	"TestSpannerIO.*",
	// The prism runner does not support pipeline drain for SDF.
	"TestDrain",
	// FhirIO currently only supports Dataflow runner
	"TestFhirIO.*",
	// OOMs currently only lead to heap dumps on Dataflow runner
	"TestOomParDo",
}

var flinkFilters = []string{
	// TODO(https://github.com/apache/beam/issues/20723): Flink tests timing out on reads.
	"TestXLang_Combine.*",
	"TestDebeziumIO_BasicRead",
	// TODO(BEAM-13215): GCP IOs currently do not work in non-Dataflow portable runners.
	"TestBigQueryIO.*",
	"TestBigtableIO.*",
	"TestSpannerIO.*",
	// The number of produced outputs in AfterSynchronizedProcessingTime varies in different runs.
	"TestTriggerAfterSynchronizedProcessingTime",
	// The flink runner does not support pipeline drain for SDF.
	"TestDrain",
	// FhirIO currently only supports Dataflow runner
	"TestFhirIO.*",
	// OOMs currently only lead to heap dumps on Dataflow runner
	"TestOomParDo",
	// Flink does not support map based state types.
	"TestMapState",
	"TestMapStateClear",
	"TestSetStateClear",
	"TestSetState",

	// With TestStream Flink adds extra length prefixs some data types, causing SDK side failures.
	"TestTestStreamStrings",
	"TestTestStreamByteSliceSequence",
	"TestTestStreamTwoUserTypeSequences",
	"TestTestStreamInt16Sequence",
	"TestTestStreamSimple",
	"TestTestStreamSimple_InfinityDefault",
	"TestTestStreamToGBK",
	"TestTestStreamTimersEventTime",

	"TestTimers_EventTime_Unbounded", // (failure when comparing on side inputs (NPE on window lookup))
	"TestTimers_ProcessingTime.*",    // Flink doesn't support processing time timers.

	// no support for BundleFinalizer
	"TestParDoBundleFinalizer.*",
}

var samzaFilters = []string{
	// TODO(https://github.com/apache/beam/issues/20987): Samza tests invalid encoding.
	"TestReshuffle",
	"TestReshuffleKV",
	// The Samza runner does not support the TestStream primitive
	"TestTestStream.*",
	// The trigger and pane tests uses TestStream
	"TestTrigger.*",
	"TestPanes",
	// TODO(https://github.com/apache/beam/issues/21244): Samza doesn't yet support post job metrics, used by WordCount
	"TestWordCount.*",
	// TODO(BEAM-13215): GCP IOs currently do not work in non-Dataflow portable runners.
	"TestBigQueryIO.*",
	"TestBigtableIO.*",
	"TestSpannerIO.*",
	// The Samza runner does not support self-checkpointing
	"TestCheckpointing",
	// The samza runner does not support pipeline drain for SDF.
	"TestDrain",
	// FhirIO currently only supports Dataflow runner
	"TestFhirIO.*",
	// OOMs currently only lead to heap dumps on Dataflow runner
	"TestOomParDo",
	// The samza runner does not support user state.
	"TestValueState",
	"TestValueStateWindowed",
	"TestValueStateClear",
	"TestBagState",
	"TestBagStateClear",
	"TestCombiningState",
	"TestMapState",
	"TestMapStateClear",
	"TestSetState",
	"TestSetStateClear",
	// TODO(https://github.com/apache/beam/issues/26126): Java runner issue (AcitveBundle has no regsitered handler)
	"TestDebeziumIO_BasicRead",

	// Samza does not support state.
	"TestTimers.*",

	// no support for BundleFinalizer
	"TestParDoBundleFinalizer.*",
}

var sparkFilters = []string{
	// TODO(BEAM-11498): XLang tests broken with Spark runner.
	"TestXLang.*",
	"TestParDoSideInput",
	"TestParDoKVSideInput",
	// The Spark runner does not support the TestStream primitive
	"TestTestStream.*",
	// The trigger and pane tests uses TestStream
	"TestTrigger.*",
	"TestPanes",
	// [BEAM-13921]: Spark doesn't support side inputs to executable stages
	"TestDebeziumIO_BasicRead",
	// TODO(BEAM-13215): GCP IOs currently do not work in non-Dataflow portable runners.
	"TestBigQueryIO.*",
	"TestBigtableIO.*",
	"TestSpannerIO.*",
	// The spark runner does not support self-checkpointing
	"TestCheckpointing",
	// The spark runner does not support pipeline drain for SDF.
	"TestDrain",
	// FhirIO currently only supports Dataflow runner
	"TestFhirIO.*",
	// OOMs currently only lead to heap dumps on Dataflow runner
	"TestOomParDo",
	// Spark does not support map based state types.
	"TestMapState",
	"TestMapStateClear",
	"TestSetStateClear",
	"TestSetState",

	"TestTimers_EventTime_Unbounded",     // Side inputs in executable stage not supported.
	"TestTimers_ProcessingTime_Infinity", // Spark doesn't support test stream.

	// no support for BundleFinalizer
	"TestParDoBundleFinalizer.*",
}

var dataflowFilters = []string{
	// The Dataflow runner doesn't work with tests using testcontainers locally.
	"TestJDBCIO_BasicReadWrite",
	"TestJDBCIO_PostgresReadWrite",
	"TestDebeziumIO_BasicRead",
	"TestMongoDBIO.*",
	// TODO(BEAM-11576): TestFlattenDup failing on this runner.
	"TestFlattenDup",
	// The Dataflow runner does not support the TestStream primitive
	"TestTestStream.*",
	// The trigger and pane tests uses TestStream
	"TestTrigger.*",
	"TestPanes",
	// There is no infrastructure for running KafkaIO tests with Dataflow.
	"TestKafkaIO.*",
	"TestSpannerIO.*",
	// Dataflow doesn't support any test that requires loopback.
	// Eg. For FileIO examples.
	".*Loopback.*",
	// Dataflow does not automatically terminate the TestCheckpointing pipeline when
	// complete.
	"TestCheckpointing",
	// TODO(21761): This test needs to provide GCP project to expansion service.
	"TestBigQueryIO_BasicWriteQueryRead",
	// Can't handle the test spanner container or access a local spanner.
	"TestSpannerIO.*",
	// Dataflow does not drain jobs by itself.
	"TestDrain",
	// Timers
	"TestTimers_ProcessingTime_Infinity", // Uses test stream.
	"TestTimers_ProcessingTime_Bounded",  // Dataflow ignores processing time timers in batch.
}

// CheckFilters checks if an integration test is filtered to be skipped, either
// because the intended runner does not support it, or the test is sickbayed.
// This method should be called at the beginning of any integration test. If
// t.Run is used, CheckFilters should be called within the t.Run callback, so
// that sub-tests can be skipped individually.
func CheckFilters(t *testing.T) {
	if !ptest.MainCalled() {
		panic("ptest.Main() has not been called: please override TestMain to ensure that the integration test runs properly.")
	}

	// Check for sickbaying first.
	n := t.Name()
	for _, f := range sickbay {
		// Add start and end of string regexp matchers so only a full match is
		// counted.
		f = fmt.Sprintf("^%v$", f)
		match, err := regexp.MatchString(f, n)
		if err != nil {
			t.Errorf("Matching of regex '%v' with test '%v' failed: %v", f, n, err)
		}
		if match {
			t.Skipf("Test %v is currently sickbayed on all runners", n)
		}
	}
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	*jobopts.JobName = fmt.Sprintf("go-%v-%v", strings.ToLower(n), r1.Intn(1000))
	// Test for runner-specific skipping second.
	var filters []string
	runner := *ptest.Runner
	if runner == "" {
		runner = ptest.DefaultRunner()
	}
	switch runner {
	case "direct", "DirectRunner":
		filters = directFilters
	case "prism", "PrismRunner":
		filters = prismFilters
	case "portable", "PortableRunner":
		filters = portableFilters
	case "flink", "FlinkRunner":
		filters = flinkFilters
	case "samza", "SamzaRunner":
		filters = samzaFilters
	case "spark", "SparkRunner":
		filters = sparkFilters
	case "dataflow", "DataflowRunner":
		filters = dataflowFilters
	default:
		return
	}

	for _, f := range filters {
		// Add start and end of string regexp matchers so only a full match is
		// counted.
		f = fmt.Sprintf("^%v$", f)
		match, err := regexp.MatchString(f, n)
		if err != nil {
			t.Errorf("Matching of regex '%v' with test '%v' failed: %v", f, n, err)
		}
		if match {
			t.Skipf("Test %v is currently filtered for runner %v", n, runner)
		}
	}
}
