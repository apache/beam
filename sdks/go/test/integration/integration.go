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
//    go test -v ./sdks/go/test/integration/... --runner=portable --endpoint=localhost:8099
//
// Alternatively, tests can be executed by running the
// run_validatesrunner_tests.sh script, which also performs much of the
// environment setup, or by calling gradle commands in :sdks:go:test.
package integration

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	// common runner flag.
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

// Filters for temporarily skipping integration tests. All filters are regex
// matchers that must match the full name of a test at the point where
// CheckFilters is called. Multiple tests can be skipped by using regex
// wildcards. (ex. "TestXLang_.*" filters all tests starting with TestXLang_)
//
// It is strongly recommended to include, TODOs, Jira issues, or just comments
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
	"TestDebeziumIO_BasicRead",
	"TestJDBCIO_BasicReadWrite",
	// Triggers, Panes are not yet supported
	"TestTrigger.*",
	"TestPanes",
	// The direct runner does not support the TestStream primitive
	"TestTestStream.*",
	// (BEAM-13075): The direct runner does not support windowed side inputs
	"TestValidateWindowedSideInputs",
	// (BEAM-13075): The direct runner does not currently support multimap side inputs
	"TestParDoMultiMapSideInput",
	"TestLargeWordcount_Loopback",
}

var portableFilters = []string{
	// The portable runner does not support the TestStream primitive
	"TestTestStream.*",
	// The trigger and pane tests uses TestStream
	"TestTrigger.*",
	"TestPanes",
	// TODO(BEAM-12797): Python portable runner times out on Kafka reads.
	"TestKafkaIO.*",
}

var flinkFilters = []string{
	// TODO(BEAM-11500): Flink tests timing out on reads.
	"TestXLang_Combine.*",
	// TODO(BEAM-12815): Test fails: "Insufficient number of network buffers".
	"TestXLang_Multi",
	"TestDebeziumIO_BasicRead",
	// TODO(BEAM-12753): Flink test stream fails for non-string/byte slice inputs
	"TestTestStream.*Sequence.*",
	// Triggers are not yet supported
	"TestTrigger.*",
}

var samzaFilters = []string{
	// TODO(BEAM-12608): Samza tests invalid encoding.
	"TestReshuffle",
	"TestReshuffleKV",
	// The Samza runner does not support the TestStream primitive
	"TestTestStream.*",
	// The trigger and pane tests uses TestStream
	"TestTrigger.*",
	"TestPanes",
	// TODO(BEAM-13006): Samza doesn't yet support post job metrics, used by WordCount
	"TestWordCount.*",
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
}

var dataflowFilters = []string{
	// The Dataflow runner doesn't work with tests using testcontainers locally.
	"TestJDBCIO_BasicReadWrite",
	"TestDebeziumIO_BasicRead",
	// TODO(BEAM-11576): TestFlattenDup failing on this runner.
	"TestFlattenDup",
	// The Dataflow runner does not support the TestStream primitive
	"TestTestStream.*",
	// The trigger and pane tests uses TestStream
	"TestTrigger.*",
	"TestPanes",
	// There is no infrastructure for running KafkaIO tests with Dataflow.
	"TestKafkaIO.*",
	// Dataflow doesn't support any test that requires loopback.
	// Eg. For FileIO examples.
	".*Loopback.*",
}

// CheckFilters checks if an integration test is filtered to be skipped, either
// because the intended runner does not support it, or the test is sickbayed.
// This method should be called at the beginning of any integration test. If
// t.Run is used, CheckFilters should be called within the t.Run callback, so
// that sub-tests can be skipped individually.
func CheckFilters(t *testing.T) {
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
	// TODO(lostluck): Improve default job names.
	*jobopts.JobName = fmt.Sprintf("go-%v", strings.ToLower(n))

	// Test for runner-specific skipping second.
	var filters []string
	runner := *ptest.Runner
	if runner == "" {
		runner = ptest.DefaultRunner()
	}
	switch runner {
	case "direct", "DirectRunner":
		filters = directFilters
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
