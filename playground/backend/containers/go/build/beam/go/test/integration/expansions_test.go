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

package integration

import (
	"fmt"
	"testing"
	"time"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/test/integration/internal/jars"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

type testProcess struct {
	killed bool
	jar    string
}

func (p *testProcess) Kill() error {
	p.killed = true
	return nil
}

func failRun(_ time.Duration, _ string, _ ...string) (jars.Process, error) {
	return nil, fmt.Errorf("unexpectedly running a jar, failing")
}

func succeedRun(_ time.Duration, jar string, _ ...string) (jars.Process, error) {
	return &testProcess{jar: jar}, nil
}

// TestExpansionServices_GetAddr_Addresses tests calling GetAddr on provided addresses.
func TestExpansionServices_GetAddr_Addresses(t *testing.T) {
	addrsMap := map[string]string{
		"label1": "testAddr1",
		"label2": "testAddr2",
		"label3": "testAddr3",
	}
	jarsMap := map[string]string{
		"label2": "jarFilepath2",
	}
	es := &ExpansionServices{
		addrs:    addrsMap,
		jars:     jarsMap,
		procs:    make([]jars.Process, 0),
		run:      failRun,
		waitTime: 0,
	}

	// Ensure we get the same map we put in, and that addresses take priority over jars if
	// both are given for the same label.
	for label, wantAddr := range addrsMap {
		gotAddr, err := es.GetAddr(label)
		if err != nil {
			t.Errorf("unexpected error when getting address for \"%v\": %v", label, err)
			continue
		}
		if gotAddr != wantAddr {
			t.Errorf("incorrect address for \"%v\", want %v, got %v", label, wantAddr, gotAddr)
		}
	}
	// Check that nonexistent labels fail.
	if _, err := es.GetAddr("nonexistent_label"); err == nil {
		t.Errorf("did not receive error when calling GetAddr with nonexistent label")
	}
}

// TestExpansionServices_GetAddr_Jars tests calling GetAddr on provided jars.
func TestExpansionServices_GetAddr_Jars(t *testing.T) {
	addrsMap := map[string]string{}
	jarsMap := map[string]string{
		"label1": "jarFilepath1",
		"label2": "jarFilepath2",
		"label3": "jarFilepath3",
	}
	es := &ExpansionServices{
		addrs:    addrsMap,
		jars:     jarsMap,
		procs:    make([]jars.Process, 0),
		run:      succeedRun,
		waitTime: 0,
	}

	// Call GetAddr on each jar twice, checking that the addresses remain consistent.
	gotMap := make(map[string]string)
	for label := range jarsMap {
		gotAddr, err := es.GetAddr(label)
		if err != nil {
			t.Errorf("unexpected error when getting address for \"%v\": %v", label, err)
			continue
		}
		gotMap[label] = gotAddr
	}
	for label, gotAddr := range gotMap {
		secondAddr, err := es.GetAddr(label)
		if err != nil {
			t.Errorf("unexpected error when getting address for \"%v\": %v", label, err)
			continue
		}
		if secondAddr != gotAddr {
			t.Errorf("getAddr returned different address when called twice for \"%v\", "+
				"attempt 1: %v, attempt 2: %v", label, gotAddr, secondAddr)
		}
	}
	// Check that all jars were run.
	gotJars := make([]string, 0)
	for _, proc := range es.procs {
		testProc := proc.(*testProcess)
		gotJars = append(gotJars, testProc.jar)
	}
	wantJars := make([]string, 0)
	for _, jar := range jarsMap {
		wantJars = append(wantJars, jar)
	}
	lessFunc := func(a, b string) bool { return a < b }
	if diff := cmp.Diff(wantJars, gotJars, cmpopts.SortSlices(lessFunc)); diff != "" {
		t.Errorf("processes in ExpansionServices does not match jars that should be running: diff(-want,+got):\n%v", diff)
	}
}

// TestExpansionServices_Shutdown tests that a shutdown correctly kills all jars started by an
// ExpansionServices.
func TestExpansionServices_Shutdown(t *testing.T) {
	addrsMap := map[string]string{}
	jarsMap := map[string]string{
		"label1": "jarFilepath1",
		"label2": "jarFilepath2",
		"label3": "jarFilepath3",
	}
	es := &ExpansionServices{
		addrs:    addrsMap,
		jars:     jarsMap,
		procs:    make([]jars.Process, 0),
		run:      succeedRun,
		waitTime: 0,
	}
	// Call getAddr on each label to run jars.
	for label := range addrsMap {
		_, err := es.GetAddr(label)
		if err != nil {
			t.Errorf("unexpected error when getting address for \"%v\": %v", label, err)
			continue
		}
	}

	// Shutdown and confirm that jars are killed and addresses can no longer be retrieved.
	procs := es.procs
	es.Shutdown()
	for _, proc := range procs {
		testProc := proc.(*testProcess)
		if !testProc.killed {
			t.Errorf("process for jar %v was not killed on Shutdown()", testProc.jar)
		}
	}
	for label := range addrsMap {
		_, err := es.GetAddr(label)
		if err == nil {
			t.Errorf("calling GetAddr after Shutdown did not return an error for \"%v\"", label)
		}
	}
}
