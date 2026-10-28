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
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/test/integration/internal/jars"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
)

type testProcess struct {
	killed bool
	jar    string
	done   chan error
}

func (p *testProcess) Kill() error {
	p.killed = true
	if p.done != nil {
		select {
		case <-p.done:
		default:
			close(p.done)
		}
	}
	return nil
}

func (p *testProcess) Wait() error {
	return <-p.done
}

type readyExpansionService struct {
	jobpb.UnimplementedExpansionServiceServer
}

func (readyExpansionService) DiscoverSchemaTransform(context.Context, *jobpb.DiscoverSchemaTransformRequest) (*jobpb.DiscoverSchemaTransformResponse, error) {
	return &jobpb.DiscoverSchemaTransformResponse{}, nil
}

type expandOnlyService struct {
	jobpb.UnimplementedExpansionServiceServer
	delay time.Duration
	calls atomic.Int32
}

func (s *expandOnlyService) Expand(ctx context.Context, _ *jobpb.ExpansionRequest) (*jobpb.ExpansionResponse, error) {
	s.calls.Add(1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(s.delay):
	}
	return &jobpb.ExpansionResponse{Error: "missing transform specification"}, nil
}

func startExpansionListener(t *testing.T, port string, expansion jobpb.ExpansionServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	if expansion != nil {
		jobpb.RegisterExpansionServiceServer(server, expansion)
	}
	t.Cleanup(server.Stop)
	go server.Serve(listener)
	return listener.Addr().String()
}

func TestExpansionServices_GetAddr_RejectsWrongService(t *testing.T) {
	var procs []*testProcess
	es := &ExpansionServices{
		addrs: map[string]string{},
		jars:  map[string]string{"test": "test.jar"},
		run: func(_ time.Duration, _ string, args ...string) (jars.Process, error) {
			// Another service claims the selected port before the jar can bind it.
			startExpansionListener(t, args[0], nil)
			proc := &testProcess{done: make(chan error)}
			procs = append(procs, proc)
			return proc, nil
		},
	}
	t.Cleanup(es.Shutdown)
	if addr, err := es.GetAddr("test"); err == nil {
		t.Fatalf("GetAddr() = %q, nil; want an error for a different gRPC service", addr)
	}
	if len(es.addrs) != 0 || len(es.procs) != 0 {
		t.Fatalf("GetAddr() cached an unsuccessful startup: addrs=%v, procs=%v", es.addrs, es.procs)
	}
	for _, proc := range procs {
		if !proc.killed {
			t.Error("GetAddr() did not kill the unsuccessful jar process")
		}
	}
	if len(procs) != 3 {
		t.Errorf("GetAddr() started %d jars, want 3 bounded startup attempts", len(procs))
	}
}

func TestExpansionServices_GetAddr_RetriesWrongService(t *testing.T) {
	var procs []*testProcess
	var ports []string
	es := &ExpansionServices{
		addrs: map[string]string{},
		jars:  map[string]string{"test": "test.jar"},
		run: func(_ time.Duration, _ string, args ...string) (jars.Process, error) {
			ports = append(ports, args[0])
			var service jobpb.ExpansionServiceServer
			if len(procs) > 0 {
				service = readyExpansionService{}
			}
			startExpansionListener(t, args[0], service)
			proc := &testProcess{done: make(chan error, 1)}
			if len(procs) == 0 {
				// The jar cannot bind the port held by the other gRPC service.
				proc.done <- nil
				close(proc.done)
			}
			procs = append(procs, proc)
			return proc, nil
		},
	}
	t.Cleanup(es.Shutdown)
	addr, err := es.GetAddr("test")
	if err != nil {
		t.Fatal(err)
	}
	if len(ports) != 2 || ports[0] == ports[1] {
		t.Fatalf("GetAddr() selected ports %v, want a fresh port after the collision", ports)
	}
	if addr != "localhost:"+ports[1] || !procs[0].killed || procs[1].killed {
		t.Errorf("GetAddr() = %q, killed processes = [%v, %v]", addr, procs[0].killed, procs[1].killed)
	}
	if got, err := es.GetAddr("test"); err != nil || got != addr || len(ports) != 2 {
		t.Errorf("cached GetAddr() = %q, %v; ports = %v", got, err, ports)
	}
}

func TestWaitForExpansionService_RejectsExitedJar(t *testing.T) {
	// A compatible listener still cannot prove that our child is running.
	addr := startExpansionListener(t, "0", readyExpansionService{})
	exited := make(chan error, 1)
	exited <- fmt.Errorf("jar failed to start")
	if err := waitForExpansionService(addr, exited); err == nil || !strings.Contains(err.Error(), "jar failed to start") {
		t.Errorf("waitForExpansionService() = %v; want the child startup error", err)
	}
}

func TestExpansionServices_GetAddr_ExpandOnlyService(t *testing.T) {
	// The first expansion can initialize classes after the server binds.
	service := &expandOnlyService{delay: 1200 * time.Millisecond}
	es := &ExpansionServices{
		addrs: map[string]string{},
		jars:  map[string]string{"test": "test.jar"},
		run: func(_ time.Duration, _ string, args ...string) (jars.Process, error) {
			startExpansionListener(t, args[0], service)
			return &testProcess{done: make(chan error)}, nil
		},
	}
	t.Cleanup(es.Shutdown)
	if addr, err := es.GetAddr("test"); err != nil || addr == "" {
		t.Errorf("GetAddr() = %q, %v; want the Expand-only service address", addr, err)
	}
	if got := service.calls.Load(); got != 1 {
		t.Errorf("Expand called %d times, want a single startup probe", got)
	}
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
		testMode: true,
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
		testMode: true,
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
		testMode: true,
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
