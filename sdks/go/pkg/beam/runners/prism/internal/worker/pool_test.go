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

package worker

import (
	"bytes"
	"context"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func serveTestWorker(t *testing.T) (context.Context, *grpc.ClientConn) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	w := Pool.NewWorker("test", "testEnv")
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("worker_id", w.ID))
	ctx = grpcx.WriteWorkerID(ctx, w.ID)
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	addr := "localhost:" + port
	g := NewMultiplexW()
	t.Cleanup(func() {
		w.Stop()
		g.Stop()
	})
	go g.Serve(lis)

	clientConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return ctx, clientConn
}

func serveTestWorkerStateStream(t *testing.T) (context.Context, fnpb.BeamFnState_StateClient, closeSend) {
	ctx, clientConn := serveTestWorker(t)

	stateCli := fnpb.NewBeamFnStateClient(clientConn)
	stateStream, err := stateCli.State(ctx)
	if err != nil {
		t.Fatal("couldn't create state client:", err)
	}
	return ctx, stateStream, func() {
		if err := stateStream.CloseSend(); err != nil {
			t.Errorf("stateStream.CloseSend() = %v", err)
		}
	}
}

func TestWorker_Logging(t *testing.T) {
	ctx, clientConn := serveTestWorker(t)

	logCli := fnpb.NewBeamFnLoggingClient(clientConn)
	logStream, err := logCli.Logging(ctx)
	if err != nil {
		t.Fatal("couldn't create log client:", err)
	}

	logStream.Send(&fnpb.LogEntry_List{
		LogEntries: []*fnpb.LogEntry{{
			Severity:    fnpb.LogEntry_Severity_INFO,
			Message:     "squeamish ossiphrage",
			LogLocation: "intentionally.go:124",
		}},
	})

	logStream.Send(&fnpb.LogEntry_List{
		LogEntries: []*fnpb.LogEntry{{
			Severity:    fnpb.LogEntry_Severity_INFO,
			Message:     "squeamish ossiphrage the second",
			LogLocation: "intentionally bad log location",
		}},
	})

	// TODO: Connect to the job management service.
	// At this point job messages are just logged to wherever the prism runner executes
	// But this should pivot to anyone connecting to the Job Management service for the
	// job.
	// In the meantime, sleep to validate execution via coverage.
	time.Sleep(20 * time.Millisecond)
}

func TestWorker_Control_HappyPath(t *testing.T) {
	ctx, clientConn := serveTestWorker(t)
	wk, err := Pool.workerFromMetadataCtx(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ctrlCli := fnpb.NewBeamFnControlClient(clientConn)
	ctrlStream, err := ctrlCli.Control(ctx)
	if err != nil {
		t.Fatal("couldn't create control client:", err)
	}

	instID := wk.NextInst()

	b := &B{}
	b.Init()
	wk.activeInstructions[instID] = b
	b.ProcessOn(ctx, wk)

	ctrlStream.Send(&fnpb.InstructionResponse{
		InstructionId: instID,
		Response: &fnpb.InstructionResponse_ProcessBundle{
			ProcessBundle: &fnpb.ProcessBundleResponse{
				RequiresFinalization: true, // Simple thing to check.
			},
		},
	})

	if err := ctrlStream.CloseSend(); err != nil {
		t.Errorf("ctrlStream.CloseSend() = %v", err)
	}
	resp := <-b.Resp

	if resp == nil {
		t.Fatal("resp is nil from bundle")
	}

	if !resp.RequiresFinalization {
		t.Errorf("got %v, want response that Requires Finalization", resp)
	}
}

func TestWorker_Data_HappyPath(t *testing.T) {
	ctx, clientConn := serveTestWorker(t)
	wk, err := Pool.workerFromMetadataCtx(ctx)
	if err != nil {
		t.Fatal(err)
	}

	dataCli := fnpb.NewBeamFnDataClient(clientConn)
	dataStream, err := dataCli.Data(ctx)
	if err != nil {
		t.Fatal("couldn't create data client:", err)
	}

	instID := wk.NextInst()

	b := &B{
		InstID: instID,
		PBDID:  "teststageID",
		Input: []*engine.Block{
			{
				Kind:  engine.BlockData,
				Bytes: [][]byte{{1, 1, 1, 1, 1, 1}},
			}},
		OutputCount: 1,
	}
	b.Init()
	wk.activeInstructions[instID] = b

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.ProcessOn(ctx, wk)
	}()

	wk.InstReqs <- &fnpb.InstructionRequest{
		InstructionId: instID,
	}

	elements, err := dataStream.Recv()
	if err != nil {
		t.Fatal("couldn't receive data elements:", err)
	}

	if got, want := elements.GetData()[0].GetInstructionId(), b.InstID; got != want {
		t.Fatalf("couldn't receive data elements ID: got %v, want %v", got, want)
	}
	if got, want := elements.GetData()[0].GetData(), []byte{1, 1, 1, 1, 1, 1}; !bytes.Equal(got, want) {
		t.Fatalf("client Data received %v, want %v", got, want)
	}
	if got, want := elements.GetData()[0].GetIsLast(), false; got != want {
		t.Fatalf("client Data received was last: got %v, want %v", got, want)
	}

	elements, err = dataStream.Recv()
	if err != nil {
		t.Fatal("expected 2nd data elements:", err)
	}
	if got, want := elements.GetData()[0].GetInstructionId(), b.InstID; got != want {
		t.Fatalf("couldn't receive data elements ID: got %v, want %v", got, want)
	}
	if got, want := elements.GetData()[0].GetData(), []byte(nil); !bytes.Equal(got, want) {
		t.Fatalf("client Data received %v, want %v", got, want)
	}
	if got, want := elements.GetData()[0].GetIsLast(), true; got != want {
		t.Fatalf("client Data received wasn't last: got %v, want %v", got, want)
	}

	dataStream.Send(elements)

	if err := dataStream.CloseSend(); err != nil {
		t.Errorf("ctrlStream.CloseSend() = %v", err)
	}

	wg.Wait()
	t.Log("ProcessOn successfully exited")
}

func TestWorker_State_Iterable(t *testing.T) {
	ctx, clientConn := serveTestWorker(t)
	wk, err := Pool.workerFromMetadataCtx(ctx)
	if err != nil {
		t.Fatal(err)
	}

	stateCli := fnpb.NewBeamFnStateClient(clientConn)
	stateStream, err := stateCli.State(ctx)
	if err != nil {
		t.Fatal("couldn't create state client:", err)
	}

	instID := wk.NextInst()
	wk.activeInstructions[instID] = &B{
		IterableSideInputData: map[SideInputKey]map[typex.Window][][]byte{
			{TransformID: "transformID", Local: "i1"}: {
				window.GlobalWindow{}: [][]byte{
					{42},
				},
			},
		},
	}

	stateStream.Send(&fnpb.StateRequest{
		Id:            "first",
		InstructionId: instID,
		Request: &fnpb.StateRequest_Get{
			Get: &fnpb.StateGetRequest{},
		},
		StateKey: &fnpb.StateKey{Type: &fnpb.StateKey_IterableSideInput_{
			IterableSideInput: &fnpb.StateKey_IterableSideInput{
				TransformId: "transformID",
				SideInputId: "i1",
				Window:      []byte{}, // Global Windows
			},
		}},
	})

	resp, err := stateStream.Recv()
	if err != nil {
		t.Fatal("couldn't receive state response:", err)
	}

	if got, want := resp.GetId(), "first"; got != want {
		t.Fatalf("didn't receive expected state response: got %v, want %v", got, want)
	}

	if got, want := resp.GetGet().GetData(), []byte{42}; !bytes.Equal(got, want) {
		t.Fatalf("didn't receive expected state response data: got %v, want %v", got, want)
	}

	if err := stateStream.CloseSend(); err != nil {
		t.Errorf("stateStream.CloseSend() = %v", err)
	}
}

func TestWorker_State_MultimapKeysSideInput(t *testing.T) {
	for _, tt := range []struct {
		name string
		w    typex.Window
	}{
		{
			name: "global window",
			w:    window.GlobalWindow{},
		},
		{
			name: "interval window",
			w: window.IntervalWindow{
				Start: 1000,
				End:   2000,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var encW []byte
			if !tt.w.Equals(window.GlobalWindow{}) {
				buf := bytes.Buffer{}
				if err := exec.MakeWindowEncoder(coder.NewIntervalWindow()).EncodeSingle(tt.w, &buf); err != nil {
					t.Fatalf("error encoding window: %v, err: %v", tt.w, err)
				}
				encW = buf.Bytes()
			}
			ctx, stateStream, done := serveTestWorkerStateStream(t)
			defer done()

			wk, err := Pool.workerFromMetadataCtx(ctx)
			if err != nil {
				t.Fatal(err)
			}

			instID := wk.NextInst()
			wk.activeInstructions[instID] = &B{
				MultiMapSideInputData: map[SideInputKey]map[typex.Window]map[string][][]byte{
					SideInputKey{
						TransformID: "transformID",
						Local:       "i1",
					}: {
						tt.w: map[string][][]byte{"a": {{1}}, "b": {{2}}},
					},
				},
			}

			stateStream.Send(&fnpb.StateRequest{
				Id:            "first",
				InstructionId: instID,
				Request: &fnpb.StateRequest_Get{
					Get: &fnpb.StateGetRequest{},
				},
				StateKey: &fnpb.StateKey{Type: &fnpb.StateKey_MultimapKeysSideInput_{
					MultimapKeysSideInput: &fnpb.StateKey_MultimapKeysSideInput{
						TransformId: "transformID",
						SideInputId: "i1",
						Window:      encW,
					},
				}},
			})

			resp, err := stateStream.Recv()
			if err != nil {
				t.Fatal("couldn't receive state response:", err)
			}

			want := []int{97, 98}
			var got []int
			for _, b := range resp.GetGet().GetData() {
				got = append(got, int(b))
			}
			sort.Ints(got)

			if !cmp.Equal(got, want) {
				t.Errorf("didn't receive expected state response data: got %v, want %v", got, want)
			}
		})
	}
}

func TestWorker_State_MultimapSideInput(t *testing.T) {
	for _, tt := range []struct {
		name string
		w    typex.Window
	}{
		{
			name: "global window",
			w:    window.GlobalWindow{},
		},
		{
			name: "interval window",
			w: window.IntervalWindow{
				Start: 1000,
				End:   2000,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var encW []byte
			if !tt.w.Equals(window.GlobalWindow{}) {
				buf := bytes.Buffer{}
				if err := exec.MakeWindowEncoder(coder.NewIntervalWindow()).EncodeSingle(tt.w, &buf); err != nil {
					t.Fatalf("error encoding window: %v, err: %v", tt.w, err)
				}
				encW = buf.Bytes()
			}
			ctx, stateStream, done := serveTestWorkerStateStream(t)
			defer done()

			wk, err := Pool.workerFromMetadataCtx(ctx)
			if err != nil {
				t.Fatal(err)
			}

			instID := wk.NextInst()
			wk.activeInstructions[instID] = &B{
				MultiMapSideInputData: map[SideInputKey]map[typex.Window]map[string][][]byte{
					SideInputKey{
						TransformID: "transformID",
						Local:       "i1",
					}: {
						tt.w: map[string][][]byte{"a": {{5}}, "b": {{12}}},
					},
				},
			}
			var testKey = []string{"a", "b", "x"}
			expectedResult := map[string][]int{
				"a": {5},
				"b": {12},
			}
			for _, key := range testKey {
				stateStream.Send(&fnpb.StateRequest{
					Id:            "first",
					InstructionId: instID,
					Request: &fnpb.StateRequest_Get{
						Get: &fnpb.StateGetRequest{},
					},
					StateKey: &fnpb.StateKey{Type: &fnpb.StateKey_MultimapSideInput_{
						MultimapSideInput: &fnpb.StateKey_MultimapSideInput{
							TransformId: "transformID",
							SideInputId: "i1",
							Window:      encW,
							Key:         []byte(key),
						},
					}},
				})

				resp, err := stateStream.Recv()
				if err != nil {
					t.Fatal("Couldn't receive state response:", err)
				}

				var got []int
				for _, b := range resp.GetGet().GetData() {
					got = append(got, int(b))
				}
				if !cmp.Equal(got, expectedResult[key]) {
					t.Errorf("For test key: %v, didn't receive expected state response data: got %v, want %v", key, got, expectedResult[key])
				}
			}
		})
	}
}
