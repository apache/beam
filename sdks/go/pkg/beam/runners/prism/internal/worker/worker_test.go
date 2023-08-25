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
	"sync"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestWorker_New(t *testing.T) {
	w := New("test")
	if got, want := w.ID, "test"; got != want {
		t.Errorf("New(%q) = %v, want %v", want, got, want)
	}
}

func TestWorker_NextInst(t *testing.T) {
	w := New("test")

	instIDs := map[string]struct{}{}
	for i := 0; i < 100; i++ {
		instIDs[w.NextInst()] = struct{}{}
	}
	if got, want := len(instIDs), 100; got != want {
		t.Errorf("calling w.NextInst() got %v unique ids, want %v", got, want)
	}
}

func TestWorker_NextStage(t *testing.T) {
	w := New("test")

	stageIDs := map[string]struct{}{}
	for i := 0; i < 100; i++ {
		stageIDs[w.NextStage()] = struct{}{}
	}
	if got, want := len(stageIDs), 100; got != want {
		t.Errorf("calling w.NextStage() got %v unique ids, want %v", got, want)
	}
}

func TestWorker_GetProcessBundleDescriptor(t *testing.T) {
	w := New("test")

	id := "available"
	w.Descriptors[id] = &fnpb.ProcessBundleDescriptor{
		Id: id,
	}

	pbd, err := w.GetProcessBundleDescriptor(context.Background(), &fnpb.GetProcessBundleDescriptorRequest{
		ProcessBundleDescriptorId: id,
	})
	if err != nil {
		t.Errorf("got GetProcessBundleDescriptor(%q) error: %v, want nil", id, err)
	}
	if got, want := pbd.GetId(), id; got != want {
		t.Errorf("got GetProcessBundleDescriptor(%q) = %v, want id %v", id, got, want)
	}

	pbd, err = w.GetProcessBundleDescriptor(context.Background(), &fnpb.GetProcessBundleDescriptorRequest{
		ProcessBundleDescriptorId: "unknown",
	})
	if err == nil {
		t.Errorf("got GetProcessBundleDescriptor(%q) = %v, want error", "unknown", pbd)
	}
}

func serveTestWorker(t *testing.T) (context.Context, *W, *grpc.ClientConn) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)

	w := New("test")
	lis := bufconn.Listen(2048)
	w.lis = lis
	t.Cleanup(func() { w.Stop() })
	go w.Serve()

	clientConn, err := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal("couldn't create bufconn grpc connection:", err)
	}
	return ctx, w, clientConn
}

func TestWorker_Logging(t *testing.T) {
	ctx, _, clientConn := serveTestWorker(t)

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
	ctx, wk, clientConn := serveTestWorker(t)

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

	if !resp.RequiresFinalization {
		t.Errorf("got %v, want response that Requires Finalization", resp)
	}
}

func TestWorker_Data_HappyPath(t *testing.T) {
	ctx, wk, clientConn := serveTestWorker(t)

	dataCli := fnpb.NewBeamFnDataClient(clientConn)
	dataStream, err := dataCli.Data(ctx)
	if err != nil {
		t.Fatal("couldn't create data client:", err)
	}

	instID := wk.NextInst()

	b := &B{
		InstID: instID,
		PBDID:  wk.NextStage(),
		InputData: [][]byte{
			{1, 1, 1, 1, 1, 1},
		},
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
	ctx, wk, clientConn := serveTestWorker(t)

	stateCli := fnpb.NewBeamFnStateClient(clientConn)
	stateStream, err := stateCli.State(ctx)
	if err != nil {
		t.Fatal("couldn't create state client:", err)
	}

	instID := wk.NextInst()
	wk.activeInstructions[instID] = &B{
		IterableSideInputData: map[string]map[string]map[typex.Window][][]byte{
			"transformID": {
				"i1": {
					window.GlobalWindow{}: [][]byte{
						{42},
					},
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
