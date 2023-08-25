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

// Package worker handles interactions with SDK side workers, representing
// the worker services, communicating with those services, and SDK environments.
package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
)

// A W manages worker environments, sending them work
// that they're able to execute, and manages the server
// side handlers for FnAPI RPCs.
type W struct {
	fnpb.UnimplementedBeamFnControlServer
	fnpb.UnimplementedBeamFnDataServer
	fnpb.UnimplementedBeamFnStateServer
	fnpb.UnimplementedBeamFnLoggingServer
	fnpb.UnimplementedProvisionServiceServer

	ID string

	// Server management
	lis    net.Listener
	server *grpc.Server

	// These are the ID sources
	inst, bund uint64

	InstReqs chan *fnpb.InstructionRequest
	DataReqs chan *fnpb.Elements

	mu                 sync.Mutex
	activeInstructions map[string]controlResponder              // Active instructions keyed by InstructionID
	Descriptors        map[string]*fnpb.ProcessBundleDescriptor // Stages keyed by PBDID

	D *DataService
}

type controlResponder interface {
	Respond(*fnpb.InstructionResponse)
}

// New starts the worker server components of FnAPI Execution.
func New(id string) *W {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	var opts []grpc.ServerOption
	wk := &W{
		ID:     id,
		lis:    lis,
		server: grpc.NewServer(opts...),

		InstReqs: make(chan *fnpb.InstructionRequest, 10),
		DataReqs: make(chan *fnpb.Elements, 10),

		activeInstructions: make(map[string]controlResponder),
		Descriptors:        make(map[string]*fnpb.ProcessBundleDescriptor),

		D: &DataService{},
	}
	slog.Debug("Serving Worker components", slog.String("endpoint", wk.Endpoint()))
	fnpb.RegisterBeamFnControlServer(wk.server, wk)
	fnpb.RegisterBeamFnDataServer(wk.server, wk)
	fnpb.RegisterBeamFnLoggingServer(wk.server, wk)
	fnpb.RegisterBeamFnStateServer(wk.server, wk)
	return wk
}

func (wk *W) Endpoint() string {
	return wk.lis.Addr().String()
}

// Serve serves on the started listener. Blocks.
func (wk *W) Serve() {
	wk.server.Serve(wk.lis)
}

func (wk *W) String() string {
	return "worker[" + wk.ID + "]"
}

func (wk *W) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("ID", wk.ID),
		slog.String("endpoint", wk.Endpoint()),
	)
}

// Stop the GRPC server.
func (wk *W) Stop() {
	slog.Debug("stopping", "worker", wk)
	close(wk.InstReqs)
	close(wk.DataReqs)
	wk.server.Stop()
	wk.lis.Close()
	slog.Debug("stopped", "worker", wk)
}

func (wk *W) NextInst() string {
	return fmt.Sprintf("inst%03d", atomic.AddUint64(&wk.inst, 1))
}

func (wk *W) NextStage() string {
	return fmt.Sprintf("stage%03d", atomic.AddUint64(&wk.bund, 1))
}

// TODO set logging level.
var minsev = fnpb.LogEntry_Severity_DEBUG

func (wk *W) GetProvisionInfo(_ context.Context, _ *fnpb.GetProvisionInfoRequest) (*fnpb.GetProvisionInfoResponse, error) {
	endpoint := &pipepb.ApiServiceDescriptor{
		Url: wk.Endpoint(),
	}
	resp := &fnpb.GetProvisionInfoResponse{
		Info: &fnpb.ProvisionInfo{
			// TODO: Add the job's Pipeline options
			// TODO: Include runner capabilities with the per job configuration.
			RunnerCapabilities: []string{
				urns.CapabilityMonitoringInfoShortIDs,
			},
			LoggingEndpoint:  endpoint,
			ControlEndpoint:  endpoint,
			ArtifactEndpoint: endpoint,
			// TODO add this job's RetrievalToken
			// TODO add this job's artifact Dependencies

			Metadata: map[string]string{
				"runner":         "prism",
				"runner_version": core.SdkVersion,
			},
		},
	}
	return resp, nil
}

// Logging relates SDK worker messages back to the job that spawned them.
// Messages are received from the SDK,
func (wk *W) Logging(stream fnpb.BeamFnLogging_LoggingServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			switch status.Code(err) {
			case codes.Canceled:
				return nil
			default:
				slog.Error("logging.Recv", err, "worker", wk)
				return err
			}
		}
		for _, l := range in.GetLogEntries() {
			if l.Severity >= minsev {
				// TODO: Connect to the associated Job for this worker instead of
				// logging locally for SDK side logging.
				file := l.GetLogLocation()
				i := strings.LastIndex(file, ":")
				line, _ := strconv.Atoi(file[i+1:])
				if i > 0 {
					file = file[:i]
				}

				slog.LogAttrs(context.TODO(), toSlogSev(l.GetSeverity()), l.GetMessage(),
					slog.Any(slog.SourceKey, &slog.Source{
						File: file,
						Line: line,
					}),
					slog.Time(slog.TimeKey, l.GetTimestamp().AsTime()),
					slog.Any("worker", wk),
				)
			}
		}
	}
}

func toSlogSev(sev fnpb.LogEntry_Severity_Enum) slog.Level {
	switch sev {
	case fnpb.LogEntry_Severity_TRACE:
		return slog.Level(-8)
	case fnpb.LogEntry_Severity_DEBUG:
		return slog.LevelDebug // -4
	case fnpb.LogEntry_Severity_INFO:
		return slog.LevelInfo // 0
	case fnpb.LogEntry_Severity_NOTICE:
		return slog.Level(2)
	case fnpb.LogEntry_Severity_WARN:
		return slog.LevelWarn // 4
	case fnpb.LogEntry_Severity_ERROR:
		return slog.LevelError // 8
	case fnpb.LogEntry_Severity_CRITICAL:
		return slog.Level(10)
	}
	return slog.LevelInfo
}

func (wk *W) GetProcessBundleDescriptor(ctx context.Context, req *fnpb.GetProcessBundleDescriptorRequest) (*fnpb.ProcessBundleDescriptor, error) {
	desc, ok := wk.Descriptors[req.GetProcessBundleDescriptorId()]
	if !ok {
		return nil, fmt.Errorf("descriptor %v not found", req.GetProcessBundleDescriptorId())
	}
	return desc, nil
}

// Control relays instructions to SDKs and back again, coordinated via unique instructionIDs.
//
// Requests come from the runner, and are sent to the client in the SDK.
func (wk *W) Control(ctrl fnpb.BeamFnControl_ControlServer) error {
	done := make(chan struct{})
	go func() {
		for {
			resp, err := ctrl.Recv()
			if err == io.EOF {
				slog.Debug("ctrl.Recv finished; marking done", "worker", wk)
				done <- struct{}{} // means stream is finished
				return
			}
			if err != nil {
				switch status.Code(err) {
				case codes.Canceled:
					done <- struct{}{} // means stream is finished
					return
				default:
					slog.Error("ctrl.Recv failed", err, "worker", wk)
					panic(err)
				}
			}

			// TODO: Do more than assume these are ProcessBundleResponses.
			wk.mu.Lock()
			if b, ok := wk.activeInstructions[resp.GetInstructionId()]; ok {
				b.Respond(resp)
			} else {
				slog.Debug("ctrl.Recv: %v", resp)
			}
			wk.mu.Unlock()
		}
	}()

	for {
		select {
		case req := <-wk.InstReqs:
			err := ctrl.Send(req)
			if err != nil {
				return err
			}
		case <-ctrl.Context().Done():
			slog.Debug("Control context canceled")
			return ctrl.Context().Err()
		case <-done:
			slog.Debug("Control done")
			return nil
		}
	}
}

// Data relays elements and timer bytes to SDKs and back again, coordinated via
// ProcessBundle instructionIDs, and receiving input transforms.
//
// Data is multiplexed on a single stream for all active bundles on a worker.
func (wk *W) Data(data fnpb.BeamFnData_DataServer) error {
	go func() {
		for {
			resp, err := data.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				switch status.Code(err) {
				case codes.Canceled:
					return
				default:
					slog.Error("data.Recv failed", err, "worker", wk)
					panic(err)
				}
			}
			wk.mu.Lock()
			for _, d := range resp.GetData() {
				cr, ok := wk.activeInstructions[d.GetInstructionId()]
				if !ok {
					slog.Info("data.Recv for unknown bundle", "response", resp)
					continue
				}
				// Received data is always for an active ProcessBundle instruction
				b := cr.(*B)
				colID := b.SinkToPCollection[d.GetTransformId()]

				// There might not be data, eg. for side inputs, so we need to reconcile this elsewhere for
				// downstream side inputs.
				if len(d.GetData()) > 0 {
					b.OutputData.WriteData(colID, d.GetData())
				}
				if d.GetIsLast() {
					b.DataDone()
				}
			}
			wk.mu.Unlock()
		}
	}()
	for {
		select {
		case req, ok := <-wk.DataReqs:
			if !ok {
				return nil
			}
			if err := data.Send(req); err != nil {
				slog.LogAttrs(context.TODO(), slog.LevelDebug, "data.Send error", slog.Any("error", err))
			}
		case <-data.Context().Done():
			slog.Debug("Data context canceled")
			return data.Context().Err()
		}
	}
}

// State relays elements and timer bytes to SDKs and back again, coordinated via
// ProcessBundle instructionIDs, and receiving input transforms.
//
// State requests come from SDKs, and the runner responds.
func (wk *W) State(state fnpb.BeamFnState_StateServer) error {
	responses := make(chan *fnpb.StateResponse)
	go func() {
		// This go routine creates all responses to state requests from the worker
		// so we want to close the State handler when it's all done.
		defer close(responses)
		for {
			req, err := state.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				switch status.Code(err) {
				case codes.Canceled:
					return
				default:
					slog.Error("state.Recv failed", err, "worker", wk)
					panic(err)
				}
			}
			switch req.GetRequest().(type) {
			case *fnpb.StateRequest_Get:
				// TODO: move data handling to be pcollection based.

				// State requests are always for an active ProcessBundle instruction
				wk.mu.Lock()
				b := wk.activeInstructions[req.GetInstructionId()].(*B)
				wk.mu.Unlock()
				key := req.GetStateKey()
				slog.Debug("StateRequest_Get", prototext.Format(req), "bundle", b)

				var data [][]byte
				switch key.GetType().(type) {
				case *fnpb.StateKey_IterableSideInput_:
					ikey := key.GetIterableSideInput()
					wKey := ikey.GetWindow()
					var w typex.Window
					if len(wKey) == 0 {
						w = window.GlobalWindow{}
					} else {
						w, err = exec.MakeWindowDecoder(coder.NewIntervalWindow()).DecodeSingle(bytes.NewBuffer(wKey))
						if err != nil {
							panic(fmt.Sprintf("error decoding iterable side input window key %v: %v", wKey, err))
						}
					}
					winMap := b.IterableSideInputData[ikey.GetTransformId()][ikey.GetSideInputId()]
					var wins []typex.Window
					for w := range winMap {
						wins = append(wins, w)
					}
					slog.Debug(fmt.Sprintf("side input[%v][%v] I Key: %v Windows: %v", req.GetId(), req.GetInstructionId(), w, wins))
					data = winMap[w]

				case *fnpb.StateKey_MultimapSideInput_:
					mmkey := key.GetMultimapSideInput()
					wKey := mmkey.GetWindow()
					var w typex.Window
					if len(wKey) == 0 {
						w = window.GlobalWindow{}
					} else {
						w, err = exec.MakeWindowDecoder(coder.NewIntervalWindow()).DecodeSingle(bytes.NewBuffer(wKey))
						if err != nil {
							panic(fmt.Sprintf("error decoding iterable side input window key %v: %v", wKey, err))
						}
					}
					dKey := mmkey.GetKey()
					winMap := b.MultiMapSideInputData[mmkey.GetTransformId()][mmkey.GetSideInputId()]
					var wins []typex.Window
					for w := range winMap {
						wins = append(wins, w)
					}
					slog.Debug(fmt.Sprintf("side input[%v][%v] MM Key: %v Windows: %v", req.GetId(), req.GetInstructionId(), w, wins))

					data = winMap[w][string(dKey)]

				default:
					panic(fmt.Sprintf("unsupported StateKey Access type: %T: %v", key.GetType(), prototext.Format(key)))
				}

				// Encode the runner iterable (no length, just consecutive elements), and send it out.
				// This is also where we can handle things like State Backed Iterables.
				var buf bytes.Buffer
				for _, value := range data {
					buf.Write(value)
				}
				responses <- &fnpb.StateResponse{
					Id: req.GetId(),
					Response: &fnpb.StateResponse_Get{
						Get: &fnpb.StateGetResponse{
							Data: buf.Bytes(),
						},
					},
				}
			default:
				panic(fmt.Sprintf("unsupported StateRequest kind %T: %v", req.GetRequest(), prototext.Format(req)))
			}
		}
	}()
	for resp := range responses {
		if err := state.Send(resp); err != nil {
			slog.Error("state.Send error", err)
		}
	}
	return nil
}

var chanResponderPool = sync.Pool{
	New: func() any {
		return &chanResponder{make(chan *fnpb.InstructionResponse, 1)}
	},
}

type chanResponder struct {
	Resp chan *fnpb.InstructionResponse
}

func (cr *chanResponder) Respond(resp *fnpb.InstructionResponse) {
	cr.Resp <- resp
}

// sendInstruction is a helper for creating and sending worker single RPCs, blocking
// until the response returns.
func (wk *W) sendInstruction(req *fnpb.InstructionRequest) *fnpb.InstructionResponse {
	cr := chanResponderPool.Get().(*chanResponder)
	progInst := wk.NextInst()
	wk.mu.Lock()
	wk.activeInstructions[progInst] = cr
	wk.mu.Unlock()

	defer func() {
		wk.mu.Lock()
		delete(wk.activeInstructions, progInst)
		wk.mu.Unlock()
		chanResponderPool.Put(cr)
	}()

	req.InstructionId = progInst

	// Tell the SDK to start processing the bundle.
	wk.InstReqs <- req
	// Protos are safe as nil, so just return directly.
	return <-cr.Resp
}

// MonitoringMetadata is a convenience method to request the metadata for monitoring shortIDs.
func (wk *W) MonitoringMetadata(unknownIDs []string) *fnpb.MonitoringInfosMetadataResponse {
	return wk.sendInstruction(&fnpb.InstructionRequest{
		Request: &fnpb.InstructionRequest_MonitoringInfos{
			MonitoringInfos: &fnpb.MonitoringInfosMetadataRequest{
				MonitoringInfoId: unknownIDs,
			},
		},
	}).GetMonitoringInfos()
}

// DataService is slated to be deleted in favour of stage based state
// management for side inputs.
type DataService struct {
	mu sync.Mutex
	// TODO actually quick process the data to windows here as well.
	raw map[string][][]byte
}

// Commit tentative data to the datastore.
func (d *DataService) Commit(tent engine.TentativeData) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.raw == nil {
		d.raw = map[string][][]byte{}
	}
	for colID, data := range tent.Raw {
		d.raw[colID] = append(d.raw[colID], data...)
	}
}

// GetAllData is a hack for Side Inputs until watermarks are sorted out.
func (d *DataService) GetAllData(colID string) [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.raw[colID]
}
