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
	"log/slog"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/structpb"
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

	ID, Env string

	JobKey, ArtifactEndpoint string
	EnvPb                    *pipepb.Environment
	PipelineOptions          *structpb.Struct

	// Server management
	lis    net.Listener
	server *grpc.Server

	// These are the ID sources
	inst               uint64
	connected, stopped atomic.Bool

	InstReqs chan *fnpb.InstructionRequest
	DataReqs chan *fnpb.Elements

	mu                 sync.Mutex
	activeInstructions map[string]controlResponder              // Active instructions keyed by InstructionID
	Descriptors        map[string]*fnpb.ProcessBundleDescriptor // Stages keyed by PBDID
}

type controlResponder interface {
	Respond(*fnpb.InstructionResponse)
}

// New starts the worker server components of FnAPI Execution.
func New(id, env string) *W {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(math.MaxInt32),
	}
	wk := &W{
		ID:     id,
		Env:    env,
		lis:    lis,
		server: grpc.NewServer(opts...),

		InstReqs: make(chan *fnpb.InstructionRequest, 10),
		DataReqs: make(chan *fnpb.Elements, 10),

		activeInstructions: make(map[string]controlResponder),
		Descriptors:        make(map[string]*fnpb.ProcessBundleDescriptor),
	}
	slog.Debug("Serving Worker components", slog.String("endpoint", wk.Endpoint()))
	fnpb.RegisterBeamFnControlServer(wk.server, wk)
	fnpb.RegisterBeamFnDataServer(wk.server, wk)
	fnpb.RegisterBeamFnLoggingServer(wk.server, wk)
	fnpb.RegisterBeamFnStateServer(wk.server, wk)
	fnpb.RegisterProvisionServiceServer(wk.server, wk)
	return wk
}

func (wk *W) Endpoint() string {
	_, port, _ := net.SplitHostPort(wk.lis.Addr().String())
	return fmt.Sprintf("localhost:%v", port)
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
	wk.stopped.Store(true)
	close(wk.InstReqs)
	close(wk.DataReqs)

	// Give the SDK side 5 seconds to gracefully stop, before
	// hard stopping all RPCs.
	tim := time.AfterFunc(5*time.Second, func() {
		wk.server.Stop()
	})
	wk.server.GracefulStop()
	tim.Stop()

	wk.lis.Close()
	slog.Debug("stopped", "worker", wk)
}

func (wk *W) NextInst() string {
	return fmt.Sprintf("inst-%v-%03d", wk.Env, atomic.AddUint64(&wk.inst, 1))
}

// TODO set logging level.
var minsev = fnpb.LogEntry_Severity_DEBUG

func (wk *W) GetProvisionInfo(_ context.Context, _ *fnpb.GetProvisionInfoRequest) (*fnpb.GetProvisionInfoResponse, error) {
	endpoint := &pipepb.ApiServiceDescriptor{
		Url: wk.Endpoint(),
	}
	resp := &fnpb.GetProvisionInfoResponse{
		Info: &fnpb.ProvisionInfo{
			// TODO: Include runner capabilities with the per job configuration.
			RunnerCapabilities: []string{
				urns.CapabilityMonitoringInfoShortIDs,
			},
			LoggingEndpoint: endpoint,
			ControlEndpoint: endpoint,
			ArtifactEndpoint: &pipepb.ApiServiceDescriptor{
				Url: wk.ArtifactEndpoint,
			},

			RetrievalToken:  wk.JobKey,
			Dependencies:    wk.EnvPb.GetDependencies(),
			PipelineOptions: wk.PipelineOptions,

			Metadata: map[string]string{
				"runner":         "prism",
				"runner_version": core.SdkVersion,
				"variant":        "test",
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
				slog.Error("logging.Recv", slog.Any("error", err), slog.Any("worker", wk))
				return err
			}
		}
		for _, l := range in.GetLogEntries() {
			// TODO base this on a per pipeline logging setting.
			if l.Severity < minsev {
				continue
			}

			// Ideally we'd be writing these to per-pipeline files, but for now re-log them on the Prism process.
			// We indicate they're from the SDK, and which worker, keeping the same log severity.
			// SDK specific and worker specific fields are in separate groups for legibility.

			attrs := []any{
				slog.String("transformID", l.GetTransformId()), // TODO: pull the unique name from the pipeline graph.
				slog.String("location", l.GetLogLocation()),
				slog.Time(slog.TimeKey, l.GetTimestamp().AsTime()),
				slog.String(slog.MessageKey, l.GetMessage()),
			}
			if fs := l.GetCustomData().GetFields(); len(fs) > 0 {
				var grp []any
				for n, v := range l.GetCustomData().GetFields() {
					var attr slog.Attr
					switch v.Kind.(type) {
					case *structpb.Value_BoolValue:
						attr = slog.Bool(n, v.GetBoolValue())
					case *structpb.Value_NumberValue:
						attr = slog.Float64(n, v.GetNumberValue())
					case *structpb.Value_StringValue:
						attr = slog.String(n, v.GetStringValue())
					default:
						attr = slog.Any(n, v.AsInterface())
					}
					grp = append(grp, attr)
				}
				attrs = append(attrs, slog.Group("customData", grp...))
			}

			slog.LogAttrs(stream.Context(), toSlogSev(l.GetSeverity()), "log from SDK worker", slog.Any("worker", wk), slog.Group("sdk", attrs...))
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

// Connected indicates whether the worker has connected to the control RPC.
func (wk *W) Connected() bool {
	return wk.connected.Load()
}

// Stopped indicates that the worker has stopped.
func (wk *W) Stopped() bool {
	return wk.stopped.Load()
}

// Control relays instructions to SDKs and back again, coordinated via unique instructionIDs.
//
// Requests come from the runner, and are sent to the client in the SDK.
func (wk *W) Control(ctrl fnpb.BeamFnControl_ControlServer) error {
	wk.connected.Store(true)
	done := make(chan error, 1)
	go func() {
		for {
			resp, err := ctrl.Recv()
			if err == io.EOF {
				slog.Debug("ctrl.Recv finished; marking done", "worker", wk)
				done <- nil // means stream is finished
				return
			}
			if err != nil {
				switch status.Code(err) {
				case codes.Canceled:
					done <- err // means stream is finished
					return
				default:
					slog.Error("ctrl.Recv failed", "error", err, "worker", wk)
					panic(err)
				}
			}

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
		case req, ok := <-wk.InstReqs:
			if !ok {
				slog.Debug("Worker shutting down.", "worker", wk)
				return nil
			}
			if err := ctrl.Send(req); err != nil {
				return err
			}
		case <-ctrl.Context().Done():
			wk.mu.Lock()
			// Fail extant instructions
			slog.Debug("SDK Disconnected", "worker", wk, "ctx_error", ctrl.Context().Err(), "outstanding_instructions", len(wk.activeInstructions))

			msg := fmt.Sprintf("SDK worker disconnected: %v, %v active instructions", wk.String(), len(wk.activeInstructions))
			for instID, b := range wk.activeInstructions {
				b.Respond(&fnpb.InstructionResponse{
					InstructionId: instID,
					Error:         msg,
				})
			}
			wk.mu.Unlock()
			return context.Cause(ctrl.Context())
		case err := <-done:
			if err != nil {
				slog.Warn("Control done", "error", err, "worker", wk)
			} else {
				slog.Debug("Control done", "worker", wk)
			}
			return err
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
					slog.Info("data.Recv data for unknown bundle", "response", resp)
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
					b.DataOrTimerDone()
				}
			}
			for _, t := range resp.GetTimers() {
				cr, ok := wk.activeInstructions[t.GetInstructionId()]
				if !ok {
					slog.Info("data.Recv timers for unknown bundle", "response", resp)
					continue
				}
				// Received data is always for an active ProcessBundle instruction
				b := cr.(*B)

				if len(t.GetTimers()) > 0 {
					b.OutputData.WriteTimers(t.GetTransformId(), t.GetTimerFamilyId(), t.GetTimers())
				}
				if t.GetIsLast() {
					b.DataOrTimerDone()
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
			return context.Cause(data.Context())
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

			// State requests are always for an active ProcessBundle instruction
			wk.mu.Lock()
			b, ok := wk.activeInstructions[req.GetInstructionId()].(*B)
			wk.mu.Unlock()
			if !ok {
				slog.Warn("state request after bundle inactive", "instruction", req.GetInstructionId(), "worker", wk)
				continue
			}
			switch req.GetRequest().(type) {
			case *fnpb.StateRequest_Get:
				// TODO: move data handling to be pcollection based.

				key := req.GetStateKey()
				slog.Debug("StateRequest_Get", "request", prototext.Format(req), "bundle", b)
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
					winMap := b.IterableSideInputData[SideInputKey{TransformID: ikey.GetTransformId(), Local: ikey.GetSideInputId()}]

					var wins []typex.Window
					for w := range winMap {
						wins = append(wins, w)
					}
					slog.Debug(fmt.Sprintf("side input[%v][%v] I Key: %v Windows: %v", req.GetId(), req.GetInstructionId(), w, wins))

					data = winMap[w]

				case *fnpb.StateKey_MultimapKeysSideInput_:
					mmkey := key.GetMultimapKeysSideInput()
					wKey := mmkey.GetWindow()
					var w typex.Window = window.GlobalWindow{}
					if len(wKey) > 0 {
						w, err = exec.MakeWindowDecoder(coder.NewIntervalWindow()).DecodeSingle(bytes.NewBuffer(wKey))
						if err != nil {
							panic(fmt.Sprintf("error decoding multimap side input window key %v: %v", wKey, err))
						}
					}
					winMap := b.MultiMapSideInputData[SideInputKey{TransformID: mmkey.GetTransformId(), Local: mmkey.GetSideInputId()}]
					for k := range winMap[w] {
						data = append(data, []byte(k))
					}

				case *fnpb.StateKey_MultimapSideInput_:
					mmkey := key.GetMultimapSideInput()
					wKey := mmkey.GetWindow()
					var w typex.Window
					if len(wKey) == 0 {
						w = window.GlobalWindow{}
					} else {
						w, err = exec.MakeWindowDecoder(coder.NewIntervalWindow()).DecodeSingle(bytes.NewBuffer(wKey))
						if err != nil {
							panic(fmt.Sprintf("error decoding multimap side input window key %v: %v", wKey, err))
						}
					}
					dKey := mmkey.GetKey()
					winMap := b.MultiMapSideInputData[SideInputKey{TransformID: mmkey.GetTransformId(), Local: mmkey.GetSideInputId()}]

					slog.Debug(fmt.Sprintf("side input[%v][%v] MultiMap Window: %v", req.GetId(), req.GetInstructionId(), w))

					data = winMap[w][string(dKey)]

				case *fnpb.StateKey_BagUserState_:
					bagkey := key.GetBagUserState()
					data = b.OutputData.GetBagState(engine.LinkID{Transform: bagkey.GetTransformId(), Local: bagkey.GetUserStateId()}, bagkey.GetWindow(), bagkey.GetKey())
				case *fnpb.StateKey_MultimapUserState_:
					mmkey := key.GetMultimapUserState()
					data = b.OutputData.GetMultimapState(engine.LinkID{Transform: mmkey.GetTransformId(), Local: mmkey.GetUserStateId()}, mmkey.GetWindow(), mmkey.GetKey(), mmkey.GetMapKey())
				case *fnpb.StateKey_MultimapKeysUserState_:
					mmkey := key.GetMultimapKeysUserState()
					data = b.OutputData.GetMultimapKeysState(engine.LinkID{Transform: mmkey.GetTransformId(), Local: mmkey.GetUserStateId()}, mmkey.GetWindow(), mmkey.GetKey())
				default:
					panic(fmt.Sprintf("unsupported StateKey Get type: %T: %v", key.GetType(), prototext.Format(key)))
				}

				// Encode the runner iterable (no length, just consecutive elements), and send it out.
				// This is also where we can handle things like State Backed Iterables.
				responses <- &fnpb.StateResponse{
					Id: req.GetId(),
					Response: &fnpb.StateResponse_Get{
						Get: &fnpb.StateGetResponse{
							Data: bytes.Join(data, []byte{}),
						},
					},
				}

			case *fnpb.StateRequest_Append:
				key := req.GetStateKey()
				switch key.GetType().(type) {
				case *fnpb.StateKey_BagUserState_:
					bagkey := key.GetBagUserState()
					b.OutputData.AppendBagState(engine.LinkID{Transform: bagkey.GetTransformId(), Local: bagkey.GetUserStateId()}, bagkey.GetWindow(), bagkey.GetKey(), req.GetAppend().GetData())
				case *fnpb.StateKey_MultimapUserState_:
					mmkey := key.GetMultimapUserState()
					b.OutputData.AppendMultimapState(engine.LinkID{Transform: mmkey.GetTransformId(), Local: mmkey.GetUserStateId()}, mmkey.GetWindow(), mmkey.GetKey(), mmkey.GetMapKey(), req.GetAppend().GetData())
				default:
					panic(fmt.Sprintf("unsupported StateKey Append type: %T: %v", key.GetType(), prototext.Format(key)))
				}

				responses <- &fnpb.StateResponse{
					Id: req.GetId(),
					Response: &fnpb.StateResponse_Append{
						Append: &fnpb.StateAppendResponse{},
					},
				}

			case *fnpb.StateRequest_Clear:
				key := req.GetStateKey()
				switch key.GetType().(type) {
				case *fnpb.StateKey_BagUserState_:
					bagkey := key.GetBagUserState()
					b.OutputData.ClearBagState(engine.LinkID{Transform: bagkey.GetTransformId(), Local: bagkey.GetUserStateId()}, bagkey.GetWindow(), bagkey.GetKey())
				case *fnpb.StateKey_MultimapUserState_:
					mmkey := key.GetMultimapUserState()
					b.OutputData.ClearMultimapState(engine.LinkID{Transform: mmkey.GetTransformId(), Local: mmkey.GetUserStateId()}, mmkey.GetWindow(), mmkey.GetKey(), mmkey.GetMapKey())
				case *fnpb.StateKey_MultimapKeysUserState_:
					mmkey := key.GetMultimapUserState()
					b.OutputData.ClearMultimapKeysState(engine.LinkID{Transform: mmkey.GetTransformId(), Local: mmkey.GetUserStateId()}, mmkey.GetWindow(), mmkey.GetKey())
				default:
					panic(fmt.Sprintf("unsupported StateKey Clear type: %T: %v", key.GetType(), prototext.Format(key)))
				}
				responses <- &fnpb.StateResponse{
					Id: req.GetId(),
					Response: &fnpb.StateResponse_Clear{
						Clear: &fnpb.StateClearResponse{},
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
func (wk *W) sendInstruction(ctx context.Context, req *fnpb.InstructionRequest) *fnpb.InstructionResponse {
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

	if wk.Stopped() {
		return nil
	}
	wk.InstReqs <- req

	select {
	case <-ctx.Done():
		return &fnpb.InstructionResponse{
			InstructionId: progInst,
			Error:         "context canceled before receive",
		}
	case resp := <-cr.Resp:
		// Protos are safe as nil, so just return directly.
		return resp
	}
}

// MonitoringMetadata is a convenience method to request the metadata for monitoring shortIDs.
func (wk *W) MonitoringMetadata(ctx context.Context, unknownIDs []string) *fnpb.MonitoringInfosMetadataResponse {
	return wk.sendInstruction(ctx, &fnpb.InstructionRequest{
		Request: &fnpb.InstructionRequest_MonitoringInfos{
			MonitoringInfos: &fnpb.MonitoringInfosMetadataRequest{
				MonitoringInfoId: unknownIDs,
			},
		},
	}).GetMonitoringInfos()
}
