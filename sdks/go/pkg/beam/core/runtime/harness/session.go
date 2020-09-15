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

package harness

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness/session"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/proto"
)

// capture is set by the capture hook below.
var capture io.WriteCloser

var (
	selectedOptions = make(map[string]bool)
	sessionLock     sync.Mutex
	bufPool         = sync.Pool{
		New: func() interface{} {
			return proto.NewBuffer(nil)
		},
	}

	storagePath string
)

func isEnabled(option string) bool {
	return runtime.GlobalOptions.Get(option) == "true"
}

func recordMessage(opcode session.Kind, pb *session.Entry) error {
	if !isEnabled("session_recording") {
		return nil
	}

	// This is called inline with the message handling code.
	// It'd be nicer to be a bit more well-behaved and not block the main thread
	// of execution. However, this is for recording profiles, and shouldn't be called
	// when measuring performance, so maybe this perf hit isn't a big deal.

	// The format of the file is a sequence of message elements. Each element consists of
	// three parts.
	// 1) Varint encoded length of the EntryHeader
	// 2) Encoded EntryHeader message. This is a lightweight message designed to contain
	//    enough information for a consumer to determine whether the Entry is of interest
	//    which allows optionally skipping that expensive decode.
	// 3) Encoded Entry message.

	body := bufPool.Get().(*proto.Buffer)
	defer bufPool.Put(body)
	if err := body.Marshal(pb); err != nil {
		return errors.Wrap(err, "unable to marshal message for session recording")
	}

	eh := &session.EntryHeader{
		Kind: pb.Kind,
		Len:  int64(len(body.Bytes())),
	}

	hdr := bufPool.Get().(*proto.Buffer)
	defer bufPool.Put(hdr)
	if err := hdr.Marshal(eh); err != nil {
		return errors.Wrap(err, "unable to marshal message header for session recording")
	}

	l := bufPool.Get().(*proto.Buffer)
	defer bufPool.Put(l)
	if err := l.EncodeVarint(uint64(len(hdr.Bytes()))); err != nil {
		return errors.Wrap(err, "unable to write entry header length")
	}

	// Acquire the lock to write the file.
	sessionLock.Lock()
	defer sessionLock.Unlock()

	if _, err := capture.Write(l.Bytes()); err != nil {
		return errors.Wrap(err, "unable to write entry header length")
	}
	if _, err := capture.Write(hdr.Bytes()); err != nil {
		return errors.Wrap(err, "unable to write entry header")
	}
	if _, err := capture.Write(body.Bytes()); err != nil {
		return errors.Wrap(err, "unable to write entry body")
	}
	return nil
}

func recordInstructionRequest(req *fnpb.InstructionRequest) error {
	return recordMessage(session.Kind_INSTRUCTION_REQUEST,
		&session.Entry{
			Kind: session.Kind_INSTRUCTION_REQUEST,
			Msg: &session.Entry_InstReq{
				InstReq: req,
			},
		})
}

func recordInstructionResponse(resp *fnpb.InstructionResponse) error {
	return recordMessage(session.Kind_INSTRUCTION_RESPONSE,
		&session.Entry{
			Kind: session.Kind_INSTRUCTION_RESPONSE,
			Msg: &session.Entry_InstResp{
				InstResp: resp,
			},
		})
}

func recordStreamReceive(data *fnpb.Elements) error {
	return recordMessage(session.Kind_DATA_RECEIVED,
		&session.Entry{
			Kind: session.Kind_DATA_RECEIVED,
			Msg: &session.Entry_Elems{
				Elems: data,
			},
		})
}

func recordStreamSend(data *fnpb.Elements) error {
	return recordMessage(session.Kind_DATA_SENT,
		&session.Entry{
			Kind: session.Kind_DATA_SENT,
			Msg: &session.Entry_Elems{
				Elems: data,
			},
		})
}

func recordLogEntries(entries *fnpb.LogEntry_List) error {
	return recordMessage(session.Kind_LOG_ENTRIES,
		&session.Entry{
			Kind: session.Kind_LOG_ENTRIES,
			Msg: &session.Entry_LogEntries{
				LogEntries: entries,
			},
		})

}

func recordHeader() error {
	return recordMessage(session.Kind_HEADER,
		&session.Entry{
			Kind: session.Kind_HEADER,
			Msg: &session.Entry_Header{
				Header: &session.Header{
					Version:   "0.0.1",
					MaxMsgLen: 4000000, // TODO(wcn): get from DataChannelManager.
				},
			},
		})
}

// TODO(wcn): footer is designed to be the last thing recorded in the log. However,
// there's currently no coordination with the logging channel, so this isn't true.
func recordFooter() error {
	return recordMessage(session.Kind_FOOTER, &session.Entry{
		Kind: session.Kind_FOOTER,
		Msg: &session.Entry_Footer{
			Footer: &session.Footer{},
		},
	})
}

// CaptureHook writes the messaging content consumed and
// produced by the worker, allowing the data to be used as
// an input for the session runner. Since workers can exist
// in a variety of environments, this allows the runner
// to tailor the behavior best for its particular needs.
type CaptureHook io.WriteCloser

// CaptureHookFactory produces a CaptureHook from the supplied
// options.
type CaptureHookFactory func([]string) CaptureHook

var captureHookRegistry = make(map[string]CaptureHookFactory)

func init() {
	hf := func(opts []string) hooks.Hook {
		return hooks.Hook{
			Init: func(ctx context.Context) (context.Context, error) {
				if len(opts) > 0 {
					name, opts := hooks.Decode(opts[0])
					capture = captureHookRegistry[name](opts)
				}
				return ctx, nil
			},
		}
	}

	hooks.RegisterHook("session", hf)
}

// RegisterCaptureHook registers a CaptureHookFactory for the
// supplied identifier.
func RegisterCaptureHook(name string, c CaptureHookFactory) {
	if _, exists := captureHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterSessionCaptureHook: %s registered twice", name))
	}
	captureHookRegistry[name] = c
}

// EnableCaptureHook is called to request the use of a hook in a pipeline.
// It updates the supplied pipelines to capture this request.
func EnableCaptureHook(name string, opts []string) {
	if _, exists := captureHookRegistry[name]; !exists {
		panic(fmt.Sprintf("EnableHook: %s not registered", name))
	}
	if exists, opts := hooks.IsEnabled("session"); exists {
		n, _ := hooks.Decode(opts[0])
		if n != name {
			panic(fmt.Sprintf("EnableHook: can't enable hook %s, hook %s already enabled", name, n))
		}
	}

	hooks.EnableHook("session", hooks.Encode(name, opts))
}
