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
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness/session"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/proto"
)

const (
	unknown = iota
	instructionRequest
	instructionResponse
	dataReceive
	dataSend
)

var (
	selectedOptions = make(map[string]bool)
	// TODO(wcn): add a buffered writer around capture and use it.
	capture     *os.File
	sessionLock sync.Mutex
	bufPool     = sync.Pool{
		New: func() interface{} {
			return proto.NewBuffer(nil)
		},
	}

	storagePath string
)

// TODO(wcn): the plan is to make these hooks available in the harness in a fashion
// similar to net/http/httptrace. They are simple function calls now to get this
// code underway.
func setupDiagnosticRecording() error {
	// No recording options specified? We're done.
	if runtime.GlobalOptions.Get("cpu_profiling") == "" && runtime.GlobalOptions.Get("session_recording") == "" {
		return nil
	}

	var err error

	storagePath = runtime.GlobalOptions.Get("storage_path")
	// Any form of recording requires the destination directory to exist.
	if err = os.MkdirAll(storagePath, 0755); err != nil {
		return fmt.Errorf("Unable to create session directory: %v", err)
	}

	if !isEnabled("session_recording") {
		return nil
	}

	// Set up the session recorder.
	if capture, err = os.Create(fmt.Sprintf("%s/session-%v", storagePath, time.Now().Unix())); err != nil {
		return fmt.Errorf("Unable to create session file: %v", err)
	}

	return nil
}

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
		return fmt.Errorf("Unable to marshal message for session recording: %v", err)
	}

	eh := &session.EntryHeader{
		Kind: pb.Kind,
		Len:  int64(len(body.Bytes())),
	}

	hdr := bufPool.Get().(*proto.Buffer)
	defer bufPool.Put(hdr)
	if err := hdr.Marshal(eh); err != nil {
		return fmt.Errorf("Unable to marshal message header for session recording: %v", err)
	}

	l := bufPool.Get().(*proto.Buffer)
	defer bufPool.Put(l)
	if err := l.EncodeVarint(uint64(len(hdr.Bytes()))); err != nil {
		return fmt.Errorf("Unable to write entry header length: %v", err)
	}

	// Acquire the lock to write the file.
	sessionLock.Lock()
	defer sessionLock.Unlock()

	if _, err := capture.Write(l.Bytes()); err != nil {
		return fmt.Errorf("Unable to write entry header length: %v", err)
	}
	if _, err := capture.Write(hdr.Bytes()); err != nil {
		return fmt.Errorf("Unable to write entry header: %v", err)
	}
	if _, err := capture.Write(body.Bytes()); err != nil {
		return fmt.Errorf("Unable to write entry body: %v", err)
	}
	return nil
}

func recordInstructionRequest(req *pb.InstructionRequest) error {
	return recordMessage(session.Kind_INSTRUCTION_REQUEST,
		&session.Entry{
			Kind: session.Kind_INSTRUCTION_REQUEST,
			Msg: &session.Entry_InstReq{
				InstReq: req,
			},
		})
}

func recordInstructionResponse(resp *pb.InstructionResponse) error {
	return recordMessage(session.Kind_INSTRUCTION_RESPONSE,
		&session.Entry{
			Kind: session.Kind_INSTRUCTION_RESPONSE,
			Msg: &session.Entry_InstResp{
				InstResp: resp,
			},
		})
}

func recordStreamReceive(data *pb.Elements) error {
	return recordMessage(session.Kind_DATA_RECEIVED,
		&session.Entry{
			Kind: session.Kind_DATA_RECEIVED,
			Msg: &session.Entry_Elems{
				Elems: data,
			},
		})
}

func recordStreamSend(data *pb.Elements) error {
	return recordMessage(session.Kind_DATA_SENT,
		&session.Entry{
			Kind: session.Kind_DATA_SENT,
			Msg: &session.Entry_Elems{
				Elems: data,
			},
		})
}

func recordLogEntries(entries *pb.LogEntry_List) error {
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
					MaxMsgLen: 4000000, // TODO(wcn): get from DataManager.
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
