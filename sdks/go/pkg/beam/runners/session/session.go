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

package session

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness/session"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

const (
	// The maximum length of an encoded varint. We can Peek this much data
	// and find a value to decode.
	peekLen = 9
)

func init() {
	beam.RegisterRunner("session", Execute)
}

var sessionFile = flag.String("session_file", "", "Session file for the runner")

// controlServer manages the FnAPI control channel.
type controlServer struct {
	filename   string
	wg         *sync.WaitGroup // used to signal when the session is completed
	ctrlStream fnpb.BeamFnControl_ControlServer
	dataServer *grpc.Server
	dataStream fnpb.BeamFnData_DataServer
	dwg        *sync.WaitGroup
}

func (c *controlServer) Control(stream fnpb.BeamFnControl_ControlServer) error {
	fmt.Println("Go SDK connected")
	c.ctrlStream = stream
	// We have a connected worker. Start reading the session file and issuing
	// commands.

	c.readSession(c.filename)
	c.wg.Done()
	fmt.Println("session replay complete")
	return nil
}

func (c *controlServer) GetProcessBundleDescriptor(ctx context.Context, r *fnpb.GetProcessBundleDescriptorRequest) (*fnpb.ProcessBundleDescriptor, error) {
	return nil, nil
}

func (c *controlServer) establishDataChannel(beamPort, tcpPort string) {
	if c.dataServer != nil {
		// Already a data server, we're done
		return
	}

	// grpc can allow a grpc service running on two different ports, but there's
	// no way (in Go at least) to differentiate the two of them to identify the
	// source of the incoming data. So we don't even try. Session files that
	// specify data ports have the content rewritten to use the port that
	// the data server is listening on.

	c.dataServer = grpc.NewServer()
	fnpb.RegisterBeamFnDataServer(c.dataServer, &dataServer{ctrl: c})
	dp, err := net.Listen("tcp", tcpPort)
	if err != nil {
		panic(err)
	}
	c.dwg = &sync.WaitGroup{}
	c.dwg.Add(1)
	go c.dataServer.Serve(dp)
}

func (c *controlServer) registerStream(stream fnpb.BeamFnData_DataServer) {
	c.dataStream = stream
	c.dwg.Done()
}

// TODO(wcn): move this code to a session file framework. I imagine this will
// take an additional function argument that performs the handleEntry() work.
func (c *controlServer) readSession(filename string) {
	// Keep the reading simple by ensuring the buffer is large enough
	// to hold any single recorded message. Since grpc has a message
	// cap of 4 megs, we make our buffer larger. Future versions of the
	// header will include this constant, so we can read the header
	// unbuffered, then move to the appropriately sized buffer reader.
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	br := bufio.NewReaderSize(f, 5000000)
	for {
		b, err := br.Peek(peekLen)
		if err != nil && err != io.EOF {
			panic(errors.Wrap(err, "Problem peeking length value"))
		}
		if err == io.EOF {
			break
		}
		l, inc := proto.DecodeVarint(b)
		br.Discard(inc)

		// Read out the entry header message.
		b, err = br.Peek(int(l))
		var hMsg session.EntryHeader
		if err := proto.Unmarshal(b, &hMsg); err != nil {
			panic(errors.Wrap(err, "Error decoding entry header"))
		}
		br.Discard(int(l))

		msgBytes, err := br.Peek(int(hMsg.Len))
		if err != nil {
			panic(errors.Wrap(err, "Couldn't peek message"))
		}

		var bMsg session.Entry
		if err := proto.Unmarshal(msgBytes, &bMsg); err != nil {
			panic(errors.Wrap(err, "Error decoding message"))
		}
		c.handleEntry(&bMsg)
		br.Discard(int(hMsg.Len))
	}
}

func (c *controlServer) handleEntry(msg *session.Entry) {
	/*
		if msg.Kind != session.Entry_LOG_ENTRIES {
			fmt.Printf("handleEntry: %v\n", msg.Kind.String())
		}
	*/
	switch msg.Msg.(type) {
	case *session.Entry_Elems:
		if msg.GetKind() == session.Kind_DATA_RECEIVED {
			c.dwg.Wait()
			c.dataStream.Send(msg.GetElems())
		}
	case *session.Entry_InstResp:
		_, err := c.ctrlStream.Recv()
		if err == io.EOF {
			panic("SDK closed connection but work remaining")
		}

		if err != nil {
			return
		}

	case *session.Entry_InstReq:
		// Look for the register requests and extract the port information.
		ir := msg.GetInstReq()
		c.ctrlStream.Send(ir)

		if rr := ir.GetRegister(); rr != nil {
			for _, desc := range rr.GetProcessBundleDescriptor() {
				for beamPort, t := range desc.GetTransforms() {
					s := t.GetSpec()
					if s.GetUrn() == "beam:runner:source:v1" {
						tcpPort := extractPortSpec(s)
						c.establishDataChannel(beamPort, tcpPort)
					}
					if s.GetUrn() == "beam:runner:sink:v1" {
						tcpPort := extractPortSpec(s)
						c.establishDataChannel(beamPort, tcpPort)
					}
				}
			}
		}
	}
}

func extractPortSpec(spec *pipepb.FunctionSpec) string {
	var port fnpb.RemoteGrpcPort
	if err := proto.Unmarshal(spec.GetPayload(), &port); err != nil {
		panic(err)
	}
	lp := port.ApiServiceDescriptor.Url
	// Leave the colon, so as to match the form net.Listen uses.
	bp := strings.Replace(lp, "localhost", "", 1)
	if bp != lp {
		return bp
	}
	panic("unable to extract port")
}

// dataServer manages the FnAPI data channel.
type dataServer struct {
	ctrl *controlServer
}

func (d *dataServer) Data(stream fnpb.BeamFnData_DataServer) error {
	// This goroutine is only used for reading data. The stream object
	// is passed to the control server so that all data is sent from
	// a single goroutine to ensure proper ordering.

	d.ctrl.registerStream(stream)

	// Consume data messages that are received
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		_ = in
		//log.Printf("Data received: %v", in)
	}
}

// loggingServer manages the FnAPI logging channel.
type loggingServer struct{} // no data content

func (l *loggingServer) Logging(stream fnpb.BeamFnLogging_LoggingServer) error {
	// This stream object is only used here. The stream is used for receiving, and
	// no sends happen on it.
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		for _, e := range in.GetLogEntries() {
			log.Info(stream.Context(), e.GetMessage())
		}
	}
}

// Execute launches the supplied pipeline using a session file as the source of inputs.
func Execute(ctx context.Context, p *beam.Pipeline) error {
	worker, err := buildLocalBinary(ctx)
	if err != nil {
		return errors.WithContext(err, "building worker binary")
	}

	log.Infof(ctx, "built worker binary at %s\n", worker)

	// Start up the grpc logging service.
	ls := grpc.NewServer()
	fnpb.RegisterBeamFnLoggingServer(ls, &loggingServer{})
	logPort, err := net.Listen("tcp", ":0")
	if err != nil {
		panic("No logging port")
	}
	go ls.Serve(logPort)

	// The wait group is used by the control service goroutine to signal
	// completion.
	var wg sync.WaitGroup
	wg.Add(1)

	cs := grpc.NewServer()
	fnpb.RegisterBeamFnControlServer(cs, &controlServer{
		filename: *sessionFile,
		wg:       &wg,
	})

	ctrlPort, err := net.Listen("tcp", ":0")
	if err != nil {
		panic("No control port")
	}
	go cs.Serve(ctrlPort)

	fmt.Println("fake harness initialized")
	cmd := exec.Command(
		worker,
		"--worker",
		fmt.Sprintf("--logging_endpoint=%s", logPort.Addr().String()),
		fmt.Sprintf("--control_endpoint=%s", ctrlPort.Addr().String()),
		"--persist_dir=/tmp/worker")
	go cmd.Start()

	wg.Wait()
	return nil
}

// buildLocalBinary is cribbed from the Dataflow runner, but doesn't force the
// Linux architecture, since the worker runs in the pipeline launch
// environment.
func buildLocalBinary(ctx context.Context) (string, error) {
	ret := filepath.Join(os.TempDir(), fmt.Sprintf("session-runner-%v", time.Now().UnixNano()))

	program := ""
	for i := 3; ; i++ {
		_, file, _, ok := runtime.Caller(i)
		if !ok || strings.HasSuffix(file, "runtime/proc.go") {
			break
		}
		program = file
	}
	if program == "" {
		return "", errors.New("could not detect user main")
	}

	log.Infof(ctx, "Compiling %v as %v", program, ret)

	// Cross-compile given go program. Not awesome.
	build := []string{"go", "build", "-o", ret, program}

	cmd := exec.Command(build[0], build[1:]...)
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Info(ctx, string(out))
		return "", errors.Wrapf(err, "failed to compile %v", program)
	}
	return ret, nil
}
