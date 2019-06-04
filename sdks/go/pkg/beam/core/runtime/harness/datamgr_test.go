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
	"io"
	"io/ioutil"
	"log"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

type fakeClient struct {
	t     *testing.T
	done  chan bool
	calls int
}

func (f *fakeClient) Recv() (*pb.Elements, error) {
	f.calls++
	data := []byte{1, 2, 3, 4}
	elemData := pb.Elements_Data{
		InstructionReference: "inst_ref",
		Data:                 data,
		Target: &pb.Target{
			PrimitiveTransformReference: "ptr",
			Name: "instruction_name",
		},
	}

	msg := pb.Elements{}

	for i := 0; i < bufElements+1; i++ {
		msg.Data = append(msg.Data, &elemData)
	}

	// The first two calls fill up the buffer completely to stimulate the deadlock
	// The third call ends the data stream normally.
	// Subsequent calls return no data.
	switch f.calls {
	case 1:
		return &msg, nil
	case 2:
		return &msg, nil
	case 3:
		elemData.Data = []byte{}
		msg.Data = []*pb.Elements_Data{&elemData}
		// Broadcasting done here means that this code providing messages
		// has not been blocked by the bug blocking the dataReader
		// from getting more messages.
		return &msg, nil
	default:
		f.done <- true
		return nil, io.EOF
	}
}

func (f *fakeClient) Send(*pb.Elements) error {
	return nil
}

func TestDataChannelTerminateOnClose(t *testing.T) {
	// The logging of channels closed is quite noisy for this test
	log.SetOutput(ioutil.Discard)
	done := make(chan bool, 1)
	client := &fakeClient{t: t, done: done}
	c := makeDataChannel(context.Background(), "id", client)

	r := c.OpenRead(context.Background(), exec.Target{ID: "ptr", Name: "instruction_name"}, "inst_ref")
	var read = make([]byte, 4)

	// We don't read up all the buffered data, but immediately close the reader.
	// Previously, since nothing was consuming the incoming gRPC data, the whole
	// data channel would get stuck, and the client.Recv() call was eventually
	// no longer called.
	_, err := r.Read(read)
	if err != nil {
		t.Errorf("Unexpected error from read: %v", err)
	}
	r.Close()

	// If done is signaled, that means client.Recv() has been called to flush the
	// channel, meaning consumer code isn't stuck.
	<-done
}
