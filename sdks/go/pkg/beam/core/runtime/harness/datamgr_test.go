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
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

const extraData = 2

// fakeDataClient attempts to mimic the semantics of a GRPC stream
// and also permit configurability.
type fakeDataClient struct {
	t              *testing.T
	done           chan bool
	calls          int
	err            error
	skipFirstError bool

	blocked sync.Mutex // Prevent data from being read by the gotourtinr.
}

func (f *fakeDataClient) Recv() (*fnpb.Elements, error) {
	f.blocked.Lock()
	defer f.blocked.Unlock()

	f.calls++
	data := []byte{1, 2, 3, 4, 1, 2, 3, 4}
	elemData := fnpb.Elements_Data{
		InstructionId: "inst_ref",
		Data:          data,
		TransformId:   "ptr",
	}

	msg := fnpb.Elements{}

	// Send extraData more than the number of elements buffered in the channel.
	for i := 0; i < bufElements+extraData; i++ {
		msg.Data = append(msg.Data, &elemData)
	}

	// The first two calls fill up the buffer completely to stimulate the deadlock
	// The third call ends the data stream normally.
	// Subsequent calls return no data.
	switch f.calls {
	case 1:
		return &msg, f.err
	case 2:
		return &msg, f.err
	case 3:
		elemData.Data = []byte{}
		msg.Data = []*fnpb.Elements_Data{&elemData}
		// Broadcasting done here means that this code providing messages
		// has not been blocked by the bug blocking the dataReader
		// from getting more messages.
		return &msg, f.err
	default:
		f.done <- true
		return nil, io.EOF
	}
}

func (f *fakeDataClient) Send(*fnpb.Elements) error {
	// We skip errors on the first call to test that  errors can be returned
	// on the sentinel value send in dataWriter.Close
	// Otherwise, we return an io.EOF similar to semantics documented
	// in https://godoc.org/google.golang.org/grpc#ClientConn.NewStream
	if f.skipFirstError && f.err != nil {
		f.skipFirstError = false
		return nil
	} else if f.err != nil {
		return io.EOF
	}
	return nil
}

func TestDataChannelTerminate_dataReader(t *testing.T) {
	// The logging of channels closed is quite noisy for this test
	log.SetOutput(ioutil.Discard)

	expectedError := fmt.Errorf("EXPECTED ERROR")

	tests := []struct {
		name          string
		expectedError error
		caseFn        func(t *testing.T, r io.ReadCloser, client *fakeDataClient, c *DataChannel)
	}{
		{
			name:          "onClose",
			expectedError: io.EOF,
			caseFn: func(t *testing.T, r io.ReadCloser, client *fakeDataClient, c *DataChannel) {
				// We don't read up all the buffered data, but immediately close the reader.
				// Previously, since nothing was consuming the incoming gRPC data, the whole
				// data channel would get stuck, and the client.Recv() call was eventually
				// no longer called.
				r.Close()

				// If done is signaled, that means client.Recv() has been called to flush the
				// channel, meaning consumer code isn't stuck.
				<-client.done
			},
		}, {
			name:          "onSentinel",
			expectedError: io.EOF,
			caseFn: func(t *testing.T, r io.ReadCloser, client *fakeDataClient, c *DataChannel) {
				// fakeDataClient eventually returns a sentinel element.
			},
		}, {
			name:          "onRecvError",
			expectedError: expectedError,
			caseFn: func(t *testing.T, r io.ReadCloser, client *fakeDataClient, c *DataChannel) {
				// The SDK starts reading in a goroutine immeadiately after open.
				// Set the 2nd Recv call to have an error.
				client.err = expectedError
			},
		}, {
			name:          "onInstructionEnd",
			expectedError: io.EOF,
			caseFn: func(t *testing.T, r io.ReadCloser, client *fakeDataClient, c *DataChannel) {
				c.removeInstruction("inst_ref")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			done := make(chan bool, 1)
			client := &fakeDataClient{t: t, done: done}
			ctx, cancelFn := context.WithCancel(context.Background())
			c := makeDataChannel(ctx, "id", client, cancelFn)

			r := c.OpenRead(ctx, "ptr", "inst_ref")

			n, err := r.Read(make([]byte, 4))
			if err != nil {
				t.Errorf("Unexpected error from read: %v, read %d bytes.", err, n)
			}
			test.caseFn(t, r, client, c)
			// Drain the reader.
			i := 1 // For the earlier Read.
			for err == nil {
				read := make([]byte, 4)
				_, err = r.Read(read)
				i++
			}

			if got, want := err, test.expectedError; got != want {
				t.Errorf("Unexpected error from read %d: got %v, want %v", i, got, want)
			}
			// Verify that new readers return the same error on their reads after client.Recv is done.
			if n, err := c.OpenRead(ctx, "ptr", "inst_ref").Read(make([]byte, 4)); err != test.expectedError {
				t.Errorf("Unexpected error from read: got %v, want, %v read %d bytes.", err, test.expectedError, n)
			}

			select {
			case <-ctx.Done(): // Assert that the context must have been cancelled on read failures.
				return
			case <-time.After(time.Second * 5):
				t.Fatal("context wasn't cancelled")
			}
		})
	}
}

func TestDataChannelRemoveInstruction_dataAfterClose(t *testing.T) {
	done := make(chan bool, 1)
	client := &fakeDataClient{t: t, done: done}
	client.blocked.Lock()

	ctx, cancelFn := context.WithCancel(context.Background())
	c := makeDataChannel(ctx, "id", client, cancelFn)
	c.removeInstruction("inst_ref")

	client.blocked.Unlock()

	r := c.OpenRead(ctx, "ptr", "inst_ref")

	dr := r.(*dataReader)
	if !dr.completed || dr.err != io.EOF {
		t.Errorf("Expected a closed reader, but was still open: completed: %v, err: %v", dr.completed, dr.err)
	}

	n, err := r.Read(make([]byte, 4))
	if err != io.EOF {
		t.Errorf("Unexpected error from read: %v, read %d bytes.", err, n)
	}
}

func TestDataChannelRemoveInstruction_limitInstructionCap(t *testing.T) {
	done := make(chan bool, 1)
	client := &fakeDataClient{t: t, done: done}
	ctx, cancelFn := context.WithCancel(context.Background())
	c := makeDataChannel(ctx, "id", client, cancelFn)

	for i := 0; i < endedInstructionCap+10; i++ {
		instID := instructionID(fmt.Sprintf("inst_ref%d", i))
		c.OpenRead(ctx, "ptr", instID)
		c.removeInstruction(instID)
	}
	if got, want := len(c.endedInstructions), endedInstructionCap; got != want {
		t.Errorf("unexpected len(endedInstructions) got %v, want %v,", got, want)
	}
}

func TestDataChannelTerminate_Writes(t *testing.T) {
	// The logging of channels closed is quite noisy for this test
	log.SetOutput(ioutil.Discard)

	expectedError := fmt.Errorf("EXPECTED ERROR")

	instID := instructionID("inst_ref")
	tests := []struct {
		name   string
		caseFn func(t *testing.T, w io.WriteCloser, client *fakeDataClient, c *DataChannel) error
	}{
		{
			name: "onClose_Flush",
			caseFn: func(t *testing.T, w io.WriteCloser, client *fakeDataClient, c *DataChannel) error {
				return w.Close()
			},
		}, {
			name: "onClose_Sentinel",
			caseFn: func(t *testing.T, w io.WriteCloser, client *fakeDataClient, c *DataChannel) error {
				client.skipFirstError = true
				return w.Close()
			},
		}, {
			name: "onWrite",
			caseFn: func(t *testing.T, w io.WriteCloser, client *fakeDataClient, c *DataChannel) error {
				_, err := w.Write([]byte{'d', 'o', 'n', 'e'})
				return err
			},
		}, {
			name: "onInstructionEnd",
			caseFn: func(t *testing.T, w io.WriteCloser, client *fakeDataClient, c *DataChannel) error {
				c.removeInstruction(instID)
				return expectedError
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			done := make(chan bool, 1)
			client := &fakeDataClient{t: t, done: done, err: expectedError}
			ctx, cancelFn := context.WithCancel(context.Background())
			c := makeDataChannel(ctx, "id", client, cancelFn)

			w := c.OpenWrite(ctx, "ptr", instID)

			msg := []byte{'b', 'y', 't', 'e'}
			var bufSize int
			for bufSize+len(msg) <= chunkSize {
				bufSize += len(msg)
				n, err := w.Write(msg)
				if err != nil {
					t.Errorf("Unexpected error from write: %v, wrote %d bytes, after %d total bytes", err, n, bufSize)
				}
			}

			err := test.caseFn(t, w, client, c)

			if got, want := err, expectedError; err == nil || !strings.Contains(err.Error(), expectedError.Error()) {
				t.Errorf("Unexpected error: got %v, want %v", got, want)
			}
			// Verify that new readers return the same error for writes after stream termination.
			// TODO(lostluck) 2019.11.26: use the the go 1.13 errors package to check this rather
			// than a strings.Contains check once testing infrastructure can use go 1.13.
			if n, err := c.OpenWrite(ctx, "ptr", instID).Write(msg); err != nil && !strings.Contains(err.Error(), expectedError.Error()) {
				t.Errorf("Unexpected error from write: got %v, want, %v read %d bytes.", err, expectedError, n)
			}
			select {
			case <-ctx.Done(): // Assert that the context must have been cancelled on write failures.
				return
			case <-time.After(time.Second * 5):
				t.Fatal("context wasn't cancelled")
			}
		})
	}
}
