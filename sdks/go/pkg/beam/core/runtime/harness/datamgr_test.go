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
	"testing"

	pb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

const extraData = 2

type fakeClient struct {
	t     *testing.T
	done  chan bool
	calls int
	err   error
}

func (f *fakeClient) Recv() (*pb.Elements, error) {
	f.calls++
	data := []byte{1, 2, 3, 4}
	elemData := pb.Elements_Data{
		InstructionId: "inst_ref",
		Data:          data,
		TransformId:   "ptr",
	}

	msg := pb.Elements{}

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
		msg.Data = []*pb.Elements_Data{&elemData}
		// Broadcasting done here means that this code providing messages
		// has not been blocked by the bug blocking the dataReader
		// from getting more messages.
		return &msg, f.err
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

	tests := []struct {
		name   string
		caseFn func(t *testing.T, r io.ReadCloser, client *fakeClient, c *DataChannel)
	}{
		{
			name: "onClose",
			caseFn: func(t *testing.T, r io.ReadCloser, client *fakeClient, c *DataChannel) {
				// We don't read up all the buffered data, but immediately close the reader.
				// Previously, since nothing was consuming the incoming gRPC data, the whole
				// data channel would get stuck, and the client.Recv() call was eventually
				// no longer called.
				r.Close()

				// If done is signaled, that means client.Recv() has been called to flush the
				// channel, meaning consumer code isn't stuck.
				<-client.done
				i := 1 // For the earlier Read.
				for {
					read := make([]byte, 4)
					if n, err := r.Read(read); err != io.EOF {
						i++
						// There are bufElements+extraData available from the fakeClient, so drain the channel
						// before marking a test failure.
						if i > bufElements+extraData {
							t.Errorf("Unexpected error from read: %v, read %d bytes, %v", err, n, read)
						}
						continue
					}
					break
				}
				// This Test can hang on failure.
			},
		}, {
			name: "onRecvError",
			caseFn: func(t *testing.T, r io.ReadCloser, client *fakeClient, c *DataChannel) {
				// Set the 3rd Recv call to have an error.
				// The SDK starts reading in a goroutine immeadiately after open.
				expectedError := fmt.Errorf("EXPECTED ERROR")
				client.err = expectedError
				t.Log("ERROR SET")

				i := 1 // For the earlier Read.
				for {
					read := make([]byte, 4)
					i++
					if n, err := r.Read(read); err != expectedError {
						// There are bufElements+extraData available from the fakeClient, so drain the channel
						// before marking a test failure.
						if i > bufElements+extraData {
							t.Errorf("Unexpected error from read %d: %v, read %d bytes, %v", i, err, n, read)
						}
						continue
					}
					break
				}
				t.Logf("exited after %d Read calls", i)

				// Verify that new readers return errors on their reads after a client.Recv error.
				if n, err := c.OpenRead(context.Background(), "ptr", "inst_ref").Read(make([]byte, 4)); err != expectedError {
					t.Errorf("Unexpected error from read: got %v, want, %v read %d bytes.", err, expectedError, n)
				}
			},
		}, {
			name: "onSentinel",
			caseFn: func(t *testing.T, r io.ReadCloser, client *fakeClient, c *DataChannel) {
				i := 1 // For the earlier Read.
				for {
					read := make([]byte, 4)
					i++
					if n, err := r.Read(read); err == io.EOF {
						break
					} else if err != nil {
						t.Errorf("Unexpected error from read %d: %v, read %d bytes, %v", i, err, n, read)
					}
					continue
				}
				t.Logf("exited after %d Read calls", i)
				// This Test can hang on failure.
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			done := make(chan bool, 1)
			client := &fakeClient{t: t, done: done}
			c := makeDataChannel(context.Background(), "id", client)

			r := c.OpenRead(context.Background(), "ptr", "inst_ref")

			n, err := r.Read(make([]byte, 4))
			if err != nil {
				t.Errorf("Unexpected error from read: %v, read %d bytes.", err, n)
			}
			test.caseFn(t, r, client, c)
		})
	}

}
