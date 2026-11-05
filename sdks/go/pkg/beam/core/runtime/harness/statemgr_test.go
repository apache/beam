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
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeStateClient replicates the call and response protocol
// of the state channel.
type fakeStateClient struct {
	// Blocks the read routine
	recv    chan *fnpb.StateResponse
	recvErr error
	recvMu  sync.Mutex

	// Blocks the write routine
	send    chan *fnpb.StateRequest
	sendErr error
	sendMu  sync.Mutex
}

func (f *fakeStateClient) Recv() (*fnpb.StateResponse, error) {
	// Blocks until something is sent.
	v := <-f.recv
	f.recvMu.Lock()
	defer f.recvMu.Unlock()
	return v, f.recvErr
}

func (f *fakeStateClient) Send(req *fnpb.StateRequest) error {
	f.send <- req // blocks until consumed.
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	return f.sendErr
}

func (f *fakeStateClient) setRecvErr(err error) {
	f.recvMu.Lock()
	defer f.recvMu.Unlock()
	f.recvErr = err
}

func (f *fakeStateClient) setSendErr(err error) {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	f.sendErr = err
}

func TestStateChannel(t *testing.T) {
	// The logging of channels closed is quite noisy for this test
	log.SetOutput(io.Discard)

	expectedError := fmt.Errorf("EXPECTED ERROR")

	tests := []struct {
		name              string
		caseFn            func(t *testing.T, c *StateChannel, client *fakeStateClient) error
		expectedErr       error
		validateCancelled bool
	}{
		{
			name: "HappyPath",
			caseFn: func(t *testing.T, c *StateChannel, client *fakeStateClient) error {
				// Verify that we can send a bunch of requests and get the expected paired response back.
				// This is not a real StateAPI protocol, since that's handled at a higher level than the StateChannel,
				// but validates that the routing occurs correctly.
				const count = 10
				for i := 0; i < count; i++ {
					go func() {
						req := <-client.send
						client.recv <- &fnpb.StateResponse{
							Id: req.Id, // Ids need to match up to ensure routing can occur properly.
							Response: &fnpb.StateResponse_Get{
								Get: &fnpb.StateGetResponse{
									ContinuationToken: req.GetGet().GetContinuationToken(),
								},
							},
						}
					}()
				}
				for i := 0; i < count; i++ {
					token := []byte(fmt.Sprintf("%d", i))
					resp, err := c.Send(&fnpb.StateRequest{
						Request: &fnpb.StateRequest_Get{
							Get: &fnpb.StateGetRequest{
								ContinuationToken: token,
							},
						},
					})
					if err != nil {
						t.Fatalf("unexpected error from Send: %v", err)
					}
					if got, want := string(resp.GetGet().GetContinuationToken()), string(token); got != want {
						t.Fatalf("req/response mismatch from Send: got %v, want %v", got, want)
					}
				}
				return nil
			},
		}, {
			name: "readEOF",
			caseFn: func(t *testing.T, c *StateChannel, client *fakeStateClient) error {
				go func() {
					req := <-client.send // Send should succeed.

					client.setRecvErr(io.EOF)
					client.recv <- &fnpb.StateResponse{
						Id: req.Id,
					}
				}()
				_, err := c.Send(&fnpb.StateRequest{})
				return err
			},
			expectedErr:       io.EOF,
			validateCancelled: true,
		}, {
			name: "readCanceled",
			caseFn: func(t *testing.T, c *StateChannel, client *fakeStateClient) error {
				go func() {
					req := <-client.send
					client.setRecvErr(status.Error(codes.Canceled, "context canceled"))
					client.recv <- &fnpb.StateResponse{
						Id: req.Id,
					}
				}()
				_, err := c.Send(&fnpb.StateRequest{})
				return err
			},
			expectedErr:       status.Error(codes.Canceled, "context canceled"),
			validateCancelled: true,
		}, {
			name: "readOtherErr",
			caseFn: func(t *testing.T, c *StateChannel, client *fakeStateClient) error {
				go func() {
					req := <-client.send // Send should succeed.

					client.setRecvErr(expectedError)
					client.recv <- &fnpb.StateResponse{
						Id: req.Id,
					}
				}()
				_, err := c.Send(&fnpb.StateRequest{})
				return err
			},
			expectedErr:       expectedError,
			validateCancelled: true,
		}, {
			name: "readResponseChannelDeleted",
			caseFn: func(t *testing.T, c *StateChannel, client *fakeStateClient) error {
				go func() {
					req := <-client.send // Send should succeed.

					c.mu.Lock()
					ch := c.responses[req.Id]
					delete(c.responses, req.Id)
					c.mu.Unlock()

					resp := &fnpb.StateResponse{
						Id: req.Id,
					}
					client.recv <- resp
					// unblock Send.
					ch <- resp
				}()
				_, err := c.Send(&fnpb.StateRequest{})
				return err
			},
		}, {
			name: "writeEOF",
			caseFn: func(t *testing.T, c *StateChannel, client *fakeStateClient) error {
				go func() {
					client.setSendErr(io.EOF)
					<-client.send
				}()
				_, err := c.Send(&fnpb.StateRequest{})
				return err
			},
			expectedErr:       io.EOF,
			validateCancelled: true,
		}, {
			name: "writeOtherError",
			caseFn: func(t *testing.T, c *StateChannel, client *fakeStateClient) error {
				go func() {
					client.setSendErr(expectedError)
					<-client.send
					// Shouldn't need to unblock any Recv calls.
				}()
				_, err := c.Send(&fnpb.StateRequest{})
				return err
			},
			expectedErr:       expectedError,
			validateCancelled: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeStateClient{
				recv: make(chan *fnpb.StateResponse),
				send: make(chan *fnpb.StateRequest),
			}
			ctx, cancelFn := context.WithCancel(context.Background())
			c := makeStateChannel(ctx, "id", client, cancelFn)
			forceRecreateCalled := false
			var forceRecreateError error
			c.forceRecreate = func(_ string, err error) {
				forceRecreateCalled = true
				forceRecreateError = err
			}

			retErr := test.caseFn(t, c, client)

			if got, want := retErr, test.expectedErr; !contains(got, want) {
				t.Errorf("Unexpected error: got %v, want %v", got, want)
			}

			// Verify that new Sends return the same error on their reads after client.Recv is done.
			go func() {
				// Ensure that the client isn't helping us.
				client.setSendErr(nil)
				client.setRecvErr(nil)
				// Drain the next send, and ensure the response is unblocked.
				req := <-client.send
				client.recv <- &fnpb.StateResponse{Id: req.Id} // Ids need to match up to ensure routing can occur properly.
			}()
			if _, err := c.Send(&fnpb.StateRequest{}); !contains(err, test.expectedErr) {
				t.Errorf("Unexpected error from Send: got %v, want %v", err, test.expectedErr)
			}

			if test.validateCancelled {
				select {
				case <-ctx.Done(): // Assert that the context must have been cancelled on read failures.
				case <-time.After(time.Second * 5):
					t.Fatal("context wasn't cancelled")
				}
				if !forceRecreateCalled {
					t.Fatal("forceRecreate wasn't called")
				}

				if got, want := forceRecreateError, test.expectedErr; !contains(got, want) {
					t.Errorf("Unexpected error from forceRecreate: got %v, want %v", got, want)
				}
			}
		})
	}
}

// TestStateKeyReader validates ordinary Read cases
func TestStateKeyReader(t *testing.T) {
	const readLen = 4
	tests := []struct {
		name     string
		buflens  []int // sizes of the buffers received on the state channel.
		numReads int
		closed   bool // tries to read from closed reader
		noGet    bool // tries to read from nil get response reader
	}{
		{
			name:     "emptyData",
			buflens:  []int{-1},
			numReads: 1,
		}, {
			name:     "singleBufferSingleRead",
			buflens:  []int{readLen},
			numReads: 2,
		}, {
			name:     "singleBufferMultipleReads",
			buflens:  []int{2 * readLen},
			numReads: 3,
		}, {
			name:     "singleBufferShortRead",
			buflens:  []int{readLen - 1},
			numReads: 2,
		}, {
			name:     "multiBuffer",
			buflens:  []int{readLen, readLen},
			numReads: 3,
		}, {
			name:     "multiBuffer-short-reads",
			buflens:  []int{readLen - 1, readLen - 1, readLen - 2},
			numReads: 4,
		}, {
			name:     "emptyDataFirst", // Shouldn't happen, but not unreasonable to handle.
			buflens:  []int{-1, readLen, readLen},
			numReads: 4,
		}, {
			name:     "emptyDataMid", // Shouldn't happen, but not unreasonable to handle.
			buflens:  []int{readLen, readLen, -1, readLen},
			numReads: 5,
		}, {
			name:     "emptyDataLast", // Shouldn't happen, but not unreasonable to handle.
			buflens:  []int{readLen, readLen, -1},
			numReads: 3,
		}, {
			name:     "emptyDataLast-short",
			buflens:  []int{3*readLen - 2, -1},
			numReads: 4,
		}, {
			name:     "closed",
			buflens:  []int{-1, -1},
			numReads: 1,
			closed:   true,
		}, {
			name:     "noGet",
			buflens:  []int{-1},
			numReads: 1,
			noGet:    true,
		},
	}
	for _, test := range tests {
		test := test // avoid issues on parallel execution.
		t.Run(test.name, func(t *testing.T) {
			ctx, cancelFn := context.WithCancel(context.Background())
			ch := &StateChannel{
				id:        "test",
				requests:  make(chan *fnpb.StateRequest),
				responses: make(map[string]chan<- *fnpb.StateResponse),
				cancelFn:  cancelFn,
				DoneCh:    ctx.Done(),
			}

			// Handle the channel behavior asynchronously.
			go func() {
				if test.noGet {
					req := <-ch.requests
					ch.responses[req.Id] <- &fnpb.StateResponse{
						Id: req.Id,
					}
					return
				}
				for i, buflen := range test.buflens {
					var buf []byte
					if buflen >= 0 {
						buf = bytes.Repeat([]byte{42}, buflen)
					}
					token := []byte(fmt.Sprint(i))
					if i+1 == len(test.buflens) {
						// On the last request response pair, send no token.
						token = nil
					}
					req := <-ch.requests

					ch.responses[req.Id] <- &fnpb.StateResponse{
						Id: req.Id,
						Response: &fnpb.StateResponse_Get{
							Get: &fnpb.StateGetResponse{
								ContinuationToken: token,
								Data:              buf,
							},
						},
					}
				}
			}()

			r := stateKeyReader{
				ch: ch,
			}

			if test.closed {
				err := r.Close()
				if err != nil {
					t.Errorf("unexpected error on Close(), got %v", err)
				}
			}

			var totalBytes int
			for _, l := range test.buflens {
				if l > 0 {
					totalBytes += l
				}
			}
			var finalerr error
			var count, reads int

			// Read all the bytes.
			for count <= totalBytes {
				reads++
				b := make([]byte, readLen) // io.Read is keyed off of length, not capacity.
				n, err := r.Read(b)
				if err != nil {
					finalerr = err
					break
				}
				count += n
				// Special check to avoid spurious zero elements.
				if count == totalBytes && n == 0 {
					t.Error("expected byte count read, last read is 0, but no EOF")
				}
			}
			if got, want := reads, test.numReads; got != want {
				t.Errorf("read %d times, want %d", got, want)
			}
			if got, want := count, totalBytes; got != want {
				t.Errorf("read %v bytes, want %v", got, want)
			}
			if test.closed {
				if got, want := finalerr, errors.New("side input closed"); !contains(got, want) {
					t.Errorf("got err %q, want to contain %q", got.Error(), want.Error())
				}
				return
			}
			if got, want := finalerr, io.EOF; got != want {
				t.Errorf("got err %q, want %q", got.Error(), want.Error())
			}
		})
	}
}

func TestStateKeyWriter(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		writeType writeTypeEnum
	}{
		{
			name:      "appendEmptyData",
			data:      []byte{},
			writeType: writeTypeAppend,
		},
		{
			name:      "appendSomeData",
			data:      []byte{65, 12},
			writeType: writeTypeAppend,
		},
		{
			name:      "clear",
			data:      []byte{65, 12},
			writeType: writeTypeClear,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancelFn := context.WithCancel(context.Background())
			ch := &StateChannel{
				id:        "test",
				requests:  make(chan *fnpb.StateRequest),
				responses: make(map[string]chan<- *fnpb.StateResponse),
				cancelFn:  cancelFn,
				DoneCh:    ctx.Done(),
			}

			// Handle the channel behavior asynchronously.
			go func() {
				req := <-ch.requests

				switch test.writeType {
				case writeTypeAppend:
					sra, ok := req.Request.(*fnpb.StateRequest_Append)
					if !ok {
						t.Errorf("Append write: got %v, want data of type StateRequest_Append", req.Request)
					} else {
						data := sra.Append.Data
						if !bytes.Equal(data, test.data) {
							t.Errorf("Expected request data: got %v, want %v", data, test.data)
						}
					}
					ch.responses[req.Id] <- &fnpb.StateResponse{
						Id:       req.Id,
						Response: &fnpb.StateResponse_Append{},
					}
				case writeTypeClear:
					_, ok := req.Request.(*fnpb.StateRequest_Clear)
					if !ok {
						t.Errorf("Append write: got %v, want data of type StateRequest_Append", req.Request)
					}
					ch.responses[req.Id] <- &fnpb.StateResponse{
						Id:       req.Id,
						Response: &fnpb.StateResponse_Clear{},
					}
				default:
					// Still return response so that write doesn't hang.
					ch.responses[req.Id] <- &fnpb.StateResponse{
						Id:       req.Id,
						Response: &fnpb.StateResponse_Append{},
					}
				}
			}()

			r := stateKeyWriter{
				ch:        ch,
				writeType: test.writeType,
			}

			r.Write(test.data)
		})
	}
}

type teardownStateClient struct{ block chan struct{} }

func (f *teardownStateClient) Recv() (*fnpb.StateResponse, error) {
	<-f.block
	return nil, status.Error(codes.Canceled, "context canceled")
}
func (f *teardownStateClient) Send(*fnpb.StateRequest) error { return nil }

func TestStateChannelManagerClose(t *testing.T) {
	m := &StateChannelManager{}
	block := make(chan struct{})
	var cancelled atomic.Bool
	var unblock sync.Once
	ch := makeStateChannel(context.Background(), "port", &teardownStateClient{block: block}, func() {
		cancelled.Store(true)
		unblock.Do(func() { close(block) })
	})
	ch.forceRecreate = func(string, error) {
		m.mu.Lock()
		_ = m.ports
		m.mu.Unlock()
	}
	m.ports = map[string]*StateChannel{"p": ch}

	m.Close()

	if !cancelled.Load() {
		t.Error("channel not cancelled")
	}
	m.mu.Lock()
	left := m.ports
	m.mu.Unlock()
	if left != nil {
		t.Error("ports not cleared")
	}
	m.Close()
}

type eofOnSendStateClient struct {
	recvForever chan struct{}
}

func (c *eofOnSendStateClient) Send(*fnpb.StateRequest) error { return io.EOF }
func (c *eofOnSendStateClient) Recv() (*fnpb.StateResponse, error) {
	<-c.recvForever
	return nil, io.EOF
}

func TestStateChannelWriteEOF(t *testing.T) {
	c := &StateChannel{
		id:        "id",
		client:    &eofOnSendStateClient{recvForever: make(chan struct{})},
		requests:  make(chan *fnpb.StateRequest, 1),
		responses: make(map[string]chan<- *fnpb.StateResponse),
		cancelFn:  func() {},
		DoneCh:    make(chan struct{}),
	}
	c.responses["r1"] = make(chan *fnpb.StateResponse, 1)
	c.requests <- &fnpb.StateRequest{Id: "r1"}

	done := make(chan struct{})
	go func() {
		c.write(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("write blocked")
	}
}

// This likely can't be replaced by the "errors" package helpers,
// since we serialize errors in some cases.
func contains(got, want error) bool {
	if got == want {
		return true
	}
	return strings.Contains(got.Error(), want.Error())
}
