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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
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
	isLastCall     int

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
	if f.isLastCall == f.calls {
		elemData.IsLast = true
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
	// in https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream
	if f.skipFirstError && f.err != nil {
		f.skipFirstError = false
		return nil
	} else if f.err != nil {
		return io.EOF
	}
	return nil
}

type fakeChanClient struct {
	ch  chan *fnpb.Elements
	err error
}

func (f *fakeChanClient) Recv() (*fnpb.Elements, error) {
	e, ok := <-f.ch
	if !ok {
		return nil, f.err
	}
	return e, nil
}

func (f *fakeChanClient) Send(e *fnpb.Elements) error {
	f.ch <- e
	return nil
}

func (f *fakeChanClient) Close() {
	f.err = io.EOF
	close(f.ch)
}

func (f *fakeChanClient) CloseWith(err error) {
	f.err = err
	close(f.ch)
}

func TestElementChan(t *testing.T) {
	const instID = "inst_ref"
	dataID := "dataTransform"
	timerID := "timerTransform"
	timerFamily := "timerFamily"
	setupClient := func(t *testing.T) (context.Context, *fakeChanClient, *DataChannel) {
		t.Helper()
		client := &fakeChanClient{ch: make(chan *fnpb.Elements, bufElements)}
		ctx, cancelFn := context.WithCancel(context.Background())
		t.Cleanup(cancelFn)
		t.Cleanup(func() { client.Close() })

		c := makeDataChannel(ctx, "id", client, cancelFn)
		return ctx, client, c
	}
	drainAndSum := func(t *testing.T, elms <-chan exec.Elements) (sum, count int) {
		t.Helper()
		for e := range elms { // only exits if data channel is closed.
			if len(e.Data) != 0 {
				sum += int(e.Data[0])
				count++
			}
			if len(e.Timers) != 0 {
				if e.TimerFamilyID != timerFamily {
					t.Errorf("timer received without family set: %v, state= sum %v, count %v", e, sum, count)
				}
				sum += int(e.Timers[0])
				count++
			}
		}
		return sum, count
	}

	timerElm := func(val byte, isLast bool) *fnpb.Elements_Timers {
		return &fnpb.Elements_Timers{InstructionId: instID, TransformId: timerID, Timers: []byte{val}, IsLast: isLast, TimerFamilyId: timerFamily}
	}
	dataElm := func(val byte, isLast bool) *fnpb.Elements_Data {
		return &fnpb.Elements_Data{InstructionId: instID, TransformId: dataID, Data: []byte{val}, IsLast: isLast}
	}
	noTimerElm := func() *fnpb.Elements_Timers {
		return &fnpb.Elements_Timers{InstructionId: instID, TransformId: timerID, Timers: []byte{}, IsLast: true}
	}
	noDataElm := func() *fnpb.Elements_Data {
		return &fnpb.Elements_Data{InstructionId: instID, TransformId: dataID, Data: []byte{}, IsLast: true}
	}
	openChan := func(ctx context.Context, t *testing.T, c *DataChannel, timers ...string) <-chan exec.Elements {
		t.Helper()
		runtime.Gosched() // Encourage the "read" goroutine to schedule before this call, if necessary.
		elms, err := c.OpenElementChan(ctx, dataID, instID, timers)
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}
		return elms
	}

	// Most Cases
	tests := []struct {
		name               string
		sequenceFn         func(context.Context, *testing.T, *fakeChanClient, *DataChannel) <-chan exec.Elements
		wantSum, wantCount int
	}{
		{
			name: "ReadThenData_singleRecv",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				elms := openChan(ctx, t, c)
				client.Send(&fnpb.Elements{
					Data: []*fnpb.Elements_Data{
						dataElm(1, false),
						dataElm(2, false),
						dataElm(3, true),
					},
				})
				return elms
			},
			wantSum: 6, wantCount: 3,
		}, {
			name: "ReadThenData_multipleRecv",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				elms := openChan(ctx, t, c)
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(1, false)}})
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(2, false)}})
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(3, true)}})
				return elms
			},
			wantSum: 6, wantCount: 3,
		}, {
			name: "ReadThenNoData",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				elms := openChan(ctx, t, c)
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{noDataElm()}})
				return elms
			},
			wantSum: 0, wantCount: 0,
		}, {
			name: "NoDataThenRead",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{noDataElm()}})
				elms := openChan(ctx, t, c)
				return elms
			},
			wantSum: 0, wantCount: 0,
		}, {
			name: "NoDataInstEndsThenRead",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{noDataElm()}})
				c.removeInstruction(instID)
				elms := openChan(ctx, t, c)
				return elms
			},
			wantSum: 0, wantCount: 0,
		}, {
			name: "ReadThenDataAndTimers",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				elms := openChan(ctx, t, c, timerID)
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(1, false)}})
				client.Send(&fnpb.Elements{Timers: []*fnpb.Elements_Timers{timerElm(2, true)}})
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(3, true)}})
				return elms
			},
			wantSum: 6, wantCount: 3,
		}, {
			name: "AllDataAndTimersThenRead",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(1, false)}})
				client.Send(&fnpb.Elements{Timers: []*fnpb.Elements_Timers{timerElm(2, true)}})
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(3, true)}})
				elms := openChan(ctx, t, c, timerID)
				return elms
			},
			wantSum: 6, wantCount: 3,
		}, {
			name: "DataThenReaderThenLast",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				client.Send(&fnpb.Elements{
					Data: []*fnpb.Elements_Data{
						dataElm(1, false),
						dataElm(2, false),
					},
				})
				elms := openChan(ctx, t, c)
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(3, true)}})
				return elms
			},
			wantSum: 6, wantCount: 3,
		}, {
			name: "PartialTimersAllDataReadThenLastTimer",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				client.Send(&fnpb.Elements{
					Timers: []*fnpb.Elements_Timers{
						timerElm(1, false),
						timerElm(2, false),
					},
				})
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{noDataElm()}})

				elms := openChan(ctx, t, c, timerID)
				client.Send(&fnpb.Elements{Timers: []*fnpb.Elements_Timers{timerElm(3, true)}})

				return elms
			},
			wantSum: 6, wantCount: 3,
		}, {
			name: "AllTimerThenReaderThenDataClose",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				client.Send(&fnpb.Elements{
					Timers: []*fnpb.Elements_Timers{
						timerElm(1, false),
						timerElm(2, false),
						timerElm(3, true),
					},
				})

				elms := openChan(ctx, t, c, timerID)
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{noDataElm()}})

				return elms
			},
			wantSum: 6, wantCount: 3,
		}, {
			name: "NoTimersThenReaderThenNoData",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				client.Send(&fnpb.Elements{Timers: []*fnpb.Elements_Timers{noTimerElm()}})
				elms := openChan(ctx, t, c, timerID)
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{noDataElm()}})
				return elms
			},
			wantSum: 0, wantCount: 0,
		}, {
			name: "SomeTimersThenReaderThenAData",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				client.Send(&fnpb.Elements{Timers: []*fnpb.Elements_Timers{timerElm(1, false), timerElm(2, true)}})
				elms := openChan(ctx, t, c, timerID)
				client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(3, true)}})
				return elms
			},
			wantSum: 6, wantCount: 3,
		}, {
			name: "SomeTimersAndADataThenReader",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				client.Send(&fnpb.Elements{
					Timers: []*fnpb.Elements_Timers{timerElm(1, false), timerElm(2, true)},
					Data:   []*fnpb.Elements_Data{dataElm(3, true)},
				})
				elms := openChan(ctx, t, c, timerID)
				return elms
			},
			wantSum: 6, wantCount: 3,
		}, {
			name: "PartialReadThenEndInstruction",
			sequenceFn: func(ctx context.Context, t *testing.T, client *fakeChanClient, c *DataChannel) <-chan exec.Elements {
				client.Send(&fnpb.Elements{
					Data: []*fnpb.Elements_Data{
						dataElm(1, false),
						dataElm(2, false),
					},
				})
				elms := openChan(ctx, t, c)
				var sum int
				e := <-elms
				sum += int(e.Data[0])
				e = <-elms
				sum += int(e.Data[0])

				if got, want := sum, 3; got != want {
					t.Errorf("got sum %v, want sum %v", got, want)
				}

				// Simulate a split, where the remaining buffer wouldn't be read further, and the instruction ends.
				c.removeInstruction(instID)

				// Instruction is ended, so further data for this instruction is ignored.
				client.Send(&fnpb.Elements{
					Data: []*fnpb.Elements_Data{
						dataElm(3, false),
						dataElm(4, true),
					},
				})

				elms = openChan(ctx, t, c)
				return elms
			},
			wantSum: 0, wantCount: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, client, c := setupClient(t)
			elms := test.sequenceFn(ctx, t, client, c)
			sum, count := drainAndSum(t, elms)
			if wantSum, wantCount := test.wantSum, test.wantCount; sum != wantSum || count != wantCount {
				t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantCount)
			}
		})
	}
}

func BenchmarkElementChan(b *testing.B) {
	benches := []struct {
		size int
	}{
		{1},
		{10},
		{100},
		{1000},
		{10000},
	}

	for _, bench := range benches {
		b.Run(fmt.Sprintf("batchSize:%v", bench.size), func(b *testing.B) {
			client := &fakeChanClient{ch: make(chan *fnpb.Elements, bufElements)}
			ctx, cancelFn := context.WithCancel(context.Background())
			c := makeDataChannel(ctx, "id", client, cancelFn)

			const instID = "inst_ref"
			dataID := "dataTransform"
			elms, err := c.OpenElementChan(ctx, dataID, instID, nil)
			if err != nil {
				b.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
			}
			e := &fnpb.Elements_Data{InstructionId: instID, TransformId: dataID, Data: []byte{1}, IsLast: false}
			es := make([]*fnpb.Elements_Data, 0, bench.size)
			for i := 0; i < bench.size; i++ {
				es = append(es, e)
			}
			batch := &fnpb.Elements{Data: es}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range elms {
				}
			}()
			// Batch elements sizes.
			for i := 0; i < b.N; i += bench.size {
				client.Send(batch)
			}
			client.Close()
			// Wait until we've consumed all sent batches.
			wg.Wait()
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

	_, err := c.OpenElementChan(ctx, "ptr", "inst_ref", nil)
	if err != nil {
		t.Errorf("Unexpected error from read: %v,", err)
	}
}

func TestDataChannelRemoveInstruction_limitInstructionCap(t *testing.T) {
	done := make(chan bool, 1)
	client := &fakeDataClient{t: t, done: done}
	ctx, cancelFn := context.WithCancel(context.Background())
	c := makeDataChannel(ctx, "id", client, cancelFn)

	for i := 0; i < endedInstructionCap+10; i++ {
		instID := instructionID(fmt.Sprintf("inst_ref%d", i))
		c.OpenElementChan(ctx, "ptr", instID, nil)
		c.removeInstruction(instID)
	}
	if got, want := len(c.endedInstructions), endedInstructionCap; got != want {
		t.Errorf("unexpected len(endedInstructions) got %v, want %v,", got, want)
	}
}

func TestDataChannelTerminate_Writes(t *testing.T) {
	// The logging of channels closed is quite noisy for this test
	log.SetOutput(io.Discard)

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
			// TODO(lostluck) 2019.11.26: use the go 1.13 errors package to check this rather
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

type noopDataClient struct {
}

func (*noopDataClient) Recv() (*fnpb.Elements, error) {
	return nil, nil
}

func (*noopDataClient) Send(*fnpb.Elements) error {
	return nil
}

	func TestScopedDataManager_Open_Closed(t *testing.T) {
	mgr := &DataChannelManager{}
	s := NewScopedDataManager(mgr, "inst")
	s.closed = true

	port := exec.Port{URL: "test:1234"}
	_, err := s.open(context.Background(), port)
	if err == nil {
		t.Error("expected error when opening closed ScopedDataManager")
	}
}

func TestScopedDataManager_CloseEmpty(t *testing.T) {
	mgr := &DataChannelManager{}
	s := NewScopedDataManager(mgr, "inst")

	err := s.Close()
	if err != nil {
		t.Errorf("Close on empty ScopedDataManager returned error: %v", err)
	}
	if s.mgr != nil {
		t.Error("expected mgr to be nil after Close")
	}
}

func TestElementsChan_InstructionEnded(t *testing.T) {
	ec := &elementsChan{
		ch:   make(chan exec.Elements, 1),
		done: make(chan struct{}),
	}
	ec.InstructionEnded()

	select {
	case <-ec.done:
		// ok, channel was closed
	default:
		t.Error("InstructionEnded did not close done channel")
	}
}

func TestElementsChan_Closed(t *testing.T) {
	ec := &elementsChan{}
	if ec.Closed() {
		t.Error("Closed returned true for zero-value elementsChan")
	}
	ec.ch = make(chan exec.Elements, 1)
	close(ec.ch)
	atomic.StoreUint32(&ec.closed, 1)
	if !ec.Closed() {
		t.Error("Closed returned false after setting closed flag")
	}
}

func TestElementsChan_PTransformDone(t *testing.T) {
	t.Run("singleTransform", func(t *testing.T) {
		ec := &elementsChan{
			ch:   make(chan exec.Elements, 1),
			want: 1,
		}
		ec.PTransformDone()
		if !ec.Closed() {
			t.Error("expected channel to be closed after all transforms done")
		}
	})
	t.Run("multipleTransforms_notYetDone", func(t *testing.T) {
		ec := &elementsChan{
			ch:   make(chan exec.Elements, 1),
			want: 2,
		}
		ec.PTransformDone()
		if ec.Closed() {
			t.Error("expected channel to stay open before all transforms done")
		}
	})
	t.Run("multipleTransforms_allDone", func(t *testing.T) {
		ec := &elementsChan{
			ch:   make(chan exec.Elements, 1),
			want: 2,
		}
		ec.PTransformDone()
		ec.PTransformDone()
		if !ec.Closed() {
			t.Error("expected channel to be closed after all transforms done")
		}
	})
}

func TestDataChannel_terminateStreamOnError(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	recreated := false
	expectedErr := fmt.Errorf("test error")
	// Use a noopDataClient as the underlying client to avoid nil pointer panics
	c := makeDataChannel(ctx, "id", &noopDataClient{}, cancelFn)
	c.forceRecreate = func(id string, err error) {
		recreated = true
	}
	c.terminateStreamOnError(expectedErr)

	// The cancelFn should have been called, so the context should be cancelled.
	select {
	case <-ctx.Done():
		// expected
	case <-time.After(time.Second):
		t.Error("context not cancelled after terminateStreamOnError")
	}
	if !recreated {
		t.Error("forceRecreate was not called")
	}
}

func TestScopedDataManager_OpenWrite_CachedPort(t *testing.T) {
	dc := &DataChannel{
		id:      "test:1234",
		writers: map[instructionID]map[string]*dataWriter{},
	}
	mgr := &DataChannelManager{
		ports: map[string]*DataChannel{"test:1234": dc},
	}
	s := NewScopedDataManager(mgr, "inst")
	w, err := s.OpenWrite(context.Background(), exec.StreamID{
		Port:         exec.Port{URL: "test:1234"},
		PtransformID: "ptr",
	})
	if err != nil {
		t.Errorf("OpenWrite returned error: %v", err)
	}
	if w == nil {
		t.Error("OpenWrite returned nil writer")
	}
}

func TestScopedDataManager_OpenElementChan_CachedPort(t *testing.T) {
	dc := &DataChannel{
		id:                "test:1234",
		channels:          map[instructionID]*elementsChan{},
		endedInstructions: map[instructionID]struct{}{},
	}
	mgr := &DataChannelManager{
		ports: map[string]*DataChannel{"test:1234": dc},
	}
	s := NewScopedDataManager(mgr, "inst")
	ch, err := s.OpenElementChan(context.Background(), exec.StreamID{
		Port:         exec.Port{URL: "test:1234"},
		PtransformID: "ptr",
	}, nil)
	if err != nil {
		t.Errorf("OpenElementChan returned error: %v", err)
	}
	if ch == nil {
		t.Error("OpenElementChan returned nil channel")
	}
}

func TestScopedDataManager_OpenTimerWrite_CachedPort(t *testing.T) {
	dc := &DataChannel{
		id:           "test:1234",
		timerWriters: map[instructionID]map[timerKey]*timerWriter{},
	}
	mgr := &DataChannelManager{
		ports: map[string]*DataChannel{"test:1234": dc},
	}
	s := NewScopedDataManager(mgr, "inst")
	w, err := s.OpenTimerWrite(context.Background(), exec.StreamID{
		Port:         exec.Port{URL: "test:1234"},
		PtransformID: "ptr",
	}, "family1")
	if err != nil {
		t.Errorf("OpenTimerWrite returned error: %v", err)
	}
	if w == nil {
		t.Error("OpenTimerWrite returned nil writer")
	}
}

func TestDataChannelManager_CloseInstruction(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		mgr := &DataChannelManager{}
		err := mgr.closeInstruction("inst", nil)
		if err != nil {
			t.Errorf("CloseInstruction on empty returned error: %v", err)
		}
	})
	t.Run("unknownPort", func(t *testing.T) {
		mgr := &DataChannelManager{
			ports: map[string]*DataChannel{},
		}
		err := mgr.closeInstruction("inst", []exec.Port{{URL: "unknown"}})
		if err != nil {
			t.Errorf("CloseInstruction on unknown port returned error: %v", err)
		}
	})
}
func BenchmarkDataWriter(b *testing.B) {
	fourB := []byte{42, 23, 78, 159}
	sixteenB := bytes.Repeat(fourB, 4)
	oneKiloB := bytes.Repeat(sixteenB, 64)
	oneMegaB := bytes.Repeat(oneKiloB, 1024)
	benches := []struct {
		name string
		data []byte
	}{
		{"4B", fourB},
		{"16B", sixteenB},
		{"1KB", oneKiloB},
		{"4KB", bytes.Repeat(oneKiloB, 4)},
		{"100KB", bytes.Repeat(oneKiloB, 100)},
		{"1MB", oneMegaB},
		{"10MB", bytes.Repeat(oneMegaB, 10)},
		{"100MB", bytes.Repeat(oneMegaB, 100)},
		{"256MB", bytes.Repeat(oneMegaB, 256)},
	}
	for _, bench := range benches {
		b.Run(bench.name, func(b *testing.B) {
			ndc := &noopDataClient{}
			dc := &DataChannel{
				id:      "dcid",
				client:  ndc,
				writers: map[instructionID]map[string]*dataWriter{},
			}
			w := dataWriter{
				ch: dc,
				id: clientID{
					ptransformID: "pid",
					instID:       instructionID("instID"),
				},
			}

			for i := 0; i < b.N; i++ {
				w.Write(bench.data)
			}
			w.Close()
		})
	}
}
