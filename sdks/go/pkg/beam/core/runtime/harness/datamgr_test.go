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
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
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
	ch chan *fnpb.Elements
}

func (f *fakeChanClient) Recv() (*fnpb.Elements, error) {
	e, ok := <-f.ch
	if !ok {
		return nil, io.EOF
	}
	return e, nil
}

func (f *fakeChanClient) Send(e *fnpb.Elements) error {
	f.ch <- e
	return nil
}

func (f *fakeChanClient) Close() error {
	close(f.ch)
	return nil
}

func TestElementChan(t *testing.T) {
	const instID = "inst_ref"
	dataID := "dataTransform"
	timerID := "timerTransform"
	timerFamily := "timerFamily"
	setupClient := func(t *testing.T) (context.Context, *fakeChanClient, *DataChannel) {
		fmt.Println("TTTTT ", t.Name())
		t.Helper()
		client := &fakeChanClient{ch: make(chan *fnpb.Elements, 20)}
		ctx, cancelFn := context.WithCancel(context.Background())
		t.Cleanup(cancelFn)
		t.Cleanup(func() { client.Close() })

		c := makeDataChannel(ctx, "id", client, cancelFn)
		return ctx, client, c
	}
	drainAndSum := func(t *testing.T, elms <-chan exec.Elements) (sum, count int) {
		t.Helper()
		for e := range elms { // only exits if data channel is closed.
			count++
			if len(e.Data) != 0 {
				sum += int(e.Data[0])
			}
			if len(e.Timers) != 0 {
				if e.TimerFamilyID != timerFamily {
					t.Errorf("timer received without family set: %v, state= sum %v, count %v", e, sum, count)
				}
				sum += int(e.Timers[0])
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

	// Simple batch HappyPath.
	t.Run("readerThenData_singleRecv", func(t *testing.T) {
		ctx, client, c := setupClient(t)

		elms, err := c.OpenElementChan(ctx, dataID, instID, nil)
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}

		client.Send(&fnpb.Elements{
			Data: []*fnpb.Elements_Data{
				dataElm(1, false),
				dataElm(2, false),
				dataElm(3, true),
			},
		})

		sum, count := drainAndSum(t, elms)
		if wantSum, wantCount := 6, 3; sum != wantSum && count != wantCount {
			t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantSum)
		}
	})

	t.Run("readerThenData_multipleRecv", func(t *testing.T) {
		ctx, client, c := setupClient(t)

		elms, err := c.OpenElementChan(ctx, dataID, instID, nil)
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}
		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(1, false)}})
		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(2, false)}})
		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(3, true)}})

		sum, count := drainAndSum(t, elms)
		if wantSum, wantCount := 6, 3; sum != wantSum && count != wantCount {
			t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantSum)
		}
	})
	t.Run("readerThenDataAndTimers", func(t *testing.T) {
		ctx, client, c := setupClient(t)

		elms, err := c.OpenElementChan(ctx, dataID, instID, []string{timerID})
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}
		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(1, false)}})

		client.Send(&fnpb.Elements{Timers: []*fnpb.Elements_Timers{timerElm(2, true)}})

		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(3, true)}})

		sum, count := drainAndSum(t, elms)
		if wantSum, wantCount := 6, 3; sum != wantSum && count != wantCount {
			t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantSum)
		}
	})

	t.Run("DataThenReaderThenLast", func(t *testing.T) {
		ctx, client, c := setupClient(t)
		client.Send(&fnpb.Elements{
			Data: []*fnpb.Elements_Data{
				dataElm(1, false),
				dataElm(2, false),
			},
		})
		elms, err := c.OpenElementChan(ctx, dataID, instID, nil)
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}
		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(3, true)}})

		sum, count := drainAndSum(t, elms)
		if wantSum, wantCount := 6, 3; sum != wantSum && count != wantCount {
			t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantSum)
		}
	})

	t.Run("AllDataThenReader", func(t *testing.T) {
		ctx, client, c := setupClient(t)

		client.Send(&fnpb.Elements{
			Data: []*fnpb.Elements_Data{
				dataElm(1, false),
				dataElm(2, false),
				dataElm(3, true),
			},
		})

		elms, err := c.OpenElementChan(ctx, dataID, instID, nil)
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}

		sum, count := drainAndSum(t, elms)
		if wantSum, wantCount := 6, 3; sum != wantSum && count != wantCount {
			t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantSum)
		}
	})

	t.Run("TimerThenReaderThenDataCloseThenLastTimer", func(t *testing.T) {
		ctx, client, c := setupClient(t)
		client.Send(&fnpb.Elements{
			Timers: []*fnpb.Elements_Timers{
				timerElm(1, false),
				timerElm(2, false),
			},
		})
		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{noDataElm()}})

		elms, err := c.OpenElementChan(ctx, dataID, instID, []string{timerID})
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}
		var wg sync.WaitGroup
		wg.Add(1)
		var sum, count int
		go func() {
			defer wg.Done()
			sum, count = drainAndSum(t, elms)
		}()

		client.Send(&fnpb.Elements{Timers: []*fnpb.Elements_Timers{timerElm(3, true)}})

		wg.Wait()
		if wantSum, wantCount := 6, 3; sum != wantSum && count != wantCount {
			t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantSum)
		}
	})

	t.Run("AllTimerThenReaderThenDataClose", func(t *testing.T) {
		ctx, client, c := setupClient(t)
		client.Send(&fnpb.Elements{
			Timers: []*fnpb.Elements_Timers{
				timerElm(1, false),
				timerElm(2, false),
				timerElm(3, true),
			},
		})

		elms, err := c.OpenElementChan(ctx, dataID, instID, []string{timerID})
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}
		var wg sync.WaitGroup
		wg.Add(1)
		var sum, count int
		go func() {
			defer wg.Done()
			sum, count = drainAndSum(t, elms)
		}()
		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{noDataElm()}})

		wg.Wait()
		if wantSum, wantCount := 6, 3; sum != wantSum && count != wantCount {
			t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantSum)
		}
	})
	t.Run("NoTimersThenReaderThenNoData", func(t *testing.T) {
		ctx, client, c := setupClient(t)
		client.Send(&fnpb.Elements{Timers: []*fnpb.Elements_Timers{noTimerElm()}})

		elms, err := c.OpenElementChan(ctx, dataID, instID, []string{timerID})
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}
		var wg sync.WaitGroup
		wg.Add(1)
		var sum, count int
		go func() {
			defer wg.Done()
			sum, count = drainAndSum(t, elms)
		}()

		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{noDataElm()}})

		wg.Wait()
		if wantSum, wantCount := 0, 0; sum != wantSum && count != wantCount {
			t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantSum)
		}
	})
	t.Run("SomeTimersThenReaderThenAData", func(t *testing.T) {
		ctx, client, c := setupClient(t)
		client.Send(&fnpb.Elements{Timers: []*fnpb.Elements_Timers{timerElm(1, false), timerElm(2, true)}})

		elms, err := c.OpenElementChan(ctx, dataID, instID, []string{timerID})
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}
		var wg sync.WaitGroup
		wg.Add(1)
		var sum, count int
		go func() {
			defer wg.Done()
			sum, count = drainAndSum(t, elms)
		}()

		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(3, true)}})

		wg.Wait()
		if wantSum, wantCount := 6, 3; sum != wantSum && count != wantCount {
			t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantSum)
		}
	})

	t.Run("SomeTimersThenADataThenReader", func(t *testing.T) {
		ctx, client, c := setupClient(t)
		client.Send(&fnpb.Elements{Timers: []*fnpb.Elements_Timers{timerElm(1, false), timerElm(2, true)}})
		client.Send(&fnpb.Elements{Data: []*fnpb.Elements_Data{dataElm(3, true)}})

		elms, err := c.OpenElementChan(ctx, dataID, instID, []string{timerID})
		if err != nil {
			t.Errorf("Unexpected error from OpenElementChan(%v, %v, nil): %v", dataID, instID, err)
		}
		var wg sync.WaitGroup
		wg.Add(1)
		var sum, count int
		go func() {
			defer wg.Done()
			sum, count = drainAndSum(t, elms)
		}()

		wg.Wait()
		if wantSum, wantCount := 6, 3; sum != wantSum && count != wantCount {
			t.Errorf("got sum %v, count %v, want sum %v, count %v", sum, count, wantSum, wantSum)
		}
	})
}

func TestDataChannelTerminate_dataReader(t *testing.T) {
	// The logging of channels closed is quite noisy for this test
	log.SetOutput(io.Discard)

	expectedError := fmt.Errorf("EXPECTED ERROR")

	tests := []struct {
		name          string
		expectedError error
		caseFn        func(t *testing.T, ch *elementsChan, client *fakeDataClient, c *DataChannel)
	}{
		{
			name:          "onInstructionEnded",
			expectedError: io.EOF,
			caseFn: func(t *testing.T, ch *elementsChan, client *fakeDataClient, c *DataChannel) {
				ch.InstructionEnded()
			},
		}, {
			name:          "onSentinel",
			expectedError: io.EOF,
			caseFn: func(t *testing.T, ch *elementsChan, client *fakeDataClient, c *DataChannel) {
				// fakeDataClient eventually returns a sentinel element.
			},
		}, {
			name:          "onIsLast_withData",
			expectedError: io.EOF,
			caseFn: func(t *testing.T, ch *elementsChan, client *fakeDataClient, c *DataChannel) {
				// Set the last call with data to use is_last.
				client.isLastCall = 2
			},
		}, {
			name:          "onIsLast_withoutData",
			expectedError: io.EOF,
			caseFn: func(t *testing.T, ch *elementsChan, client *fakeDataClient, c *DataChannel) {
				// Set the call without data to use is_last.
				client.isLastCall = 3
			},
		}, {
			name:          "onRecvError",
			expectedError: expectedError,
			caseFn: func(t *testing.T, ch *elementsChan, client *fakeDataClient, c *DataChannel) {
				// The SDK starts reading in a goroutine immeadiately after open.
				// Set the 2nd Recv call to have an error.
				client.err = expectedError
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fmt.Println("TTTTT ", test.name)
			done := make(chan bool, 1)
			client := &fakeDataClient{t: t, done: done}
			client.blocked.Lock()
			ctx, cancelFn := context.WithCancel(context.Background())
			c := makeDataChannel(ctx, "id", client, cancelFn)

			elms, err := c.OpenElementChan(ctx, "ptr", "inst_ref", nil)
			if err != nil {
				t.Errorf("Unexpected error from OpenElementChan: %v", err)
			}
			ech := c.channels["inst_ref"]

			test.caseFn(t, ech, client, c)
			client.blocked.Unlock()
			// Drain channel
			for range elms {
			}

			// Verify that new readers return the same error on their reads after client.Recv is done.
			if _, err = c.OpenElementChan(ctx, "ptr", "inst_ref", nil); !errors.Is(err, test.expectedError) {
				t.Errorf("Unexpected error from read: got %v, want %v", err, test.expectedError)
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
