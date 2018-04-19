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
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"google.golang.org/grpc"
)

const (
	chunkSize   = int(4e6) // Bytes to put in a single gRPC message. Max is slightly higher.
	bufElements = 20       // Number of chunks buffered per reader.
)

// This is a reduced version of the full gRPC interface to help with testing.
// TODO(wcn): need a compile-time assertion to make sure this stays synced with what's
// in pb.BeamFnData_DataClient
type dataClient interface {
	Send(*pb.Elements) error
	Recv() (*pb.Elements, error)
}

// DataManager manages data channels to the FnHarness. A fixed number of channels
// are generally used, each managing multiple logical byte streams.
type DataManager struct {
	ports map[string]*DataChannel
	mu    sync.Mutex
}

func (m *DataManager) OpenRead(ctx context.Context, id exec.StreamID) (io.ReadCloser, error) {
	ch, err := m.open(ctx, id.Port)
	if err != nil {
		return nil, err
	}
	return ch.OpenRead(ctx, id)
}

func (m *DataManager) OpenWrite(ctx context.Context, id exec.StreamID) (io.WriteCloser, error) {
	ch, err := m.open(ctx, id.Port)
	if err != nil {
		return nil, err
	}
	return ch.OpenWrite(ctx, id)
}

func (m *DataManager) open(ctx context.Context, port exec.Port) (*DataChannel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ports == nil {
		m.ports = make(map[string]*DataChannel)
	}
	if con, ok := m.ports[port.URL]; ok {
		return con, nil
	}

	ch, err := NewDataChannel(ctx, port)
	if err != nil {
		return nil, err
	}
	m.ports[port.URL] = ch
	return ch, nil
}

// DataChannel manages a single grpc connection to the FnHarness.
type DataChannel struct {
	cc     *grpc.ClientConn
	client dataClient
	port   exec.Port

	writers map[string]*dataWriter
	readers map[string]*dataReader
	// TODO: early/late closed, bad instructions, finer locks, reconnect?

	mu sync.Mutex
}

func NewDataChannel(ctx context.Context, port exec.Port) (*DataChannel, error) {
	cc, err := dial(ctx, port.URL, 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}
	client, err := pb.NewBeamFnDataClient(cc).Data(ctx)
	if err != nil {
		cc.Close()
		return nil, fmt.Errorf("failed to connect to data service: %v", err)
	}
	return makeDataChannel(ctx, cc, client, port)
}

func makeDataChannel(ctx context.Context, cc *grpc.ClientConn, client dataClient, port exec.Port) (*DataChannel, error) {
	ret := &DataChannel{
		cc:      cc,
		client:  client,
		port:    port,
		writers: make(map[string]*dataWriter),
		readers: make(map[string]*dataReader),
	}
	go ret.read(ctx)

	return ret, nil
}

func (c *DataChannel) OpenRead(ctx context.Context, id exec.StreamID) (io.ReadCloser, error) {
	return c.makeReader(ctx, id), nil
}

func (c *DataChannel) OpenWrite(ctx context.Context, id exec.StreamID) (io.WriteCloser, error) {
	return c.makeWriter(ctx, id), nil
}

func (c *DataChannel) read(ctx context.Context) {
	cache := make(map[string]*dataReader)
	for {
		msg, err := c.client.Recv()
		if err != nil {
			if err == io.EOF {
				// TODO(herohde) 10/12/2017: can this happen before shutdown? Reconnect?
				log.Warnf(ctx, "DataChannel %v closed", c.port)
				return
			}
			panic(fmt.Errorf("channel %v bad: %v", c.port, err))
		}

		recordStreamReceive(msg)

		// Each message may contain segments for multiple streams, so we
		// must treat each segment in isolation. We maintain a local cache
		// to reduce lock contention.

		for _, elm := range msg.GetData() {
			id := exec.StreamID{Port: c.port, Target: exec.Target{ID: elm.GetTarget().PrimitiveTransformReference, Name: elm.GetTarget().GetName()}, InstID: elm.GetInstructionReference()}
			sid := id.String()

			// log.Printf("Chan read (%v): %v\n", sid, elm.GetData())

			var r *dataReader
			if local, ok := cache[sid]; ok {
				r = local
			} else {
				r = c.makeReader(ctx, id)
				cache[sid] = r
			}

			if r.completed {
				// The local reader has closed but the remote is still sending data.
				// Just ignore it. We keep the reader config in the cache so we don't
				// treat it as a new reader. Eventually the stream will finish and go
				// through normal teardown.
				continue
			}
			if len(elm.GetData()) == 0 {
				// Sentinel EOF segment for stream. Close buffer to signal EOF.
				close(r.buf)

				// Clean up local bookkeeping. We'll never see another message
				// for it again. We have to be careful not to remove the real
				// one, because readers may be initialized after we've seen
				// the full stream.
				delete(cache, sid)
				continue
			}

			// This send is deliberately blocking, if we exceed the buffering for
			// a reader. We can't buffer the entire main input, if some user code
			// is slow (or gets stuck). If the local side closes, the reader
			// will be marked as completed and further remote data will be ingored.
			select {
			case r.buf <- elm.GetData():
			case <-r.done:
				r.completed = true
			}
		}
	}
}

func (c *DataChannel) makeReader(ctx context.Context, id exec.StreamID) *dataReader {
	c.mu.Lock()
	defer c.mu.Unlock()

	sid := id.String()
	if r, ok := c.readers[sid]; ok {
		return r
	}

	r := &dataReader{id: sid, buf: make(chan []byte, bufElements), done: make(chan bool, 1), channel: c}
	c.readers[sid] = r
	return r
}

func (c *DataChannel) removeReader(id string) {
	delete(c.readers, id)
}

func (c *DataChannel) makeWriter(ctx context.Context, id exec.StreamID) *dataWriter {
	c.mu.Lock()
	defer c.mu.Unlock()

	sid := id.String()
	if w, ok := c.writers[sid]; ok {
		return w
	}

	w := &dataWriter{ch: c, id: id}
	c.writers[sid] = w
	return w
}

type dataReader struct {
	id        string
	buf       chan []byte
	done      chan bool
	cur       []byte
	channel   *DataChannel
	completed bool
}

func (r *dataReader) Close() error {
	r.done <- true
	r.channel.removeReader(r.id)
	return nil
}

func (r *dataReader) Read(buf []byte) (int, error) {
	if r.cur == nil {
		b, ok := <-r.buf
		if !ok {
			return 0, io.EOF
		}
		r.cur = b
	}

	n := copy(buf, r.cur)

	if len(r.cur) == n {
		r.cur = nil
	} else {
		r.cur = r.cur[n:]
	}

	return n, nil
}

type dataWriter struct {
	buf []byte

	id exec.StreamID
	ch *DataChannel
}

func (w *dataWriter) Close() error {
	// Don't acquire the locks as Flush will do so.
	err := w.Flush()
	if err != nil {
		return err
	}

	// Now acquire the locks since we're sending.
	w.ch.mu.Lock()
	defer w.ch.mu.Unlock()
	delete(w.ch.writers, w.id.String())
	target := &pb.Target{PrimitiveTransformReference: w.id.Target.ID, Name: w.id.Target.Name}
	msg := &pb.Elements{
		Data: []*pb.Elements_Data{
			{
				InstructionReference: w.id.InstID,
				Target:               target,
				// Empty data == sentinel
			},
		},
	}

	// TODO(wcn): if this send fails, we have a data channel that's lingering that
	// the runner is still waiting on. Need some way to identify these and resolve them.
	recordStreamSend(msg)
	return w.ch.client.Send(msg)
}

func (w *dataWriter) Flush() error {
	w.ch.mu.Lock()
	defer w.ch.mu.Unlock()

	if w.buf == nil {
		return nil
	}

	target := &pb.Target{PrimitiveTransformReference: w.id.Target.ID, Name: w.id.Target.Name}
	msg := &pb.Elements{
		Data: []*pb.Elements_Data{
			{
				InstructionReference: w.id.InstID,
				Target:               target,
				Data:                 w.buf,
			},
		},
	}
	w.buf = nil
	recordStreamSend(msg)
	return w.ch.client.Send(msg)
}

func (w *dataWriter) Write(p []byte) (n int, err error) {
	if len(p) > chunkSize {
		panic(fmt.Sprintf("Incoming message too big for transport: %d > %d", len(p), chunkSize))
	}

	if len(w.buf)+len(p) > chunkSize {
		// We can't fit this message into the buffer. We need to flush the buffer
		if err := w.Flush(); err != nil {
			return 0, err
		}
	}

	// At this point there's room in the buffer one way or another.
	w.buf = append(w.buf, p...)
	return len(p), nil
}
