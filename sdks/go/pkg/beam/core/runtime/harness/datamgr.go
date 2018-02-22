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

const chunkSize = int(4e6) // Bytes to put in a single gRPC message. Max is slightly higher.

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
	client pb.BeamFnData_DataClient
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

func (m *DataChannel) OpenRead(ctx context.Context, id exec.StreamID) (io.ReadCloser, error) {
	return m.makeReader(ctx, id), nil
}

func (m *DataChannel) OpenWrite(ctx context.Context, id exec.StreamID) (io.WriteCloser, error) {
	return m.makeWriter(ctx, id), nil
}

func (m *DataChannel) read(ctx context.Context) {
	cache := make(map[string]*dataReader)
	for {
		msg, err := m.client.Recv()
		if err != nil {
			if err == io.EOF {
				// TODO(herohde) 10/12/2017: can this happen before shutdown? Reconnect?
				log.Warnf(ctx, "DataChannel %v closed", m.port)
				return
			}
			panic(fmt.Errorf("channel %v bad: %v", m.port, err))
		}

		recordStreamReceive(msg)

		// Each message may contain segments for multiple streams, so we
		// must treat each segment in isolation. We maintain a local cache
		// to reduce lock contention.

		for _, elm := range msg.GetData() {
			id := exec.StreamID{Port: m.port, Target: exec.Target{ID: elm.GetTarget().PrimitiveTransformReference, Name: elm.GetTarget().GetName()}, InstID: elm.GetInstructionReference()}
			sid := id.String()

			// log.Printf("Chan read (%v): %v", sid, elm.GetData())

			var r *dataReader
			if local, ok := cache[sid]; ok {
				r = local
			} else {
				r = m.makeReader(ctx, id)
				cache[sid] = r
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
			// is slow (or gets stuck).
			r.buf <- elm.GetData()
		}
	}
}

func (m *DataChannel) makeReader(ctx context.Context, id exec.StreamID) *dataReader {
	m.mu.Lock()
	defer m.mu.Unlock()

	sid := id.String()
	if r, ok := m.readers[sid]; ok {
		return r
	}

	r := &dataReader{buf: make(chan []byte, 20)}
	m.readers[sid] = r
	return r
}

func (m *DataChannel) makeWriter(ctx context.Context, id exec.StreamID) *dataWriter {
	m.mu.Lock()
	defer m.mu.Unlock()

	sid := id.String()
	if w, ok := m.writers[sid]; ok {
		return w
	}

	w := &dataWriter{ch: m, id: id}
	m.writers[sid] = w
	return w
}

type dataReader struct {
	buf chan []byte
	cur []byte
}

func (r *dataReader) Close() error {
	// TODO(herohde) 6/27/2017: allow early close to throw away data async. We also need
	// to garbage collect readers.
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

type DataConnectionContext struct {
	InstID string `beam:"opt"`
}
