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
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

const (
	chunkSize   = int(4e6) // Bytes to put in a single gRPC message. Max is slightly higher.
	bufElements = 20       // Number of chunks buffered per reader.
)

// ScopedDataManager scopes the global gRPC data manager to a single instruction.
// The indirection makes it easier to control access.
type ScopedDataManager struct {
	mgr    *DataChannelManager
	instID string

	// TODO(herohde) 7/20/2018: capture and force close open reads/writes. However,
	// we would need the underlying Close to be idempotent or a separate method.
	closed bool
	mu     sync.Mutex
}

// NewScopedDataManager returns a ScopedDataManager for the given instruction.
func NewScopedDataManager(mgr *DataChannelManager, instID string) *ScopedDataManager {
	return &ScopedDataManager{mgr: mgr, instID: instID}
}

func (s *ScopedDataManager) OpenRead(ctx context.Context, id exec.StreamID) (io.ReadCloser, error) {
	ch, err := s.open(ctx, id.Port)
	if err != nil {
		return nil, err
	}
	return ch.OpenRead(ctx, id.Target, s.instID), nil
}

func (s *ScopedDataManager) OpenWrite(ctx context.Context, id exec.StreamID) (io.WriteCloser, error) {
	ch, err := s.open(ctx, id.Port)
	if err != nil {
		return nil, err
	}
	return ch.OpenWrite(ctx, id.Target, s.instID), nil
}

func (s *ScopedDataManager) open(ctx context.Context, port exec.Port) (*DataChannel, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.Errorf("instruction %v no longer processing", s.instID)
	}
	local := s.mgr
	s.mu.Unlock()

	return local.Open(ctx, port) // don't hold lock over potentially slow operation
}

func (s *ScopedDataManager) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mgr = nil
	s.mu.Unlock()
	return nil
}

// DataChannelManager manages data channels over the Data API. A fixed number of channels
// are generally used, each managing multiple logical byte streams. Thread-safe.
type DataChannelManager struct {
	ports map[string]*DataChannel
	mu    sync.Mutex // guards the ports map
}

// Open opens a R/W DataChannel over the given port.
func (m *DataChannelManager) Open(ctx context.Context, port exec.Port) (*DataChannel, error) {
	if port.URL == "" {
		panic("empty port")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ports == nil {
		m.ports = make(map[string]*DataChannel)
	}
	if con, ok := m.ports[port.URL]; ok {
		return con, nil
	}

	ch, err := newDataChannel(ctx, port)
	if err != nil {
		return nil, err
	}
	m.ports[port.URL] = ch
	return ch, nil
}

// clientID identifies a client of a connected channel.
type clientID struct {
	target exec.Target
	instID string
}

// This is a reduced version of the full gRPC interface to help with testing.
// TODO(wcn): need a compile-time assertion to make sure this stays synced with what's
// in pb.BeamFnData_DataClient
type dataClient interface {
	Send(*pb.Elements) error
	Recv() (*pb.Elements, error)
}

// DataChannel manages a single multiplexed gRPC connection over the Data API. Data is
// pushed over the channel, so data for a reader may arrive before the reader connects.
// Thread-safe.
type DataChannel struct {
	id     string
	client dataClient

	writers map[clientID]*dataWriter
	readers map[clientID]*dataReader
	// TODO: early/late closed, bad instructions, finer locks, reconnect?

	mu sync.Mutex // guards both the readers and writers maps.
}

func newDataChannel(ctx context.Context, port exec.Port) (*DataChannel, error) {
	cc, err := dial(ctx, port.URL, 15*time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}
	client, err := pb.NewBeamFnDataClient(cc).Data(ctx)
	if err != nil {
		cc.Close()
		return nil, errors.Wrap(err, "failed to connect to data service")
	}
	return makeDataChannel(ctx, port.URL, client), nil
}

func makeDataChannel(ctx context.Context, id string, client dataClient) *DataChannel {
	ret := &DataChannel{
		id:      id,
		client:  client,
		writers: make(map[clientID]*dataWriter),
		readers: make(map[clientID]*dataReader),
	}
	go ret.read(ctx)

	return ret
}

func (c *DataChannel) OpenRead(ctx context.Context, target exec.Target, instID string) io.ReadCloser {
	return c.makeReader(ctx, clientID{target: target, instID: instID})
}

func (c *DataChannel) OpenWrite(ctx context.Context, target exec.Target, instID string) io.WriteCloser {
	return c.makeWriter(ctx, clientID{target: target, instID: instID})
}

func (c *DataChannel) read(ctx context.Context) {
	cache := make(map[clientID]*dataReader)
	for {
		msg, err := c.client.Recv()
		if err != nil {
			if err == io.EOF {
				// TODO(herohde) 10/12/2017: can this happen before shutdown? Reconnect?
				log.Warnf(ctx, "DataChannel %v closed", c.id)
				return
			}
			panic(errors.Wrapf(err, "channel %v bad", c.id))
		}

		recordStreamReceive(msg)

		// Each message may contain segments for multiple streams, so we
		// must treat each segment in isolation. We maintain a local cache
		// to reduce lock contention.

		for _, elm := range msg.GetData() {
			id := clientID{target: exec.Target{ID: elm.GetTarget().PrimitiveTransformReference, Name: elm.GetTarget().GetName()}, instID: elm.GetInstructionReference()}

			// log.Printf("Chan read (%v): %v\n", sid, elm.GetData())

			var r *dataReader
			if local, ok := cache[id]; ok {
				r = local
			} else {
				r = c.makeReader(ctx, id)
				cache[id] = r
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
				delete(cache, id)
				continue
			}

			// This send is deliberately blocking, if we exceed the buffering for
			// a reader. We can't buffer the entire main input, if some user code
			// is slow (or gets stuck). If the local side closes, the reader
			// will be marked as completed and further remote data will be ignored.
			select {
			case r.buf <- elm.GetData():
			case <-r.done:
				r.completed = true
			}
		}
	}
}

func (c *DataChannel) makeReader(ctx context.Context, id clientID) *dataReader {
	c.mu.Lock()
	defer c.mu.Unlock()

	if r, ok := c.readers[id]; ok {
		return r
	}

	r := &dataReader{id: id, buf: make(chan []byte, bufElements), done: make(chan bool, 1), channel: c}
	c.readers[id] = r
	return r
}

func (c *DataChannel) removeReader(id clientID) {
	c.mu.Lock()
	delete(c.readers, id)
	c.mu.Unlock()
}

func (c *DataChannel) makeWriter(ctx context.Context, id clientID) *dataWriter {
	c.mu.Lock()
	defer c.mu.Unlock()

	if w, ok := c.writers[id]; ok {
		return w
	}

	w := &dataWriter{ch: c, id: id}
	c.writers[id] = w
	return w
}

type dataReader struct {
	id        clientID
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

// TODO(herohde) 7/20/2018: we should probably either not be tracking writers or
// make dataWriter threadsafe. Either case is likely a corruption generator.

type dataWriter struct {
	buf []byte

	id clientID
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
	delete(w.ch.writers, w.id)
	target := &pb.Target{PrimitiveTransformReference: w.id.target.ID, Name: w.id.target.Name}
	msg := &pb.Elements{
		Data: []*pb.Elements_Data{
			{
				InstructionReference: w.id.instID,
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

	target := &pb.Target{PrimitiveTransformReference: w.id.target.ID, Name: w.id.target.Name}
	msg := &pb.Elements{
		Data: []*pb.Elements_Data{
			{
				InstructionReference: w.id.instID,
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
	if len(w.buf)+len(p) > chunkSize {
		l := len(w.buf)
		// We can't fit this message into the buffer. We need to flush the buffer
		if err := w.Flush(); err != nil {
			return 0, errors.Wrapf(err, "datamgr.go: error flushing buffer of length %d", l)
		}
	}

	// At this point there's room in the buffer one way or another.
	w.buf = append(w.buf, p...)
	return len(p), nil
}
