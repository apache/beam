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
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

const (
	chunkSize   = int(4e6) // Bytes to put in a single gRPC message. Max is slightly higher.
	bufElements = 20       // Number of chunks buffered per reader.
)

// ScopedDataManager scopes the global gRPC data manager to a single instruction.
// The indirection makes it easier to control access.
type ScopedDataManager struct {
	mgr    *DataChannelManager
	instID instructionID

	closed bool
	mu     sync.Mutex
}

// NewScopedDataManager returns a ScopedDataManager for the given instruction.
func NewScopedDataManager(mgr *DataChannelManager, instID instructionID) *ScopedDataManager {
	return &ScopedDataManager{mgr: mgr, instID: instID}
}

// OpenRead opens an io.ReadCloser on the given stream.
func (s *ScopedDataManager) OpenRead(ctx context.Context, id exec.StreamID) (io.ReadCloser, error) {
	ch, err := s.open(ctx, id.Port)
	if err != nil {
		return nil, err
	}
	return ch.OpenRead(ctx, id.PtransformID, s.instID), nil
}

// OpenWrite opens an io.WriteCloser on the given stream.
func (s *ScopedDataManager) OpenWrite(ctx context.Context, id exec.StreamID) (io.WriteCloser, error) {
	ch, err := s.open(ctx, id.Port)
	if err != nil {
		return nil, err
	}
	return ch.OpenWrite(ctx, id.PtransformID, s.instID), nil
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

// Close prevents new IO for this instruction.
func (s *ScopedDataManager) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.mgr.closeInstruction(s.instID)
	s.mgr = nil
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
	ch.forceRecreate = func(id string, err error) {
		log.Warnf(ctx, "forcing DataChannel[%v] reconnection on port %v due to %v", id, port, err)
		m.mu.Lock()
		delete(m.ports, port.URL)
		m.mu.Unlock()
	}
	m.ports[port.URL] = ch
	return ch, nil
}

func (m *DataChannelManager) closeInstruction(instID instructionID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ch := range m.ports {
		ch.removeInstruction(instID)
	}
}

// clientID identifies a client of a connected channel.
type clientID struct {
	ptransformID string
	instID       instructionID
}

// This is a reduced version of the full gRPC interface to help with testing.
// TODO(wcn): need a compile-time assertion to make sure this stays synced with what's
// in fnpb.BeamFnData_DataClient
type dataClient interface {
	Send(*fnpb.Elements) error
	Recv() (*fnpb.Elements, error)
}

// DataChannel manages a single gRPC stream over the Data API. Data from
// multiple bundles can be multiplexed over this stream. Data is pushed
// over the channel, so data for a reader may arrive before the reader
// connects.
// Thread-safe.
type DataChannel struct {
	id     string
	client dataClient

	writers map[instructionID]map[string]*dataWriter
	readers map[instructionID]map[string]*dataReader

	// recently terminated instructions
	endedInstructions map[instructionID]struct{}
	rmQueue           []instructionID

	// readErr indicates a client.Recv error and is used to prevent new readers.
	readErr error

	// a closure that forces the data manager to recreate this stream.
	forceRecreate func(id string, err error)
	cancelFn      context.CancelFunc // Allows writers to stop the grpc reading goroutine.

	mu sync.Mutex // guards mutable internal data, notably the maps and readErr.
}

func newDataChannel(ctx context.Context, port exec.Port) (*DataChannel, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	cc, err := dial(ctx, port.URL, 15*time.Second)
	if err != nil {
		cancelFn()
		return nil, errors.Wrapf(err, "failed to connect to data service at %v", port.URL)
	}
	client, err := fnpb.NewBeamFnDataClient(cc).Data(ctx)
	if err != nil {
		cc.Close()
		cancelFn()
		return nil, errors.Wrapf(err, "failed to create data client on %v", port.URL)
	}
	return makeDataChannel(ctx, port.URL, client, cancelFn), nil
}

func makeDataChannel(ctx context.Context, id string, client dataClient, cancelFn context.CancelFunc) *DataChannel {
	ret := &DataChannel{
		id:                id,
		client:            client,
		writers:           make(map[instructionID]map[string]*dataWriter),
		readers:           make(map[instructionID]map[string]*dataReader),
		endedInstructions: make(map[instructionID]struct{}),
		cancelFn:          cancelFn,
	}
	go ret.read(ctx)

	return ret
}

// terminateStreamOnError requires the lock to be held.
func (c *DataChannel) terminateStreamOnError(err error) {
	c.cancelFn() // A context.CancelFunc is threadsafe and indempotent.
	if c.forceRecreate != nil {
		c.forceRecreate(c.id, err)
		c.forceRecreate = nil
	}
}

// OpenRead returns an io.ReadCloser of the data elements for the given instruction and ptransform.
func (c *DataChannel) OpenRead(ctx context.Context, ptransformID string, instID instructionID) io.ReadCloser {
	c.mu.Lock()
	defer c.mu.Unlock()
	cid := clientID{ptransformID: ptransformID, instID: instID}
	if c.readErr != nil {
		log.Errorf(ctx, "opening a reader %v on a closed channel", cid)
		return &errReader{c.readErr}
	}
	return c.makeReader(ctx, cid)
}

// OpenWrite returns an io.WriteCloser of the data elements for the given instruction and ptransform.
func (c *DataChannel) OpenWrite(ctx context.Context, ptransformID string, instID instructionID) io.WriteCloser {
	return c.makeWriter(ctx, clientID{ptransformID: ptransformID, instID: instID})
}

func (c *DataChannel) read(ctx context.Context) {
	cache := make(map[clientID]*dataReader)
	for {
		msg, err := c.client.Recv()
		if err != nil {
			// This connection is bad, so we should close and delete all extant streams.
			c.mu.Lock()
			c.readErr = err // prevent not yet opened readers from hanging.
			// Readers must be closed from this goroutine, since we can't
			// close the r.buf channels twice, or send on a closed channel.
			// Any other approach is racy, and may cause one of the above
			// panics.
			for _, m := range c.readers {
				for _, r := range m {
					log.Errorf(ctx, "DataChannel.read %v reader %v closing due to error on channel", c.id, r.id)
					if !r.completed {
						r.completed = true
						r.err = err
						close(r.buf)
					}
					delete(cache, r.id)
				}
			}
			c.terminateStreamOnError(err)
			c.mu.Unlock()

			if err == io.EOF {
				log.Warnf(ctx, "DataChannel.read %v closed", c.id)
				return
			}
			log.Errorf(ctx, "DataChannel.read %v bad: %v", c.id, err)
			return
		}

		recordStreamReceive(msg)

		// Each message may contain segments for multiple streams, so we
		// must treat each segment in isolation. We maintain a local cache
		// to reduce lock contention.

		for _, elm := range msg.GetData() {
			id := clientID{ptransformID: elm.TransformId, instID: instructionID(elm.GetInstructionId())}

			var r *dataReader
			if local, ok := cache[id]; ok {
				r = local
			} else {
				c.mu.Lock()
				r = c.makeReader(ctx, id)
				c.mu.Unlock()
				cache[id] = r
			}

			if elm.GetIsLast() {
				// If this reader hasn't closed yet, do so now.
				if !r.completed {
					// Sentinel EOF segment for stream. Close buffer to signal EOF.
					r.completed = true
					close(r.buf)
				}

				// Clean up local bookkeeping. We'll never see another message
				// for it again. We have to be careful not to remove the real
				// one, because readers may be initialized after we've seen
				// the full stream.
				delete(cache, id)
				continue
			}

			if r.completed {
				// The local reader has closed but the remote is still sending data.
				// Just ignore it. We keep the reader config in the cache so we don't
				// treat it as a new reader. Eventually the stream will finish and go
				// through normal teardown.
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
				close(r.buf)
			}
		}
	}
}

type errReader struct {
	err error
}

func (r *errReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

func (r *errReader) Close() error {
	return r.err
}

// makeReader creates a dataReader. It expects to be called while c.mu is held.
func (c *DataChannel) makeReader(ctx context.Context, id clientID) *dataReader {
	var m map[string]*dataReader
	var ok bool
	if m, ok = c.readers[id.instID]; !ok {
		m = make(map[string]*dataReader)
		c.readers[id.instID] = m
	}

	if r, ok := m[id.ptransformID]; ok {
		return r
	}

	r := &dataReader{id: id, buf: make(chan []byte, bufElements), done: make(chan bool, 1), channel: c}

	// Just in case initial data for an instruction arrives *after* an instructon has ended.
	// eg. it was blocked by another reader being slow, or the other instruction failed.
	// So we provide a pre-completed reader, and do not cache it, as there's no further cleanup for it.
	if _, ok := c.endedInstructions[id.instID]; ok {
		r.completed = true
		close(r.buf)
		r.err = io.EOF // In case of any actual data readers, so they terminate without error.
		return r
	}

	m[id.ptransformID] = r
	return r
}

func (c *DataChannel) removeReader(id clientID) {
	c.mu.Lock()
	if m, ok := c.readers[id.instID]; ok {
		delete(m, id.ptransformID)
	}
	c.mu.Unlock()
}

const endedInstructionCap = 32

// removeInstruction closes all readers and writers registered for the instruction
// and deletes this instruction from the channel's reader and writer maps.
func (c *DataChannel) removeInstruction(instID instructionID) {
	c.mu.Lock()

	// We don't want to leak memory, so cap the endedInstructions list.
	if len(c.rmQueue) >= endedInstructionCap {
		toRemove := c.rmQueue[0]
		c.rmQueue = c.rmQueue[1:]
		delete(c.endedInstructions, toRemove)
	}
	c.endedInstructions[instID] = struct{}{}
	c.rmQueue = append(c.rmQueue, instID)

	rs := c.readers[instID]
	ws := c.writers[instID]

	// Prevent other users while we iterate.
	delete(c.readers, instID)
	delete(c.writers, instID)
	c.mu.Unlock()

	// Close grabs the channel lock, so this must be outside the critical section.
	for _, r := range rs {
		r.Close()
	}
	for _, w := range ws {
		w.Close()
	}
}

func (c *DataChannel) makeWriter(ctx context.Context, id clientID) *dataWriter {
	c.mu.Lock()
	defer c.mu.Unlock()

	var m map[string]*dataWriter
	var ok bool
	if m, ok = c.writers[id.instID]; !ok {
		m = make(map[string]*dataWriter)
		c.writers[id.instID] = m
	}

	if w, ok := m[id.ptransformID]; ok {
		return w
	}

	// We don't check for ended instructions for writers, as writers
	// can only be created if an instruction is in scope, and aren't
	// runner or user directed.

	w := &dataWriter{ch: c, id: id}
	m[id.ptransformID] = w
	return w
}

type dataReader struct {
	id        clientID
	buf       chan []byte
	done      chan bool
	cur       []byte
	channel   *DataChannel
	completed bool
	err       error
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
			if r.err == nil {
				return 0, io.EOF
			}
			return 0, r.err
		}
		r.cur = b
	}

	// We don't need to check for a 0 length copy from r.cur here, since that's
	// checked before buffers are handed to the r.buf channel.
	n := copy(buf, r.cur)

	switch {
	case len(r.cur) == n:
		r.cur = nil
	default:
		r.cur = r.cur[n:]
	}

	return n, nil
}

type dataWriter struct {
	buf []byte

	id clientID
	ch *DataChannel
}

// send requires the ch.mu lock to be held.
func (w *dataWriter) send(msg *fnpb.Elements) error {
	recordStreamSend(msg)
	if err := w.ch.client.Send(msg); err != nil {
		if err == io.EOF {
			log.Warnf(context.TODO(), "dataWriter[%v;%v] EOF on send; fetching real error", w.id, w.ch.id)
			err = nil
			for err == nil {
				// Per GRPC stream documentation, if there's an EOF, we must call Recv
				// until a non-nil error is returned, to ensure resources are cleaned up.
				// https://godoc.org/google.golang.org/grpc#ClientConn.NewStream
				_, err = w.ch.client.Recv()
			}
		}
		log.Warnf(context.TODO(), "dataWriter[%v;%v] error on send: %v", w.id, w.ch.id, err)
		w.ch.terminateStreamOnError(err)
		return err
	}
	return nil
}

func (w *dataWriter) Close() error {
	// Don't acquire the locks as Flush will do so.
	l := len(w.buf)
	err := w.Flush()
	if err != nil {
		return errors.Wrapf(err, "dataWriter[%v;%v].Close: error flushing buffer of length %d", w.id, w.ch.id, l)
	}

	// Now acquire the locks since we're sending.
	w.ch.mu.Lock()
	defer w.ch.mu.Unlock()
	delete(w.ch.writers[w.id.instID], w.id.ptransformID)
	msg := &fnpb.Elements{
		Data: []*fnpb.Elements_Data{
			{
				InstructionId: string(w.id.instID),
				TransformId:   w.id.ptransformID,
				// Empty data == sentinel
				IsLast: true,
			},
		},
	}
	return w.send(msg)
}

const largeBufferNotificationThreshold = 1024 * 1024 * 1024 // 1GB

func (w *dataWriter) Flush() error {
	w.ch.mu.Lock()
	defer w.ch.mu.Unlock()

	if w.buf == nil {
		return nil
	}

	msg := &fnpb.Elements{
		Data: []*fnpb.Elements_Data{
			{
				InstructionId: string(w.id.instID),
				TransformId:   w.id.ptransformID,
				Data:          w.buf,
			},
		},
	}
	if l := len(w.buf); l > largeBufferNotificationThreshold {
		log.Infof(context.TODO(), "dataWriter[%v;%v].Flush flushed large buffer of length %d", w.id, w.ch.id, l)
	}
	w.buf = nil
	return w.send(msg)
}

func (w *dataWriter) Write(p []byte) (n int, err error) {
	if len(w.buf)+len(p) > chunkSize {
		l := len(w.buf)
		// We can't fit this message into the buffer. We need to flush the buffer
		if err := w.Flush(); err != nil {
			return 0, errors.Wrapf(err, "datamgr.go [%v]: error flushing buffer of length %d", w.id, l)
		}
	}

	// At this point there's room in the buffer one way or another.
	w.buf = append(w.buf, p...)
	return len(p), nil
}
