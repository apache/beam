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
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/harness/statecache"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/proto"
)

// ScopedStateReader scopes the global gRPC state manager to a single instruction
// for side input use. The indirection makes it easier to control access.
type ScopedStateReader struct {
	mgr    *StateChannelManager
	instID instructionID

	closed bool
	mu     sync.Mutex

	cache *statecache.SideInputCache
}

// NewScopedStateReader returns a ScopedStateReader for the given instruction.
func NewScopedStateReader(mgr *StateChannelManager, instID instructionID) *ScopedStateReader {
	return &ScopedStateReader{mgr: mgr, instID: instID, cache: nil}
}

// NewScopedStateReaderWithCache returns a ScopedState reader for the given instruction with a pointer to a SideInputCache.
func NewScopedStateReaderWithCache(mgr *StateChannelManager, instID instructionID, cache *statecache.SideInputCache) *ScopedStateReader {
	return &ScopedStateReader{mgr: mgr, instID: instID, cache: cache}
}

// OpenIterableSideInput opens a byte stream for reading iterable side input
func (s *ScopedStateReader) OpenIterableSideInput(ctx context.Context, id exec.StreamID, sideInputID string, w []byte) (io.ReadCloser, error) {
	return s.openReader(ctx, id, func(ch *StateChannel) *stateKeyReader {
		return newIterableSideInputReader(ch, id, sideInputID, s.instID, w)
	})
}

// OpenMultiMapSideInput opens a byte stream for reading multimap side input.
func (s *ScopedStateReader) OpenMultiMapSideInput(ctx context.Context, id exec.StreamID, sideInputID string, key, w []byte) (io.ReadCloser, error) {
	return s.openReader(ctx, id, func(ch *StateChannel) *stateKeyReader {
		return newSideInputReader(ch, id, sideInputID, s.instID, key, w)
	})
}

// OpenIterable opens a byte stream for reading unwindowed iterables from the runner.
func (s *ScopedStateReader) OpenIterable(ctx context.Context, id exec.StreamID, key []byte) (io.ReadCloser, error) {
	return s.openReader(ctx, id, func(ch *StateChannel) *stateKeyReader {
		return newRunnerReader(ch, s.instID, key)
	})
}

// GetSideInputCache returns a pointer to the SideInputCache being used by the SDK harness.
func (s *ScopedStateReader) GetSideInputCache() exec.SideCache {
	return s.cache
}

func (s *ScopedStateReader) openReader(ctx context.Context, id exec.StreamID, readerFn func(*StateChannel) *stateKeyReader) (*stateKeyReader, error) {
	ch, err := s.open(ctx, id.Port)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.Errorf("instruction %v no longer processing", s.instID)
	}
	ret := readerFn(ch)
	s.mu.Unlock()
	return ret, nil
}

func (s *ScopedStateReader) open(ctx context.Context, port exec.Port) (*StateChannel, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.Errorf("instruction %v no longer processing", s.instID)
	}
	localMgr := s.mgr
	s.mu.Unlock()

	return localMgr.Open(ctx, port) // don't hold lock over potentially slow operation
}

// Close closes all open readers.
func (s *ScopedStateReader) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mgr = nil
	s.mu.Unlock()
	return nil
}

type stateKeyReader struct {
	instID instructionID
	key    *fnpb.StateKey

	token []byte
	buf   []byte
	eof   bool

	ch     *StateChannel
	closed bool
	mu     sync.Mutex
}

func newSideInputReader(ch *StateChannel, id exec.StreamID, sideInputID string, instID instructionID, k, w []byte) *stateKeyReader {
	key := &fnpb.StateKey{
		Type: &fnpb.StateKey_MultimapSideInput_{
			MultimapSideInput: &fnpb.StateKey_MultimapSideInput{
				TransformId: id.PtransformID,
				SideInputId: sideInputID,
				Window:      w,
				Key:         k,
			},
		},
	}
	return &stateKeyReader{
		instID: instID,
		key:    key,
		ch:     ch,
	}
}

func newIterableSideInputReader(ch *StateChannel, id exec.StreamID, sideInputID string, instID instructionID, w []byte) *stateKeyReader {
	key := &fnpb.StateKey{
		Type: &fnpb.StateKey_IterableSideInput_{
			IterableSideInput: &fnpb.StateKey_IterableSideInput{
				TransformId: id.PtransformID,
				SideInputId: sideInputID,
				Window:      w,
			},
		},
	}

	return &stateKeyReader{
		instID: instID,
		key:    key,
		ch:     ch,
	}
}

func newRunnerReader(ch *StateChannel, instID instructionID, k []byte) *stateKeyReader {
	key := &fnpb.StateKey{
		Type: &fnpb.StateKey_Runner_{
			Runner: &fnpb.StateKey_Runner{
				Key: k,
			},
		},
	}
	return &stateKeyReader{
		instID: instID,
		key:    key,
		ch:     ch,
	}
}

func (r *stateKeyReader) Read(buf []byte) (int, error) {
	if r.buf == nil {
		if r.eof {
			return 0, io.EOF
		}

		// Buffer empty. Get next segment.

		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return 0, errors.New("side input closed")
		}
		localChannel := r.ch
		r.mu.Unlock()

		req := &fnpb.StateRequest{
			// Id: set by StateChannel
			InstructionId: string(r.instID),
			StateKey:      r.key,
			Request: &fnpb.StateRequest_Get{
				Get: &fnpb.StateGetRequest{
					ContinuationToken: r.token,
				},
			},
		}
		resp, err := localChannel.Send(req)
		if err != nil {
			r.Close()
			return 0, err
		}
		get := resp.GetGet()
		if get == nil { // no data associated with this segment.
			r.eof = true
			if err := r.Close(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		r.token = get.GetContinuationToken()
		r.buf = get.GetData()

		if r.token == nil {
			r.eof = true // no token == this is the last segment.
		}
	}

	n := copy(buf, r.buf)

	switch {
	case n == 0 && len(buf) != 0 && r.eof:
		// If no data was copied, and this is the last segment anyway, return EOF now.
		// This prevent spurious zero elements.
		r.buf = nil
		if err := r.Close(); err != nil {
			return 0, err
		}
		return 0, io.EOF
	case len(r.buf) == n:
		r.buf = nil
	default:
		r.buf = r.buf[n:]
	}
	return n, nil
}

func (r *stateKeyReader) Close() error {
	r.mu.Lock()
	r.closed = true
	r.ch = nil // StateChannels might be re-used if they're ok, so don't close them here.
	r.mu.Unlock()
	return nil
}

// StateChannelManager manages data channels over the State API. A fixed number of channels
// are generally used, each managing multiple logical byte streams. Thread-safe.
type StateChannelManager struct {
	ports map[string]*StateChannel
	mu    sync.Mutex
}

// Open opens a R/W StateChannel over the given port.
func (m *StateChannelManager) Open(ctx context.Context, port exec.Port) (*StateChannel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ports == nil {
		m.ports = make(map[string]*StateChannel)
	}
	if con, ok := m.ports[port.URL]; ok {
		return con, nil
	}

	ch, err := newStateChannel(ctx, port)
	if err != nil {
		return nil, err
	}
	ch.forceRecreate = func(id string, err error) {
		log.Warnf(ctx, "forcing StateChannel[%v] reconnection on port %v due to %v", id, port, err)
		m.mu.Lock()
		delete(m.ports, port.URL)
		m.mu.Unlock()
	}
	m.ports[port.URL] = ch
	return ch, nil
}

type stateClient interface {
	Send(*fnpb.StateRequest) error
	Recv() (*fnpb.StateResponse, error)
}

// StateChannel manages state transactions over a single gRPC connection.
// It does not need to track readers and writers as carefully as the
// DataChannel, because the state protocol is request-based.
type StateChannel struct {
	id     string
	client stateClient

	requests      chan *fnpb.StateRequest
	nextRequestNo int32

	responses map[string]chan<- *fnpb.StateResponse
	mu        sync.Mutex

	// a closure that forces the state manager to recreate this stream.
	forceRecreate func(id string, err error)
	cancelFn      context.CancelFunc
	closedErr     error
	DoneCh        <-chan struct{}
}

func (c *StateChannel) terminateStreamOnError(err error) {
	c.mu.Lock()
	if c.forceRecreate != nil {
		c.closedErr = err
		c.forceRecreate(c.id, err)
		c.forceRecreate = nil
	}
	// Cancelling context after forcing recreation to ensure closedErr is set.
	c.cancelFn()
	c.mu.Unlock()
}

func newStateChannel(ctx context.Context, port exec.Port) (*StateChannel, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	cc, err := dial(ctx, port.URL, 15*time.Second)
	if err != nil {
		cancelFn()
		return nil, errors.Wrapf(err, "failed to connect to state service %v", port.URL)
	}
	client, err := fnpb.NewBeamFnStateClient(cc).State(ctx)
	if err != nil {
		cc.Close()
		cancelFn()
		return nil, errors.Wrapf(err, "failed to create state client %v", port.URL)
	}
	return makeStateChannel(ctx, cancelFn, port.URL, client), nil
}

func makeStateChannel(ctx context.Context, cancelFn context.CancelFunc, id string, client stateClient) *StateChannel {
	ret := &StateChannel{
		id:        id,
		client:    client,
		requests:  make(chan *fnpb.StateRequest, 10),
		responses: make(map[string]chan<- *fnpb.StateResponse),
		cancelFn:  cancelFn,
		DoneCh:    ctx.Done(),
	}
	go ret.read(ctx)
	go ret.write(ctx)

	return ret
}

func (c *StateChannel) read(ctx context.Context) {
	for {
		// Closing the context will have an error return from this call.
		msg, err := c.client.Recv()
		if err != nil {
			c.terminateStreamOnError(err)
			if err == io.EOF {
				log.Warnf(ctx, "StateChannel[%v].read: closed", c.id)
				return
			}
			log.Errorf(ctx, "StateChannel[%v].read bad: %v", c.id, err)
			return
		}

		c.mu.Lock()
		ch, ok := c.responses[msg.Id]
		delete(c.responses, msg.Id)
		c.mu.Unlock()
		if !ok {
			// This can happen if Send returns an error that write handles, but
			// the message was actually sent.
			log.Errorf(ctx, "StateChannel[%v].read: no consumer for state response: %v", c.id, proto.MarshalTextString(msg))
			continue
		}

		select {
		case ch <- msg:
			// ok
		default:
			panic(fmt.Sprintf("StateChannel[%v].read: failed to consume state response: %v", c.id, proto.MarshalTextString(msg)))
		}
	}
}

func (c *StateChannel) write(ctx context.Context) {
	var err error
	var id string
	for {
		var req *fnpb.StateRequest
		select {
		case req = <-c.requests:
		case <-c.DoneCh: // Close the goroutine on context cancel.
			return
		}
		err = c.client.Send(req)
		if err != nil {
			id = req.Id
			break // non-nil errors mean the stream is broken and can't be re-used.
		}
	}

	if err == io.EOF {
		log.Warnf(ctx, "StateChannel[%v].write EOF on send; fetching real error", c.id)
		err = nil
		for err == nil {
			// Per GRPC stream documentation, if there's an EOF, we must call Recv
			// until a non-nil error is returned, to ensure resources are cleaned up.
			// https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream
			_, err = c.client.Recv()
		}
	}
	log.Errorf(ctx, "StateChannel[%v].write error on send: %v", c.id, err)

	// Failed to send. Return error & unblock Send.
	c.mu.Lock()
	ch, ok := c.responses[id]
	delete(c.responses, id)
	c.mu.Unlock()
	// Clean up everything else, this stream is done.
	c.terminateStreamOnError(err)

	if ok {
		ch <- &fnpb.StateResponse{Id: id, Error: fmt.Sprintf("StateChannel[%v].write failed to send: %v", c.id, err)}
	}
}

// Send sends a state request and returns the response.
func (c *StateChannel) Send(req *fnpb.StateRequest) (*fnpb.StateResponse, error) {
	id := fmt.Sprintf("r%v", atomic.AddInt32(&c.nextRequestNo, 1))
	req.Id = id

	ch := make(chan *fnpb.StateResponse, 1)
	c.mu.Lock()
	if c.closedErr != nil {
		defer c.mu.Unlock()
		return nil, errors.Wrapf(c.closedErr, "StateChannel[%v].Send(%v): channel closed due to: %v", c.id, id, c.closedErr)
	}
	c.responses[id] = ch
	c.mu.Unlock()

	c.requests <- req

	var resp *fnpb.StateResponse
	select {
	case resp = <-ch:
	case <-c.DoneCh:
		c.mu.Lock()
		defer c.mu.Unlock()
		return nil, errors.Wrapf(c.closedErr, "StateChannel[%v].Send(%v): context canceled", c.id, id)
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp, nil
}
