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

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/proto"
)

// ScopedSideInputReader scopes the global gRPC state manager to a single instruction
// for side input use. The indirection makes it easier to control access.
type ScopedSideInputReader struct {
	mgr    *StateChannelManager
	instID string

	opened []io.Closer // track open readers to force close all
	closed bool
	mu     sync.Mutex
}

// NewScopedSideInputReader returns a ScopedSideInputReader for the given instruction.
func NewScopedSideInputReader(mgr *StateChannelManager, instID string) *ScopedSideInputReader {
	return &ScopedSideInputReader{mgr: mgr, instID: instID}
}

func (s *ScopedSideInputReader) Open(ctx context.Context, id exec.StreamID, sideInputID string, key, w []byte) (io.ReadCloser, error) {
	ch, err := s.open(ctx, id.Port)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.Errorf("instruction %v no longer processing", s.instID)
	}
	ret := newSideInputReader(ch, id, sideInputID, s.instID, key, w)
	s.opened = append(s.opened, ret)
	s.mu.Unlock()
	return ret, nil
}

func (s *ScopedSideInputReader) open(ctx context.Context, port exec.Port) (*StateChannel, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.Errorf("instruction %v no longer processing", s.instID)
	}
	local := s.mgr
	s.mu.Unlock()

	return local.Open(ctx, port) // don't hold lock over potentially slow operation
}

func (s *ScopedSideInputReader) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mgr = nil
	for _, r := range s.opened {
		r.Close() // force close all opened readers
	}
	s.opened = nil
	s.mu.Unlock()
	return nil
}

type sideInputReader struct {
	instID string
	key    *pb.StateKey

	token []byte
	buf   []byte
	eof   bool

	ch     *StateChannel
	closed bool
	mu     sync.Mutex
}

func newSideInputReader(ch *StateChannel, id exec.StreamID, sideInputID string, instID string, k, w []byte) *sideInputReader {
	key := &pb.StateKey{
		Type: &pb.StateKey_MultimapSideInput_{
			MultimapSideInput: &pb.StateKey_MultimapSideInput{
				PtransformId: id.PtransformID,
				SideInputId:  sideInputID,
				Window:       w,
				Key:          k,
			},
		},
	}
	return &sideInputReader{
		instID: instID,
		key:    key,
		ch:     ch,
	}
}

func (r *sideInputReader) Read(buf []byte) (int, error) {
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
		local := r.ch
		r.mu.Unlock()

		req := &pb.StateRequest{
			// Id: set by channel
			InstructionReference: r.instID,
			StateKey:             r.key,
			Request: &pb.StateRequest_Get{
				Get: &pb.StateGetRequest{
					ContinuationToken: r.token,
				},
			},
		}
		resp, err := local.Send(req)
		if err != nil {
			return 0, err
		}
		get := resp.GetGet()
		r.token = get.ContinuationToken
		r.buf = get.Data

		if r.token == nil {
			r.eof = true // no token == this is the last segment.
		}
	}

	n := copy(buf, r.buf)

	if len(r.buf) == n {
		r.buf = nil
	} else {
		r.buf = r.buf[n:]
	}
	return n, nil
}

func (r *sideInputReader) Close() error {
	r.mu.Lock()
	r.closed = true
	r.ch = nil
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
	m.ports[port.URL] = ch
	return ch, nil
}

// StateChannel manages state transactions over a single gRPC connection.
// It does not need to track readers and writers as carefully as the
// DataChannel, because the state protocol is request-based.
type StateChannel struct {
	id     string
	client pb.BeamFnState_StateClient

	requests      chan *pb.StateRequest
	nextRequestNo int32

	responses map[string]chan<- *pb.StateResponse
	mu        sync.Mutex
}

func newStateChannel(ctx context.Context, port exec.Port) (*StateChannel, error) {
	cc, err := dial(ctx, port.URL, 15*time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}
	client, err := pb.NewBeamFnStateClient(cc).State(ctx)
	if err != nil {
		cc.Close()
		return nil, errors.Wrap(err, "failed to connect to data service")
	}

	ret := &StateChannel{
		id:        port.URL,
		client:    client,
		requests:  make(chan *pb.StateRequest, 10),
		responses: make(map[string]chan<- *pb.StateResponse),
	}
	go ret.read(ctx)
	go ret.write(ctx)

	return ret, nil
}

func (c *StateChannel) read(ctx context.Context) {
	for {
		msg, err := c.client.Recv()
		if err != nil {
			if err == io.EOF {
				// TODO(herohde) 10/12/2017: can this happen before shutdown? Reconnect?
				log.Warnf(ctx, "StateChannel %v closed", c.id)
				return
			}
			panic(errors.Wrapf(err, "state channel %v bad", c.id))
		}

		c.mu.Lock()
		ch, ok := c.responses[msg.Id]
		delete(c.responses, msg.Id)
		c.mu.Unlock()
		if !ok {
			// This can happen if Send returns an error that write handles, but
			// the message was actually sent.
			log.Errorf(ctx, "no consumer for state response: %v", proto.MarshalTextString(msg))
			continue
		}

		select {
		case ch <- msg:
			// ok
		default:
			panic(fmt.Sprintf("failed to consume state response: %v", proto.MarshalTextString(msg)))
		}
	}
}

func (c *StateChannel) write(ctx context.Context) {
	for req := range c.requests {
		err := c.client.Send(req)
		if err == nil {
			continue // ok
		}

		// Failed to send. Return error.
		c.mu.Lock()
		ch, ok := c.responses[req.Id]
		delete(c.responses, req.Id)
		c.mu.Unlock()

		if ok {
			ch <- &pb.StateResponse{Id: req.Id, Error: fmt.Sprintf("failed to send: %v", err)}
		} // else ignore: already received response due to race
	}
}

// Send sends a state request and returns the response.
func (c *StateChannel) Send(req *pb.StateRequest) (*pb.StateResponse, error) {
	id := fmt.Sprintf("r%v", atomic.AddInt32(&c.nextRequestNo, 1))
	req.Id = id

	ch := make(chan *pb.StateResponse, 1)
	c.mu.Lock()
	c.responses[id] = ch
	c.mu.Unlock()

	c.requests <- req

	// TODO(herohde) 7/21/2018: time out?
	resp := <-ch
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp, nil
}
