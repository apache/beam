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

package direct

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

// buffer buffers all input and notifies on FinishBundle. It is also a ReStream.
// It is used as a guard for the wait node to buffer data used as side input.
type buffer struct {
	uid    exec.UnitID
	next   exec.UnitID // debug only
	read   exec.UnitID // debug only
	notify func(ctx context.Context) error

	buf  []exec.FullValue
	done bool
}

func (n *buffer) ID() exec.UnitID {
	return n.uid
}

func (n *buffer) Up(ctx context.Context) error {
	return nil
}

func (n *buffer) StartBundle(ctx context.Context, id string, data exec.DataManager) error {
	n.buf = nil
	n.done = false
	return nil
}

func (n *buffer) ProcessElement(ctx context.Context, elm exec.FullValue, values ...exec.ReStream) error {
	n.buf = append(n.buf, elm)
	return nil
}

func (n *buffer) FinishBundle(ctx context.Context) error {
	n.done = true
	return n.notify(ctx)
}

func (n *buffer) Down(ctx context.Context) error {
	return nil
}

func (n *buffer) Open() exec.Stream {
	if !n.done {
		panic(fmt.Sprintf("buffer[%v] incomplete: %v", n.uid, len(n.buf)))
	}
	return &exec.FixedStream{Buf: n.buf}
}

func (n *buffer) String() string {
	return fmt.Sprintf("buffer[%v]. wait:%v Out:%v", n.uid, n.next, n.read)
}

// wait buffers all input until the guard condition is triggered. It then
// proceeds normally. The main purpose is to delay bundle processing until side input
// is ready.
type wait struct {
	UID  exec.UnitID
	need int // guards needed
	next exec.Node

	instID string
	mgr    exec.DataManager

	buf   []exec.FullValue
	ready int  // guards ready
	done  bool // FinishBundle called for main input?
}

func (w *wait) ID() exec.UnitID {
	return w.UID
}

func (w *wait) notify(ctx context.Context) error {
	if w.ready == w.need {
		panic("Too many notify")
	}
	w.ready++
	if w.ready < w.need {
		return nil
	}

	// All ready: continue the processing. We may or may not have buffered
	// all the data. If not, wait is a pass-through going forward.

	log.Debugf(ctx, "wait[%v] unblocked w/ %v [%v]", w.UID, len(w.buf), w.done)

	if err := w.next.StartBundle(ctx, w.instID, w.mgr); err != nil {
		return err
	}
	for _, elm := range w.buf {
		if err := w.next.ProcessElement(ctx, elm); err != nil {
			return err
		}
	}
	w.buf = nil
	if w.done {
		if err := w.next.FinishBundle(ctx); err != nil {
			return err
		}
	}

	log.Debugf(ctx, "wait[%v] done", w.UID)
	return nil
}

func (w *wait) Up(ctx context.Context) error {
	return nil
}

func (w *wait) StartBundle(ctx context.Context, id string, data exec.DataManager) error {
	return nil // done in notify
}

func (w *wait) ProcessElement(ctx context.Context, elm exec.FullValue, values ...exec.ReStream) error {
	if w.ready < w.need {
		// log.Printf("buffer[%v]: %v", w.UID, elm)
		w.buf = append(w.buf, elm)
		return nil
	}

	// log.Printf("NOT buffer[%v]: %v", w.UID, elm)
	return w.next.ProcessElement(ctx, elm)
}

func (w *wait) FinishBundle(ctx context.Context) error {
	if w.ready < w.need || w.done {
		w.done = true
		return nil
	}
	w.done = true
	return w.next.FinishBundle(ctx)

}

func (w *wait) Down(ctx context.Context) error {
	return nil
}

func (w *wait) String() string {
	return fmt.Sprintf("wait[%v] Out:%v", w.need, w.next.ID())
}
