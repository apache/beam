package local

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/exec"
)

// Impulse emits its single element in one invocation.
type Impulse struct {
	UID  exec.UnitID
	Edge *graph.MultiEdge
	Out  []exec.Node
}

func (n *Impulse) ID() exec.UnitID {
	return n.UID
}

func (n *Impulse) Up(ctx context.Context) error {
	return exec.Up(ctx, n.Out...)
}

func (n *Impulse) Process(ctx context.Context) error {
	value := exec.FullValue{Elm: reflect.ValueOf(n.Edge.Value)}
	// TODO(herohde) 6/23/2017: set value.Timestamp

	for _, out := range n.Out {
		if err := out.ProcessElement(ctx, value); err != nil {
			panic(err)
		}
	}
	return nil
}

func (n *Impulse) Down(ctx context.Context) error {
	return exec.Down(ctx, n.Out...)
}

func (n *Impulse) String() string {
	return fmt.Sprintf("Impulse[%v]", len(n.Edge.Value))
}

type group struct {
	key    exec.FullValue
	values []exec.FullValue
}

// GBK buffers all input and continues on Down.
type GBK struct {
	UID  exec.UnitID
	Edge *graph.MultiEdge
	Out  exec.Node

	m map[string]*group
}

func (n *GBK) ID() exec.UnitID {
	return n.UID
}

func (n *GBK) Up(ctx context.Context) error {
	n.m = make(map[string]*group)
	return nil
}

func (n *GBK) ProcessElement(ctx context.Context, elm exec.FullValue, _ ...exec.ReStream) error {
	var buf bytes.Buffer
	if err := exec.EncodeElement(coder.SkipW(n.Edge.Input[0].From.Coder).Components[0], exec.FullValue{Elm: elm.Elm}, &buf); err != nil {
		return fmt.Errorf("failed to encode key %v for GBK: %v", elm, err)
	}
	key := buf.String()

	g, ok := n.m[key]
	if !ok {
		g = &group{key: exec.FullValue{Elm: elm.Elm, Timestamp: elm.Timestamp}}
		n.m[key] = g
	}
	g.values = append(g.values, exec.FullValue{Elm: elm.Elm2, Timestamp: elm.Timestamp})
	return nil
}

func (n *GBK) Down(ctx context.Context) error {
	if err := n.Out.Up(ctx); err != nil {
		return err
	}
	for key, g := range n.m {
		values := &exec.FixedReStream{Buf: g.values}
		if err := n.Out.ProcessElement(ctx, g.key, values); err != nil {
			return err
		}
		delete(n.m, key)
	}
	return n.Out.Down(ctx)
}

func (n *GBK) String() string {
	return fmt.Sprintf("GBK. Out:%v", n.Out.ID())
}

// Buffer buffers all input and notifies on Down. It is also a ReStream.
type Buffer struct {
	UID  exec.UnitID
	next exec.UnitID
	read exec.UnitID

	buf    []exec.FullValue
	notify func(ctx context.Context) error
	done   bool
}

func (n *Buffer) ID() exec.UnitID {
	return n.UID
}

func (n *Buffer) Up(ctx context.Context) error {
	return nil
}

func (n *Buffer) ProcessElement(ctx context.Context, elm exec.FullValue, values ...exec.ReStream) error {
	n.buf = append(n.buf, elm)
	return nil
}

func (n *Buffer) Down(ctx context.Context) error {
	n.done = true
	return n.notify(ctx)
}

func (n *Buffer) Open() exec.Stream {
	if !n.done {
		panic("Buffer incompete")
	}
	return &exec.FixedStream{Buf: n.buf}
}

func (n *Buffer) String() string {
	return fmt.Sprintf("Buffer. Wait:%v Out:%v", n.next, n.read)
}

// Wait buffers all input until the guard condition is triggered. It then
// proceeds normally. The main purpose is to delay processing until side input
// is ready.
type Wait struct {
	UID  exec.UnitID
	buf  []exec.FullValue
	next exec.Node

	ready int  // guards ready
	need  int  // guards needed
	down  bool // down called?
}

func (w *Wait) ID() exec.UnitID {
	return w.UID
}

func (w *Wait) notify(ctx context.Context) error {
	if w.ready == w.need {
		panic("Too many notify")
	}
	w.ready++
	if w.ready < w.need {
		return nil
	}

	// All ready: continue the processing. We may or may not have buffered
	// all the data. If not, Wait is a pass-through going forward.

	log.Printf("Wait[%v] unblocked w/ %v[%v]", w.UID, len(w.buf), w.down)

	if err := w.next.Up(ctx); err != nil {
		return err
	}
	for _, elm := range w.buf {
		if err := w.next.ProcessElement(ctx, elm); err != nil {
			return err
		}
	}
	w.buf = nil
	if w.down {
		if err := w.next.Down(ctx); err != nil {
			return err
		}
	}

	log.Printf("Wait[%v] done", w.UID)
	return nil
}

func (w *Wait) Up(ctx context.Context) error {
	return nil // done in notify
}

func (w *Wait) ProcessElement(ctx context.Context, elm exec.FullValue, values ...exec.ReStream) error {
	if w.ready < w.need {
		// log.Printf("Buffer[%v]: %v", w.UID, elm)
		w.buf = append(w.buf, elm)
		return nil
	}

	// log.Printf("NOT buffer[%v]: %v", w.UID, elm)
	return w.next.ProcessElement(ctx, elm)
}

func (w *Wait) Down(ctx context.Context) error {
	if w.ready < w.need || w.down {
		w.down = true
		return nil
	}
	w.down = true
	return w.next.Down(ctx)
}

func (w *Wait) String() string {
	return fmt.Sprintf("Wait[%v] Out:%v", w.need, w.next.ID())
}
