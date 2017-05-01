package exec

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
	"io"
	"log"
	"path"
	"reflect"
)

// Discard silently discard all elements. It is implicitly inserted for any
// loose ends in the pipeline.
type Discard struct {
	UID UnitID
}

func (d *Discard) ID() UnitID {
	return d.UID
}

func (_ *Discard) Up(ctx context.Context) error {
	return nil
}

func (_ *Discard) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	return nil
}

func (_ *Discard) Down(ctx context.Context) error {
	return nil
}

func (_ *Discard) String() string {
	return "Discard"
}

// Multiplex is a fan-out node.
type Multiplex struct {
	UID UnitID
	Out []Node
}

func (m *Multiplex) ID() UnitID {
	return m.UID
}

func (m *Multiplex) Up(ctx context.Context) error {
	return Up(ctx, m.Out...)
}

func (m *Multiplex) ProcessElement(ctx context.Context, elm FullValue, values ...ReStream) error {
	for _, out := range m.Out {
		if err := out.ProcessElement(ctx, elm, values...); err != nil {
			return err
		}
	}
	return nil
}

func (m *Multiplex) Down(ctx context.Context) error {
	return Down(ctx, m.Out...)
}

func (m *Multiplex) String() string {
	return fmt.Sprintf("Multiplex. Out:%v", IDs(m.Out...))
}

// NOTE(herohde) 5/1/2017: flatten is implicit in the execution model, so there
// is no need for a separate unit.

// Source is a simplified source. It emits all elements in one invocation.
type Source struct {
	UID  UnitID
	Edge *graph.MultiEdge
	Out  []Node
}

func (n *Source) ID() UnitID {
	return n.UID
}

func (n *Source) Up(ctx context.Context) error {
	return Up(ctx, n.Out...)
}

func (n *Source) Process(ctx context.Context) error {
	// (1) Populate contexts

	fn := n.Edge.DoFn
	args := make([]reflect.Value, len(fn.Param))

	if index, ok := fn.Context(); ok {
		args[index] = reflect.ValueOf(ctx)
	}
	if index, ok := fn.Options(); ok {
		arg := reflect.New(fn.Param[index].T).Elem()
		reflectx.SetTaggedFieldValue(arg, typex.OptTag, reflect.ValueOf(fn.Opt[0]))
		args[index] = arg
	}

	// NOTE: sources have no main or side input. We do not allow direct form to
	// support "single value" sources.

	// (2) Outputs

	out := fn.Params(userfn.FnEmit)
	if len(out) != len(n.Out) {
		return fmt.Errorf("incorrect number of output nodes: %v, want %v", len(n.Out), len(out))
	}
	for i := 0; i < len(out); i++ {
		param := fn.Param[out[i]]
		args[out[i]] = makeEmit(ctx, param.T, n.Out[i])
	}

	// (3) Invoke

	ret := fn.Fn.Call(args)
	if index, ok := fn.Error(); ok && ret[index].Interface() != nil {
		return fmt.Errorf("Source %v failed: %v", n.Edge.DoFn.Name, ret[index].Interface())
	}
	return nil
}

func (n *Source) Down(ctx context.Context) error {
	return Down(ctx, n.Out...)
}

func (n *Source) String() string {
	return fmt.Sprintf("Source[%v] Out:%v", path.Base(n.Edge.DoFn.Name), IDs(n.Out...))
}

// TODO(herohde) 4/26/2017: SideInput representation? We want it to be amenable
// to the State API. For now, just use Stream.

// TODO(herohde) 4/27/2017: What is the semantics and enforcement for side
// input? Immutable? Bug or feature, if touched by user code?

type ParDo struct {
	UID  UnitID
	Edge *graph.MultiEdge
	Side []ReStream
	Out  []Node
}

func (n *ParDo) ID() UnitID {
	return n.UID
}

func (n *ParDo) Up(ctx context.Context) error {
	if err := Up(ctx, n.Out...); err != nil {
		return err
	}

	// TODO: setup, validate side input
	// TODO: specialize based on type?
	return nil
}

func (n *ParDo) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	// TODO: precompute args construction to re-use args and just plug in
	// context and FullValue as well as reset iterators.

	// (1) Populate contexts

	fn := n.Edge.DoFn
	args := make([]reflect.Value, len(fn.Param))

	if index, ok := fn.Context(); ok {
		args[index] = reflect.ValueOf(ctx)
	}
	if index, ok := fn.Options(); ok {
		arg := reflect.New(fn.Param[index].T).Elem()
		reflectx.SetTaggedFieldValue(arg, typex.OptTag, reflect.ValueOf(fn.Opt[0]))
		args[index] = arg
	}

	// (2) Main input from value

	if index, ok := fn.EventTime(); ok {
		args[index] = reflect.ValueOf(value.Timestamp)
	}
	in := fn.Params(userfn.FnValue | userfn.FnIter | userfn.FnReIter)
	i := 0

	args[in[i]] = Convert(value.Elm, fn.Param[i].T)
	i++
	if typex.IsWKV(n.Edge.Input[0].From.Type()) {
		args[in[i]] = Convert(value.Elm2, fn.Param[i].T)
		i++
	}

	for _, iter := range values {
		param := fn.Param[in[i]]

		if param.Kind != userfn.FnIter {
			return fmt.Errorf("GBK result values must be iterable: %v", param)
		}

		// TODO: allow form conversion on GBK results?

		args[in[i]] = makeIter(param.T, iter.Open())
		i++
	}

	// (3) Side inputs and form conversion

	for k, side := range n.Side {
		input := n.Edge.Input[k+1]
		param := fn.Param[in[i]]

		switch input.Kind {
		case graph.Singleton:
			elms, err := ReadAll(side.Open())
			if err != nil {
				return err
			}
			if len(elms) != 1 {
				return fmt.Errorf("singleton side input %v for %v ill-defined", i-1, n.Edge)
			}
			args[in[i]] = Convert(elms[0].Elm, param.T)

		case graph.Slice:
			elms, err := ReadAll(side.Open())
			if err != nil {
				return err
			}
			slice := reflect.MakeSlice(param.T, len(elms), len(elms))
			for i := 0; i < len(elms); i++ {
				slice.Index(i).Set(elms[i].Elm)
			}
			args[in[i]] = slice

		case graph.Iter:
			args[in[i]] = makeIter(param.T, side.Open())

		default:
			panic(fmt.Sprintf("Unexpected side input kind: %v", input))
		}

		i++
	}

	// (4) Outputs

	offset := 0
	hasDirectOutput := len(fn.Returns(userfn.RetValue)) > 0
	if hasDirectOutput {
		offset = 1
	}

	out := fn.Params(userfn.FnEmit)
	for i := 0; i < len(out); i++ {
		param := fn.Param[out[i]]
		args[out[i]] = makeEmit(ctx, param.T, n.Out[i+offset])
	}

	// (5) Invoke

	ret := fn.Fn.Call(args)
	if index, ok := fn.Error(); ok && ret[index].Interface() != nil {
		panic(fmt.Sprintf("Call %v failed: %v", n.Edge.DoFn.Name, ret[index].Interface()))
		// return ret[index].Interface().(error)
	}

	// (6) Direct output, if any

	if hasDirectOutput {
		value := FullValue{}
		if index, ok := fn.OutEventTime(); ok {
			value.Timestamp = ret[index].Interface().(typex.EventTime)
		}

		indices := fn.Returns(userfn.RetValue)
		value.Elm = ret[indices[0]]
		if len(indices) > 1 {
			value.Elm2 = ret[indices[1]]
		}

		if err := n.Out[0].ProcessElement(ctx, value); err != nil {
			panic(err) // see below
		}
	}
	return nil
}

func (n *ParDo) Down(ctx context.Context) error {
	// TODO: teardown
	return Down(ctx, n.Out...)
}

func (n *ParDo) String() string {
	return fmt.Sprintf("ParDo[%v] Out:%v", path.Base(n.Edge.DoFn.Name), IDs(n.Out...))
}

// DataSource is a Root execution unit.
type DataSource struct {
	UID  UnitID
	Edge *graph.MultiEdge

	InstID string
	Source DataReader
	Out    Node
}

func (n *DataSource) ID() UnitID {
	return n.UID
}

func (n *DataSource) Up(ctx context.Context) error {
	return n.Out.Up(ctx)
}

func (n *DataSource) Process(ctx context.Context) error {
	sid := StreamID{Port: *n.Edge.Port, Target: *n.Edge.Target, InstID: n.InstID}

	r, err := n.Source.OpenRead(ctx, sid)
	if err != nil {
		return err
	}
	defer r.Close()

	c := n.Edge.Output[0].To.Coder
	for {
		t, err := DecodeWindowedValueHeader(c, r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("source failed: %v", err)
		}

		switch {
		case coder.IsWGBK(c):
			ck := coder.SkipW(c).Components[0]
			cv := coder.SkipW(c).Components[1]

			// Decode key

			key, err := DecodeElement(ck, r)
			if err != nil {
				return fmt.Errorf("source decode failed: %v", err)
			}
			key.Timestamp = t

			// TODO(herohde) 4/30/2017: the State API will be handle re-iterations
			// and only "small" value streams would be inline. Presumably, that
			// would entail buffering the whole stream. We do that for now.

			var buf []FullValue

			size, err := coder.DecodeInt32(r)
			if err != nil {
				return fmt.Errorf("stream size decoding failed: %v", err)
			}

			if size > -1 {
				// Single chunk stream.

				log.Printf("Fixed size=%v", size)

				for i := int32(0); i < size; i++ {
					value, err := DecodeElement(cv, r)
					if err != nil {
						return fmt.Errorf("stream value decode failed: %v", err)
					}
					buf = append(buf, value)
				}
			} else {
				// Multi-chunked stream.

				for {
					chunk, err := coder.DecodeVarUint64(r)
					if err != nil {
						return fmt.Errorf("stream chunk size decoding failed: %v", err)
					}

					log.Printf("Chunk size=%v", chunk)

					if chunk == 0 {
						break
					}

					for i := uint64(0); i < chunk; i++ {
						value, err := DecodeElement(cv, r)
						if err != nil {
							return fmt.Errorf("stream value decode failed: %v", err)
						}
						buf = append(buf, value)
					}
				}
			}

			values := &FixedReStream{Buf: buf}
			if err := n.Out.ProcessElement(ctx, key, values); err != nil {
				panic(err)
			}

		case coder.IsWCoGBK(c):
			panic("NYI")

		default:
			elm, err := DecodeElement(coder.SkipW(c), r)
			if err != nil {
				return fmt.Errorf("source decode failed: %v", err)
			}
			elm.Timestamp = t

			if err := n.Out.ProcessElement(ctx, elm); err != nil {
				panic(err)
			}
		}
	}
	return nil
}

func (n *DataSource) Down(ctx context.Context) error {
	return n.Out.Down(ctx)
}

func (n *DataSource) String() string {
	sid := StreamID{Port: *n.Edge.Port, Target: *n.Edge.Target, InstID: n.InstID}
	return fmt.Sprintf("DataSource[%v] Out:%v", sid, n.Out.ID())
}

// DataSink is a Node.
type DataSink struct {
	UID  UnitID
	Edge *graph.MultiEdge

	InstID string
	Sink   DataWriter

	w io.WriteCloser
}

func (n *DataSink) ID() UnitID {
	return n.UID
}

func (n *DataSink) Up(ctx context.Context) error {
	sid := StreamID{Port: *n.Edge.Port, Target: *n.Edge.Target, InstID: n.InstID}

	w, err := n.Sink.OpenWrite(ctx, sid)
	if err != nil {
		return err
	}
	n.w = w
	return nil
}

func (n *DataSink) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	c := n.Edge.Input[0].From.Coder

	if err := EncodeWindowedValueHeader(c, value.Timestamp, n.w); err != nil {
		return err
	}
	return EncodeElement(coder.SkipW(c), value, n.w)
}

func (n *DataSink) Down(ctx context.Context) error {
	return n.w.Close()
}

func (n *DataSink) String() string {
	sid := StreamID{Port: *n.Edge.Port, Target: *n.Edge.Target, InstID: n.InstID}
	return fmt.Sprintf("DataSink[%v]", sid)
}

func makeIter(t reflect.Type, s Stream) reflect.Value {
	types, ok := userfn.UnfoldIter(t)
	if !ok {
		panic(fmt.Sprintf("illegal iter type: %v", t))
	}
	return reflect.MakeFunc(t, func(args []reflect.Value) []reflect.Value {
		elm, err := s.Read()
		if err != nil {
			if err == io.EOF {
				return []reflect.Value{reflect.ValueOf(false)}
			}
			panic(fmt.Sprintf("Broken stream: %v", err))
		}

		// We expect 1-3 out parameters: func (*int, *string) bool.

		isKey := true
		for i, t := range types {
			var v reflect.Value
			switch {
			case t == typex.EventTimeType:
				v = reflect.ValueOf(elm.Timestamp)
			case isKey:
				v = Convert(elm.Elm, t)
				isKey = false
			default:
				v = Convert(elm.Elm2, t)
			}
			args[i].Elem().Set(v)
		}
		return []reflect.Value{reflect.ValueOf(true)}
	})
}

func makeEmit(ctx context.Context, t reflect.Type, n Node) reflect.Value {
	types, ok := userfn.UnfoldEmit(t)
	if !ok {
		panic(fmt.Sprintf("illegal emit type: %v", t))
	}
	return reflect.MakeFunc(t, func(args []reflect.Value) []reflect.Value {
		value := FullValue{}
		isKey := true
		for i, t := range types {
			switch {
			case t == typex.EventTimeType:
				value.Timestamp = args[i].Interface().(typex.EventTime)
			case isKey:
				value.Elm = args[i]
				isKey = false
			default:
				value.Elm2 = args[i]
			}
		}

		// log.Printf("Emit to %v: %v", n.ID(), value)

		if err := n.ProcessElement(ctx, value); err != nil {
			// TODO: use panic to unwind errors _through_ user code? Or capture
			// stack trace and store in node until the end?
			panic(err)
		}
		return nil
	})
}
