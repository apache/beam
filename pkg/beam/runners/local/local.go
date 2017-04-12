package local

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/coderx"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/typex"
	"log"
	"reflect"
	"sync"
	"time"
)

// GOOD: Run doesn't have to be a method on the pipeline. Each runner could simply have
// a function to execute a job: dataflow.Execute(p, options). A wrapper that knows about
// all runners would then pick one from a flag, say.

// GOOD: Would remove some of the job-level config from the runtime config. For example,
// whether autoscaling enabled is an option solely meaningful to the dataflow runner at
// submission time? Java has the superset of everything, which is less then awesome.

// NEUTRAL: We don't validate options until execution time. Java does syntax up front, but
// otherwise can't validate completeness until the runner is selected. Given that the transforms
// are determined at pipeline construction time, they can't be validated earlier (without
// lots of semantically-empty defaults that partly defeats the value of validation anyway).

type chanID struct {
	from  int // Node
	to    int // MultiEdge
	input int // input index
}

func Execute(ctx context.Context, p *beam.Pipeline) error {
	edges, err := p.FakeBuild()
	if err != nil {
		return err
	}
	return ExecuteBundle(ctx, edges)
}

func ExecuteBundle(ctx context.Context, edges map[int]*graph.MultiEdge) error {
	// (1) create channels for each edge-to-edge connection. We need to insert
	// multiplexing for outputs that are consumed by multiple transformations.

	write := make(map[int]reflect.Value)   // node id -> chan T, T is produced input.
	read := make(map[chanID]reflect.Value) // chanId -> chan T, T is accepted input.

	consumers := make(map[int][]chanID)
	for _, edge := range edges {
		for i, in := range edge.Input {
			from := in.From.ID()
			consumers[from] = append(consumers[from], chanID{from, edge.ID(), i})
		}

		for _, out := range edge.Output {
			t := reflect.ChanOf(reflect.BothDir, out.T)
			write[out.To.ID()] = reflect.MakeChan(t, 100)
		}
	}

	for id, list := range consumers {
		if len(list) == 1 {
			input := edges[list[0].to].Input[list[0].input]
			read[list[0]] = shim(write[id], input.From.Coder, input.From.T, input.T)
			continue
		}

		// Insert multiplexing. This output has multiple consumers, so we need to
		// duplicate and buffer the data.

		var dup []reflect.Value
		for i := 0; i < len(list); i++ {
			dup = append(dup, reflect.MakeChan(write[id].Type(), 100))
		}

		go multiplex(write[id], dup)

		for i, elm := range list {
			ch := buffer(dup[i])
			input := edges[elm.to].Input[elm.input]
			read[elm] = shim(ch, input.From.Coder, input.From.T, input.T)
		}
	}

	// (2) start gorutines to execute each step

	var wg sync.WaitGroup

	for _, edge := range edges {
		var in []reflect.Value
		for i, node := range edge.Input {
			in = append(in, read[chanID{node.From.ID(), edge.ID(), i}])
		}
		var out []reflect.Value
		for _, node := range edge.Output {
			out = append(out, write[node.To.ID()])
		}

		switch edge.Op {
		case graph.Source, graph.ParDo, graph.External:
			dofn := edge.DoFn
			data := edge.Data

			wg.Add(1)
			go func() {
				defer wg.Done()
				call(ctx, dofn, data, in, out)
			}()

		case graph.GBK:
			coder := edge.Input[0].From.Coder

			wg.Add(1)
			go func() {
				defer wg.Done()
				gbk(ctx, coder, in[0], out[0])
			}()

		default:
			log.Fatalf("Unexpected opcode: %v", edge)
		}
	}

	wg.Wait()
	return nil
}

func multiplex(ch reflect.Value, out []reflect.Value) {
	defer closeout(out)

	for {
		val, ok := ch.Recv()
		if !ok {
			break
		}

		// TODO(herohde): we would need to dup the values from a GBK as well.
		for i := 0; i < len(out); i++ {
			out[i].Send(val)
		}
	}
}

type bufChan struct {
	buf  []reflect.Value
	done bool
	mu   sync.Mutex
}

func (ch *bufChan) Write(val reflect.Value) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.buf = append(ch.buf, val)
}

func (ch *bufChan) Close() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.done = true
}

func (ch *bufChan) Read() ([]reflect.Value, bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	tmp := ch.buf
	ch.buf = nil

	return tmp, len(tmp) == 0 && ch.done
}

func buffer(in reflect.Value) reflect.Value {
	// NOTE(herohde): side input must be fully available and the different
	// rates of consumption implies unbounded buffering, when split. In the
	// service all this is handled for us.

	ch := &bufChan{}
	go func() {
		defer ch.Close()

		for {
			val, ok := in.Recv()
			if !ok {
				break
			}
			ch.Write(val)
		}
	}()

	ret := reflect.MakeChan(in.Type(), 100)
	go func() {
		defer ret.Close()

		for {
			buf, done := ch.Read()
			if done {
				break
			}

			if len(buf) == 0 {
				time.Sleep(100 * time.Millisecond) // Lame
			} else {
				for _, elm := range buf {
					ret.Send(elm)
				}
			}
		}
	}()

	return ret
}

func call(ctx context.Context, userfn *graph.UserFn, data interface{}, in, out []reflect.Value) {
	defer closeout(out)

	log.Printf("Call: %v [Data: %v : %v]", userfn, data, reflect.TypeOf(data))

	userCtx, hasCtx := makeContext(userfn, data)

	switch {
	case !userfn.HasDirectInput() && !userfn.HasDirectOutput():
		// (1) chan only

		var args []reflect.Value
		if hasCtx {
			args = append(args, userCtx)
		}
		args = append(args, in...)
		args = append(args, out...)

		ret := userfn.Fn.Call(args)
		if index, ok := userfn.HasError(); ok && !ret[index].IsNil() {
			log.Printf("UserFn error: %v", ret[index].Interface())
		}

	case userfn.HasDirectInput() && !userfn.HasDirectOutput():
		// (1) input de-chan. Must be KV or GBK type.

		primary := in[0]
		rest := append(in[1:], out...)

		keyFn, valueFn := reflectx.UnpackFn(primary.Type().Elem())

		for {
			val, ok := primary.Recv()
			if !ok {
				break
			}

			var args []reflect.Value
			if hasCtx {
				args = append(args, userCtx)
			}
			args = append(args, keyFn(val), valueFn(val))
			args = append(args, rest...)

			ret := userfn.Fn.Call(args)
			if index, ok := userfn.HasError(); ok && !ret[index].IsNil() {
				log.Printf("UserFn error: %v", ret[index].Interface())
			}
		}

	default:
		// TODO(herohde): handle direct output
		log.Fatalf("Invalid userfn: %v", userfn)
	}

	// log.Printf("Call exit: %v", userfn)
}

func makeContext(fn *graph.UserFn, data interface{}) (reflect.Value, bool) {
	if ctxType, hasCtx := fn.Context(); hasCtx {
		ctx := reflect.New(ctxType).Elem()

		if f, ok := reflectx.FindTaggedField(ctxType, reflectx.DataTag); ok {
			ctx.FieldByIndex(f.Index).Set(reflect.ValueOf(data))
		}
		// TODO(herohde): other static context, such as context.Context

		return ctx, true
	}
	return reflect.Value{}, false
}

func shim(ch reflect.Value, coder *graph.Coder, real, to reflect.Type) reflect.Value {
	from := ch.Type().Elem()
	if from == to {
		// Same type: shortcuts Universal -> Universal, too.
		return ch
	}

	log.Printf("Shim: %v -> %v -> %v", from, real, to)

	ret := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, to), 100)
	go func() {
		defer ret.Close()

		for {
			val, ok := ch.Recv()
			if !ok {
				break
			}
			ret.Send(reflectx.Convert(val, to))
		}
	}()
	return ret
}

// gbk uses naive buffering, for now.

type group struct {
	ID     string
	Key    reflect.Value
	Values []reflect.Value
}

func gbk(ctx context.Context, coder *graph.Coder, in, out reflect.Value) error {
	defer out.Close()

	keyFn, valueFn := reflectx.UnpackFn(in.Type().Elem())
	keyCoder := findKeyCoder(coder)
	packFn := reflectx.PackFn(out.Type().Elem())

	// (1) read all elements.

	buffer := make(map[string]*group)
	for {
		val, ok := in.Recv()
		if !ok {
			break
		}

		key := keyFn(val)
		encoded, err := Encode(keyCoder, key)
		if err != nil {
			return fmt.Errorf("failed to encode key: %v", key.Interface())
		}
		id := string(encoded)

		g, ok := buffer[id]
		if !ok {
			g = &group{ID: id, Key: key}
			buffer[id] = g
		}

		value := valueFn(val)
		g.Values = append(g.Values, value)
	}

	// (2) write each group, with a fully closed values channel.

	values, _ := reflectx.FindTaggedField(out.Type().Elem(), reflectx.ValuesTag)
	for _, g := range buffer {
		ch := reflect.MakeChan(values.Type, len(g.Values))
		for _, elm := range g.Values {
			ch.Send(elm)
		}
		ch.Close()

		out.Send(packFn(g.Key, ch))
	}
	return nil
}

// TODO(herohde) 4/6/2017: move runtime execution parts out of local. Open
// questions are where (graph/coder + graph/runtime?) and how to best handle
// "magic" context, if any.

func Encode(coder *graph.Coder, value reflect.Value) ([]byte, error) {
	switch coder.Kind {
	case graph.Custom:
		c, _ := coder.UnfoldCustom()

		var args []reflect.Value

		userCtx, hasCtx := makeContext(c.Enc, c.Data)
		if hasCtx {
			args = append(args, userCtx)
		}
		args = append(args, value)

		ret := c.Enc.Fn.Call(args)
		if index, ok := c.Enc.HasError(); ok && !ret[index].IsNil() {
			return nil, fmt.Errorf("encode error: %v", ret[index].Interface())
		}
		return ret[0].Interface().([]byte), nil

	case graph.LengthPrefix:
		c, _ := coder.UnfoldLengthPrefix()

		data, err := Encode(c, value)
		if err != nil {
			return nil, err
		}

		size := len(data)
		prefix := coderx.EncodeVarInt((int32)(size))
		return append(prefix, data...), nil

	case graph.Pair:
		k, v, _ := coder.UnfoldPair()
		keyFn, valFn := reflectx.UnpackFn(value.Type())

		key, err := Encode(k, keyFn(value))
		if err != nil {
			return nil, err
		}
		val, err := Encode(v, valFn(value))
		if err != nil {
			return nil, err
		}
		return append(key, val...), nil

	case graph.WindowedValue:
		c, _, _ := coder.UnfoldWindowedValue()

		// TODO(herohde) 4/7/2017: actually handle windows. Backfilling implicit
		// context is the death of chan-based bundle processing.

		// Encoding: Timestamp, Window, Pane, Element

		ret := coderx.EncodeTimestamp(typex.Timestamp(time.Now()))

		ret = append(ret, 0x0, 0x0, 0x0, 0x1) // #windows
		// Ignore GlobalWindow, for now. It encoded into the empty string.

		ret = append(ret, 0xf) // NO_FIRING pane

		val, err := Encode(c, value)
		if err != nil {
			return nil, err
		}
		return append(ret, val...), nil

	default:
		return nil, fmt.Errorf("Unexpected coder: %v", coder)
	}
}

func Decode(coder *graph.Coder, data []byte) (reflect.Value, int, error) {
	switch coder.Kind {
	case graph.Custom:
		c, _ := coder.UnfoldCustom()

		var args []reflect.Value

		userCtx, hasCtx := makeContext(c.Dec, c.Data)
		if hasCtx {
			args = append(args, userCtx)
		}
		args = append(args, reflect.ValueOf(data))

		ret := c.Dec.Fn.Call(args)
		if index, ok := c.Enc.HasError(); ok && !ret[index].IsNil() {
			return reflect.Value{}, 0, fmt.Errorf("decode error: %v", ret[index].Interface())
		}
		return reflectx.Convert(ret[0], c.T), len(data), nil

	case graph.LengthPrefix:
		c, _ := coder.UnfoldLengthPrefix()

		size, offset, err := coderx.DecodeVarInt(data)
		if err != nil {
			return reflect.Value{}, 0, err
		}

		value, _, err := Decode(c, data[offset:offset+int(size)])
		if err != nil {
			return reflect.Value{}, 0, err
		}
		return value, offset + int(size), nil

	case graph.Pair:
		k, v, _ := coder.UnfoldPair()
		pack := reflectx.PackFn(coder.T)

		key, offset, err := Decode(k, data)
		if err != nil {
			return reflect.Value{}, 0, err
		}
		val, offset2, err := Decode(v, data[offset:])
		if err != nil {
			return reflect.Value{}, 0, err
		}
		return pack(key, val), offset + offset2, nil

	case graph.Stream:
		c, _ := coder.UnfoldStream()

		size, offset, err := coderx.DecodeInt32(data)
		if err != nil {
			return reflect.Value{}, 0, err
		}

		ch := reflect.MakeChan(coder.T, 1000) // hack
		if size > -1 {
			// Known length stream.

			for i := int32(0); i < size; i++ {
				elm, o, err := Decode(c, data[offset:])
				if err != nil {
					return reflect.Value{}, 0, fmt.Errorf("decode failed at %v:%v: %v", offset, data, err)
				}

				offset += o
				ch.Send(elm)
			}
		} else {
			// Unknown length stream.

			for {
				block, o, err := coderx.DecodeVarUint64(data[offset:])
				if err != nil {
					return reflect.Value{}, 0, err
				}

				log.Printf("Block: %v", block)

				offset += o
				if block == 0 {
					break
				}

				for i := uint64(0); i < block; i++ {
					elm, o, err := Decode(c, data[offset:])
					if err != nil {
						return reflect.Value{}, 0, fmt.Errorf("decode failed at %v:%v: %v", offset, data, err)
					}

					log.Printf("Elm %v: %v [size: %v @ %v]", i, elm, o, offset)

					offset += o
					ch.Send(reflectx.Convert(elm, c.T))
				}
			}
		}
		ch.Close()

		return ch, offset, nil

	case graph.WindowedValue:
		c, _, _ := coder.UnfoldWindowedValue()

		// TODO(herohde) 4/7/2017: actually handle windows. Skip
		// timestamp, window and pane for now.

		// Encoding: Timestamp, Window, Pane, Element

		val, offset, err := Decode(c, data[13:])
		if err != nil {
			return reflect.Value{}, 0, err
		}
		return val, offset, nil

	default:
		return reflect.Value{}, 0, fmt.Errorf("Unexpected coder: %v", coder)
	}
}

func findKeyCoder(coder *graph.Coder) *graph.Coder {
	if c, _, ok := coder.UnfoldWindowedValue(); ok {
		return findKeyCoder(c)
	}

	k, _, ok := coder.UnfoldPair()
	if !ok {
		panic(fmt.Errorf("Expected pair coder: %v", coder))
	}
	return k
}

func closeout(pipes []reflect.Value) {
	for _, pipe := range pipes {
		pipe.Close()
	}
}
