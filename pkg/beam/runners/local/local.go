package local

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
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
			read[list[0]] = shim(write[id], beam.InternalCoder /* input.From.Coder */, input.From.T, input.T)
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
			read[elm] = shim(ch, beam.InternalCoder /* input.From.Coder */, input.From.T, input.T)
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
			wg.Add(1)
			go func() {
				defer wg.Done()
				gbk(ctx, in[0], out[0])
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

	userCtx, hasCtx := makeContext(userfn, data, nil)

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

		keyFn, valueFn := unpack(primary.Type().Elem())

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

func makeContext(fn *graph.UserFn, data interface{}, t reflect.Type) (reflect.Value, bool) {
	if ctxType, hasCtx := fn.Context(); hasCtx {
		ctx := reflect.New(ctxType).Elem()

		if f, ok := reflectx.FindTaggedField(ctxType, reflectx.DataTag); ok {
			ctx.FieldByIndex(f.Index).Set(reflect.ValueOf(data))
		}
		if f, ok := reflectx.FindTaggedField(ctxType, reflectx.TypeTag); ok {
			ctx.FieldByIndex(f.Index).Set(reflect.ValueOf(t))
		}
		// TODO(herohde): other static context, such as context.Context

		return ctx, true
	}
	return reflect.Value{}, false
}

func shim(ch reflect.Value, coder *graph.Coder, real, to reflect.Type) reflect.Value {
	from := ch.Type().Elem()

	log.Printf("Shim: %v -> %v -> %v", from, real, to)

	if from == to {
		// Same type: shortcuts Universal -> Universal and Encoded -> Encoded.
		return ch
	}

	// (1) Front conversion.

	switch reflectx.ClassOf(from) {
	case reflectx.Encoded:
		// Must decode.

		if coder == nil {
			log.Fatalf("Missing coder: %v", coder)
		}

		ret := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, real), 100)
		go func() {
			defer ret.Close()

			if err := decode(coder, ch, ret); err != nil {
				log.Printf("Code failed: %v", err)
			}
		}()
		return shim(ret, coder, real, to)

	case reflectx.Universal:
		// Must reify.

		ret := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, real), 100)
		go func() {
			defer ret.Close()

			for {
				val, ok := ch.Recv()
				if !ok {
					break
				}
				ret.Send(val.Interface().(reflect.Value))
			}
		}()
		return shim(ret, coder, real, to)
	}

	switch {
	case reflectx.IsKV(from) && reflectx.IsKV(real) && reflectx.IsKV(to):
		// Recursive KV.

		fromK, fromV, _ := reflectx.UnfoldKV(from)
		realK, realV, _ := reflectx.UnfoldKV(real)
		toK, toV, _ := reflectx.UnfoldKV(to)

		ret := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, to), 100)
		retK := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, fromK), 100)
		retV := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, fromV), 100)

		recvK := shim(retK, beam.InternalCoder, realK, toK)
		recvV := shim(retV, beam.InternalCoder, realV, toV)

		keyFn, valueFn := unpack(from)
		packFn := pack(to)

		go func() {
			defer retK.Close()
			defer retV.Close()
			defer ret.Close()

			for {
				val, ok := ch.Recv()
				if !ok {
					break
				}

				k, v := keyFn(val), valueFn(val)

				retK.Send(k)
				retV.Send(v)

				k2, _ := recvK.Recv()
				v2, _ := recvV.Recv()

				ret.Send(packFn(k2, v2))
			}
		}()

		return ret

	case reflectx.IsGBK(from) && reflectx.IsGBK(real) && reflectx.IsGBK(to):
		// Recursive GBK

		fromK, fromV, _ := reflectx.UnfoldKV(from)
		realK, realV, _ := reflectx.UnfoldKV(real)
		toK, toV, _ := reflectx.UnfoldKV(to)

		if fromV != realV || realV != toV {
			log.Fatalf("Bad GBK values: %v != %v != %v", fromV, realV, toV)
		}

		ret := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, to), 100)
		retK := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, fromK), 100)
		recvK := shim(retK, beam.InternalCoder, realK, toK)

		keyFn, valueFn := unpack(from)
		packFn := pack(to)

		go func() {
			defer retK.Close()
			defer ret.Close()

			for {
				val, ok := ch.Recv()
				if !ok {
					break
				}

				k, v := keyFn(val), valueFn(val)

				retK.Send(k)
				k2, _ := recvK.Recv()
				ret.Send(packFn(k2, v))
			}
		}()

		return ret
	}

	// (2) Back conversion.

	switch reflectx.ClassOf(to) {
	case reflectx.Encoded:
		// Must encode.

		if coder == nil {
			log.Fatalf("Missing coder: %v", coder)
		}

		ret := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, to), 100)
		go func() {
			defer ret.Close()

			if err := encode(coder, ch, ret); err != nil {
				log.Printf("Code failed: %v", err)
			}
		}()
		return ret

	case reflectx.Universal:
		// Must reflect.

		ret := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, to), 100)
		go func() {
			defer ret.Close()

			for {
				val, ok := ch.Recv()
				if !ok {
					break
				}
				ret.Send(reflect.ValueOf(val))
			}
		}()
		return ret
	}

	log.Fatalf("Bad conversion: %v -> %v -> %v", from, real, to)
	return reflect.Value{}
}

// NOTE: encode/decode use a reflected channel as FnValue, so they are invoked in
// a slightly different way than normal calls.

// TODO(herohde): support concrete coders

func encode(coder *graph.Coder, in, out reflect.Value /* chan T, chan []byte */) error {
	userCtx, hasCtx := makeContext(coder.Enc, coder.Data, in.Type().Elem())

	var args []reflect.Value
	if hasCtx {
		args = append(args, userCtx)
	}
	args = append(args, reflect.ValueOf(in) /* 2x reflect */, out)

	// log.Printf("Enc: %v <- %v", coder.Enc, args)

	ret := coder.Enc.Fn.Call(args)
	if index, ok := coder.Enc.HasError(); ok && !ret[index].IsNil() {
		return fmt.Errorf("Encode error: %v", ret[index].Interface())
	}
	return nil
}

func decode(coder *graph.Coder, in, out reflect.Value /* chan []byte, chan T */) error {
	userCtx, hasCtx := makeContext(coder.Dec, coder.Data, out.Type().Elem())

	var args []reflect.Value
	if hasCtx {
		args = append(args, userCtx)
	}
	args = append(args, reflect.ValueOf(out) /* 2x reflect */, in)

	// log.Printf("Dec: %v <- %v", coder.Dec, args)

	ret := coder.Dec.Fn.Call(args)
	if index, ok := coder.Dec.HasError(); ok && !ret[index].IsNil() {
		return fmt.Errorf("Decode error: %v", ret[index].Interface())
	}
	return nil
}

// gbk uses naive buffering, for now.

type group struct {
	ID     string
	Key    reflect.Value
	Values []reflect.Value
}

func gbk(ctx context.Context, in, out reflect.Value) {
	defer out.Close()

	keyFn, valueFn := unpack(in.Type().Elem())
	packFn := pack(out.Type().Elem())

	// (1) read all elements.

	buffer := make(map[string]*group)
	for {
		val, ok := in.Recv()
		if !ok {
			break
		}

		key := keyFn(val)
		id := string(key.Interface().([]byte))

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
}

// pack, unpack avoids some reflection for each element.

func pack(t reflect.Type) func(reflect.Value, reflect.Value) reflect.Value {
	key, _ := reflectx.FindTaggedField(t, reflectx.KeyTag)
	value, _ := reflectx.FindTaggedField(t, reflectx.ValueTag, reflectx.ValuesTag)

	return func(k reflect.Value, v reflect.Value) reflect.Value {
		ret := reflect.New(t).Elem()
		ret.FieldByIndex(key.Index).Set(k)
		ret.FieldByIndex(value.Index).Set(v)
		return ret
	}
}

func unpack(t reflect.Type) (func(reflect.Value) reflect.Value, func(reflect.Value) reflect.Value) {
	key, _ := reflectx.FindTaggedField(t, reflectx.KeyTag)
	value, _ := reflectx.FindTaggedField(t, reflectx.ValueTag, reflectx.ValuesTag)

	keyFn := func(v reflect.Value) reflect.Value {
		return v.FieldByIndex(key.Index)
	}
	valueFn := func(v reflect.Value) reflect.Value {
		return v.FieldByIndex(value.Index)
	}
	return keyFn, valueFn
}

func closeout(pipes []reflect.Value) {
	for _, pipe := range pipes {
		pipe.Close()
	}

}
