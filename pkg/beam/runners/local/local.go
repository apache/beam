package local

import (
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"log"
	"reflect"
	"sync"
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

func Execute(ctx context.Context, p *beam.Pipeline) error {
	edges, nodes := p.FakeBuild()

	for _, edge := range edges {
		log.Printf("%v", edge)
	}

	// (1) create channels for each node. Note that GBK results is chan { K, chan V }.

	pipes := make(map[int]reflect.Value) // id -> chan T
	for _, node := range nodes {
		t := reflect.ChanOf(reflect.BothDir, node.T)
		pipes[node.ID()] = reflect.MakeChan(t, 100)
	}

	// (2) start gorutines to execute each step

	var wg sync.WaitGroup

	for _, edge := range edges {
		var in []reflect.Value
		for _, node := range edge.Input {
			in = append(in, pipes[node.From.ID()])
		}
		var out []reflect.Value
		for _, node := range edge.Output {
			out = append(out, pipes[node.To.ID()])
		}

		switch edge.Op {
		case graph.Source, graph.ParDo:
			dofn := edge.DoFn
			data := edge.Data

			// There is no need to shim output in the direct runner, because we don't
			// have different requirements than what the DoFn returns. We do need to
			// shim input to convert between different KV types.
			in = shim(dofn, in)

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

func call(ctx context.Context, userfn *graph.UserFn, data interface{}, in, out []reflect.Value) {
	defer closeout(out)

	// log.Printf("Call enter: %v", userfn)

	var userCtx reflect.Value
	ctxType, hasCtx := userfn.Context()
	if hasCtx {
		userCtx = reflect.New(ctxType).Elem()

		if f, ok := graph.FindTaggedField(ctxType, graph.DataTag); ok {
			userCtx.FieldByIndex(f.Index).Set(reflect.ValueOf(data))
		}
		// TODO(herohde): other static context, such as context.Context
	}

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
		// (1) input de-chan. Must be KV or GBKResult.

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
			args = append(args, keyFn(val), valueFn(val)) // shim "values", too?
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

func shim(userfn *graph.UserFn, in []reflect.Value) []reflect.Value {
	if len(in) == 0 {
		return in // ok: source
	}

	var ret []reflect.Value

	main, side := userfn.Input()
	if userfn.HasDirectInput() {
		// Don't shim exploded input. Assume assignable, for now.
		ret = append(ret, in[0])

		if _, _, ok := graph.IsGBKResult(in[0].Type().Elem()); ok {
			return ret // GBK has no side input
		}
	} else {
		ret = append(ret, shimChan(in[0], main[0]))
	}

	for i, t := range side {
		ret = append(ret, shimChan(in[i+1], t))
	}
	return ret
}

func shimChan(ch reflect.Value, to reflect.Type) reflect.Value {
	from := ch.Type().Elem()

	if from == to {
		return ch
	}
	if _, _, ok := graph.IsKV(from); !ok {
		return ch
	}
	if _, _, ok := graph.IsKV(to); !ok {
		return ch
	}

	// We need to repack the KVs to convert correctly in this case. We
	// do that conversion with a separate channel + go routine.

	keyFn, valueFn := unpack(from)
	packFn := pack(to)

	ret := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, to), 100)

	go func() {
		defer ret.Close()

		for {
			val, ok := ch.Recv()
			if !ok {
				break
			}

			updated := packFn(keyFn(val), valueFn(val))
			ret.Send(updated)
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
		id := key.String()

		g, ok := buffer[id]
		if !ok {
			g = &group{ID: id, Key: key}
			buffer[id] = g
		}

		value := valueFn(val)
		g.Values = append(g.Values, value)
	}

	// (2) write each group, with a fully closed values channel.

	values, _ := graph.FindTaggedField(out.Type().Elem(), graph.ValuesTag)
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
	key, _ := graph.FindTaggedField(t, graph.KeyTag)
	value, _ := graph.FindTaggedField(t, graph.ValueTag, graph.ValuesTag)

	return func(k reflect.Value, v reflect.Value) reflect.Value {
		ret := reflect.New(t).Elem()
		ret.FieldByIndex(key.Index).Set(k)
		ret.FieldByIndex(value.Index).Set(v)
		return ret
	}
}

func unpack(t reflect.Type) (func(reflect.Value) reflect.Value, func(reflect.Value) reflect.Value) {
	key, _ := graph.FindTaggedField(t, graph.KeyTag)
	value, _ := graph.FindTaggedField(t, graph.ValueTag, graph.ValuesTag)

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
