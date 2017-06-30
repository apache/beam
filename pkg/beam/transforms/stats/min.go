package stats

import (
	"fmt"

	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
)

// Min returns the minimal element -- per key, if keyed -- in a collection.
// It expects a PCollection<A> or PCollection<KV<A,B>> as input and returns
// a singleton PCollection<A> or a PCollection<KV<A,B>>, respectively. It
// can only be used for numbers, such as int, uint16, float32, etc.
//
// For example:
//
//    col := beam.Create(p, 1, 11, 7, 5, 10)
//    min := stats.Min(col)   // PCollection<int> with 11 as the only element.
//
func Min(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Composite("stats.Min")

	var t reflect.Type
	switch {
	case typex.IsWKV(col.Type()):
		t = typex.SkipW(col.Type()).Components()[1].Type()
		col = beam.GroupByKey(p, col)

	case typex.IsWGBK(col.Type()):
		t = typex.SkipW(col.Type()).Components()[1].Type()

	default:
		t = typex.SkipW(col.Type()).Type()
	}

	if !reflectx.IsNumber(t) || reflectx.IsComplex(t) {
		panic(fmt.Sprintf("Min requires a non-complex number: %v", t))
	}

	// Do a pipeline-construction-time type switch to select the right
	// runtime operation.

	switch t.Kind() {
	case reflect.Int:
		return beam.Combine(p, minIntFn, col)
	case reflect.Int8:
		return beam.Combine(p, minInt8Fn, col)
	case reflect.Int16:
		return beam.Combine(p, minInt16Fn, col)
	case reflect.Int32:
		return beam.Combine(p, minInt32Fn, col)
	case reflect.Int64:
		return beam.Combine(p, minInt64Fn, col)
	case reflect.Uint:
		return beam.Combine(p, minUintFn, col)
	case reflect.Uint8:
		return beam.Combine(p, minUint8Fn, col)
	case reflect.Uint16:
		return beam.Combine(p, minUint16Fn, col)
	case reflect.Uint32:
		return beam.Combine(p, minUint32Fn, col)
	case reflect.Uint64:
		return beam.Combine(p, minUint64Fn, col)
	case reflect.Float32:
		return beam.Combine(p, minFloat32Fn, col)
	case reflect.Float64:
		return beam.Combine(p, minFloat64Fn, col)
	default:
		panic(fmt.Sprintf("Unexpected number type: %v", t))
	}
}

func minIntFn(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func minInt8Fn(x, y int8) int8 {
	if x < y {
		return x
	}
	return y
}

func minInt16Fn(x, y int16) int16 {
	if x < y {
		return x
	}
	return y
}

func minInt32Fn(x, y int32) int32 {
	if x < y {
		return x
	}
	return y
}

func minInt64Fn(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func minUintFn(x, y uint) uint {
	if x < y {
		return x
	}
	return y
}

func minUint8Fn(x, y uint8) uint8 {
	if x < y {
		return x
	}
	return y
}

func minUint16Fn(x, y uint16) uint16 {
	if x < y {
		return x
	}
	return y
}

func minUint32Fn(x, y uint32) uint32 {
	if x < y {
		return x
	}
	return y
}

func minUint64Fn(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func minFloat32Fn(x, y float32) float32 {
	if x < y {
		return x
	}
	return y
}

func minFloat64Fn(x, y float64) float64 {
	if x < y {
		return x
	}
	return y
}
