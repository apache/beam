// Package passert contains verification transformations for testing pipelines.
// The transformations are not tied to any particular runner, i.e., they can
// notably be used for remote execution runners, such as Dataflow.
package passert

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*diffFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*failFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*failKVFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*failGBKFn)(nil)))
}

// Equals verifies the the given collection has the same values as the given
// values, under coder equality. The values can be provided as single
// PCollection.
func Equals(p *beam.Pipeline, col beam.PCollection, values ...interface{}) beam.PCollection {
	if len(values) == 0 {
		return Empty(p, col)
	}
	if other, ok := values[0].(beam.PCollection); ok && len(values) == 1 {
		return equals(p, col, other)
	}

	other := beam.Create(p, values...)
	return equals(p, col, other)
}

// equals verifies that the actual values match the expected ones.
func equals(p *beam.Pipeline, actual, expected beam.PCollection) beam.PCollection {
	bad, _, bad2 := Diff(p, actual, expected)
	fail(p, bad, "value %v present, but not expected")
	fail(p, bad2, "value %v expected, but not present")
	return actual
}

// Diff splits 2 incoming PCollections into 3: left only, both, right only. Duplicates are
// preserved, so a value may appear multiple times and in multiple collections. Coder
// equality is used to determine equality. Should only be used for small collections,
// because all values are held in memory at the same time.
func Diff(p *beam.Pipeline, a, b beam.PCollection) (left, both, right beam.PCollection) {
	imp := beam.Impulse(p)
	return beam.ParDo3(p, &diffFn{Coder: beam.EncodedCoder{Coder: a.Coder()}}, imp, beam.SideInput{Input: a}, beam.SideInput{Input: b})
}

// TODO(herohde) 7/11/2017: should there be a first-class way to obtain the coder,
// such a a specially-typed parameter?

// diffFn computes the symmetrical multi-set difference of 2 collections, under
// coder equality. The Go values returned may be any of the coder-equal ones.
type diffFn struct {
	Coder beam.EncodedCoder `json:"coder"`
}

func (f *diffFn) ProcessElement(_ []byte, ls, rs func(*typex.T) bool, left, both, right func(t typex.T)) error {
	c := coder.SkipW(beam.UnwrapCoder(f.Coder.Coder))

	indexL, err := index(c, ls)
	if err != nil {
		return err
	}
	indexR, err := index(c, rs)
	if err != nil {
		return err
	}

	for key, entry := range indexL {
		other, ok := indexR[key]
		if !ok {
			emitN(entry.value, entry.count, left)
			continue
		}

		diff := entry.count - other.count
		switch {
		case diff < 0:
			// More elements in rs.
			emitN(entry.value, entry.count, both)
			emitN(entry.value, -diff, right)

		case diff == 0:
			// Same amount of elements
			emitN(entry.value, entry.count, both)

		case diff > 0:
			// More elements in ls.
			emitN(entry.value, other.count, both)
			emitN(entry.value, diff, left)
		}
	}

	for key, entry := range indexR {
		if _, ok := indexL[key]; !ok {
			emitN(entry.value, entry.count, right)
		}
		// else already processed in indexL loop
	}
	return nil
}

func emitN(val typex.T, n int, emit func(t typex.T)) {
	for i := 0; i < n; i++ {
		emit(val)
	}
}

type indexEntry struct {
	count int
	value typex.T
}

func index(c *coder.Coder, iter func(*typex.T) bool) (map[string]indexEntry, error) {
	ret := make(map[string]indexEntry)

	var val typex.T
	for iter(&val) {
		encoded, err := encode(c, val)
		if err != nil {
			return nil, err
		}

		cur := ret[encoded]
		ret[encoded] = indexEntry{count: cur.count + 1, value: val}
	}
	return ret, nil
}

// TODO(herohde) 7/11/2017: perhaps extract the coder helpers as more
// general and polished utilities for working with coders in user code.

func encode(c *coder.Coder, value interface{}) (string, error) {
	var buf bytes.Buffer
	if err := exec.EncodeElement(c, exec.FullValue{Elm: reflect.ValueOf(value)}, &buf); err != nil {
		return "", fmt.Errorf("value %v not encodable by %v", value, c)
	}
	return buf.String(), nil
}

// True asserts that all elements satisfy the given predicate.
func True(p *beam.Pipeline, col beam.PCollection, fn interface{}) beam.PCollection {
	fail(p, filter.Exclude(p, col, fn), "predicate(%v) = false, want true")
	return col
}

// False asserts that the given predicate does not satisfy any element in the condition.
func False(p *beam.Pipeline, col beam.PCollection, fn interface{}) beam.PCollection {
	fail(p, filter.Include(p, col, fn), "predicate(%v) = true, want false")
	return col
}

// Empty asserts that col is empty.
func Empty(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	fail(p, col, "PCollection contains %v, want empty collection")
	return col
}

func fail(p *beam.Pipeline, col beam.PCollection, format string) {
	switch {
	case typex.IsWKV(col.Type()):
		beam.ParDo0(p, &failKVFn{Format: format}, col)

	case typex.IsWGBK(col.Type()):
		beam.ParDo0(p, &failGBKFn{Format: format}, col)

	default:
		beam.ParDo0(p, &failFn{Format: format}, col)
	}
}

type failFn struct {
	Format string `json:"format"`
}

func (f *failFn) ProcessElement(x typex.X) error {
	return fmt.Errorf(f.Format, x)
}

type failKVFn struct {
	Format string `json:"format"`
}

func (f *failKVFn) ProcessElement(x typex.X, y typex.Y) error {
	return fmt.Errorf(f.Format, fmt.Sprintf("(%v,%v)", x, y))
}

type failGBKFn struct {
	Format string `json:"format"`
}

func (f *failGBKFn) ProcessElement(x typex.X, _ func(*typex.Y) bool) error {
	return fmt.Errorf(f.Format, fmt.Sprintf("(%v,*)", x))
}
