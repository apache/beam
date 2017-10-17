package debug

import (
	"fmt"
	"reflect"

	"context"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*printFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*printKVFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*printGBKFn)(nil)))
}

// Print prints out all data. Use with care.
func Print(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	return Printf(p, "Elm: %v", col)
}

// Printf prints out all data with custom formatting. The given format string
// is used as log.Printf(format, elm) for each element. Use with care.
func Printf(p *beam.Pipeline, format string, col beam.PCollection) beam.PCollection {
	p = p.Scope("debug.Print")

	switch {
	case typex.IsWKV(col.Type()):
		return beam.ParDo(p, &printKVFn{Format: format}, col)
	case typex.IsWGBK(col.Type()):
		return beam.ParDo(p, &printGBKFn{Format: format}, col)
	default:
		return beam.ParDo(p, &printFn{Format: format}, col)
	}
}

type printFn struct {
	Format string `json:"format"`
}

func (f *printFn) ProcessElement(ctx context.Context, t beam.T) beam.T {
	log.Infof(ctx, f.Format, t)
	return t
}

type printKVFn struct {
	Format string `json:"format"`
}

func (f *printKVFn) ProcessElement(ctx context.Context, x beam.X, y beam.Y) (beam.X, beam.Y) {
	log.Infof(ctx, f.Format, fmt.Sprintf("(%v,%v)", x, y))
	return x, y
}

type printGBKFn struct {
	Format string `json:"format"`
}

func (f *printGBKFn) ProcessElement(ctx context.Context, x beam.X, iter func(*beam.Y) bool) beam.X {
	var ys []string
	var y beam.Y
	for iter(&y) {
		ys = append(ys, fmt.Sprintf("%v", y))
	}
	log.Infof(ctx, f.Format, fmt.Sprintf("(%v,%v)", x, ys))
	return x
}

// Discard is a sink that discards all data.
func Discard(p *beam.Pipeline, col beam.PCollection) {
	p = p.Scope("debug.Discard")
	beam.ParDo0(p, discardFn, col)
}

func discardFn(t beam.T) {
	// nop
}
