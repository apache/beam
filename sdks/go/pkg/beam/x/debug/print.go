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

package debug

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*printFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*printKVFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*printGBKFn)(nil)))
	beam.RegisterFunction(discardFn)
}

// Print prints out all data. Use with care.
func Print(s beam.Scope, col beam.PCollection) beam.PCollection {
	return Printf(s, "Elm: %v", col)
}

// Printf prints out all data with custom formatting. The given format string
// is used as log.Printf(format, elm) for each element. Use with care.
func Printf(s beam.Scope, format string, col beam.PCollection) beam.PCollection {
	s = s.Scope("debug.Print")

	switch {
	case typex.IsKV(col.Type()):
		return beam.ParDo(s, &printKVFn{Format: format}, col)
	case typex.IsCoGBK(col.Type()):
		return beam.ParDo(s, &printGBKFn{Format: format}, col)
	default:
		return beam.ParDo(s, &printFn{Format: format}, col)
	}
}

// TODO(herohde) 1/24/2018: use DynFn for a unified signature here instead.

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
func Discard(s beam.Scope, col beam.PCollection) {
	s = s.Scope("debug.Discard")
	beam.ParDo0(s, discardFn, col)
}

func discardFn(t beam.T) {
	// nop
}
