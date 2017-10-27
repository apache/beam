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

package beam

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// TryCombine attempts to insert a Combine transform into the pipeline. It may fail
// for multiple reasons, notably that the combinefn is not valid or cannot be bound
// -- due to type mismatch, say -- to the incoming PCollections.
func TryCombine(p *Pipeline, combinefn interface{}, col PCollection, opts ...Option) (PCollection, error) {
	if !col.IsValid() {
		return PCollection{}, fmt.Errorf("invalid main pcollection")
	}
	side, _ := parseOpts(opts)
	for i, in := range side {
		if !in.Input.IsValid() {
			return PCollection{}, fmt.Errorf("invalid side pcollection: index %v", i)
		}
	}

	fn, err := graph.NewCombineFn(combinefn)
	if err != nil {
		return PCollection{}, fmt.Errorf("invalid CombineFn: %v", err)
	}

	if typex.IsWKV(col.Type()) {
		// If KV, we need to first insert a GBK.

		col, err = TryGroupByKey(p, col)
		if err != nil {
			return PCollection{}, fmt.Errorf("failed to group by key: %v", err)
		}
	}

	in := []*graph.Node{col.n}
	for _, s := range side {
		in = append(in, s.Input.n)
	}
	edge, err := graph.NewCombine(p.real, p.parent, fn, in)
	if err != nil {
		return PCollection{}, err
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}

// Combine inserts a Combine transform into the pipeline. The Combine is either
// global or per-key, depending on the input:
//
//    PCollection<T>        : global combine
//    PCollection<KV<K,T>>  : per-key combine (and a GBK is inserted)
//    PCollection<GBK<K,T>> : per-key combine
//
// For a per-key combine, the Combine may optionally take a key parameter.
func Combine(p *Pipeline, combinefn interface{}, col PCollection, opts ...Option) PCollection {
	return Must(TryCombine(p, combinefn, col, opts...))
}

// FindCombineType returns the value type, A, of a global or per-key combine from
// an incoming PCollection<A>, PCollection<KV<K,A>> or PCollection<GBK<K,A>>. The
// value type is expected to be a concrete type.
func FindCombineType(col PCollection) reflect.Type {
	switch {
	case typex.IsWKV(col.Type()):
		return typex.SkipW(col.Type()).Components()[1].Type()

	case typex.IsWGBK(col.Type()):
		return typex.SkipW(col.Type()).Components()[1].Type()

	default:
		return typex.SkipW(col.Type()).Type()
	}
}
