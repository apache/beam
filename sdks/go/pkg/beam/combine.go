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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// Combine inserts a global Combine transform into the pipeline. It
// expects a PCollection<T> as input where T is a concrete type.
// Combine supports TypeDefinition options for binding generic types in combinefn.
func Combine(s Scope, combinefn any, col PCollection, opts ...Option) PCollection {
	return Must(TryCombine(s, combinefn, col, opts...))
}

// CombinePerKey inserts a GBK and per-key Combine transform into the pipeline. It
// expects a PCollection<KV<K,T>>. The CombineFn may optionally take a key parameter.
// CombinePerKey supports TypeDefinition options for binding generic types in combinefn.
func CombinePerKey(s Scope, combinefn any, col PCollection, opts ...Option) PCollection {
	return Must(TryCombinePerKey(s, combinefn, col, opts...))
}

// TryCombine attempts to insert a global Combine transform into the pipeline. It may fail
// for multiple reasons, notably that the combinefn is not valid or cannot be bound
// -- due to type mismatch, say -- to the incoming PCollections.
func TryCombine(s Scope, combinefn any, col PCollection, opts ...Option) (PCollection, error) {
	pre := AddFixedKey(s, col)
	post, err := TryCombinePerKey(s, combinefn, pre, opts...)
	if err != nil {
		return PCollection{}, err
	}
	return DropKey(s, post), nil
}

func addCombinePerKeyCtx(err error, s Scope) error {
	return errors.WithContextf(err, "inserting CombinePerKey in scope %s", s)
}

// TryCombinePerKey attempts to insert a per-key Combine transform into the pipeline. It may fail
// for multiple reasons, notably that the combinefn is not valid or cannot be bound
// -- due to type mismatch, say -- to the incoming PCollection.
func TryCombinePerKey(s Scope, combinefn any, col PCollection, opts ...Option) (PCollection, error) {
	s = s.Scope(graph.CombinePerKeyScope)
	ValidateKVType(col)
	side, typedefs, err := validate(s, col, opts)
	if err != nil {
		return PCollection{}, addCombinePerKeyCtx(err, s)
	}
	if len(side) > 0 {
		return PCollection{}, addCombinePerKeyCtx(errors.New("combine does not support side inputs"), s)
	}

	col, err = TryGroupByKey(s, col)
	if err != nil {
		return PCollection{}, addCombinePerKeyCtx(err, s)
	}

	fn, err := graph.NewCombineFn(combinefn)
	if err != nil {
		return PCollection{}, addCombinePerKeyCtx(err, s)
	}
	// This seems like the best place to infer the accumulator coder type, unless
	// it's a universal type.
	// We can get the fulltype from the return value of the mergeAccumulatorFn
	// TODO(lostluck): 2018/05/28 Correctly infer universal type coder if necessary.
	accumCoder, err := inferCoder(typex.New(fn.MergeAccumulatorsFn().Ret[0].T))
	if err != nil {
		wrapped := errors.Wrap(err, "unable to infer CombineFn accumulator coder")
		return PCollection{}, addCombinePerKeyCtx(wrapped, s)
	}

	edge, err := graph.NewCombine(s.real, s.scope, fn, col.n, accumCoder, typedefs)
	if err != nil {
		return PCollection{}, addCombinePerKeyCtx(err, s)
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}
