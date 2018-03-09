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

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
)

// Combine inserts a global Combine transform into the pipeline. It
// expects a PCollection<T> as input where T is a concrete type.
func Combine(s Scope, combinefn interface{}, col PCollection) PCollection {
	return Must(TryCombine(s, combinefn, col))
}

// CombinePerKey inserts a GBK and per-key Combine transform into the pipeline. It
// expects a PCollection<KV<K,T>>. The CombineFn may optionally take a key parameter.
func CombinePerKey(s Scope, combinefn interface{}, col PCollection) PCollection {
	return Must(TryCombinePerKey(s, combinefn, col))
}

// TryCombine attempts to insert a global Combine transform into the pipeline. It may fail
// for multiple reasons, notably that the combinefn is not valid or cannot be bound
// -- due to type mismatch, say -- to the incoming PCollections.
func TryCombine(s Scope, combinefn interface{}, col PCollection) (PCollection, error) {
	pre := AddFixedKey(s, col)
	post, err := TryCombinePerKey(s, combinefn, pre)
	if err != nil {
		return PCollection{}, err
	}
	return DropKey(s, post), nil
}

// TryCombinePerKey attempts to insert a per-key Combine transform into the pipeline. It may fail
// for multiple reasons, notably that the combinefn is not valid or cannot be bound
// -- due to type mismatch, say -- to the incoming PCollection.
func TryCombinePerKey(s Scope, combinefn interface{}, col PCollection) (PCollection, error) {
	ValidateKVType(col)
	col, err := TryGroupByKey(s, col)
	if err != nil {
		return PCollection{}, fmt.Errorf("failed to group by key: %v", err)
	}
	return combine(s, combinefn, col)
}

func combine(s Scope, combinefn interface{}, col PCollection) (PCollection, error) {
	fn, err := graph.NewCombineFn(combinefn)
	if err != nil {
		return PCollection{}, fmt.Errorf("invalid CombineFn: %v", err)
	}

	edge, err := graph.NewCombine(s.real, s.scope, fn, col.n)
	if err != nil {
		return PCollection{}, err
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}
