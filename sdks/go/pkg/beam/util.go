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

//go:generate go install github.com/apache/beam/sdks/v2/go/cmd/starcgen
//go:generate starcgen --package=beam --identifiers=addFixedKeyFn,dropKeyFn,dropValueFn,swapKVFn,explodeFn,jsonDec,jsonEnc,protoEnc,protoDec,schemaEnc,schemaDec,makePartitionFn
//go:generate go fmt

// We have some freedom to create various utilities, users can use depending on
// preferences. One point of keeping Pipeline transformation functions plain Go
// functions is that such utilities are more readily possible.

// For example, we can have an "easyio" package that selects a textio, gcsio,
// awsio, etc. transformation based on the filename schema. Such wrappers would
// look exactly like the more primitive sources/sinks, but be picked at
// pipeline construction time.

// NewPipelineWithRoot creates a new empty pipeline and its root scope.
func NewPipelineWithRoot() (*Pipeline, Scope) {
	p := NewPipeline()
	return p, p.Root()
}

// Seq is a convenience helper to chain single-input/single-output ParDos together
// in a sequence.
func Seq(s Scope, col PCollection, dofns ...any) PCollection {
	cur := col
	for _, dofn := range dofns {
		cur = ParDo(s, dofn, cur)
	}
	return cur
}

// AddFixedKey adds a fixed key (0) to every element.
func AddFixedKey(s Scope, col PCollection) PCollection {
	return ParDo(s, addFixedKeyFn, col)
}

func addFixedKeyFn(elm T) (int, T) {
	return 0, elm
}

// DropKey drops the key for an input PCollection<KV<A,B>>. It returns
// a PCollection<B>.
func DropKey(s Scope, col PCollection) PCollection {
	return ParDo(s, dropKeyFn, col)
}

func dropKeyFn(_ X, y Y) Y {
	return y
}

// DropValue drops the value for an input PCollection<KV<A,B>>. It returns
// a PCollection<A>.
func DropValue(s Scope, col PCollection) PCollection {
	return ParDo(s, dropValueFn, col)
}

func dropValueFn(x X, _ Y) X {
	return x
}

// SwapKV swaps the key and value for an input PCollection<KV<A,B>>. It returns
// a PCollection<KV<B,A>>.
func SwapKV(s Scope, col PCollection) PCollection {
	return ParDo(s, swapKVFn, col)
}

func swapKVFn(x X, y Y) (Y, X) {
	return y, x
}

// Explode is a PTransform that takes a single PCollection<[]A> and returns a
// PCollection<A> containing all the elements for each incoming slice.
func Explode(s Scope, col PCollection) PCollection {
	s = s.Scope("beam.Explode")
	return ParDo(s, explodeFn, col)
}

func explodeFn(list []T, emit func(T)) {
	for _, elm := range list {
		emit(elm)
	}
}

// The MustX functions are convenience helpers to create error-less functions.

// MustN returns the input, but panics if err != nil.
func MustN(list []PCollection, err error) []PCollection {
	if err != nil {
		panic(err)
	}
	return list
}

// MustTaggedN returns the input, but panics if err != nil.
func MustTaggedN(ret map[string]PCollection, err error) map[string]PCollection {
	if err != nil {
		panic(err)
	}
	return ret
}

// Must returns the input, but panics if err != nil.
func Must(a PCollection, err error) PCollection {
	if err != nil {
		panic(err)
	}
	return a
}
