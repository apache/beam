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

package graphx

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/coderx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// CoGBK support
//
// TODO(BEAM-490): CoGBK is not a supported model primitive, so similarly to other
// SDKs, a translation into GBK is performed to run on portable runners. Due to
// various constraints and to preserve CoGBK as a first-class concept in areas
// such as type-checking and for non-portability runners, the Go translation is
// done by the framework instead of user code.
//
// The basic translation approach is as follows:
//
//         In1 : KV<K,X>        In2 : KV<K,Y>
//               |                   |
//               ------ CoGBK --------
//                        |
//                Out: CoGBK<K,X,Y>
//
// is expanded to a flattened raw union value ("RUV") typed as KV<int,[]byte>:
//
//         In1 : KV<K,X>        In2 : KV<K,Y>
//               |                   |
//            Inject(1)           Inject(2)
//               |                   |
//           U1: KV<K,RUV>       U2: KV<K,RUV>
//               |                   |
//               ----- Flatten -------
//                        |
//                  U3: KV<K,RUV>
//                        |
//                      CoGBK   (now a GBK)
//                        |
//                 U4: CoGBK<K,RUV>
//                        |
//                      Expand
//                        |
//                Out: CoGBK<K,X,Y>
//
// Inject and Expand are system-defined functions. This expansion cannot be
// expressed as Go user code.

const (
	URNInject = "beam:go:transform:inject:v1"
	URNExpand = "beam:go:transform:expand:v1"
)

// MakeKVUnionCoder returns KV<K,KV<int,[]byte>> for a given CoGBK.
func MakeKVUnionCoder(gbk *graph.MultiEdge) (*coder.Coder, error) {
	if gbk.Op != graph.CoGBK {
		err := errors.Errorf("expected CoGBK, got %v", gbk)
		return nil, errors.WithContext(err, "failed to make KV Union coder")
	}

	from := gbk.Input[0].From
	key := from.Coder.Components[0]
	kvCoder, err := makeUnionCoder()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to make KV Union coder.")
	}
	return coder.NewKV([]*coder.Coder{key, kvCoder}), nil
}

// MakeGBKUnionCoder returns CoGBK<K,KV<int,[]byte>> for a given CoGBK.
func MakeGBKUnionCoder(gbk *graph.MultiEdge) (*coder.Coder, error) {
	if gbk.Op != graph.CoGBK {
		err := errors.Errorf("expected CoGBK, got %v", gbk)
		return nil, errors.WithContext(err, "failed to make GBK Union coder")
	}

	from := gbk.Input[0].From
	key := from.Coder.Components[0]
	kvCoder, err := makeUnionCoder()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to make GBK Union coder.")
	}
	return coder.NewCoGBK([]*coder.Coder{key, kvCoder}), nil
}

// makeUnionCoder returns a coder for the raw union value, KV<int,[]byte>. It uses
// varintz instead of the built-in varint to avoid the implicit length-prefixing
// of varint otherwise introduced by Dataflow.
func makeUnionCoder() (*coder.Coder, error) {
	c, err := coderx.NewVarIntZ(reflectx.Int)
	if err != nil {
		return nil, err
	}
	return coder.NewKV([]*coder.Coder{
		{Kind: coder.Custom, T: typex.New(reflectx.Int), Custom: c},
		coder.NewBytes(),
	}), nil
}
