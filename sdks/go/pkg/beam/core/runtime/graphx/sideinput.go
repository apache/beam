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
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
)

// Side input support
//
// The Beam model supports only MultiMap side input, so for
// iterable side input (= the only kind in Go so far) we must
// implicitly use a fixed key (""). We use a special execution
// unit as well to handle nested KVs.

const (
	URNIterableSideInputKey = "beam:go:transform:iterablesideinputkey:v1"
)

// MakeBytesKeyedCoder returns KV<[]byte,A,> for any coder,
// even if the coder is already a KV coder.
func MakeBytesKeyedCoder(c *coder.Coder) *coder.Coder {
	return coder.NewKV([]*coder.Coder{coder.NewBytes(), c})
}
