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

package internal

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

// Test DoFns are registered in the test file, to allow them to be pruned
// by the compiler outside of test use.
func init() {
	register.Function2x0(dofn1)
	register.Function2x0(dofn1kv)
	register.Function3x0(dofn1x2)
	register.Function6x0(dofn1x5)
	register.Function3x0(dofn2x1)
	register.Function4x0(dofn2x2KV)
	register.Function4x0(dofnMultiMap)
	register.Iter1[int64]()
	register.Function4x0(dofn3x1)
	register.Iter2[string, int64]()
	register.Emitter1[string]()

	register.Function2x0(dofn2)
	register.Function2x0(dofnKV)
	register.Function2x0(dofnKV2)
	register.Function3x0(dofnGBK)
	register.Function3x0(dofnGBK2)
	register.DoFn3x0[beam.Window, int64, func(int64)]((*int64Check)(nil))
	register.DoFn2x0[string, func(string)]((*stringCheck)(nil))
	register.Function2x0(dofnKV3)
	register.Function3x0(dofnGBK3)
	register.Function3x0(dofn1Counter)
	register.Function2x0(dofnSink)
	register.Function3x1(doFnFail)

	register.Function2x1(combineIntSum)

	register.DoFn3x1[*sdf.LockRTracker, SourceConfig, func(int64), error]((*intRangeFn)(nil))
	register.Emitter1[int64]()
	register.Emitter2[int64, int64]()
}
