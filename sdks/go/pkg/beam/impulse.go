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
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
)

// Impulse emits a single empty []byte into the global window. The resulting
// PCollection is a singleton of type []byte.
//
// The purpose of Impulse is to trigger another transform, such as
// ones that take all information as side inputs.
func Impulse(s Scope) PCollection {
	return ImpulseValue(s, []byte{})
}

// ImpulseValue emits the supplied byte slice into the global window. The resulting
// PCollection is a singleton of type []byte.
func ImpulseValue(s Scope, value []byte) PCollection {
	if !s.IsValid() {
		panic("Invalid scope")
	}
	edge := graph.NewImpulse(s.real, s.scope, value)
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret
}
