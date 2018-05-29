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

package funcx

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// IsEmit returns true iff the supplied type is an emitter.
func IsEmit(t reflect.Type) bool {
	_, ok := UnfoldEmit(t)
	return ok
}

// IsEmitWithEventTime return true iff the supplied type is an
// emitter and the first argument is the optional EventTime.
func IsEmitWithEventTime(t reflect.Type) bool {
	types, ok := UnfoldEmit(t)
	return ok && types[0] == typex.EventTimeType
}

// UnfoldEmit returns the parameter types, if an emitter. For example:
//
//     func (int)                  returns {int}
//     func (string, int)          returns {string, int}
//     func (typex.EventTime, int) returns {typex.EventTime, int}
//
func UnfoldEmit(t reflect.Type) ([]reflect.Type, bool) {
	if t.Kind() != reflect.Func {
		return nil, false
	}

	if t.NumOut() != 0 {
		return nil, false
	}
	if t.NumIn() == 0 {
		return nil, false
	}

	var ret []reflect.Type
	skip := 0
	if t.In(0) == typex.EventTimeType {
		ret = append(ret, typex.EventTimeType)
		skip = 1
	}
	if t.NumIn()-skip > 2 || t.NumIn() == skip {
		return nil, false
	}

	for i := skip; i < t.NumIn(); i++ {
		if !isInParam(t.In(i)) {
			return nil, false
		}
		ret = append(ret, t.In(i))
	}
	return ret, true
}

func isInParam(t reflect.Type) bool {
	return typex.IsConcrete(t) || typex.IsUniversal(t) || typex.IsContainer(t)
}
