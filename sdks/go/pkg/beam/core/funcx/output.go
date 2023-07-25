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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

var (
	errIllegalParametersInEmit = "All parameters in an emit must be universal type, container type, or concrete type"
)

// IsEmit returns true iff the supplied type is an emitter.
func IsEmit(t reflect.Type) bool {
	_, ok, _ := unfoldEmit(t)
	return ok
}

// IsMalformedEmit returns true iff the supplied type is an illegal emitter
// and an error explaining why it is illegal. For example, an emitter is illegal
// if one of its parameters is not concrete. If the type does not have the structure
// of an emitter or it is a legal emitter, IsMalformedEmit returns false and no error.
func IsMalformedEmit(t reflect.Type) (bool, error) {
	_, _, err := unfoldEmit(t)
	return err != nil, err
}

// IsEmitWithEventTime return true iff the supplied type is an
// emitter and the first argument is the optional EventTime.
func IsEmitWithEventTime(t reflect.Type) bool {
	types, ok, _ := unfoldEmit(t)
	return ok && types[0] == typex.EventTimeType
}

// UnfoldEmit returns the parameter types, if an emitter. For example:
//
//	func (int)                  returns {int}
//	func (string, int)          returns {string, int}
//	func (typex.EventTime, int) returns {typex.EventTime, int}
func UnfoldEmit(t reflect.Type) ([]reflect.Type, bool) {
	types, ok, _ := unfoldEmit(t)
	return types, ok
}

func unfoldEmit(t reflect.Type) ([]reflect.Type, bool, error) {
	if t.Kind() != reflect.Func {
		return nil, false, nil
	}

	if t.NumOut() != 0 {
		return nil, false, nil
	}
	if t.NumIn() == 0 {
		return nil, false, nil
	}

	var ret []reflect.Type
	skip := 0
	if t.In(0) == typex.EventTimeType {
		ret = append(ret, typex.EventTimeType)
		skip = 1
	}
	if t.NumIn()-skip > 2 || t.NumIn() == skip {
		return nil, false, nil
	}
	emptyInterface := reflect.TypeOf((*any)(nil)).Elem()
	for i := skip; i < t.NumIn(); i++ {
		if ok, err := isInParam(t.In(i)); !ok {
			return nil, false, errors.Wrap(err, errIllegalParametersInEmit)
		}
		if ((t.In(i).Kind() == reflect.Ptr && t.In(i).Elem() == emptyInterface) || t.In(i) == emptyInterface) && !typex.IsUniversal(t.In(i)) {
			return nil, false, errors.New("Type interface{} isn't a supported PCollection type")
		}
		ret = append(ret, t.In(i))
	}
	return ret, true, nil
}

func isInParam(t reflect.Type) (bool, error) {
	if typex.IsUniversal(t) || typex.IsContainer(t) {
		return true, nil
	}
	return typex.CheckConcrete(t)
}
