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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

var (
	errIllegalParametersInIter     = "All parameters in an iter must be universal type, container type, or concrete type"
	errIllegalParametersInReIter   = "Output of a reiter must be valid iter type"
	errIllegalParametersInMultiMap = "Output of a multimap must be valid iter type"
	errIllegalEventTimeInIter      = "Iterators with timestamp values (<ET,V> and <ET, K, V>) are not valid, as side input time stamps are not preserved after windowing. See https://github.com/apache/beam/issues/22404 for more information."
)

// IsIter returns true iff the supplied type is a "single sweep functional iterator".
//
// A single sweep functional iterator is a function taking one or more pointers
// of data as arguments, that returns a single boolean value. The semantics of
// the function are that when called, if there are values to be supplied, they
// will be copied into the supplied pointers. The function returns true if
// data was copied, and false if there is no more data available.
func IsIter(t reflect.Type) bool {
	_, ok, _ := unfoldIter(t)
	return ok
}

// IsMalformedIter returns true iff the supplied type is an illegal "single sweep
// functional iterator" and an error explaining why it is illegal. For example,
// an iterator is not legal if one of its parameters is not concrete, universal, or
// a container type. If the type does not have the structure of an iter or it is a
// legal iter, IsMalformedIter returns false and no error.
func IsMalformedIter(t reflect.Type) (bool, error) {
	_, _, err := unfoldIter(t)
	return err != nil, err
}

// UnfoldIter returns the parameter types, if a single sweep functional
// iterator. For example:
//
//	func (*int) bool                   returns {int}
//	func (*string, *int) bool          returns {string, int}
//
// EventTimes are not allowed in iterator types as per the Beam model
// (see https://github.com/apache/beam/issues/22404) for more
// information.
func UnfoldIter(t reflect.Type) ([]reflect.Type, bool) {
	types, ok, _ := unfoldIter(t)
	return types, ok
}

func unfoldIter(t reflect.Type) ([]reflect.Type, bool, error) {
	if t.Kind() != reflect.Func {
		return nil, false, nil
	}

	if t.NumOut() != 1 || t.Out(0) != reflectx.Bool {
		return nil, false, nil
	}
	if t.NumIn() == 0 {
		return nil, false, nil
	}

	var ret []reflect.Type
	skip := 0
	if t.In(0).Kind() == reflect.Ptr && t.In(0).Elem() == typex.EventTimeType {
		return nil, false, errors.New(errIllegalEventTimeInIter)
	}
	if t.NumIn()-skip > 2 || t.NumIn() == skip {
		return nil, false, nil
	}

	for i := skip; i < t.NumIn(); i++ {
		if ok, err := isOutParam(t.In(i)); !ok {
			return nil, false, errors.Wrap(err, errIllegalParametersInIter)
		}
		if reflect.TypeOf((*any)(nil)).Elem() == t.In(i).Elem() && !typex.IsUniversal(t.In(i)) {
			return nil, false, errors.New("Type interface{} isn't a supported PCollection type")
		}
		ret = append(ret, t.In(i).Elem())
	}
	return ret, true, nil
}

func isOutParam(t reflect.Type) (bool, error) {
	if t.Kind() != reflect.Ptr {
		return false, errors.Errorf("Type %v of kind %v not allowed, must be ptr type", t, t.Kind())
	}
	if typex.IsUniversal(t.Elem()) || typex.IsContainer(t.Elem()) {
		return true, nil
	}
	return typex.CheckConcrete(t.Elem())
}

// IsReIter returns true iff the supplied type is a functional iterator generator.
//
// A functional iterator generator is a parameter-less function that returns
// single sweep functional iterators.
func IsReIter(t reflect.Type) bool {
	_, ok := UnfoldReIter(t)
	return ok
}

// IsMalformedReIter returns true iff the supplied type is an illegal functional
// iterator generator and an error explaining why it is illegal. An iterator generator
// is not legal if its output is not of type iterator. If the type does not
// have the structure of an iterator generator or it is a legal iterator generator,
// IsMalformedReIter returns false and no error.
func IsMalformedReIter(t reflect.Type) (bool, error) {
	_, _, err := unfoldReIter(t)
	return err != nil, err
}

// UnfoldReIter returns the parameter types, if a functional iterator generator.
func UnfoldReIter(t reflect.Type) ([]reflect.Type, bool) {
	types, ok, _ := unfoldReIter(t)
	return types, ok
}

func unfoldReIter(t reflect.Type) ([]reflect.Type, bool, error) {
	if t.Kind() != reflect.Func {
		return nil, false, nil
	}
	if t.NumIn() != 0 || t.NumOut() != 1 {
		return nil, false, nil
	}
	types, ok, err := unfoldIter(t.Out(0))
	if err != nil {
		err = errors.Wrap(err, errIllegalParametersInReIter)
	}
	return types, ok, err
}

// IsMultiMap returns true iff the supplied type is a keyed functional iterator
// generator.
//
// A keyed functional iterator generator is a function taking a single parameter
// (a key) and returns a corresponding single sweep functional iterator.
func IsMultiMap(t reflect.Type) bool {
	_, ok := UnfoldMultiMap(t)
	return ok
}

// IsMalformedMultiMap returns true iff the supplied type is an illegal keyed functional
// iterator generator and an error explaining why it is illegal. A keyed iterator generator
// is not legal if its output is not of type iterator. If the type does not have the
// structure of a keyed iterator generator or it is a legal iterator generator,
// IsMalformedMultiMap returns false and no error.
func IsMalformedMultiMap(t reflect.Type) (bool, error) {
	_, _, err := unfoldMultiMap(t)
	return err != nil, err
}

// UnfoldMultiMap returns the parameter types for the input key and the output
// values iff the type is a keyed functional iterator generator.
func UnfoldMultiMap(t reflect.Type) ([]reflect.Type, bool) {
	types, ok, _ := unfoldMultiMap(t)
	return types, ok
}

func unfoldMultiMap(t reflect.Type) ([]reflect.Type, bool, error) {
	if t.Kind() != reflect.Func {
		return nil, false, nil
	}
	if t.NumIn() != 1 || t.NumOut() != 1 {
		return nil, false, nil
	}
	types := []reflect.Type{t.In(0)}
	iterTypes, is, err := unfoldIter(t.Out(0))
	types = append(types, iterTypes...)
	return types, is, errors.Wrap(err, errIllegalParametersInMultiMap)
}
