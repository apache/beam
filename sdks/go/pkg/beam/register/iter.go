// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package register

import (
	"fmt"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

type iter1[T any] struct {
	s exec.ReStream

	// cur is the "current" stream, if any.
	cur exec.Stream
}

func (v *iter1[T]) Init() error {
	cur, err := v.s.Open()
	if err != nil {
		return err
	}
	v.cur = cur
	return nil
}

func (v *iter1[T]) Value() any {
	return v.invoke
}

func (v *iter1[T]) Reset() error {
	if err := v.cur.Close(); err != nil {
		return err
	}
	v.cur = nil
	return nil
}

func (v *iter1[T]) invoke(value *T) bool {
	elm, err := v.cur.Read()
	if err != nil {
		if err == io.EOF {
			return false
		}
		panic(fmt.Sprintf("broken stream: %v", err))
	}
	*value = elm.Elm.(T)
	return true
}

type iter2[T1, T2 any] struct {
	s exec.ReStream

	// cur is the "current" stream, if any.
	cur exec.Stream
}

func (v *iter2[T1, T2]) Init() error {
	cur, err := v.s.Open()
	if err != nil {
		return err
	}
	v.cur = cur
	return nil
}

func (v *iter2[T1, T2]) Value() any {
	return v.invoke
}

func (v *iter2[T1, T2]) Reset() error {
	if err := v.cur.Close(); err != nil {
		return err
	}
	v.cur = nil
	return nil
}

func (v *iter2[T1, T2]) invoke(key *T1, value *T2) bool {
	elm, err := v.cur.Read()
	if err != nil {
		if err == io.EOF {
			return false
		}
		panic(fmt.Sprintf("broken stream: %v", err))
	}
	*key = elm.Elm.(T1)
	*value = elm.Elm2.(T2)
	return true
}

func registerType(t reflect.Type) {
	// strip the pointer if present.
	t = reflectx.SkipPtr(t)
	if _, ok := runtime.TypeKey(t); !ok {
		return
	}
	runtime.RegisterType(t)
	schema.RegisterType(t)
}

// Iter1 registers parameters from your DoFn with a
// signature func(*T) bool and optimizes their execution.
// This must be done by passing in type parameters of all inputs as constraints,
// aka: register.Iter1[T]()
func Iter1[T any]() {
	i := (*func(*T) bool)(nil)
	registerFunc := func(s exec.ReStream) exec.ReusableInput {
		return &iter1[T]{s: s}
	}
	itT := reflect.TypeOf(i).Elem()
	registerType(itT.In(0).Elem())
	exec.RegisterInput(itT, registerFunc)
}

// Iter1 registers parameters from your DoFn with a
// signature func(*T1, *T2) bool and optimizes their execution.
// This must be done by passing in type parameters of all inputs as constraints,
// aka: register.Iter2[T1, T2]()
func Iter2[T1, T2 any]() {
	i := (*func(*T1, *T2) bool)(nil)
	registerFunc := func(s exec.ReStream) exec.ReusableInput {
		return &iter2[T1, T2]{s: s}
	}
	itT := reflect.TypeOf(i).Elem()
	registerType(itT.In(0).Elem())
	registerType(itT.In(1).Elem())
	exec.RegisterInput(itT, registerFunc)
}
