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
	"context"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

type myTestTypeEmitter1 struct {
	Int int
}

func TestEmitter1(t *testing.T) {
	Emitter1[int]()
	e1T := reflect.TypeOf((*func(int))(nil)).Elem()
	if !exec.IsEmitterRegistered(e1T) {
		t.Fatalf("exec.IsEmitterRegistered(%v) = false, want true", e1T)
	}

	Emitter1[myTestTypeEmitter1]()
	rt := reflect.TypeOf((*myTestTypeEmitter1)(nil)).Elem()
	checkRegisterations(t, rt)
}

type myTestTypeEmitter2A struct {
	Int int
}

type myTestTypeEmitter2B struct {
	String string
}

func TestEmitter2(t *testing.T) {
	Emitter2[int, string]()
	e2isT := reflect.TypeOf((*func(int, string))(nil)).Elem()
	if !exec.IsEmitterRegistered(e2isT) {
		t.Fatalf("exec.IsEmitterRegistered(%v) = false, want true", e2isT)
	}

	Emitter2[*myTestTypeEmitter2A, myTestTypeEmitter2B]()
	e2ABT := reflect.TypeOf((*func(*myTestTypeEmitter2A, myTestTypeEmitter2B))(nil)).Elem()
	if !exec.IsEmitterRegistered(e2ABT) {
		t.Fatalf("exec.IsEmitterRegistered(%v) = false, want true", e2ABT)
	}

	tA := reflect.TypeOf((*myTestTypeEmitter2A)(nil)).Elem()
	checkRegisterations(t, tA)
	tB := reflect.TypeOf((*myTestTypeEmitter2B)(nil)).Elem()
	checkRegisterations(t, tB)
}

func TestEmitter2_WithTimestamp(t *testing.T) {
	Emitter2[typex.EventTime, string]()
	e2tssT := reflect.TypeOf((*func(typex.EventTime, string))(nil)).Elem()
	if !exec.IsEmitterRegistered(e2tssT) {
		t.Fatalf("exec.IsEmitterRegistered(%v) = false, want true", e2tssT)
	}
}

type myTestTypeEmitter3A struct {
	Int int
}

type myTestTypeEmitter3B struct {
	String string
}

func TestEmitter3(t *testing.T) {
	Emitter3[typex.EventTime, int, string]()
	if !exec.IsEmitterRegistered(reflect.TypeOf((*func(typex.EventTime, int, string))(nil)).Elem()) {
		t.Fatalf("exec.IsEmitterRegistered(reflect.TypeOf((*func(typex.EventTime, int, string))(nil)).Elem()) = false, want true")
	}

	Emitter3[typex.EventTime, myTestTypeEmitter3A, *myTestTypeEmitter3B]()
	e3tsABT := reflect.TypeOf((*func(typex.EventTime, myTestTypeEmitter3A, *myTestTypeEmitter3B))(nil)).Elem()
	if !exec.IsEmitterRegistered(e3tsABT) {
		t.Fatalf("exec.IsEmitterRegistered(%v) = false, want true", e3tsABT)
	}
	tA := reflect.TypeOf((*myTestTypeEmitter3A)(nil)).Elem()
	checkRegisterations(t, tA)
	tB := reflect.TypeOf((*myTestTypeEmitter3B)(nil)).Elem()
	checkRegisterations(t, tB)
}

func TestEmit1(t *testing.T) {
	e := &emit1[int]{n: &elementProcessor{}}
	e.Init(context.Background(), []typex.Window{}, mtime.ZeroTimestamp)
	fn := e.Value().(func(int))
	fn(3)
	if got, want := e.n.(*elementProcessor).inFV.Elm, 3; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm=%v, want %v", got, want)
	}
	if got := e.n.(*elementProcessor).inFV.Elm2; got != nil {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm2=%v, want nil", got)
	}
	if got, want := e.n.(*elementProcessor).inFV.Timestamp, mtime.ZeroTimestamp; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Timestamp=%v, want %v", got, want)
	}
}

func TestEmit2(t *testing.T) {
	e := &emit2[int, string]{n: &elementProcessor{}}
	e.Init(context.Background(), []typex.Window{}, mtime.ZeroTimestamp)
	fn := e.Value().(func(int, string))
	fn(3, "hello")
	if got, want := e.n.(*elementProcessor).inFV.Elm, 3; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm=%v, want %v", got, want)
	}
	if got, want := e.n.(*elementProcessor).inFV.Elm2, "hello"; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm2=%v, want %v", got, want)
	}
	if got, want := e.n.(*elementProcessor).inFV.Timestamp, mtime.ZeroTimestamp; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Timestamp=%v, want %v", got, want)
	}
}

func TestEmit1WithTimestamp(t *testing.T) {
	e := &emit1WithTimestamp[int]{n: &elementProcessor{}}
	e.Init(context.Background(), []typex.Window{}, mtime.ZeroTimestamp)
	fn := e.Value().(func(typex.EventTime, int))
	fn(mtime.MaxTimestamp, 3)
	if got, want := e.n.(*elementProcessor).inFV.Elm, 3; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm=%v, want %v", got, want)
	}
	if got := e.n.(*elementProcessor).inFV.Elm2; got != nil {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm2=%v, want nil", got)
	}
	if got, want := e.n.(*elementProcessor).inFV.Timestamp, mtime.MaxTimestamp; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Timestamp=%v, want %v", got, want)
	}
}

func TestEmit2WithTimestamp(t *testing.T) {
	e := &emit2WithTimestamp[int, string]{n: &elementProcessor{}}
	e.Init(context.Background(), []typex.Window{}, mtime.ZeroTimestamp)
	fn := e.Value().(func(typex.EventTime, int, string))
	fn(mtime.MaxTimestamp, 3, "hello")
	if got, want := e.n.(*elementProcessor).inFV.Elm, 3; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm=%v, want %v", got, want)
	}
	if got, want := e.n.(*elementProcessor).inFV.Elm2, "hello"; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm2=%v, want %v", got, want)
	}
	if got, want := e.n.(*elementProcessor).inFV.Timestamp, mtime.MaxTimestamp; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Timestamp=%v, want %v", got, want)
	}
}
