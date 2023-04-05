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

package sql

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/sql/sqlx"
	"reflect"
	"testing"
)

func TestOptions_Add(t *testing.T) {
	test := struct {
		opt sqlx.Option
	}{
		opt: sqlx.Option{
			Urn:     "this is a string",
			Payload: []byte{1, 2, 3, 4},
		},
	}

	o := options{}
	o.Add(test.opt)
	if o.customs == nil || !reflect.DeepEqual(o.customs[len(o.customs)-1], test.opt) {
		t.Errorf("options.Add(%v) failed. For the customs field in options, got %v, want %v", test.opt, o.customs, test.opt)
	}
}

func TestInput(t *testing.T) {
	test := struct {
		inputName string
		inputIn   beam.PCollection
	}{
		inputName: "this is a string",
		inputIn:   beam.PCollection{},
	}

	o := &options{inputs: make(map[string]beam.PCollection)}
	option := Input(test.inputName, test.inputIn)
	if option == nil {
		t.Errorf("Input(%v, %v) = %v, want not nil", test.inputName, test.inputIn, option)
	}
	option(o)
	if o.inputs == nil || !reflect.DeepEqual(o.inputs[test.inputName], test.inputIn) {
		t.Errorf("The function that Input(%v, %v) returned did not work correctly. For the inputs field in options, got %v, want %v", test.inputName, test.inputIn, o.inputs, test.inputIn)
	}
}

func TestDialect(t *testing.T) {
	test := struct {
		dialect string
	}{
		dialect: "this is a string",
	}

	o := &options{}
	option := Dialect(test.dialect)
	if option == nil {
		t.Errorf("Dialect(%v) = %v, want not nil", test.dialect, option)
	}
	option(o)
	if !reflect.DeepEqual(o.dialect, test.dialect) {
		t.Errorf("The function that Input(%v) returned did not work correctly. For the dialect field in options, got %v, want %v", test.dialect, o.dialect, test.dialect)
	}
}

func TestExpansionAddr(t *testing.T) {
	test := struct {
		addr string
	}{
		addr: "this is a string",
	}

	o := &options{}
	option := ExpansionAddr(test.addr)
	if option == nil {
		t.Errorf("ExpansionAddr(%v) = %v, want not nil", test.addr, option)
	}
	option(o)
	if !reflect.DeepEqual(o.expansionAddr, test.addr) {
		t.Errorf("The function that ExpansionAddr(%v) returned did not work correctly. For the expansionAddr field in options, got %v, want %v", test.addr, o.expansionAddr, test.addr)
	}
}
