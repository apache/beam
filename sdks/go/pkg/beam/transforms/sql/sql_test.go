package sql

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/sql/sqlx"
	"reflect"
	"runtime"
	"testing"
)

func TestOptions_Add(t *testing.T) {
	o := options{}
	str := "this is a string"
	bytes := []byte{1, 2, 3, 4}
	testOpt := sqlx.Option{
		Urn:     str,
		Payload: bytes,
	}
	tests := []struct {
		opt sqlx.Option
	}{
		{
			opt: testOpt,
		},
	}

	for _, test := range tests {
		o.Add(test.opt)
		if o.customs == nil || !reflect.DeepEqual(o.customs[len(o.customs)-1], test.opt) {
			t.Errorf("Add failed.")
		}
	}
}

func TestInput(t *testing.T) {
	name1 := "this is a string"
	collection1 := beam.PCollection{}
	tests := []struct {
		name string
		in   beam.PCollection
	}{
		{
			name: name1,
			in:   collection1,
		},
	}

	o := &options{inputs: make(map[string]beam.PCollection)}
	for _, test := range tests {
		option := Input(test.name, test.in)
		if option == nil {
			t.Errorf("%v return a nil value, which is not expected", runtime.FuncForPC(reflect.ValueOf(TestInput).Pointer()).Name())
		}
		option(o)
		if o.inputs == nil || !reflect.DeepEqual(o.inputs[test.name], test.in) {
			t.Errorf("%v did not modify inputs, which is not expected. Expected: %v. Got: %v", runtime.FuncForPC(reflect.ValueOf(TestExpansionAddr).Pointer()).Name(), test.in, o.inputs[test.name])
		}
	}
}

func TestDialect(t *testing.T) {
	dialect1 := "this is a string"
	tests := []struct {
		dialect string
	}{
		{
			dialect: dialect1,
		},
	}

	o := &options{}
	for _, test := range tests {
		option := Dialect(test.dialect)
		if option == nil {
			t.Errorf("%v return a nil value, which is not expected", runtime.FuncForPC(reflect.ValueOf(TestDialect).Pointer()).Name())
		}
		option(o)
		if !reflect.DeepEqual(o.dialect, test.dialect) {
			t.Errorf("%v did not modify dialect, which is not expected. Expected: %v. Got: %v", runtime.FuncForPC(reflect.ValueOf(TestExpansionAddr).Pointer()).Name(), test.dialect, o.dialect)
		}
	}
}

func TestExpansionAddr(t *testing.T) {
	addr1 := "this is a string"
	tests := []struct {
		addr string
	}{
		{
			addr: addr1,
		},
	}

	o := &options{}
	for _, test := range tests {
		option := ExpansionAddr(test.addr)
		if option == nil {
			t.Errorf("%v return a nil value, which is not expected", runtime.FuncForPC(reflect.ValueOf(TestExpansionAddr).Pointer()).Name())
		}
		option(o)
		if !reflect.DeepEqual(o.expansionAddr, test.addr) {
			t.Errorf("%v did not modify expansionAddr, which is not expected. Expected: %v. Got: %v", runtime.FuncForPC(reflect.ValueOf(TestExpansionAddr).Pointer()).Name(), test.addr, o.expansionAddr)
		}
	}
}
