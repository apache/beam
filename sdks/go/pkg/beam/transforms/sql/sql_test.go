package sql

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/sql/sqlx"
	"reflect"
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
		out  Option
	}{
		{
			name: name1,
			in:   collection1,
			out: func(o sqlx.Options) {
				o.(*options).inputs[name1] = collection1
			},
		},
	}

	o := &options{inputs: make(map[string]beam.PCollection)}
	for _, test := range tests {
		option := Input(test.name, test.in)
		if option == nil {
			t.Errorf("Execting funtion Input failed")
		}
		option(o)
		if o.inputs == nil || !reflect.DeepEqual(o.inputs[test.name], test.in) {
			t.Errorf("Execting funtion Input failed")
		}
	}
}

func TestDialect(t *testing.T) {
	dialect1 := "this is a string"
	tests := []struct {
		dialect string
		out     Option
	}{
		{
			dialect: dialect1,
			out: func(o sqlx.Options) {
				o.(*options).dialect = dialect1
			},
		},
	}

	o := &options{}
	for _, test := range tests {
		option := Dialect(test.dialect)
		if option == nil {
			t.Errorf("Execting funtion Dialect failed")
		}
		option(o)
		if !reflect.DeepEqual(o.dialect, test.dialect) {
			t.Errorf("Execting funtion Input failed")
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
		option := Dialect(test.addr)
		if option == nil {
			t.Errorf("Execting funtion Dialect failed")
		}
		option(o)
		if !reflect.DeepEqual(o.expansionAddr, test.addr) {
			t.Errorf("Execting funtion Input failed")
		}
	}
}
