package typex

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
)

func TestIsBound(t *testing.T) {
	tests := []struct {
		T   FullType
		Exp bool
	}{
		{New(reflectx.Int), true},
		{New(TType), false},
		{NewWGBK(New(TType), New(reflectx.String)), false},
		{NewWGBK(New(reflectx.String), New(reflectx.String)), true},
		{NewWKV(New(reflectx.String), New(reflect.SliceOf(reflectx.Int))), true},
		{NewWKV(New(reflectx.String), New(reflect.SliceOf(XType))), false},
		{NewWKV(New(reflectx.String), New(reflectx.String)), true},
	}

	for _, test := range tests {
		if actual := IsBound(test.T); actual != test.Exp {
			t.Errorf("IsBound(%v) = %v, want %v", test.T, actual, test.Exp)
		}
	}
}

func TestIsStructurallyAssignable(t *testing.T) {
	tests := []struct {
		A, B FullType
		Exp  bool
	}{
		{New(reflectx.Int), New(reflectx.Int), true},
		{New(reflectx.Int32), New(reflectx.Int64), false}, // from Go assignability
		{New(reflectx.Int64), New(reflectx.Int32), false}, // from Go assignability
		{New(reflectx.Int), New(TType), true},
		{New(XType), New(TType), true},
		{NewWKV(New(XType), New(YType)), New(TType), false},                                                  // T cannot match composites
		{NewWKV(New(reflectx.Int), New(reflectx.Int)), NewWGBK(New(reflectx.Int), New(reflectx.Int)), false}, // structural mismatch
		{NewWKV(New(XType), New(reflectx.Int)), NewWKV(New(TType), New(UType)), true},
		{NewWKV(New(XType), New(reflectx.Int)), NewWKV(New(TType), New(XType)), true},
		{NewWKV(New(reflectx.String), New(reflectx.Int)), NewWKV(New(TType), New(TType)), true},
		{NewWKV(New(reflectx.Int), New(reflectx.Int)), NewWKV(New(TType), New(TType)), true},
		{NewWKV(New(reflectx.Int), New(reflectx.String)), NewWKV(New(TType), New(reflectx.String)), true},
	}

	for _, test := range tests {
		if actual := IsStructurallyAssignable(test.A, test.B); actual != test.Exp {
			t.Errorf("IsStructurallyAssignable(%v -> %v) = %v, want %v", test.A, test.B, actual, test.Exp)
		}
	}
}

func TestBindSubstitute(t *testing.T) {
	tests := []struct {
		Model, In, Out, Exp FullType
	}{
		{
			New(reflectx.String),
			New(XType),
			NewWKV(New(reflectx.Int), New(XType)),
			NewWKV(New(reflectx.Int), New(reflectx.String)),
		},
		{
			New(reflectx.String),
			New(XType),
			NewWKV(New(reflectx.Int), New(reflectx.Int)),
			NewWKV(New(reflectx.Int), New(reflectx.Int)),
		},
		{
			NewWKV(New(reflectx.Int), New(reflectx.String)),
			NewWKV(New(XType), New(YType)),
			NewWGBK(New(XType), New(XType)),
			NewWGBK(New(reflectx.Int), New(reflectx.Int)),
		},
		{
			NewWGBK(New(reflectx.Int), New(reflectx.String)),
			NewWGBK(New(XType), New(YType)),
			NewWGBK(New(YType), New(XType)),
			NewWGBK(New(reflectx.String), New(reflectx.Int)),
		},
		{
			NewWGBK(New(ZType), New(XType)),
			NewWGBK(New(XType), New(YType)),
			NewWGBK(New(YType), New(XType)),
			NewWGBK(New(XType), New(ZType)),
		},
	}

	for _, test := range tests {
		binding, err := Bind([]FullType{test.In}, []FullType{test.Model})
		if err != nil {
			t.Errorf("Bind(%v <- %v) failed: %v", test.In, test.Model, err)
			continue
		}

		actual, err := Substitute([]FullType{test.Out}, binding)
		if err != nil {
			t.Errorf("Substitute(%v, %v) failed: %v", test.Out, binding, err)
			continue
		}

		if !IsEqual(actual[0], test.Exp) {
			t.Errorf("Substitute(%v,%v) = %v, want %v", test.Out, binding, actual, test.Exp)
		}
	}
}
