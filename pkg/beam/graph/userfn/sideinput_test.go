package userfn

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
)

func TestIsIter(t *testing.T) {
	tests := []struct {
		Fn  interface{}
		Exp bool
	}{
		{func(*int) {}, false},                                // no return
		{func() bool { return false }, false},                 // no value
		{func(*int) int { return 0 }, false},                  // no bool return
		{func(int) bool { return false }, false},              // no ptr value
		{func(*typex.EventTime) bool { return false }, false}, // no values
		{func(*int) bool { return false }, true},
		{func(*typex.EventTime, *int) bool { return false }, true},
		{func(*int, *string) bool { return false }, true},
		{func(*typex.Y, *typex.Z) bool { return false }, true},
		{func(*typex.EventTime, *int, *string) bool { return false }, true},
		{func(*int, *typex.Y, *typex.Z) bool { return false }, false},                   // too many values
		{func(*typex.EventTime, *int, *typex.Y, *typex.Z) bool { return false }, false}, // too many values
	}

	for _, test := range tests {
		val := reflect.TypeOf(test.Fn)
		if actual := IsIter(val); actual != test.Exp {
			t.Errorf("IsIter(%v) = %v, want %v", val, actual, test.Exp)
		}
	}
}

func TestIsReIter(t *testing.T) {
	tests := []struct {
		Fn  interface{}
		Exp bool
	}{
		{func() bool { return false }, false},                                      // not returning an Iter
		{func(*int) func(*int) bool { return nil }, false},                         // takes parameters
		{func(*int) (func(*int) bool, func(*int) bool) { return nil, nil }, false}, // too many iterators
		{func() func(*int) bool { return nil }, true},
		{func() func(*typex.EventTime, *int, *string) bool { return nil }, true},
	}

	for _, test := range tests {
		val := reflect.TypeOf(test.Fn)
		if actual := IsReIter(val); actual != test.Exp {
			t.Errorf("IsReIter(%v) = %v, want %v", val, actual, test.Exp)
		}
	}
}
