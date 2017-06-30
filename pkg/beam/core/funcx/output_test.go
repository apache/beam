package funcx

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

func TestIsEmit(t *testing.T) {
	tests := []struct {
		Fn  interface{}
		Exp bool
	}{
		{func() {}, false}, // no values
		{func(string) {}, true},
		{func(string) bool { return true }, false}, // return value
		{func(string, int) {}, true},
		{func(typex.T, string) {}, true},
		{func(typex.Z, typex.Z) {}, true},
		{func(typex.EventTime) {}, false}, // no actual values
		{func(typex.EventTime, string) {}, true},
		{func(typex.EventTime, typex.T, typex.X) {}, true},
		{func(typex.EventTime, typex.T, typex.X, int) {}, false}, // too many values
	}

	for _, test := range tests {
		val := reflect.TypeOf(test.Fn)
		if actual := IsEmit(val); actual != test.Exp {
			t.Errorf("IsEmit(%v) = %v, want %v", val, actual, test.Exp)
		}
	}
}
