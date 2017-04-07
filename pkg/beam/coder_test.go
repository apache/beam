package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"testing"
)

func TestJSONCoder(t *testing.T) {
	tests := []int{43, 12431235, -2, 0, 1}

	for _, test := range tests {
		data, err := jsonEnc(jsonContext{}, test)
		if err != nil {
			t.Fatalf("Failed to encode %v: %v", tests, err)
		}
		decoded, err := jsonDec(jsonContext{T: graph.DataType{reflectx.Int}}, data)
		if err != nil {
			t.Fatalf("Failed to decode: %v", err)
		}
		actual := decoded.(int)

		if test != actual {
			t.Errorf("Corrupt coding: %v, want %v", actual, test)
		}
	}
}
