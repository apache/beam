package beam

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

func TestJSONCoder(t *testing.T) {
	tests := []int{43, 12431235, -2, 0, 1}

	for _, test := range tests {
		data, err := JSONEnc(test)
		if err != nil {
			t.Fatalf("Failed to encode %v: %v", tests, err)
		}
		decoded, err := JSONDec(reflectx.Int, data)
		if err != nil {
			t.Fatalf("Failed to decode: %v", err)
		}
		actual := decoded.(int)

		if test != actual {
			t.Errorf("Corrupt coding: %v, want %v", actual, test)
		}
	}
}
