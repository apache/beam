package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"reflect"
	"testing"
)

func TestJSONCoder(t *testing.T) {
	numbers := []int{43, 12431235, -2, 0, 1}

	from := make(chan int, len(numbers))
	for _, elm := range numbers {
		from <- elm
	}
	close(from)

	via := make(chan []byte, len(numbers)*2)
	if err := jsonEnc(jsonContext{}, reflect.ValueOf(from), via); err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}
	close(via)

	to := make(chan int, len(numbers)*2)
	if err := jsonDec(jsonContext{T: reflectx.Int}, reflect.ValueOf(to), via); err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}
	close(to)

	var actual []int
	for num := range to {
		actual = append(actual, num)
	}

	if !reflect.DeepEqual(actual, numbers) {
		t.Errorf("Corrupt coding: %v, want %v", actual, numbers)
	}
}
