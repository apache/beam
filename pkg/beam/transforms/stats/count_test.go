package stats

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam"
)

type count struct {
	Elm int
	Count int
}

func kvToCount(e int, c int) count {
	return count{e, c}
}

// TestCountInt verifies that Count works correctly for ints.
func TestCountInt(t *testing.T) {
	tests := []struct {
		in  []int
		exp []count
	}{
		{
			[]int{0},
			[]count{{0,1}},
		},
		{
			[]int{0, 0},
			[]count{{0,2}},
		},
		{
			[]int{1, -4, 1, -1},
			[]count{{1, 2},{-1, 1},{-4, 1}},
		},
		{
			[]int{1, -1, 1, -1, 1},
			[]count{{1, 3},{-1, 2}},
		},
	}

	for _, test := range tests {
		p, in, exp := ptest.CreateList2(test.in, test.exp)
		count := Count(p, in)
		formatted := beam.ParDo(p, kvToCount, count)
		passert.Equals(p, formatted, exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Count(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}
