package filter_test

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
)

func TestInclude(t *testing.T) {
	tests := []struct {
		in  []int
		fn  interface{}
		exp []int
	}{
		{
			[]int{1, 2, 3},
			func(a int) bool { return true },
			[]int{1, 2, 3},
		},
		{
			[]int{1, 2, 3},
			func(a int) bool { return a == 1 },
			[]int{1},
		},
		{
			[]int{1, 2, 3},
			func(a int) bool { return a > 1 },
			[]int{2, 3},
		},
	}

	for _, test := range tests {
		p, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(p, filter.Include(p, in, test.fn), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Include(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

func TestExclude(t *testing.T) {
	tests := []struct {
		in  []int
		fn  interface{}
		exp []int
	}{
		{
			[]int{1, 2, 3},
			func(a int) bool { return false },
			[]int{1, 2, 3},
		},
		{
			[]int{1, 2, 3},
			func(a int) bool { return a == 1 },
			[]int{2, 3},
		},
		{
			[]int{1, 2, 3},
			func(a int) bool { return a > 1 },
			[]int{1},
		},
	}

	for _, test := range tests {
		p, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(p, filter.Exclude(p, in, test.fn), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Exclude(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}
