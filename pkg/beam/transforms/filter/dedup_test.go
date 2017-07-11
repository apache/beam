package filter_test

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
)

type s struct {
	A int
	B string
}

func TestDedup(t *testing.T) {
	tests := []struct {
		dups []interface{}
		exp  []interface{}
	}{
		{
			[]interface{}{1, 2, 3},
			[]interface{}{1, 2, 3},
		},
		{
			[]interface{}{3, 2, 1},
			[]interface{}{1, 2, 3},
		},
		{
			[]interface{}{1, 1, 1, 2, 3},
			[]interface{}{1, 2, 3},
		},
		{
			[]interface{}{1, 2, 3, 2, 2, 2, 3, 1, 1, 1, 2, 3, 1},
			[]interface{}{1, 2, 3},
		},
		{
			[]interface{}{"1", "2", "3", "2", "1"},
			[]interface{}{"1", "2", "3"},
		},
		{
			[]interface{}{s{1, "a"}, s{2, "a"}, s{1, "a"}},
			[]interface{}{s{1, "a"}, s{2, "a"}},
		},
	}

	for _, test := range tests {
		p, in, exp := ptest.Create2(test.dups, test.exp)
		passert.Equals(p, filter.Dedup(p, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Dedup(%v) failed: %v", test.dups, err)
		}
	}
}
