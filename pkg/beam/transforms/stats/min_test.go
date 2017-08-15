package stats

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam"
)

// TestMinInt verifies that Min works correctly for ints.
func TestMinInt(t *testing.T) {
	tests := []struct {
		in  []int
		exp []int
	}{
		{
			[]int{1, -4, 3},
			[]int{-4},
		},
		{
			[]int{7, 11, 7, 5, 10},
			[]int{5},
		},
		{
			[]int{0, -2, -10},
			[]int{-10},
		},
		{
			[]int{-9},
			[]int{-9},
		},
	}

	for _, test := range tests {
		p, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(p, Min(p, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Min(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestMinFloat verifies that Min works correctly for float64s.
func TestMinFloat(t *testing.T) {
	tests := []struct {
		in  []float64
		exp []float64
	}{
		{
			[]float64{1, -2, 3.5},
			[]float64{-2},
		},
		{
			[]float64{0, -99.99, 1, 1},
			[]float64{-99.99},
		},
		{
			[]float64{5.67890},
			[]float64{5.6789},
		},
	}

	for _, test := range tests {
		p, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(p, Min(p, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Min(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestMinKeyed verifies that Min works correctly for KV values.
func TestMinKeyed(t *testing.T) {
	tests := []struct {
		in  []student
		exp []student
	}{
		{
			[]student{{"alpha", 1}, {"beta", 4}, {"charlie",3.5}},
			[]student{{"alpha", 1}, {"beta", 4}, {"charlie",3.5}},
		},
		{
			[]student{{"alpha", 1}},
			[]student{{"alpha",1}},
		},
		{
			[]student{{"alpha", 1}, {"alpha", -4},{"beta", 4}, {"charlie",0},{"charlie",5.5}},
			[]student{{"alpha", -4},{"beta", 4},{"charlie",0}},
		},
	}

	for _, test := range tests {
		p, in, exp := ptest.CreateList2(test.in, test.exp)
		kv := beam.ParDo(p, studentToKV, in)
		min := Min(p, kv)
		minStudent := beam.ParDo(p, kvToStudent, min)
		passert.Equals(p, minStudent, exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Min(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}
