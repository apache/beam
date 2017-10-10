package stats

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

type student struct {
	Name  string
	Grade float64
}

func kvToStudent(n string, g float64) student {
	return student{n, g}
}

func studentToKV(s student) (string, float64) {
	return s.Name, s.Grade
}

// TestMaxInt verifies that Max works correctly for ints.
func TestMaxInt(t *testing.T) {
	tests := []struct {
		in  []int
		exp []int
	}{
		{
			[]int{1, -4, 3},
			[]int{3},
		},
		{
			[]int{7, 11, 7, 5, 10},
			[]int{11},
		},
		{
			[]int{0, -2, -10},
			[]int{0},
		},
		{
			[]int{-9},
			[]int{-9},
		},
	}

	for _, test := range tests {
		p, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(p, Max(p, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Max(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestMaxFloat verifies that Max works correctly for float64s.
func TestMaxFloat(t *testing.T) {
	tests := []struct {
		in  []float64
		exp []float64
	}{
		{
			[]float64{1, -2, 3.5},
			[]float64{3.5},
		},
		{
			[]float64{0, -99.99, 1, 1},
			[]float64{1},
		},
		{
			[]float64{5.67890},
			[]float64{5.6789},
		},
	}

	for _, test := range tests {
		p, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(p, Max(p, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Max(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestMaxKeyed verifies that Max works correctly for KV values.
func TestMaxKeyed(t *testing.T) {
	tests := []struct {
		in  []student
		exp []student
	}{
		{
			[]student{{"alpha", 1}, {"beta", 4}, {"charlie", 3.5}},
			[]student{{"alpha", 1}, {"beta", 4}, {"charlie", 3.5}},
		},
		{
			[]student{{"alpha", 1}},
			[]student{{"alpha", 1}},
		},
		{
			[]student{{"alpha", 1}, {"alpha", -4}, {"beta", 4}, {"charlie", 0}, {"charlie", 5.5}},
			[]student{{"alpha", 1}, {"beta", 4}, {"charlie", 5.5}},
		},
	}

	for _, test := range tests {
		p, in, exp := ptest.CreateList2(test.in, test.exp)
		kv := beam.ParDo(p, studentToKV, in)
		max := Max(p, kv)
		maxStudent := beam.ParDo(p, kvToStudent, max)
		passert.Equals(p, maxStudent, exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Max(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}
