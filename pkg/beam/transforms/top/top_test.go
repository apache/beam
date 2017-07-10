package top

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
)

// TestCombineFn3String verifies that the accumulator correctly
// maintains the top 3 longest strings.
func TestCombineFn3String(t *testing.T) {
	less := func(a, b string) bool {
		return len(a) < len(b)
	}
	fn := &combineFn{N: 3, Less: graphx.DataFnValue{Fn: reflect.ValueOf(less)}}

	tests := []struct {
		Elms     []string
		Expected []string
	}{
		{[]string{}, nil},
		{[]string{"foo"}, []string{"foo"}},
		{[]string{"1", "2", "3", "4", "5"}, []string{"1", "2", "3"}},
		{[]string{"a1", "b22", "c22", "d333", "e22"}, []string{"d333", "b22", "c22"}},
	}

	for _, test := range tests {
		a := load(fn, test.Elms...)

		actual := output(fn, a)
		if !reflect.DeepEqual(actual, test.Expected) {
			t.Errorf("CombineFn(3; %v) = %v, want %v", test.Elms, actual, test.Expected)
		}
	}
}

// TestCombineFn3RevString verifies that the accumulator correctly
// maintains the top 3 shorest strings.
func TestCombineFn3RevString(t *testing.T) {
	less := func(a, b string) bool {
		return len(a) < len(b)
	}
	fn := &combineFn{N: 3, Less: graphx.DataFnValue{Fn: reflect.ValueOf(less)}, Reversed: true}

	tests := []struct {
		Elms     []string
		Expected []string
	}{
		{[]string{}, nil},
		{[]string{"foo"}, []string{"foo"}},
		{[]string{"1", "2", "3", "4", "5"}, []string{"1", "2", "3"}},
		{[]string{"a1", "b22", "c22", "d333", "e22"}, []string{"a1", "b22", "c22"}},
	}

	for _, test := range tests {
		a := load(fn, test.Elms...)

		actual := output(fn, a)
		if !reflect.DeepEqual(actual, test.Expected) {
			t.Errorf("CombineFn(3; %v) = %v, want %v", test.Elms, actual, test.Expected)
		}
	}
}

// TestCombineFnMerge verifies that accumulators merge correctly.
func TestCombineFnMerge(t *testing.T) {
	less := func(a, b string) bool {
		return len(a) < len(b)
	}
	fn := &combineFn{N: 3, Less: graphx.DataFnValue{Fn: reflect.ValueOf(less)}}

	tests := []struct {
		Elms     [][]string
		Expected []string
	}{
		{[][]string{nil}, nil},
		{[][]string{{"foo"}}, []string{"foo"}},
		{[][]string{{"1", "2"}, {"3"}, {"4", "5"}}, []string{"1", "2", "3"}},
		{[][]string{{"a1"}, {"b22", "c22"}, {"d333"}, {"e22"}}, []string{"d333", "b22", "c22"}},
	}

	for _, test := range tests {
		var list []accum
		for _, a := range test.Elms {
			list = append(list, load(fn, a...))
		}

		actual := output(fn, fn.MergeAccumulators(list))
		if !reflect.DeepEqual(actual, test.Expected) {
			t.Errorf("CombineFn(3; %v) = %v, want %v", test.Elms, actual, test.Expected)
		}
	}
}

func load(fn *combineFn, elms ...string) accum {
	a := fn.CreateAccumulator()
	for _, elm := range elms {
		a = fn.AddInput(a, elm)
	}
	return a
}

func output(fn *combineFn, a accum) []string {
	var ret []string
	for _, actual := range fn.ExtractOutput(a) {
		ret = append(ret, actual.(string))
	}
	return ret
}
