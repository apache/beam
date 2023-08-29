package exec

import (
	"context"
	"testing"
)

var toStringTestCases = []struct {
	Input    []any
	Expected []any
}{
	{Input: strInput, Expected: strInput},
	{Input: intInput, Expected: strInput},
	{Input: int64Input, Expected: strInput},
}

func TestToString(t *testing.T) {
	for _, testCase := range toStringTestCases {
		out := &CaptureNode{UID: 1}
		toString := &ToString{UID: 2, Out: out}
		a := &FixedRoot{UID: 3, Elements: makeKVInput("key", testCase.Input...), Out: toString}

		p, err := NewPlan("a", []Unit{a, toString, out}) // order matters in this test
		if err != nil {
			t.Fatalf("failed to construct plan: %v", err)
		}

		if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
			t.Fatalf("execute failed: %v", err)
		}

		if err := p.Down(context.Background()); err != nil {
			t.Fatalf("down failed: %v", err)
		}

		expected := makeKVValues("key", testCase.Expected...)
		if !equalList(out.Elements, expected) {
			t.Errorf("tostring returned %v, want %v", extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
		}
	}
}
