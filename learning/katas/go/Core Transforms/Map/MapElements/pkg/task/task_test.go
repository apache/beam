package task

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"testing"
)

func TestApplyTransform(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	tests := []struct {
		input beam.PCollection
		want []interface{}
	}{
		{
			input: beam.Create(s, -1, -2, -3, -4, -5),
			want: []interface{}{-5, -10, -15, -20, -25},
		},
		{
			input: beam.Create(s, 1, 2, 3, 4, 5),
			want: []interface{}{5, 10, 15, 20, 25},
		},
	}
	for _, tt := range tests {
		got := ApplyTransform(s, tt.input)
		passert.Equals(s, got, tt.want...)
		if err := ptest.Run(p); err != nil {
			t.Errorf("ApplyTransform(\"%v\") = %v", tt.input, err)
		}
	}
}