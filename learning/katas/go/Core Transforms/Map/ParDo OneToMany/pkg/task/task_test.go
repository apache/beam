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
			input: beam.Create(s, "Hello Beam", "It is awesome"),
			want: []interface{}{"Hello", "Beam", "It", "is", "awesome"},
		},
		{
			input: beam.Create(s, "Hello Beam. It is awesome."),
			want: []interface{}{"Hello", "Beam.", "It", "is", "awesome."},
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