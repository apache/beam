package mongodbio

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/google/go-cmp/cmp"
)

func Test_createIDFn(t *testing.T) {
	type doc struct {
		Field1 int32 `bson:"field1"`
	}

	tests := []struct {
		name string
		elem beam.Y
		want beam.Y
	}{
		{
			name: "Create key-value pair of a new object ID and element",
			elem: doc{Field1: 1},
			want: doc{Field1: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKey, gotValue := createIDFn(tt.elem)

			if gotKey.IsZero() {
				t.Error("createIDFn() gotKey is zero")
			}

			if !cmp.Equal(gotValue, tt.want) {
				t.Errorf("createIDFn() gotValue = %v, want %v", gotValue, tt.want)
			}
		})
	}
}
