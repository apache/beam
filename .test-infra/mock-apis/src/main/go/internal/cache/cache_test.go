package cache

import (
	"errors"
	"testing"
)

func TestIsNotExist(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "is ErrNotExist",
			err:  ErrNotExist,
			want: true,
		},
		{
			name: "only shares error text",
			err:  errors.New(ErrNotExist.Error()),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNotExist(tt.err); got != tt.want {
				t.Errorf("IsNotFoundErr() = %v, want %v", got, tt.want)
			}
		})
	}
}
