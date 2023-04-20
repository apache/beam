package schema

import (
	"github.com/google/go-cmp/cmp"
	"sort"
	"testing"
)

func TestVersionSort(t *testing.T) {
	tests := []struct {
		name     string
		versions []DBVersion
		want     []DBVersion
	}{
		{
			name: "Test sorting versions",
			versions: []DBVersion{
				{0, 0, 1},
				{0, 1, 10},
				{0, 0, 2},
				{15, 3, 5},
				{0, 1, 0},
				{0, 0, 10},
			},
			want: []DBVersion{
				{0, 0, 1},
				{0, 0, 2},
				{0, 0, 10},
				{0, 1, 0},
				{0, 1, 10},
				{15, 3, 5},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temp := make([]DBVersion, len(tt.versions))
			copy(temp, tt.versions)
			sort.Sort(ByVersion(temp))

			if !cmp.Equal(temp, tt.want) {
				t.Errorf("sortVersions() = %v, want %v", temp, tt.want)
			}
		})
	}
}
