package preparers

import "testing"

func TestGetScioPreparers(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			// Test case with calling GetPythonPreparers method.
			// As a result, want to receive slice of preparers with len = 1
			name: "get python preparers",
			args: args{"MOCK_FILEPATH"},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewPreparersBuilder(tt.args.filePath)
			GetScioPreparers(builder)
			if got := builder.Build().GetPreparers(); len(*got) != tt.want {
				t.Errorf("GetScioPreparers() returns %v Preparers, want %v", len(*got), tt.want)
			}
		})
	}
}
