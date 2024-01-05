package prompt

import (
	"bytes"
	"io"
	"testing"
)

func TestPrompt_show(t *testing.T) {
	type testCase[R Result] struct {
		name string
		p    Prompt[R]
		want string
	}
	tests := []testCase[string]{
		{
			name: "display not empty",
			p: Prompt[string]{
				display: "not empty",
			},
			want: "not empty",
		},
		{
			name: "display empty/no default",
			p: Prompt[string]{
				Message: "prompt:",
				Values: map[string]string{
					"a": "1",
					"b": "2",
				},
			},
			want: "prompt: (a/b) ",
		},
		{
			name: "display empty/has default",
			p: Prompt[string]{
				Message: "prompt:",
				Values: map[string]string{
					"a": "1",
					"b": "2",
				},
				def: "2",
			},
			want: "prompt: (B/a) ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.show(); got != tt.want {
				t.Errorf("show() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPrompt_hasKey(t *testing.T) {
	x := "x"
	type args[R Result] struct {
		value *R
	}
	type testCase[R Result] struct {
		name string
		p    Prompt[R]
		args args[R]
		want bool
	}
	tests := []testCase[string]{
		{
			name: "x vs {a: x, b: y}",
			p: Prompt[string]{
				Values: map[string]string{
					"a": "x",
					"b": "y",
				},
			},
			args: args[string]{
				value: &x,
			},
			want: true,
		},
		{
			name: "x vs {a: z, b: y}",
			p: Prompt[string]{
				Values: map[string]string{
					"a": "z",
					"b": "y",
				},
			},
			args: args[string]{
				value: &x,
			},
			want: false,
		},
		{
			name: "nil vs {}",
			p:    Prompt[string]{},
			args: args[string]{},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.hasKey(tt.args.value); got != tt.want {
				t.Errorf("hasKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPrompt_run(t *testing.T) {
	type args[R Result] struct {
		r     io.Reader
		input R
	}
	type testCase[R Result] struct {
		name    string
		p       Prompt[R]
		args    args[R]
		want    R
		wantErr bool
	}
	tests := []testCase[string]{
		{
			name: "no response / no default",
			p:    Prompt[string]{},
			args: args[string]{
				r: &bytes.Buffer{},
			},
			wantErr: false,
		},
		{
			name: "no response / default",
			p: Prompt[string]{
				def: "a",
			},
			args: args[string]{
				r: &bytes.Buffer{},
			},
			wantErr: false,
		},
		{
			name: "wrong then right",
			p: Prompt[string]{
				Values: map[string]string{
					"a": "x",
					"b": "y",
				},
			},
			args: args[string]{
				r: bytes.NewBufferString("c\na"),
			},
			want: "x",
		},
		{
			name: "spaces in answer",
			p: Prompt[string]{
				Values: map[string]string{
					"a": "x",
					"b": "y",
				},
			},
			args: args[string]{
				r: bytes.NewBufferString("   a      "),
			},
			want: "x",
		},
		{
			name: "provides alias",
			p: Prompt[string]{
				Values: map[string]string{
					"a": "x",
					"b": "y",
				},
				Alias: map[string]string{
					"1": "a",
					"2": "b",
				},
			},
			args: args[string]{
				r: bytes.NewBufferString("1"),
			},
			want: "x",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			if tt.args.input != "" {
				got = tt.args.input
			}
			if err := tt.p.run(tt.args.r, &got); (err != nil) != tt.wantErr {
				t.Errorf("run() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got != tt.want {
				t.Errorf("run() = %v, want %v", got, tt.want)
			}
		})
	}
}
