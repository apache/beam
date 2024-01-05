package environment

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func TestExecutable_Execute(t *testing.T) {
	type args struct {
		timeout time.Duration
		r       io.Reader
		args    []string
	}
	tests := []struct {
		name    string
		ex      Executable
		args    args
		wantW   string
		wantErr bool
	}{
		{
			name: "echo 'hi'",
			ex:   "echo",
			args: args{
				args: []string{
					"hi",
				},
			},
			wantW: "hi\n",
		},
		{
			name:  "echo",
			ex:    "echo",
			args:  args{},
			wantW: "\n",
		},
		{
			name: "STDIN|cat",
			ex:   "cat",
			args: args{
				r: bytes.NewBufferString("hi"),
			},
			wantW: "hi",
		},
		{
			name: "context with timeout",
			ex:   "sleep",
			args: args{
				timeout: time.Second,
				args: []string{
					"10000",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			if tt.args.timeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, tt.args.timeout)
			}
			defer cancel()

			w := &bytes.Buffer{}
			err := tt.ex.Execute(ctx, tt.args.r, w, tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("Execute() gotW = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}
