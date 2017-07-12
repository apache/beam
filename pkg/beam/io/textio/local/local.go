package local

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
)

func init() {
	textio.RegisterFileSystem("default", New)
}

type fs struct{}

// New creates a new local filesystem.
func New(ctx context.Context) textio.FileSystem {
	return &fs{}
}

func (f *fs) Close() error {
	return nil
}

func (f *fs) List(ctx context.Context, glob string) ([]string, error) {
	return filepath.Glob(glob)
}

func (f *fs) OpenRead(ctx context.Context, filename string) (io.ReadCloser, error) {
	return os.Open(filename)
}

func (f *fs) OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error) {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return nil, err
	}
	return os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
}
