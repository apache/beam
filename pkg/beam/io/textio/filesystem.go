package textio

import (
	"context"
	"fmt"
	"io"
)

var registry = make(map[string]func(context.Context) FileSystem)

// RegisterFileSystem registers a file system backend for textio.Read/Write,
// under the given scheme.For example, "hdfs" would be registered a HFDS file
// system and HDFS paths used transparently.
func RegisterFileSystem(scheme string, fs func(context.Context) FileSystem) {
	if _, ok := registry[scheme]; ok {
		panic(fmt.Sprintf("scheme %v already registered", scheme))
	}
	registry[scheme] = fs
}

// FileSystem is a filesystem abstraction that allows textio to use various
// underlying storage systems transparently.
type FileSystem interface {
	io.Closer

	// List expands a patten to a list of filenames.
	List(ctx context.Context, glob string) ([]string, error)

	// OpenRead opens a file for reading.
	OpenRead(ctx context.Context, filename string) (io.ReadCloser, error)
	// OpenRead opens a file for writing. If the file already exist, it will be
	// overwritten.
	OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error)
}
