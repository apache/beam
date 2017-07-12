package gcs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/util/storagex"
	"google.golang.org/api/storage/v1"
)

func init() {
	textio.RegisterFileSystem("gs", New)
}

type fs struct {
	client *storage.Service
}

// New creates a new Google Cloud Storage filesystem using application
// default credentials.
func New(ctx context.Context) textio.FileSystem {
	client, err := storagex.NewClient(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to create GCE client: %v", err))
	}
	return &fs{client: client}
}

func (f *fs) Close() error {
	f.client = nil
	return nil
}

func (f *fs) List(ctx context.Context, glob string) ([]string, error) {
	bucket, object, err := storagex.ParseObject(glob)
	if err != nil {
		return nil, err
	}

	var candidates []string
	if index := strings.Index(object, "*"); index > 0 {
		// We handle globs by list all candidates and matching them here.
		// For now, we assume * is the first matching character to make a
		// prefix listing and not list the entire bucket.

		err := f.client.Objects.List(bucket).Prefix(object[:index]).Pages(ctx, func(list *storage.Objects) error {
			for _, obj := range list.Items {
				match, err := filepath.Match(object, obj.Name)
				if err != nil {
					return err
				}
				if match {
					candidates = append(candidates, obj.Name)
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else {
		// Single object.

		candidates = []string{object}
	}

	var ret []string
	for _, obj := range candidates {
		ret = append(ret, fmt.Sprintf("gs://%v/%v", bucket, obj))
	}
	return ret, nil
}

func (f *fs) OpenRead(ctx context.Context, filename string) (io.ReadCloser, error) {
	bucket, object, err := storagex.ParseObject(filename)
	if err != nil {
		return nil, err
	}

	resp, err := f.client.Objects.Get(bucket, object).Download()
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// TODO(herohde) 7/12/2017: should we create the bucket in OpenWrite? For now, "no".

func (f *fs) OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error) {
	bucket, object, err := storagex.ParseObject(filename)
	if err != nil {
		return nil, err
	}
	return &writer{client: f.client, bucket: bucket, object: object}, nil
}

type writer struct {
	client         *storage.Service
	bucket, object string

	buf bytes.Buffer
}

func (w *writer) Write(data []byte) (n int, err error) {
	return w.buf.Write(data)
}

func (w *writer) Close() error {
	return storagex.WriteObject(w.client, w.bucket, w.object, &w.buf)
}
