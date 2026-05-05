// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package gcs contains a Google Cloud Storage (GCS) implementation of the
// Beam file system.
package gcs

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/fsx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/gcsx"
	"google.golang.org/api/iterator"
)

const (
	projectBillingHook = "beam:go:hook:filesystem:billingproject"
)

// globToRegex translates a glob pattern to a regular expression.
// It differs from filepath.Match in that:
//   - / is treated as a regular character (not a separator), since GCS object
//     names are flat with / being just another character
//   - ** matches any sequence of characters including / (zero or more)
//   - **/  matches zero or more path segments (e.g., "" or "dir/" or "dir/subdir/")
//   - * matches any sequence of characters except / (zero or more)
//   - ? matches any single character except /
//
// This matches the behavior of the Python and Java SDKs.
func globToRegex(pattern string) (*regexp.Regexp, error) {
	var result strings.Builder
	result.WriteString("^")

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		switch c {
		case '*':
			// Check for ** (double asterisk)
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				// Check if followed by / (e.g., "**/" matches zero or more path segments)
				if i+2 < len(pattern) && pattern[i+2] == '/' {
					// **/ matches "" or "something/" or "a/b/c/"
					result.WriteString("(.*/)?")
					i += 2 // Skip the second * and the /
				} else {
					// ** at end or before non-slash matches any characters
					result.WriteString(".*")
					i++ // Skip the second *
				}
			} else {
				result.WriteString("[^/]*")
			}
		case '?':
			result.WriteString("[^/]")
		case '[':
			// Character class - find the closing bracket
			j := i + 1
			if j < len(pattern) && pattern[j] == '!' {
				j++
			}
			if j < len(pattern) && pattern[j] == ']' {
				j++
			}
			for j < len(pattern) && pattern[j] != ']' {
				j++
			}
			if j >= len(pattern) {
				return nil, fmt.Errorf("syntax error: unclosed '[' in pattern %q", pattern)
			} else {
				// Copy the character class, converting ! to ^ for negation
				result.WriteByte('[')
				content := pattern[i+1 : j]
				if len(content) > 0 && content[0] == '!' {
					result.WriteByte('^')
					content = content[1:]
				}
				result.WriteString(content)
				result.WriteByte(']')
				i = j
			}
		default:
			result.WriteString(regexp.QuoteMeta(string(c)))
		}
	}

	result.WriteString("$") // match end
	return regexp.Compile(result.String())
}

var billingProject string = ""

func init() {
	filesystem.Register("gs", New)
	hf := func(opts []string) hooks.Hook {
		return hooks.Hook{
			Init: func(ctx context.Context) (context.Context, error) {
				if len(opts) == 0 {
					return ctx, nil
				}
				if len(opts) > 1 {
					return ctx, fmt.Errorf("expected 1 option, got %v: %v", len(opts), opts)
				}

				billingProject = opts[0]
				return ctx, nil
			},
		}
	}
	hooks.RegisterHook(projectBillingHook, hf)
}

type fs struct {
	client *storage.Client
}

// New creates a new Google Cloud Storage filesystem using application
// default credentials. If it fails, it falls back to unauthenticated
// access.
// It will use the environment variable named `BILLING_PROJECT_ID` as requester payer bucket attribute.
func New(ctx context.Context) filesystem.Interface {
	client, err := gcsx.NewClient(ctx, storage.ScopeReadWrite)
	if err != nil {
		log.Warnf(ctx, "Warning: falling back to unauthenticated GCS access: %v", err)

		client, err = gcsx.NewUnauthenticatedClient(ctx)
		if err != nil {
			panic(errors.Wrapf(err, "failed to create GCS client"))
		}
	}
	return &fs{
		client: client,
	}
}

func SetRequesterBillingProject(project string) {
	billingProject = project
}

// RequesterBillingProject configure project to be used in google storage operations
// with requester pays actived. More informaiton about requester pays in https://cloud.google.com/storage/docs/requester-pays
func RequesterBillingProject(project string) error {
	if project == "" {
		return fmt.Errorf("project cannot be empty, got %v", project)
	}
	// The hook itself is defined in beam/core/runtime/harness/file_system_hooks.go
	return hooks.EnableHook(projectBillingHook, project)
}

func (f *fs) Close() error {
	return f.client.Close()
}

func (f *fs) List(ctx context.Context, glob string) ([]string, error) {
	bucket, object, err := gcsx.ParseObject(glob)
	if err != nil {
		return nil, err
	}

	// Compile the glob pattern to a regex. We use a custom glob-to-regex
	// translation that treats / as a regular character (not a separator),
	// since GCS object names are flat. This also supports ** for recursive
	// matching, similar to the Java and Python SDKs.
	re, err := globToRegex(object)
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern %q: %w", object, err)
	}

	var candidates []string

	// We handle globs by list all candidates and matching them here.
	// For now, we assume * is the first matching character to make a
	// prefix listing and not list the entire bucket.
	prefix := fsx.GetPrefix(object)
	it := f.client.Bucket(bucket).UserProject(billingProject).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if re.MatchString(obj.Name) {
			candidates = append(candidates, obj.Name)
		}
	}

	var ret []string
	for _, obj := range candidates {
		ret = append(ret, fmt.Sprintf("gs://%v/%v", bucket, obj))
	}
	return ret, nil
}

func (f *fs) OpenRead(ctx context.Context, filename string) (io.ReadCloser, error) {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return nil, err
	}

	return f.client.Bucket(bucket).UserProject(billingProject).Object(object).NewReader(ctx)
}

// TODO(herohde) 7/12/2017: should we create the bucket in OpenWrite? For now, "no".

func (f *fs) OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error) {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return nil, err
	}

	return f.client.Bucket(bucket).UserProject(billingProject).Object(object).NewWriter(ctx), nil
}

func (f *fs) Size(ctx context.Context, filename string) (int64, error) {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return -1, err
	}

	obj := f.client.Bucket(bucket).UserProject(billingProject).Object(object)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return -1, err
	}

	return attrs.Size, nil
}

// LastModified returns the time at which the file was last modified.
func (f *fs) LastModified(ctx context.Context, filename string) (time.Time, error) {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return time.Time{}, err
	}

	obj := f.client.Bucket(bucket).UserProject(billingProject).Object(object)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return time.Time{}, err
	}

	return attrs.Updated, nil
}

// Remove the named file from the filesystem.
func (f *fs) Remove(ctx context.Context, filename string) error {
	bucket, object, err := gcsx.ParseObject(filename)
	if err != nil {
		return err
	}

	obj := f.client.Bucket(bucket).UserProject(billingProject).Object(object)
	return obj.Delete(ctx)
}

// Copy copies from srcpath to the dstpath.
func (f *fs) Copy(ctx context.Context, srcpath, dstpath string) error {
	bucket, src, err := gcsx.ParseObject(srcpath)
	if err != nil {
		return err
	}
	srcobj := f.client.Bucket(bucket).UserProject(billingProject).Object(src)

	bucket, dst, err := gcsx.ParseObject(dstpath)
	if err != nil {
		return err
	}
	dstobj := f.client.Bucket(bucket).UserProject(billingProject).Object(dst)

	cp := dstobj.CopierFrom(srcobj)
	_, err = cp.Run(ctx)
	return err
}

// Rename the old path to the new path.
func (f *fs) Rename(ctx context.Context, srcpath, dstpath string) error {
	bucket, src, err := gcsx.ParseObject(srcpath)
	if err != nil {
		return err
	}
	srcobj := f.client.Bucket(bucket).UserProject(billingProject).Object(src)

	bucket, dst, err := gcsx.ParseObject(dstpath)
	if err != nil {
		return err
	}
	dstobj := f.client.Bucket(bucket).UserProject(billingProject).Object(dst)

	cp := dstobj.CopierFrom(srcobj)
	_, err = cp.Run(ctx)
	if err != nil {
		return err
	}
	return srcobj.Delete(ctx)
}

// Compile time check for interface implementations.
var (
	_ filesystem.LastModifiedGetter = ((*fs)(nil))
	_ filesystem.Remover            = ((*fs)(nil))
	_ filesystem.Copier             = ((*fs)(nil))
	_ filesystem.Renamer            = ((*fs)(nil))
)
