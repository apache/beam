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

// Package rangeio adapts object-store range requests to positional reads.
package rangeio

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

// Reader opens and closes one stream per ReadAt. Close cancels in-flight requests
// and waits for their streams to close before returning.
type Reader struct {
	ctx    context.Context
	cancel context.CancelFunc
	size   int64
	open   func(context.Context, int64, int64) (io.ReadCloser, error)
	mu     sync.RWMutex
	closed bool
}

// New returns a reader for a fixed object version of the given size.
func New(ctx context.Context, size int64, open func(context.Context, int64, int64) (io.ReadCloser, error)) *Reader {
	ctx, cancel := context.WithCancel(ctx)
	return &Reader{ctx: ctx, cancel: cancel, size: size, open: open}
}

// ReadAt implements io.ReaderAt without retaining data or sharing an offset.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	if off < 0 {
		return 0, fmt.Errorf("negative read offset: %d", off)
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.size {
		return 0, io.EOF
	}
	length := min(int64(len(p)), r.size-off)
	body, err := r.open(r.ctx, off, length)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := body.Close(); err == nil {
			err = closeErr
		}
	}()
	n, err = io.ReadFull(body, p[:int(length)])
	if err == nil && n < len(p) {
		err = io.EOF
	}
	return n, err
}

// Close releases this reader. It is safe to call concurrently and repeatedly.
func (r *Reader) Close() error {
	r.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}
