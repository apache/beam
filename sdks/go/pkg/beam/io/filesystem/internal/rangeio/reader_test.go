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

package rangeio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

type trackedBody struct {
	io.Reader
	closes *atomic.Int32
	err    error
}

func (b trackedBody) Close() error { b.closes.Add(1); return b.err }

func TestReaderRanges(t *testing.T) {
	const data = "header-payload-footer"
	var opens, closes atomic.Int32
	r := New(context.Background(), int64(len(data)), func(_ context.Context, off, length int64) (io.ReadCloser, error) {
		opens.Add(1)
		return trackedBody{Reader: bytes.NewReader([]byte(data[off : off+length])), closes: &closes}, nil
	})
	defer r.Close()
	for _, tc := range []struct {
		off    int64
		length int
		want   string
		eof    bool
	}{
		{15, 6, "footer", false}, {18, 8, "ter", true}, {21, 1, "", true}, {math.MaxInt64, 2, "", true}, {20, math.MaxInt16, "r", true}, {0, 0, "", false},
	} {
		p := make([]byte, tc.length)
		n, err := r.ReadAt(p, tc.off)
		if n != len(tc.want) || string(p[:n]) != tc.want || (err == io.EOF) != tc.eof || (err != nil && err != io.EOF) {
			t.Errorf("ReadAt(%d,%d)=%q,%v", tc.off, tc.length, p[:n], err)
		}
	}
	if _, err := r.ReadAt(make([]byte, 1), -1); err == nil {
		t.Error("negative offset accepted")
	}
	if opens.Load() != 3 || closes.Load() != 3 {
		t.Errorf("requests=%d closes=%d, want 3 each", opens.Load(), closes.Load())
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Go(func() {
			p := make([]byte, 6)
			if _, err := r.ReadAt(p, 15); err != nil || string(p) != "footer" {
				t.Errorf("parallel read=%q,%v", p, err)
			}
		})
	}
	wg.Wait()
	if opens.Load() != closes.Load() {
		t.Error("range response leaked")
	}
}

func TestReaderFailures(t *testing.T) {
	sentinel := errors.New("range request failed")
	for _, tc := range []struct {
		name                  string
		openErr, errorOnClose error
		data                  string
		want                  error
	}{
		{name: "open", openErr: sentinel, want: sentinel},
		{name: "truncated", data: "x", want: io.ErrUnexpectedEOF},
		{name: "close", data: "data", errorOnClose: sentinel, want: sentinel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var closes atomic.Int32
			r := New(context.Background(), 4, func(context.Context, int64, int64) (io.ReadCloser, error) {
				if tc.openErr != nil {
					return nil, tc.openErr
				}
				return trackedBody{bytes.NewReader([]byte(tc.data)), &closes, tc.errorOnClose}, nil
			})
			defer r.Close()
			if _, err := r.ReadAt(make([]byte, 4), 0); !errors.Is(err, tc.want) {
				t.Errorf("error=%v,want %v", err, tc.want)
			}
			if tc.openErr == nil && closes.Load() != 1 {
				t.Error("response not closed")
			}
		})
	}
}

func TestReaderCloseCancelsRead(t *testing.T) {
	started := make(chan struct{})
	done := make(chan error, 1)
	r := New(context.Background(), 4, func(ctx context.Context, _, _ int64) (io.ReadCloser, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	go func() { _, err := r.ReadAt(make([]byte, 4), 0); done <- err }()
	<-started
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("read error=%v", err)
	}
	if _, err := r.ReadAt(make([]byte, 4), 0); !errors.Is(err, os.ErrClosed) {
		t.Errorf("read after close=%v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() { r.Close() })
	}
	wg.Wait()
}

func TestReaderCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := New(ctx, 4, func(context.Context, int64, int64) (io.ReadCloser, error) {
		t.Error("request after cancellation")
		return nil, nil
	})
	defer r.Close()
	if _, err := r.ReadAt(make([]byte, 1), 0); !errors.Is(err, context.Canceled) {
		t.Errorf("error=%v", err)
	}
}

type canceledBody struct {
	ctx     context.Context
	started chan struct{}
	closed  atomic.Bool
}

func (b *canceledBody) Read([]byte) (int, error) {
	close(b.started)
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (b *canceledBody) Close() error { b.closed.Store(true); return nil }

func TestReaderCloseCancelsOpenBody(t *testing.T) {
	started := make(chan struct{})
	var body *canceledBody
	r := New(context.Background(), 4, func(ctx context.Context, _, _ int64) (io.ReadCloser, error) {
		body = &canceledBody{ctx: ctx, started: started}
		return body, nil
	})
	done := make(chan error, 1)
	go func() { _, err := r.ReadAt(make([]byte, 4), 0); done <- err }()
	<-started
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if !body.closed.Load() {
		t.Error("Close returned before the range body closed")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("read error=%v", err)
	}
}
