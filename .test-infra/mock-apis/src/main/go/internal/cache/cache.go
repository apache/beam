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

package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
)

var (

	// ErrNotExist is an error indicating that a resource does not exist
	ErrNotExist = errors.New("resource does not exist")
)

// IsNotExist is true when err is ErrNotExist.
func IsNotExist(err error) bool {
	return errors.Is(err, ErrNotExist)
}

// Options for running the Refresher.
type Options struct {
	Setter UInt64Setter
	Logger *slog.Logger
}

// Refresher refreshes a value in a cache on a set interval.
type Refresher struct {
	opts *Options
	stop chan struct{}
}

// NewRefresher instantiates a Refresher.
func NewRefresher(ctx context.Context, opts *Options) (*Refresher, error) {
	if opts.Logger == nil {
		opts.Logger = logging.New(&logging.Options{
			Name: reflect.TypeOf((*Refresher)(nil)).PkgPath(),
		})
	}

	if opts.Setter == nil {
		return nil, fmt.Errorf("%T.Setter is nil but required", opts)
	}

	if err := opts.Setter.Alive(ctx); err != nil {
		return nil, err
	}

	ref := &Refresher{
		opts: opts,
	}

	return ref, nil
}

// Stop the Refresher.
func (ref *Refresher) Stop() {
	ref.stop <- struct{}{}
}

// Refresh the size of the associated key at an interval.
func (ref *Refresher) Refresh(ctx context.Context, key string, size uint64, interval time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ref.stop = make(chan struct{})
	attrs := []slog.Attr{
		{
			Key:   "key",
			Value: slog.StringValue(key),
		},
		{
			Key:   "size",
			Value: slog.Uint64Value(size),
		},
		{
			Key:   "interval",
			Value: slog.StringValue(interval.String()),
		},
	}

	ref.opts.Logger.LogAttrs(ctx, slog.LevelInfo, "starting refresher service", attrs...)

	if err := ref.opts.Setter.Set(ctx, key, size, interval); err != nil {
		return err
	}
	ref.opts.Logger.LogAttrs(ctx, slog.LevelDebug, "successful initial refresh", attrs...)

	tick := time.Tick(interval)
	for {
		select {
		case <-tick:
			if err := ref.opts.Setter.Set(ctx, key, size, interval); err != nil {
				return err
			}
			ref.opts.Logger.LogAttrs(ctx, slog.LevelDebug, "refresh successful", attrs...)
		case <-ref.stop:
			ref.opts.Logger.LogAttrs(ctx, slog.LevelInfo, "stopping refresher service", attrs...)
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}
