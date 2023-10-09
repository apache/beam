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
	"time"

	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
)

var (
	defaultLogger = logging.New("github.com/apache/beam/test-infra/mock-apis/main/go/internal/cache")

	ErrNotExist = errors.New("resource does not exist")
)

func IsNotExist(err error) bool {
	return errors.Is(err, ErrNotExist)
}

// Refresher refreshes a value in a cache on a set interval.
type Refresher struct {
	setter UInt64Setter
	logger logging.Logger
	stop   chan struct{}
}

// Option applies optional features to a Refresher.
type Option interface {
	apply(ref *Refresher)
}

// WithLogger overrides Refresher's default logging.Logger.
func WithLogger(logger logging.Logger) Option {
	return &withLoggerOpt{
		logger: logger,
	}
}

// NewRefresher instantiates a Refresher.
func NewRefresher(ctx context.Context, setter UInt64Setter, opts ...Option) (*Refresher, error) {
	if err := setter.Alive(ctx); err != nil {
		return nil, err
	}
	ref := &Refresher{
		setter: setter,
		logger: defaultLogger,
	}

	for _, opt := range opts {
		opt.apply(ref)
	}

	return ref, nil
}

func (ref *Refresher) Stop() {
	ref.stop <- struct{}{}
}

// Refresh the size of the associated key at an interval.
func (ref *Refresher) Refresh(ctx context.Context, key string, size uint64, interval time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ref.stop = make(chan struct{})
	fields := []logging.Field{
		{
			Key:   "key",
			Value: key,
		},
		{
			Key:   "size",
			Value: size,
		},
		{
			Key:   "interval",
			Value: interval.String(),
		},
	}

	ref.logger.Info(ctx, "starting refresher service", fields...)

	if err := ref.setter.Set(ctx, key, size, interval); err != nil {
		return err
	}
	tick := time.Tick(interval)
	for {
		select {
		case <-tick:
			if err := ref.setter.Set(ctx, key, size, interval); err != nil {
				return err
			}
			ref.logger.Debug(ctx, "refresh successful", fields...)
		case <-ref.stop:
			ref.logger.Info(ctx, "stopping refresher service", fields...)
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

type withLoggerOpt struct {
	logger logging.Logger
}

func (opt *withLoggerOpt) apply(ref *Refresher) {
	ref.logger = opt.logger
}
