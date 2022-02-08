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

package harness

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
)

var (
	cacheSize int = 0
)

func init() {
	hf := func(opts []string) hooks.Hook {
		return hooks.Hook{
			Init: func(ctx context.Context) (context.Context, error) {
				if len(opts) == 0 {
					return ctx, nil
				}
				if len(opts) > 1 {
					return ctx, fmt.Errorf("expected 1 option, got %v: %v", len(opts), opts)
				}

				var count int
				_, err := fmt.Sscan(opts[0], &count)
				if err != nil {
					return nil, err
				}
				cacheSize = count
				return ctx, nil
			},
		}
	}
	hooks.RegisterHook("beam:go:hook:sideinputcache:capacity", hf)
}
