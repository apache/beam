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

// Package exec contains runtime plan representation and execution. A pipeline
// must be translated to a runtime plan to be executed.
package exec

import (
	"context"
	"fmt"
)

// Execute executes contained graph segment. At least one root is required.
func Execute(ctx context.Context, units []Unit) error {
	var roots []Root
	for _, u := range units {
		if root, ok := u.(Root); ok {
			roots = append(roots, root)
		}
	}
	if len(roots) == 0 {
		return fmt.Errorf("no roots to execute")
	}
	for _, root := range roots {
		if err := root.Up(ctx); err != nil {
			return err
		}
	}
	for _, root := range roots {
		if err := root.Process(ctx); err != nil {
			return err
		}
	}
	for _, root := range roots {
		if err := root.Down(ctx); err != nil {
			return err
		}
	}
	return nil
}
