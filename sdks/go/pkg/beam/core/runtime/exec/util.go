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

package exec

import (
	"context"
	"fmt"
	"runtime/debug"
)

// GenID is a simple UnitID generator.
type GenID struct {
	last int
}

// New returns a fresh ID.
func (g *GenID) New() UnitID {
	g.last++
	return UnitID(g.last)
}

// callNoPanic calls the given function and catches any panic.
func callNoPanic(ctx context.Context, fn func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v %s", r, debug.Stack())
		}
	}()
	return fn(ctx)
}

// MultiStartBundle calls StartBundle on multiple nodes. Convenience function.
func MultiStartBundle(ctx context.Context, id string, data DataManager, list ...Node) error {
	for _, n := range list {
		if err := n.StartBundle(ctx, id, data); err != nil {
			return err
		}
	}
	return nil
}

// MultiFinishBundle calls StartBundle on multiple nodes. Convenience function.
func MultiFinishBundle(ctx context.Context, list ...Node) error {
	for _, n := range list {
		if err := n.FinishBundle(ctx); err != nil {
			return err
		}
	}
	return nil
}

// IDs returns the unit IDs of the given nodes.
func IDs(list ...Node) []UnitID {
	var ret []UnitID
	for _, n := range list {
		ret = append(ret, n.ID())
	}
	return ret
}
