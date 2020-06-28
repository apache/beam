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

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// GenID is a simple UnitID generator.
type GenID struct {
	last int
}

// doFnError is a helpful error where DoFns with emitters have failed.
//
// Golang SDK uses errors returned by DoFns to signal failures to process
// bundles and terminate bundle processing. However, in the case of  emitters,
// a panic is used as a means of bundle process termination, which  in turn is
// wrapped by callNoPanic to recover the panicking code and return golang error
// instead. However, formatting this error yields the stack trace a message
// which is both long, and unhelpful the user.
//
// This error enables to create a more helpful user error where a panic is
// caught where the origin is due to a failed DoFn execution in process bundle
// execution.
type doFnError struct {
	doFn string
	err  error
	uid  UnitID
	pid  string
}

func (e *doFnError) Error() string {
	return fmt.Sprintf("%v caused error %v, uid is %v pid is %v", e.doFn, e.err, e.uid, e.pid)
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
			// Top level error is the panic itself, but also include the stack trace as the original error.
			// Higher levels can then add appropriate context without getting pushed down by the stack trace.
			err = errors.SetTopLevelMsgf(errors.Errorf("panic: %v %s", r, debug.Stack()), "panic: %v", r)
		}
	}()
	return fn(ctx)
}

// MultiStartBundle calls StartBundle on multiple nodes. Convenience function.
func MultiStartBundle(ctx context.Context, id string, data DataContext, list ...Node) error {
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
