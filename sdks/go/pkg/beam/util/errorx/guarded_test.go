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

package errorx

import (
	"errors"
	"testing"
)

func TestTrySetError(t *testing.T) {
	var gu GuardedError
	setErr := errors.New("attempted error")
	success := gu.TrySetError(setErr)
	if !success {
		t.Fatal("got false when trying to set error, want true")
	}
	if got, want := gu.Error(), setErr; got != want {
		t.Errorf("got error %v when checking message, want %v", got, want)
	}
}

func TestTrySetError_bad(t *testing.T) {
	setErr := errors.New("old error")
	gu := &GuardedError{err: setErr}
	success := gu.TrySetError(setErr)
	if success {
		t.Fatal("got true when trying to set error, want false")
	}
	if got, want := gu.Error(), setErr; got != want {
		t.Errorf("got error %v when checking message, want %v", got, want)
	}
}
