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
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestPlan_Checkpoint(t *testing.T) {
	var p Plan
	want := []*Checkpoint{{Reapply: time.Hour}}
	p.checkpoints = want
	if got := p.Checkpoint(); !cmp.Equal(got, want) {
		t.Errorf("p.Checkpoint() = %v, want %v", got, want)
	}
	if p.checkpoints != nil {
		t.Errorf("p.Checkpoint() didn't nil checkpoints field")
	}
}

func TestPlan_BundleFinalizers(t *testing.T) {
	newPlan := func() Plan {
		var p Plan
		p.setStatus(Up)
		return p
	}
	t.Run("NoCallbacks", func(t *testing.T) {
		p := newPlan()
		p.bf = &bundleFinalizer{}
		if err := p.Finalize(); err != nil {
			t.Errorf("p.Finalize() = %v, want nil", err)
		}
		// Expiration time is no longer set to default
		if got, want := p.GetExpirationTime(), (time.Time{}); want.Equal(got) {
			t.Errorf("p.GetExpirationTime() = %v, want %v", got, want)
		}
	})

	t.Run("Callbacks", func(t *testing.T) {
		p := newPlan()
		initialDeadline := time.Now().Add(time.Hour)

		var callCount int
		inc := func() error {
			callCount++
			return nil
		}
		p.bf = &bundleFinalizer{
			callbacks: []bundleFinalizationCallback{
				{callback: inc, validUntil: initialDeadline},
				{callback: inc, validUntil: initialDeadline},
				{callback: inc, validUntil: initialDeadline},
			},
		}
		if err := p.Finalize(); err != nil {
			t.Errorf("p.Finalize() = %v, want nil", err)
		}
		// Expiration time is no longer set to default
		if got, want := p.GetExpirationTime(), (time.Time{}); want.Equal(got) {
			t.Errorf("p.GetExpirationTime() = %v, want %v", got, want)
		}
		if got, want := callCount, 3; got != want {
			t.Errorf("p.Finalize() didn't call all finalizers, got %v, want %v", got, want)
		}
	})
	t.Run("Callbacks_expired", func(t *testing.T) {
		p := newPlan()
		initialDeadline := time.Now().Add(-time.Hour)

		var callCount int
		inc := func() error {
			callCount++
			return nil
		}
		p.bf = &bundleFinalizer{
			callbacks: []bundleFinalizationCallback{
				{callback: inc, validUntil: initialDeadline},
				{callback: inc, validUntil: initialDeadline},
				{callback: inc, validUntil: initialDeadline},
			},
		}
		if err := p.Finalize(); err != nil {
			t.Errorf("p.Finalize() = %v, want nil", err)
		}
		// Expiration time is no longer set to default
		if got, want := p.GetExpirationTime(), (time.Time{}); want.Equal(got) {
			t.Errorf("p.GetExpirationTime() = %v, want %v", got, want)
		}
		if got, want := callCount, 0; got != want {
			t.Errorf("p.Finalize() didn't call all finalizers, got %v, want %v", got, want)
		}
	})

	t.Run("Callbacks_failures", func(t *testing.T) {
		p := newPlan()
		initialDeadline := time.Now().Add(time.Hour)

		var callCount int
		inc := func() error {
			callCount++
			if callCount == 1 {
				return fmt.Errorf("unable to call")
			}
			return nil
		}
		p.bf = &bundleFinalizer{
			callbacks: []bundleFinalizationCallback{
				{callback: inc, validUntil: initialDeadline},
				{callback: inc, validUntil: initialDeadline},
				{callback: inc, validUntil: initialDeadline},
			},
		}
		if err := p.Finalize(); err == nil {
			t.Errorf("p.Finalize() = %v, want an error", err)
		}
		if got, want := callCount, 3; got != want {
			t.Errorf("p.Finalize() didn't call all finalizers, got %v, want %v", got, want)
		}
		if len(p.bf.callbacks) != 1 {
			t.Errorf("p.Finalize() didn't preserve failed callbacks")
		}
	})

}
