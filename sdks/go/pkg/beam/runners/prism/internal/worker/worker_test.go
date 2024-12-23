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

package worker

import (
	"context"
	"testing"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
)

func TestWorker_New(t *testing.T) {
	w := Pool.NewWorker("test", "testEnv")
	if got, want := w.ID, "test"; got != want {
		t.Errorf("New(%q) = %v, want %v", want, got, want)
	}
}

func TestWorker_NextInst(t *testing.T) {
	w := Pool.NewWorker("test", "testEnv")

	instIDs := map[string]struct{}{}
	for i := 0; i < 100; i++ {
		instIDs[w.NextInst()] = struct{}{}
	}
	if got, want := len(instIDs), 100; got != want {
		t.Errorf("calling w.NextInst() got %v unique ids, want %v", got, want)
	}
}

func TestWorker_GetProcessBundleDescriptor(t *testing.T) {
	w := Pool.NewWorker("test", "testEnv")

	id := "available"
	w.Descriptors[id] = &fnpb.ProcessBundleDescriptor{
		Id: id,
	}

	pbd, err := w.GetProcessBundleDescriptor(context.Background(), &fnpb.GetProcessBundleDescriptorRequest{
		ProcessBundleDescriptorId: id,
	})
	if err != nil {
		t.Errorf("got GetProcessBundleDescriptor(%q) error: %v, want nil", id, err)
	}
	if got, want := pbd.GetId(), id; got != want {
		t.Errorf("got GetProcessBundleDescriptor(%q) = %v, want id %v", id, got, want)
	}

	pbd, err = w.GetProcessBundleDescriptor(context.Background(), &fnpb.GetProcessBundleDescriptorRequest{
		ProcessBundleDescriptorId: "unknown",
	})
	if err == nil {
		t.Errorf("got GetProcessBundleDescriptor(%q) = %v, want error", "unknown", pbd)
	}
}

type closeSend func()
