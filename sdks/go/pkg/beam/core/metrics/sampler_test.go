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

package metrics

import (
	"context"
	"testing"
	"time"
)

func TestSampler(t *testing.T) {
	ctx := context.Background()
	pctx := SetPTransformID(ctx, "transform")
	st := GetStore(pctx)

	s := NewSampler(pctx, st)

	SetPTransformState(pctx, "transform", StartBundle)
	time.Sleep(200 * time.Millisecond)
	go s.Start(pctx, 200*time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	go s.Start(pctx, 200*time.Millisecond)

	SetPTransformState(pctx, "transform", ProcessBundle)
	IncTransition(pctx)

	time.Sleep(200 * time.Millisecond)
	go s.Start(pctx, 200*time.Millisecond)

	SetPTransformState(pctx, "transform", FinishBundle)
	IncTransition(pctx)

	time.Sleep(200 * time.Millisecond)
	go s.Start(pctx, 200*time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	go s.Start(pctx, 200*time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	go s.Start(pctx, 200*time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	go s.Stop(pctx, 200*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	res := s.store.GetRegistry()
	if len(res[PTransformLabels("transform")]) != 4 {
		t.Errorf("incorrect number of states: %v, want: 4", len(res[PTransformLabels("transform")]))
	}
}
