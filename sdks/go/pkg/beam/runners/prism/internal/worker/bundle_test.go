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
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
)

func TestBundle_ProcessOn(t *testing.T) {
	wk := Pool.NewWorker("test", "testEnv")
	b := &B{
		InstID:      "testInst",
		PBDID:       "testPBDID",
		OutputCount: 1,
		Input: []*engine.Block{
			{
				Kind:  engine.BlockData,
				Bytes: [][]byte{{1, 2, 3}},
			}},
	}
	b.Init()
	var completed sync.WaitGroup
	completed.Add(1)
	go func() {
		b.ProcessOn(context.Background(), wk)
		completed.Done()
	}()
	b.DataOrTimerDone()
	gotData := <-wk.DataReqs
	if got, want := gotData.GetData()[0].GetData(), []byte{1, 2, 3}; !bytes.EqualFold(got, want) {
		t.Errorf("ProcessOn(): data not sent; got %v, want %v", got, want)
	}

	gotInst := <-wk.InstReqs
	if got, want := gotInst.GetInstructionId(), b.InstID; got != want {
		t.Errorf("ProcessOn(): bad instruction ID; got %v, want %v", got, want)
	}
	if got, want := gotInst.GetProcessBundle().GetProcessBundleDescriptorId(), b.PBDID; got != want {
		t.Errorf("ProcessOn(): bad process bundle descriptor ID; got %v, want %v", got, want)
	}
}
