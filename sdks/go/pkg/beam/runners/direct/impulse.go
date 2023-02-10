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

package direct

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
)

// Impulse emits its single element in one invocation.
type Impulse struct {
	UID   exec.UnitID
	Value []byte
	Out   exec.Node
}

func (n *Impulse) ID() exec.UnitID {
	return n.UID
}

func (n *Impulse) Up(ctx context.Context) error {
	return nil
}

func (n *Impulse) StartBundle(ctx context.Context, id string, data exec.DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *Impulse) Process(ctx context.Context) ([]*exec.Checkpoint, error) {
	value := &exec.FullValue{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.Now(),
		Elm:       n.Value,
	}
	return nil, n.Out.ProcessElement(ctx, value)
}

func (n *Impulse) FinishBundle(ctx context.Context) error {
	return n.Out.FinishBundle(ctx)
}

func (n *Impulse) Down(ctx context.Context) error {
	return nil
}

func (n *Impulse) String() string {
	return fmt.Sprintf("Impulse[%v]", len(n.Value))
}
