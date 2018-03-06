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

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
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

func (n *Impulse) StartBundle(ctx context.Context, id string, data exec.DataManager) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *Impulse) Process(ctx context.Context) error {
	value := exec.FullValue{Elm: n.Value}
	// TODO(herohde) 6/23/2017: set value.Timestamp

	return n.Out.ProcessElement(ctx, value)
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
