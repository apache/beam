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
)

type ToString struct {
	// UID is the unit identifier.
	UID UnitID
	// Out is the output node.
	Out Node
}

func (m *ToString) ID() UnitID {
	return m.UID
}

func (m *ToString) Up(ctx context.Context) error {
	return nil
}

func (m *ToString) StartBundle(ctx context.Context, id string, data DataContext) error {
	return m.Out.StartBundle(ctx, id, data)
}

func (m *ToString) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	ret := FullValue{
		Windows:   elm.Windows,
		Elm:       elm.Elm,
		Elm2:      fmt.Sprintf("%v", elm.Elm2),
		Timestamp: elm.Timestamp,
		Pane:      elm.Pane,
	}

	return m.Out.ProcessElement(ctx, &ret, values...)
}

func (m *ToString) FinishBundle(ctx context.Context) error {
	return m.Out.FinishBundle(ctx)
}

func (m *ToString) Down(ctx context.Context) error {
	return nil
}

func (m *ToString) String() string {
	return fmt.Sprintf("ToStringFn. Out:%v", m.Out)
}
