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

package window

// TODO [BEAM-3304](riteshghorse): add configurable parameters to trigger
type TriggerType string

const (
	Default  TriggerType = "Trigger_Default_"
	Always   TriggerType = "Trigger_Always_"
	AfterAny TriggerType = "Trigger_AfterAny_"
	AfterAll TriggerType = "Trigger_AfterAll_"
)

func (ws *WindowingStrategy) SetAfterAll() {
	ws.Trigger = AfterAll
}

func (ws *WindowingStrategy) SetAfterAny() {
	ws.Trigger = AfterAny
}

func (ws *WindowingStrategy) SetAlways() {
	ws.Trigger = Always
}

func (ws *WindowingStrategy) SetDefault() {
	ws.Trigger = Default
}
