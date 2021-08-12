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

type Trigger struct {
	Kind         string
	SubTriggers  []Trigger
	Delay        int64 // in milliseconds
	ElementCount int32
}

const (
	DefaultTrigger                         string = "Trigger_Default_"
	AlwaysTrigger                          string = "Trigger_Always_"
	AfterAnyTrigger                        string = "Trigger_AfterAny_"
	AfterAllTrigger                        string = "Trigger_AfterAll_"
	AfterProcessingTimeTrigger             string = "Trigger_AfterProcessing_Time_"
	ElementCountTrigger                    string = "Trigger_ElementCount_"
	AfterEndOfWindowTrigger                string = "Trigger_AfterEndOfWindow_"
	RepeatTrigger                          string = "Trigger_Repeat_"
	OrFinallyTrigger                       string = "Trigger_OrFinally_"
	NeverTrigger                           string = "Trigger_Never_"
	AfterSynchronizedProcessingTimeTrigger string = "Trigger_AfterSynchronizedProcessingTime_"
)
