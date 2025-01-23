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

package engine

import (
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// WinStrat configures the windowing strategy for the stage, based on the
// stage's input PCollection.
type WinStrat struct {
	AllowedLateness time.Duration // Used to extend duration
}

func (ws WinStrat) EarliestCompletion(w typex.Window) mtime.Time {
	return w.MaxTimestamp().Add(ws.AllowedLateness)
}

func (ws WinStrat) String() string {
	return fmt.Sprintf("WinStrat[AllowedLateness:%v]", ws.AllowedLateness)
}
