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

type winStrat interface {
	EarliestCompletion(typex.Window) mtime.Time
}

type defaultStrat struct{}

func (ws defaultStrat) EarliestCompletion(w typex.Window) mtime.Time {
	return w.MaxTimestamp()
}

func (defaultStrat) String() string {
	return "default"
}

type sessionStrat struct {
	GapSize time.Duration
}

func (ws sessionStrat) EarliestCompletion(w typex.Window) mtime.Time {
	return w.MaxTimestamp().Add(ws.GapSize)
}

func (ws sessionStrat) String() string {
	return fmt.Sprintf("session[GapSize:%v]", ws.GapSize)
}
