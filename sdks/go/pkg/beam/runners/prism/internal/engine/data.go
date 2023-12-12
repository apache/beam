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
	"bytes"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"golang.org/x/exp/slog"
)

// TentativeData is where data for in progress bundles is put
// until the bundle executes successfully.
type TentativeData struct {
	Raw map[string][][]byte

	// BagState is a map from transformID + UserStateID, to window, to userKey, to datavalues.
	BagState map[LinkID]map[typex.Window]map[string][][]byte
}

// WriteData adds data to a given global collectionID.
func (d *TentativeData) WriteData(colID string, data []byte) {
	if d.Raw == nil {
		d.Raw = map[string][][]byte{}
	}
	d.Raw[colID] = append(d.Raw[colID], data)
}

func (d *TentativeData) toWindow(wKey []byte) typex.Window {
	if len(wKey) == 0 {
		return window.GlobalWindow{}
	}
	w, err := exec.MakeWindowDecoder(coder.NewIntervalWindow()).DecodeSingle(bytes.NewBuffer(wKey))
	if err != nil {
		panic(fmt.Sprintf("error decoding append bag user state window key %v: %v", wKey, err))
	}
	return w
}

// GetBagState retrieves available state from the tentative bundle data.
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) GetBagState(stateID LinkID, wKey, uKey []byte) [][]byte {
	winMap := d.BagState[stateID]
	w := d.toWindow(wKey)
	data := winMap[w][string(uKey)]
	slog.Debug("State() Bag.Get", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("Window", w), slog.Any("Data", data))
	return data
}

// AppendBagState appends the incoming data to the existing tentative data bundle.
//
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) AppendBagState(stateID LinkID, wKey, uKey, data []byte) {
	if d.BagState == nil {
		d.BagState = map[LinkID]map[typex.Window]map[string][][]byte{}
	}
	winMap, ok := d.BagState[stateID]
	if !ok {
		winMap = map[typex.Window]map[string][][]byte{}
		d.BagState[stateID] = winMap
	}
	w := d.toWindow(wKey)
	kmap, ok := winMap[w]
	if !ok {
		kmap = map[string][][]byte{}
		winMap[w] = kmap
	}
	kmap[string(uKey)] = append(kmap[string(uKey)], data)
	slog.Debug("State() Bag.Append", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("Window", w), slog.Any("NewData", data))
}

// ClearBagState clears any tentative data for the state. Since state data is only initialized if any exists,
// Clear takes the approach to not create state that doesn't already exist. Existing state is nil'd
// to allow that to be committed post bundle commpletion.
//
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) ClearBagState(stateID LinkID, wKey, uKey []byte) {
	if d.BagState == nil {
		return
	}
	winMap, ok := d.BagState[stateID]
	if !ok {
		return
	}
	w := d.toWindow(wKey)
	kmap, ok := winMap[w]
	if !ok {
		return
	}
	// Nil the current entry to clear.
	// Delete makes it difficult to delete the persisted stage state for the key.
	kmap[string(uKey)] = nil
	slog.Debug("State() Bag.Clear", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("Window", w))
}
