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
	"log/slog"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// StateData is a "union" between Bag state and MultiMap state to increase common code.
type StateData struct {
	Bag      [][]byte
	Multimap map[string][][]byte
}

// TimerKey is for use as a key for timers.
type TimerKey struct {
	Transform, Family string
}

// TentativeData is where data for in progress bundles is put
// until the bundle executes successfully.
type TentativeData struct {
	Raw map[string][][]byte

	// state is a map from transformID + UserStateID, to window, to userKey, to datavalues.
	state map[LinkID]map[typex.Window]map[string]StateData
	// timers is a map from the Timer transform+family to the encoded timer.
	timers map[TimerKey][][]byte
}

// WriteData adds data to a given global collectionID.
func (d *TentativeData) WriteData(colID string, data []byte) {
	if d.Raw == nil {
		d.Raw = map[string][][]byte{}
	}
	d.Raw[colID] = append(d.Raw[colID], data)
}

// WriteTimers adds timers to the associated transform handler.
func (d *TentativeData) WriteTimers(transformID, familyID string, timers []byte) {
	if d.timers == nil {
		d.timers = map[TimerKey][][]byte{}
	}
	link := TimerKey{Transform: transformID, Family: familyID}
	d.timers[link] = append(d.timers[link], timers)
}

func (d *TentativeData) toWindow(wKey []byte) typex.Window {
	if len(wKey) == 0 {
		return window.GlobalWindow{}
	}
	// TODO: Custom Window handling.
	w, err := exec.MakeWindowDecoder(coder.NewIntervalWindow()).DecodeSingle(bytes.NewBuffer(wKey))
	if err != nil {
		panic(fmt.Sprintf("error decoding append bag user state window key %v: %v", wKey, err))
	}
	return w
}

// GetBagState retrieves available state from the tentative bundle data.
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) GetBagState(stateID LinkID, wKey, uKey []byte) [][]byte {
	winMap := d.state[stateID]
	w := d.toWindow(wKey)
	data := winMap[w][string(uKey)]
	slog.Debug("State() Bag.Get", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("Window", w), slog.Any("Data", data))
	return data.Bag
}

func (d *TentativeData) appendState(stateID LinkID, wKey []byte) map[string]StateData {
	if d.state == nil {
		d.state = map[LinkID]map[typex.Window]map[string]StateData{}
	}
	winMap, ok := d.state[stateID]
	if !ok {
		winMap = map[typex.Window]map[string]StateData{}
		d.state[stateID] = winMap
	}
	w := d.toWindow(wKey)
	kmap, ok := winMap[w]
	if !ok {
		kmap = map[string]StateData{}
		winMap[w] = kmap
	}
	return kmap
}

// AppendBagState appends the incoming data to the existing tentative data bundle.
//
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) AppendBagState(stateID LinkID, wKey, uKey, data []byte) {
	kmap := d.appendState(stateID, wKey)
	kmap[string(uKey)] = StateData{Bag: append(kmap[string(uKey)].Bag, data)}
	slog.Debug("State() Bag.Append", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("Window", wKey), slog.Any("NewData", data))
}

func (d *TentativeData) clearState(stateID LinkID, wKey []byte) map[string]StateData {
	if d.state == nil {
		return nil
	}
	winMap, ok := d.state[stateID]
	if !ok {
		return nil
	}
	w := d.toWindow(wKey)
	return winMap[w]
}

// ClearBagState clears any tentative data for the state. Since state data is only initialized if any exists,
// Clear takes the approach to not create state that doesn't already exist. Existing state is zeroed
// to allow that to be committed post bundle commpletion.
//
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) ClearBagState(stateID LinkID, wKey, uKey []byte) {
	kmap := d.clearState(stateID, wKey)
	if kmap == nil {
		return
	}
	// Zero the current entry to clear.
	// Delete makes it difficult to delete the persisted stage state for the key.
	kmap[string(uKey)] = StateData{}
	slog.Debug("State() Bag.Clear", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("WindowKey", wKey))
}

// GetMultimapState retrieves available state from the tentative bundle data.
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) GetMultimapState(stateID LinkID, wKey, uKey, mapKey []byte) [][]byte {
	winMap := d.state[stateID]
	w := d.toWindow(wKey)
	data := winMap[w][string(uKey)].Multimap[string(mapKey)]
	slog.Debug("State() Multimap.Get", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("Window", w), slog.Any("Data", data))
	return data
}

// AppendMultimapState appends the incoming data to the existing tentative data bundle.
//
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) AppendMultimapState(stateID LinkID, wKey, uKey, mapKey, data []byte) {
	kmap := d.appendState(stateID, wKey)
	stateData, ok := kmap[string(uKey)]
	if !ok || stateData.Multimap == nil { // Incase of All Key Clear tombstones, we may have a nil map.
		stateData = StateData{Multimap: map[string][][]byte{}}
		kmap[string(uKey)] = stateData
	}
	stateData.Multimap[string(mapKey)] = append(stateData.Multimap[string(mapKey)], data)
	// The Multimap field is aliased to the instance we stored in kmap,
	// so we don't need to re-assign back to kmap after appending the data to mapKey.
	slog.Debug("State() Multimap.Append", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("MapKey", mapKey), slog.Any("Window", wKey), slog.Any("NewData", data))
}

// ClearMultimapState clears any tentative data for the state. Since state data is only initialized if any exists,
// Clear takes the approach to not create state that doesn't already exist. Existing state is zeroed
// to allow that to be committed post bundle commpletion.
//
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) ClearMultimapState(stateID LinkID, wKey, uKey, mapKey []byte) {
	kmap := d.clearState(stateID, wKey)
	if kmap == nil {
		return
	}
	// Nil the current entry to clear.
	// Delete makes it difficult to delete the persisted stage state for the key.
	userMap, ok := kmap[string(uKey)]
	if !ok || userMap.Multimap == nil {
		return
	}
	userMap.Multimap[string(mapKey)] = nil
	// The Multimap field is aliased to the instance we stored in kmap,
	// so we don't need to re-assign back to kmap after clearing the data from mapKey.
	slog.Debug("State() Multimap.Clear", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("Window", wKey))
}

// GetMultimapKeysState retrieves all available user map keys.
//
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) GetMultimapKeysState(stateID LinkID, wKey, uKey []byte) [][]byte {
	winMap := d.state[stateID]
	w := d.toWindow(wKey)
	userMap := winMap[w][string(uKey)]
	var keys [][]byte
	for k := range userMap.Multimap {
		keys = append(keys, []byte(k))
	}
	slog.Debug("State() MultimapKeys.Get", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("Window", w), slog.Any("Keys", keys))
	return keys
}

// ClearMultimapKeysState clears tentative data for all user map keys. Since state data is only initialized if any exists,
// Clear takes the approach to not create state that doesn't already exist. Existing state is zeroed
// to allow that to be committed post bundle commpletion.
//
// The stateID has the Transform and Local fields populated, for the Transform and UserStateID respectively.
func (d *TentativeData) ClearMultimapKeysState(stateID LinkID, wKey, uKey []byte) {
	kmap := d.clearState(stateID, wKey)
	if kmap == nil {
		return
	}
	// Zero the current entry to clear.
	// Delete makes it difficult to delete the persisted stage state for the key.
	kmap[string(uKey)] = StateData{}
	slog.Debug("State() MultimapKeys.Clear", slog.Any("StateID", stateID), slog.Any("UserKey", uKey), slog.Any("WindowKey", wKey))
}
