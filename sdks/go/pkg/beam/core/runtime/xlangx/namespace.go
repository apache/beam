// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for Additional information regarding copyright ownership.
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

package xlangx

import (
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

func addCoderID(c *pipepb.Components, idMap map[string]string, cid string, newID func(string) string) string {
	if _, exists := idMap[cid]; exists {
		return idMap[cid]
	}

	coder, exists := c.GetCoders()[cid]
	if !exists {
		panic(errors.Errorf("attempted to add namespace to missing coder id: %v not in %v", cid, c.GetCoders()))
	}

	// Updating ComponentCoderIDs of Coder
	if coder.GetComponentCoderIds() != nil {
		var updatedComponentCoderIDs []string
		updatedComponentCoderIDs = append(updatedComponentCoderIDs, coder.GetComponentCoderIds()...)

		for i, ccid := range coder.GetComponentCoderIds() {
			updatedComponentCoderIDs[i] = addCoderID(c, idMap, ccid, newID)
		}
		coder.SetComponentCoderIds(updatedComponentCoderIDs)
	}

	idMap[cid] = newID(cid)

	// Updating Coders map
	c.GetCoders()[idMap[cid]] = coder
	delete(c.GetCoders(), cid)

	return idMap[cid]
}

func addWindowingStrategyID(c *pipepb.Components, idMap map[string]string, wid string, newID func(string) string) string {
	if _, exists := idMap[wid]; exists {
		return idMap[wid]
	}

	windowingStrategy, exists := c.GetWindowingStrategies()[wid]
	if !exists {
		panic(errors.Errorf("attempted to add namespace to missing windowing strategy id: %v not in %v", wid, c.GetWindowingStrategies()))
	}

	// Updating WindowCoderID of WindowingStrategy
	if windowingStrategy.GetWindowCoderId() != "" {
		windowingStrategy.SetWindowCoderId(addCoderID(c, idMap, windowingStrategy.GetWindowCoderId(), newID))
	}

	idMap[wid] = newID(wid)

	// Updating WindowingStrategies map
	c.GetWindowingStrategies()[idMap[wid]] = windowingStrategy
	delete(c.GetWindowingStrategies(), wid)

	return idMap[wid]
}

func addNamespace(t *pipepb.PTransform, c *pipepb.Components, namespace string) {
	newID := func(id string) string {
		return fmt.Sprintf("%v@%v", id, namespace)
	}

	idMap := make(map[string]string)

	// Note: Currently environments are not namespaced. This works under the
	// assumption that the unexpanded transform is using the default Go SDK
	// environment. If multiple Go SDK environments become possible, then
	// namespacing of non-default environments should happen here.

	for _, pcolsMap := range []map[string]string{t.GetInputs(), t.GetOutputs()} {
		for _, pid := range pcolsMap {
			if pcol, exists := c.GetPcollections()[pid]; exists {
				// Update Coder ID of PCollection
				pcol.SetCoderId(addCoderID(c, idMap, pcol.GetCoderId(), newID))

				// Update WindowingStrategyID of PCollection
				pcol.SetWindowingStrategyId(addWindowingStrategyID(c, idMap, pcol.GetWindowingStrategyId(), newID))
			}
		}
	}

	// update component coderIDs for other coders not present in t.Inputs, t.Outputs
	for id, coder := range c.GetCoders() {
		if _, exists := idMap[id]; exists {
			continue
		}
		var updatedComponentCoderIDs []string
		updatedComponentCoderIDs = append(updatedComponentCoderIDs, coder.GetComponentCoderIds()...)
		for i, ccid := range coder.GetComponentCoderIds() {
			if _, exists := idMap[ccid]; exists {
				updatedComponentCoderIDs[i] = idMap[ccid]
			}
		}
		coder.SetComponentCoderIds(updatedComponentCoderIDs)
	}

	sourceName := t.GetUniqueName()
	for _, t := range c.GetTransforms() {
		if t.GetUniqueName() != sourceName {
			if id, exists := idMap[t.GetEnvironmentId()]; exists {
				t.SetEnvironmentId(id)
			}
			for _, pcolsMap := range []map[string]string{t.GetInputs(), t.GetOutputs()} {
				for _, pid := range pcolsMap {
					if pcol, exists := c.GetPcollections()[pid]; exists {
						// Update Coder ID of PCollection
						if id, exists := idMap[pcol.GetCoderId()]; exists {
							pcol.SetCoderId(id)
						}

						// Update WindowingStrategyID of PCollection
						if id, exists := idMap[pcol.GetWindowingStrategyId()]; exists {
							pcol.SetWindowingStrategyId(id)
						}
					}
				}
			}
		}
	}
}
