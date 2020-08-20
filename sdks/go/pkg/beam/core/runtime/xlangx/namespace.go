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

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

func AddCoderID(c *pipepb.Components, idMap map[string]string, cid string, newID func(string) string) string {
	if _, exists := idMap[cid]; exists {
		return idMap[cid]
	}

	coder, exists := c.Coders[cid]
	if !exists {
		panic(errors.Errorf("attempted to add namespace to missing coder id: %v not in %v", cid, c.Coders))
	}

	// Updating ComponentCoderIDs of Coder
	if coder.GetComponentCoderIds() != nil {
		var updatedComponentCoderIDs []string
		updatedComponentCoderIDs = append(updatedComponentCoderIDs, coder.ComponentCoderIds...)

		for i, ccid := range coder.ComponentCoderIds {
			updatedComponentCoderIDs[i] = AddCoderID(c, idMap, ccid, newID)
		}
		coder.ComponentCoderIds = updatedComponentCoderIDs
	}

	idMap[cid] = newID(cid)

	// Updating Coders map
	c.Coders[idMap[cid]] = coder
	delete(c.Coders, cid)

	return idMap[cid]
}

func AddWindowingStrategyID(c *pipepb.Components, idMap map[string]string, wid string, newID func(string) string) string {
	if _, exists := idMap[wid]; exists {
		return idMap[wid]
	}

	windowingStrategy, exists := c.WindowingStrategies[wid]
	if !exists {
		panic(errors.Errorf("attempted to add namespace to missing windowing strategy id: %v not in %v", wid, c.WindowingStrategies))
	}

	// Updating WindowCoderID of WindowingStrategy
	if windowingStrategy.WindowCoderId != "" {
		windowingStrategy.WindowCoderId = AddCoderID(c, idMap, windowingStrategy.WindowCoderId, newID)
	}

	// Updating EnvironmentId of WindowingStrategy
	if windowingStrategy.EnvironmentId != "" {
		windowingStrategy.EnvironmentId = AddEnvironmentID(c, idMap, windowingStrategy.EnvironmentId, newID)
	}

	idMap[wid] = newID(wid)

	// Updating WindowingStrategies map
	c.WindowingStrategies[idMap[wid]] = windowingStrategy
	delete(c.WindowingStrategies, wid)

	return idMap[wid]
}

func AddEnvironmentID(c *pipepb.Components, idMap map[string]string, eid string, newID func(string) string) string {
	if _, exists := idMap[eid]; exists {
		return idMap[eid]
	}

	environment, exists := c.Environments[eid]
	if !exists {
		panic(errors.Errorf("attempted to add namespace to missing windowing strategy id: %v not in %v", eid, c.Environments))
	}

	idMap[eid] = newID(eid)

	// Updating Environments map
	c.Environments[idMap[eid]] = environment
	delete(c.Environments, eid)

	return idMap[eid]
}

func AddNamespace(t *pipepb.PTransform, c *pipepb.Components, namespace string) {
	newID := func(id string) string {
		return fmt.Sprintf("%v@%v", id, namespace)
	}

	idMap := make(map[string]string)

	// Update Environment ID of PTransform
	if t.EnvironmentId != "" {
		t.EnvironmentId = AddEnvironmentID(c, idMap, t.EnvironmentId, newID)
	}
	for _, pcolsMap := range []map[string]string{t.Inputs, t.Outputs} {
		for _, pid := range pcolsMap {
			if pcol, exists := c.Pcollections[pid]; exists {
				// Update Coder ID of PCollection
				pcol.CoderId = AddCoderID(c, idMap, pcol.CoderId, newID)

				// Update WindowingStrategyID of PCollection
				pcol.WindowingStrategyId = AddWindowingStrategyID(c, idMap, pcol.WindowingStrategyId, newID)
			}
		}
	}
}
