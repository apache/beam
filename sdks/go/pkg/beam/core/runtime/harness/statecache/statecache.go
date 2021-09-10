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

// Package statecache implements the state caching feature described by the
// Beam Fn API
//
// https://docs.google.com/document/d/1BOozW0bzBuz4oHJEuZNDOHdzaV5Y56ix58Ozrqm2jFg/edit#heading=h.7ghoih5aig5m
package statecache

import (
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
)

// SideInputCache stores a cache of reusable inputs for the purposes of
// eliminating redundant calls to the runner during execution of ParDos
// using side inputs.
//
// A SideInputCache should be initialized when the SDK harness is initialized,
// creating storage for side input caching. On each ProcessBundleRequest,
// the cache will process the list of tokens for cacheable side inputs and
// be queried when side inputs are requested in bundle execution. Once a
// new bundle request comes in the valid tokens will be updated and the cache
// will be re-used. In the event that the cache reaches capacity, a random,
// currently invalid cached object will be evicted.
type SideInputCache struct {
	cache       map[string]exec.ReusableInput
	idsToTokens map[string]string
	validTokens []string
	mu          sync.Mutex
	capacity    int
}

// Init makes the cache map and the map of IDs to cache tokens for the
// SideInputCache. Should only be called once.
func (c *SideInputCache) Init(cap int) error {
	if cap <= 0 {
		return errors.Errorf("capacity must be a positive integer, got %v", cap)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]exec.ReusableInput, cap)
	c.idsToTokens = make(map[string]string)
	return nil
}

// Completely clears the list of valid tokens. Should be called when
// starting to handle a new request.
func (c *SideInputCache) clearValidTokens() {
	c.validTokens = nil
}

// SetValidTokens clears the list of valid tokens then sets new ones, also updating the mapping of
// transform and side input IDs to cache tokens in the process. Should be called at the start of every
// new ProcessBundleRequest. If the runner does not support caching, the passed cache token values
// should be empty and all get/set requests will silently be no-ops.
func (c *SideInputCache) SetValidTokens(cacheTokens ...fnpb.ProcessBundleRequest_CacheToken) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearValidTokens()
	for _, tok := range cacheTokens {
		// User State caching is currently not supported, so these tokens are ignored
		if tok.GetUserState() != nil {
			continue
		}
		s := tok.GetSideInput()
		transformID := s.GetTransformId()
		sideInputID := s.GetSideInputId()
		token := string(tok.GetToken())
		c.setValidToken(transformID, sideInputID, token)
	}
}

// setValidToken adds a new valid token for a request into the SideInputCache struct
// and maps the transform ID and side input ID pairing to the cache token.
func (c *SideInputCache) setValidToken(transformID, sideInputID, token string) {
	idKey := transformID + sideInputID
	c.idsToTokens[idKey] = token
	c.validTokens = append(c.validTokens, token)
}

func (c *SideInputCache) makeAndValidateToken(transformID, sideInputID string) (string, bool) {
	idKey := transformID + sideInputID
	// Check if it's a known token
	tok, ok := c.idsToTokens[idKey]
	if !ok {
		return "", false
	}
	// Check if the known token is valid for this request
	for _, t := range c.validTokens {
		if t == tok {
			return tok, true
		}
	}
	return "", false
}

// QueryCache takes a transform ID and side input ID and checking if a corresponding side
// input has been cached. A query having a bad token (e.g. one that doesn't make a known
// token or one that makes a known but currently invalid token) is treated the same as a
// cache miss.
func (c *SideInputCache) QueryCache(transformID, sideInputID string) exec.ReusableInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	tok, ok := c.makeAndValidateToken(transformID, sideInputID)
	if !ok {
		return nil
	}
	// Check to see if cached
	input, ok := c.cache[tok]
	if !ok {
		return nil
	}
	return input
}

// SetCache allows a user to place a ReusableInput materialized from the reader into the SideInputCache
// with its corresponding transform ID and side input ID. If the IDs do not pair with a known, valid token
// then we silently do not cache the input, as this is an indication that the runner is treating that input
// as uncacheable.
func (c *SideInputCache) SetCache(transformID, sideInputID string, input exec.ReusableInput) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	tok, ok := c.makeAndValidateToken(transformID, sideInputID)
	if !ok {
		return nil
	}
	if len(c.cache) > c.capacity {
		err := c.evictElement()
		if err != nil {
			return errors.Errorf("Cache at or above capacity, got %v", err)
		}
	}
	c.cache[tok] = input
	return nil
}

func (c *SideInputCache) isValid(token string) bool {
	for _, t := range c.validTokens {
		if t == token {
			return true
		}
	}
	return false
}

// evictElement randomly evicts a ReusableInput that is not currently valid from the cache.
// It should only be called by a goroutine that obtained the lock in SetCache.
func (c *SideInputCache) evictElement() error {
	deleted := false
	// Select a key from the cache at random
	for k := range c.cache {
		// Do not evict an element if it's currently valid
		if !c.isValid(k) {
			delete(c.cache, k)
			deleted = true
			break
		}
	}
	// Nothing is deleted if every side input is still valid, meaning that the cache size
	// is likely too small.
	if !deleted {
		return errors.Errorf("Failed to evict elements from cache, every element is currently valid")
	}
	return nil
}
