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
// The Beam State API and the intended caching behavior are described here:
// https://docs.google.com/document/d/1BOozW0bzBuz4oHJEuZNDOHdzaV5Y56ix58Ozrqm2jFg/edit#heading=h.7ghoih5aig5m
package statecache

import (
	"context"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
)

type token string

type cacheToken struct {
	transformID string
	sideInputID string
	tok         token
}

type cacheKey struct {
	tok cacheToken
	win string
	key string
}

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
	capacity    int
	enabled     bool
	mu          sync.Mutex
	cache       map[cacheKey]exec.ReStream
	idsToTokens map[string]token
	validTokens map[cacheToken]int8 // Maps tokens to active bundle counts
	metrics     CacheMetrics
}

// CacheMetrics stores metrics for the cache across a pipeline run.
type CacheMetrics struct {
	Hits, Misses, Evictions, InUseEvictions, ReStreamErrors int64
}

// Init makes the cache map and the map of IDs to cache tokens for the
// SideInputCache. Should only be called once. Returns an error for
// non-positive capacities.
func (c *SideInputCache) Init(cap int) error {
	if cap < 0 {
		return errors.Errorf("capacity must be a positive integer, got %v", cap)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if cap == 0 {
		c.enabled = false
		return nil
	}
	c.cache = make(map[cacheKey]exec.ReStream, cap)
	c.idsToTokens = make(map[string]token)
	c.validTokens = make(map[cacheToken]int8)
	c.capacity = cap
	c.metrics = CacheMetrics{}
	c.enabled = true
	return nil
}

// SetValidTokens clears the list of valid tokens then sets new ones, also updating the mapping of
// transform and side input IDs to cache tokens in the process. Should be called at the start of every
// new ProcessBundleRequest. If the runner does not support caching, the passed cache token values
// should be empty and all get/set requests will silently be no-ops.
func (c *SideInputCache) SetValidTokens(cacheTokens ...*fnpb.ProcessBundleRequest_CacheToken) {
	if !c.enabled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, tok := range cacheTokens {
		// User State caching is currently not supported, so these tokens are ignored
		if tok.GetUserState() != nil {
			continue
		}
		s := tok.GetSideInput()
		transformID := s.GetTransformId()
		sideInputID := s.GetSideInputId()
		t := token(tok.GetToken())
		c.setValidToken(transformID, sideInputID, t)
	}
}

// setValidToken adds a new valid token for a request into the SideInputCache struct
// by mapping the transform ID and side input ID pairing to the cache token.
func (c *SideInputCache) setValidToken(transformID, sideInputID string, tok token) {
	idKey := transformID + sideInputID
	c.idsToTokens[idKey] = tok
	fullToken := cacheToken{transformID: transformID, sideInputID: sideInputID, tok: tok}
	count, ok := c.validTokens[fullToken]
	if !ok {
		c.validTokens[fullToken] = 1
	} else {
		c.validTokens[fullToken] = count + 1
	}
}

// CompleteBundle takes the cache tokens passed to set the valid tokens and decrements their
// usage count for the purposes of maintaining a valid count of whether or not a value is
// still in use. Should be called once ProcessBundle has completed.
func (c *SideInputCache) CompleteBundle(cacheTokens ...*fnpb.ProcessBundleRequest_CacheToken) {
	if !c.enabled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, tok := range cacheTokens {
		// User State caching is currently not supported, so these tokens are ignored
		if tok.GetUserState() != nil {
			continue
		}
		s := tok.GetSideInput()
		transformID := s.GetTransformId()
		sideInputID := s.GetSideInputId()
		t := token(tok.GetToken())
		c.decrementTokenCount(transformID, sideInputID, t)
	}
}

// decrementTokenCount decrements the validTokens entry for
// a given token by 1. Should only be called when completing
// a bundle.
func (c *SideInputCache) decrementTokenCount(transformID, sideInputID string, tok token) {
	fullToken := cacheToken{transformID: transformID, sideInputID: sideInputID, tok: tok}
	count := c.validTokens[fullToken]
	if count == 1 {
		delete(c.validTokens, fullToken)
	} else {
		c.validTokens[fullToken] = count - 1
	}
}

func (c *SideInputCache) makeAndValidateToken(transformID, sideInputID string) (token, bool) {
	idKey := transformID + sideInputID
	// Check if it's a known token
	tok, ok := c.idsToTokens[idKey]
	if !ok {
		return "", false
	}
	fullToken := cacheToken{transformID: transformID, sideInputID: sideInputID, tok: tok}
	return tok, c.isValid(fullToken)
}

func (c *SideInputCache) makeCacheKey(transformID, sideInputID string, tok token, w, key []byte) cacheKey {
	fullToken := cacheToken{transformID: transformID, sideInputID: sideInputID, tok: tok}
	return cacheKey{tok: fullToken, win: string(w), key: string(key)}
}

// QueryCache takes a transform ID and side input ID and checking if a corresponding side
// input has been cached. A query having a bad token (e.g. one that doesn't make a known
// token or one that makes a known but currently invalid token) is treated the same as a
// cache miss.
func (c *SideInputCache) QueryCache(ctx context.Context, transformID, sideInputID string, win, key []byte) exec.ReStream {
	if !c.enabled {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tok, ok := c.makeAndValidateToken(transformID, sideInputID)
	if !ok {
		return nil
	}
	ck := c.makeCacheKey(transformID, sideInputID, tok, win, key)
	// Check to see if cached
	input, ok := c.cache[ck]
	if !ok {
		c.metrics.Misses++
		return nil
	}

	c.metrics.Hits++
	return input
}

// materializeReStream reads all of the values from the input ReStream and places its
// values in memory.
func materializeReStream(input exec.ReStream) (exec.ReStream, error) {
	values, err := exec.ReadAll(input)
	if err != nil {
		return nil, err
	}
	return &exec.FixedReStream{Buf: values}, nil
}

// SetCache allows a user to place a ReusableInput materialized from the reader into the SideInputCache
// with its corresponding transform ID and side input ID. If the IDs do not pair with a known, valid token
// then we silently do not cache the input, as this is an indication that the runner is treating that input
// as uncacheable.
func (c *SideInputCache) SetCache(ctx context.Context, transformID, sideInputID string, win, key []byte, input exec.ReStream) exec.ReStream {
	if !c.enabled {
		return input
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tok, ok := c.makeAndValidateToken(transformID, sideInputID)
	if !ok {
		return input
	}
	if len(c.cache) >= c.capacity {
		c.evictElement(ctx)
	}
	mat, err := materializeReStream(input)
	if err != nil {
		c.metrics.ReStreamErrors++
		return input
	}
	ck := c.makeCacheKey(transformID, sideInputID, tok, win, key)
	c.cache[ck] = mat
	return mat
}

func (c *SideInputCache) isValid(tok cacheToken) bool {
	count, ok := c.validTokens[tok]
	// If the token is not known or not in use, return false
	return ok && count > 0
}

// evictElement randomly evicts a ReusableInput that is not currently valid from the cache.
// It should only be called by a goroutine that obtained the lock in SetCache.
func (c *SideInputCache) evictElement(ctx context.Context) {
	deleted := false
	// Select a key from the cache at random
	for k := range c.cache {
		// Do not evict an element if it's currently valid
		if !c.isValid(k.tok) {
			delete(c.cache, k)
			c.metrics.Evictions++
			deleted = true
			break
		}
	}
	// Nothing is deleted if every side input is still valid. Clear
	// out a random entry and record the in-use eviction
	if !deleted {
		for k := range c.cache {
			delete(c.cache, k)
			c.metrics.InUseEvictions++
			break
		}
	}
}

// CacheMetrics returns the cache metrics for current side input cache.
func (c *SideInputCache) CacheMetrics() CacheMetrics {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metrics
}
