/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as assert from "assert";
import {
  CachingStateProvider,
  StateProvider,
  MaybePromise,
} from "../src/apache_beam/worker/state";
import * as fnApi from "../src/apache_beam/proto/beam_fn_api";

/**
 * Mock StateProvider for testing that tracks call counts.
 */
class MockStateProvider implements StateProvider {
  callCount: number = 0;
  values: Map<string, any> = new Map();
  delayMs: number = 0;

  constructor(delayMs: number = 0) {
    this.delayMs = delayMs;
  }

  setValue(key: string, value: any) {
    this.values.set(key, value);
  }

  getState<T>(
    stateKey: fnApi.StateKey,
    decode: (data: Uint8Array) => T,
  ): MaybePromise<T> {
    this.callCount++;
    const key = Buffer.from(fnApi.StateKey.toBinary(stateKey)).toString(
      "base64",
    );
    const value = this.values.get(key);

    if (this.delayMs > 0) {
      return {
        type: "promise",
        promise: new Promise<T>((resolve) => {
          setTimeout(() => resolve(value), this.delayMs);
        }),
      };
    } else {
      return { type: "value", value };
    }
  }
}

describe("CachingStateProvider", function () {
  it("caches values and returns cached result on subsequent calls", function () {
    const mockProvider = new MockStateProvider();
    // Use large weight limit to ensure no eviction for this test
    const cache = new CachingStateProvider(mockProvider, 10 * 1024);

    const stateKey: fnApi.StateKey = {
      type: {
        oneofKind: "bagUserState",
        bagUserState: fnApi.StateKey_BagUserState.create({
          transformId: "test",
          userStateId: "state1",
          window: new Uint8Array(0),
          key: new Uint8Array(0),
        }),
      },
    };

    const decode = (data: Uint8Array) => data.toString();

    // Set value in mock
    const testValue = "cached_value";
    const key = Buffer.from(fnApi.StateKey.toBinary(stateKey)).toString(
      "base64",
    );
    mockProvider.setValue(key, testValue);

    // First call should hit underlying provider
    const result1 = cache.getState(stateKey, decode);
    assert.equal(mockProvider.callCount, 1);
    assert.equal(result1.type, "value");
    if (result1.type === "value") {
      assert.equal(result1.value, testValue);
    }

    // Second call should use cache
    const result2 = cache.getState(stateKey, decode);
    assert.equal(mockProvider.callCount, 1); // Still 1, not 2
    assert.equal(result2.type, "value");
    if (result2.type === "value") {
      assert.equal(result2.value, testValue);
    }
  });

  it("evicts least recently used entry when cache weight exceeds limit", function () {
    const mockProvider = new MockStateProvider();
    // Each small string "valueX" is approximately 52 bytes (40 + 6*2)
    // Set weight limit to hold approximately 3 entries
    const cache = new CachingStateProvider(mockProvider, 180);

    const decode = (data: Uint8Array) => data.toString();

    // Create 4 different state keys
    const keys: fnApi.StateKey[] = [];
    for (let i = 0; i < 4; i++) {
      keys.push({
        type: {
          oneofKind: "bagUserState",
          bagUserState: fnApi.StateKey_BagUserState.create({
            transformId: "test",
            userStateId: `state${i}`,
            window: new Uint8Array(0),
            key: new Uint8Array(0),
          }),
        },
      });
    }

    // Set values in mock
    for (let i = 0; i < 4; i++) {
      const key = Buffer.from(fnApi.StateKey.toBinary(keys[i])).toString(
        "base64",
      );
      mockProvider.setValue(key, `value${i}`);
    }

    // Fill cache with 3 entries
    cache.getState(keys[0], decode);
    cache.getState(keys[1], decode);
    cache.getState(keys[2], decode);
    assert.equal(mockProvider.callCount, 3);
    assert.equal(cache.cache.size, 3);

    // Access keys[0] to make it most recently used
    cache.getState(keys[0], decode);
    assert.equal(mockProvider.callCount, 3); // Still cached

    // Add 4th entry - should evict keys[1] (least recently used, not keys[0])
    cache.getState(keys[3], decode);
    assert.equal(mockProvider.callCount, 4);

    // keys[1] should be evicted (not in cache)
    const result1 = cache.getState(keys[1], decode);
    assert.equal(mockProvider.callCount, 5); // Had to fetch again
    assert.equal(result1.type, "value");
    if (result1.type === "value") {
      assert.equal(result1.value, "value1");
    }

    // keys[0] should still be cached (was most recently used)
    const result0 = cache.getState(keys[0], decode);
    assert.equal(mockProvider.callCount, 5); // Still cached, no new call
    assert.equal(result0.type, "value");
    if (result0.type === "value") {
      assert.equal(result0.value, "value0");
    }
  });

  it("handles promise-based state fetches correctly", async function () {
    const mockProvider = new MockStateProvider(10); // 10ms delay
    // Use large weight limit to ensure no eviction for this test
    const cache = new CachingStateProvider(mockProvider, 10 * 1024);

    const stateKey: fnApi.StateKey = {
      type: {
        oneofKind: "bagUserState",
        bagUserState: fnApi.StateKey_BagUserState.create({
          transformId: "test",
          userStateId: "async_state",
          window: new Uint8Array(0),
          key: new Uint8Array(0),
        }),
      },
    };

    const decode = (data: Uint8Array) => data.toString();
    const key = Buffer.from(fnApi.StateKey.toBinary(stateKey)).toString(
      "base64",
    );
    mockProvider.setValue(key, "async_value");

    // First call returns promise
    const result1 = cache.getState(stateKey, decode);
    assert.equal(result1.type, "promise");
    assert.equal(mockProvider.callCount, 1);

    // Wait for promise to resolve
    if (result1.type === "promise") {
      const value1 = await result1.promise;
      assert.equal(value1, "async_value");

      // Second call should return cached value (not promise)
      const result2 = cache.getState(stateKey, decode);
      assert.equal(result2.type, "value");
      assert.equal(mockProvider.callCount, 1); // Still only 1 call
      if (result2.type === "value") {
        assert.equal(result2.value, "async_value");
      }
    }
  });

  it("respects custom maxCacheWeight and evicts based on memory size", function () {
    const mockProvider = new MockStateProvider();
    // Set weight limit to hold approximately 2 small string entries
    const cache = new CachingStateProvider(mockProvider, 120);

    const decode = (data: Uint8Array) => data.toString();

    const keys: fnApi.StateKey[] = [];
    for (let i = 0; i < 3; i++) {
      keys.push({
        type: {
          oneofKind: "bagUserState",
          bagUserState: fnApi.StateKey_BagUserState.create({
            transformId: "test",
            userStateId: `state${i}`,
            window: new Uint8Array(0),
            key: new Uint8Array(0),
          }),
        },
      });
      const key = Buffer.from(fnApi.StateKey.toBinary(keys[i])).toString(
        "base64",
      );
      mockProvider.setValue(key, `value${i}`);
    }

    // Fill cache with 2 entries
    cache.getState(keys[0], decode);
    cache.getState(keys[1], decode);
    assert.equal(cache.cache.size, 2);

    // Add 3rd entry - should evict oldest to stay under weight limit
    cache.getState(keys[2], decode);

    // First entry should be evicted
    cache.getState(keys[0], decode);
    assert.equal(mockProvider.callCount, 4); // Had to fetch keys[0] again
  });

  it("tracks cache weight correctly", function () {
    const mockProvider = new MockStateProvider();
    const cache = new CachingStateProvider(mockProvider, 10 * 1024);

    const decode = (data: Uint8Array) => data.toString();

    const stateKey: fnApi.StateKey = {
      type: {
        oneofKind: "bagUserState",
        bagUserState: fnApi.StateKey_BagUserState.create({
          transformId: "test",
          userStateId: "state1",
          window: new Uint8Array(0),
          key: new Uint8Array(0),
        }),
      },
    };

    const key = Buffer.from(fnApi.StateKey.toBinary(stateKey)).toString(
      "base64",
    );
    mockProvider.setValue(key, "test_value");

    // Cache should start with 0 weight
    assert.equal(cache.currentWeight, 0);

    // After adding an entry, weight should increase
    cache.getState(stateKey, decode);
    assert.ok(cache.currentWeight > 0);
  });
});

