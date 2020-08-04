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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Objects;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link WindmillStateCache}.
 */
@RunWith(JUnit4.class)
public class WindmillStateCacheTest {

  private static final String COMPUTATION = "computation";
  private static final ByteString KEY = ByteString.copyFromUtf8("key");
  private static final long SHARDING_KEY = 123;
  private static final String STATE_FAMILY = "family";

  private static class TestStateTag implements StateTag<TestState> {

    final String id;

    TestStateTag(String id) {
      this.id = id;
    }

    @Override
    public void appendTo(Appendable appendable) throws IOException {
      appendable.append(id);
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public TestState bind(StateBinder binder) {
      throw new UnsupportedOperationException();
    }

    @Override
    public StateSpec<TestState> getSpec() {
      return null;
    }

    @Override
    public String toString() {
      return "Tag(" + id + ")";
    }

    @Override
    public boolean equals(Object other) {
      return (other instanceof TestStateTag) && Objects.equals(((TestStateTag) other).id, id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }

  private static class TestState implements State {

    String value = null;

    TestState(String value) {
      this.value = value;
    }

    @Override
    public void clear() {
      this.value = null;
    }

    @Override
    public boolean equals(Object other) {
      return (other instanceof TestState) && Objects.equals(((TestState) other).value, value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public String toString() {
      return "State(" + value + ")";
    }
  }

  private static StateNamespace windowNamespace(long start) {
    return StateNamespaces.window(
        IntervalWindow.getCoder(), new IntervalWindow(new Instant(start), new Instant(start + 1)));
  }

  private static StateNamespace triggerNamespace(long start, int triggerIdx) {
    return StateNamespaces.windowAndTrigger(
        IntervalWindow.getCoder(),
        new IntervalWindow(new Instant(start), new Instant(start + 1)),
        triggerIdx);
  }

  WindmillStateCache cache;
  WindmillStateCache.ForKey keyCache;

  @Before
  public void setUp() {
    cache = new WindmillStateCache();
    assertEquals(0, cache.getWeight());
  }

  @Test
  public void testBasic() throws Exception {
    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 1L);
    assertNull(keyCache.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertNull(keyCache.get(windowNamespace(0), new TestStateTag("tag2")));
    assertNull(keyCache.get(triggerNamespace(0, 0), new TestStateTag("tag3")));
    assertNull(keyCache.get(triggerNamespace(0, 0), new TestStateTag("tag2")));

    keyCache.put(StateNamespaces.global(), new TestStateTag("tag1"), new TestState("g1"), 2);
    assertEquals(129, cache.getWeight());
    keyCache.put(windowNamespace(0), new TestStateTag("tag2"), new TestState("w2"), 2);
    assertEquals(258, cache.getWeight());
    keyCache.put(triggerNamespace(0, 0), new TestStateTag("tag3"), new TestState("t3"), 2);
    assertEquals(276, cache.getWeight());
    keyCache.put(triggerNamespace(0, 0), new TestStateTag("tag2"), new TestState("t2"), 2);
    assertEquals(294, cache.getWeight());

    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 2L);
    assertEquals(
        new TestState("g1"), keyCache.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(new TestState("w2"), keyCache.get(windowNamespace(0), new TestStateTag("tag2")));
    assertEquals(
        new TestState("t3"), keyCache.get(triggerNamespace(0, 0), new TestStateTag("tag3")));
    assertEquals(
        new TestState("t2"), keyCache.get(triggerNamespace(0, 0), new TestStateTag("tag2")));
  }

  /**
   * Verifies that values are cached in the appropriate namespaces.
   */
  @Test
  public void testInvalidation() throws Exception {
    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 1L);
    assertNull(keyCache.get(StateNamespaces.global(), new TestStateTag("tag1")));
    keyCache.put(StateNamespaces.global(), new TestStateTag("tag1"), new TestState("g1"), 2);

    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 2L);
    assertEquals(129, cache.getWeight());
    assertEquals(
        new TestState("g1"), keyCache.get(StateNamespaces.global(), new TestStateTag("tag1")));

    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 1L, 3L);
    assertEquals(129, cache.getWeight());
    assertNull(keyCache.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(0, cache.getWeight());
  }

  /**
   * Verifies that the cache is invalidated when the cache token changes.
   */
  @Test
  public void testEviction() throws Exception {
    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 1L);
    keyCache.put(windowNamespace(0), new TestStateTag("tag2"), new TestState("w2"), 2);
    assertEquals(129, cache.getWeight());
    keyCache.put(triggerNamespace(0, 0), new TestStateTag("tag3"), new TestState("t3"), 2000000000);
    assertEquals(0, cache.getWeight());
    // Eviction is atomic across the whole window.
    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 2L);
    assertNull(keyCache.get(windowNamespace(0), new TestStateTag("tag2")));
    assertNull(keyCache.get(triggerNamespace(0, 0), new TestStateTag("tag3")));
  }

  /**
   * Verifies that the cache does not vend for stale work tokens.
   */
  @Test
  public void testStaleWorkItem() throws Exception {
    TestStateTag tag = new TestStateTag("tag2");

    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 2L);
    keyCache.put(windowNamespace(0), tag, new TestState("w2"), 2);
    assertEquals(129, cache.getWeight());
    // Same cache.
    assertNull(keyCache.get(windowNamespace(0), tag));

    // Previous work token.
    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 1L);
    assertNull(keyCache.get(windowNamespace(0), tag));

    // Retry of work token that inserted.
    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 2L);
    assertNull(keyCache.get(windowNamespace(0), tag));

    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 10L);
    assertEquals(new TestState("w2"), keyCache.get(windowNamespace(0), tag));
    keyCache.put(windowNamespace(0), tag, new TestState("w3"), 2);

    // Ensure that second put updated work token.
    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 5L);
    assertNull(keyCache.get(windowNamespace(0), tag));

    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, SHARDING_KEY, STATE_FAMILY, 0L, 15L);
    assertEquals(new TestState("w3"), keyCache.get(windowNamespace(0), tag));
  }

  /**
   * Verifies that caches are kept independently per-key.
   */
  @Test
  public void testMultipleKeys() throws Exception {
    TestStateTag tag = new TestStateTag("tag1");

    WindmillStateCache.ForKey keyCache1 =
        cache.forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key1"), SHARDING_KEY, STATE_FAMILY, 0L, 0L);
    WindmillStateCache.ForKey keyCache2 =
        cache
            .forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key2"), SHARDING_KEY, STATE_FAMILY, 0L, 10L);
    WindmillStateCache.ForKey keyCache3 =
        cache.forComputation("comp2")
            .forKey(ByteString.copyFromUtf8("key1"), SHARDING_KEY, STATE_FAMILY, 0L, 0L);

    TestState state1 = new TestState("g1");
    keyCache1.put(StateNamespaces.global(), tag, state1, 2);
    assertNull(keyCache1.get(StateNamespaces.global(), tag));
    keyCache1 =
        cache.forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key1"), SHARDING_KEY, STATE_FAMILY, 0L, 1L);
    assertEquals(state1, keyCache1.get(StateNamespaces.global(), tag));
    assertNull(keyCache2.get(StateNamespaces.global(), tag));
    assertNull(keyCache3.get(StateNamespaces.global(), tag));

    TestState state2 = new TestState("g2");
    keyCache2.put(StateNamespaces.global(), tag, state2, 2);
    assertNull(keyCache2.get(StateNamespaces.global(), tag));
    keyCache2 =
        cache
            .forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key2"), SHARDING_KEY, STATE_FAMILY, 0L, 20L);
    assertEquals(state2, keyCache2.get(StateNamespaces.global(), tag));
    assertEquals(state1, keyCache1.get(StateNamespaces.global(), tag));
    assertNull(keyCache3.get(StateNamespaces.global(), tag));
  }

  /**
   * Verifies that caches are kept independently per shard of key.
   */
  @Test
  public void testMultipleShardsOfKey() throws Exception {
    TestStateTag tag = new TestStateTag("tag1");
    ByteString key1 = ByteString.copyFromUtf8("key1");
    ByteString key2 = ByteString.copyFromUtf8("key2");

    WindmillStateCache.ForKey key1CacheShard1 =
        cache.forComputation(COMPUTATION)
            .forKey(key1, 1, STATE_FAMILY, 0L, 0L);
    WindmillStateCache.ForKey key1CacheShard2 =
        cache
            .forComputation(COMPUTATION)
            .forKey(key1, 2, STATE_FAMILY, 0L, 0L);
    WindmillStateCache.ForKey key2CacheShard1 =
        cache
            .forComputation(COMPUTATION)
            .forKey(key2, 1, STATE_FAMILY, 0L, 0L);

    TestState state1 = new TestState("g1");
    key1CacheShard1.put(StateNamespaces.global(), tag, state1, 2);
    assertNull(key1CacheShard1.get(StateNamespaces.global(), tag));
    key1CacheShard1 =
        cache.forComputation(COMPUTATION)
            .forKey(key1, 1, STATE_FAMILY, 0L, 1L);
    assertEquals(state1, key1CacheShard1.get(StateNamespaces.global(), tag));
    assertNull(key1CacheShard2.get(StateNamespaces.global(), tag));
    assertNull(key2CacheShard1.get(StateNamespaces.global(), tag));

    TestState state2 = new TestState("g2");
    key1CacheShard2.put(StateNamespaces.global(), tag, state2, 2);
    assertNull(key1CacheShard2.get(StateNamespaces.global(), tag));
    key1CacheShard2 =
        cache
            .forComputation(COMPUTATION)
            .forKey(key1, 2, STATE_FAMILY, 0L, 20L);
    assertEquals(state2, key1CacheShard2.get(StateNamespaces.global(), tag));
    assertEquals(state1, key1CacheShard1.get(StateNamespaces.global(), tag));
    assertNull(key2CacheShard1.get(StateNamespaces.global(), tag));
  }

  /**
   * Verifies explicit invalidation does indeed invalidate the correct entries.
   */
  @Test
  public void testExplicitInvalidation() throws Exception {
    WindmillStateCache.ForKey keyCache1 =
        cache.forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key1"), 1, STATE_FAMILY, 0L, 0L);
    WindmillStateCache.ForKey keyCache2 =
        cache.forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key2"), SHARDING_KEY, STATE_FAMILY, 0L, 0L);
    WindmillStateCache.ForKey keyCache3 =
        cache.forComputation("comp2")
            .forKey(ByteString.copyFromUtf8("key1"), SHARDING_KEY, STATE_FAMILY, 0L, 0L);
    WindmillStateCache.ForKey keyCache4 =
        cache.forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key1"), 2, STATE_FAMILY, 0L, 0L);

    keyCache1.put(StateNamespaces.global(), new TestStateTag("tag1"), new TestState("g1"), 1);
    keyCache2.put(StateNamespaces.global(), new TestStateTag("tag2"), new TestState("g2"), 2);
    keyCache3.put(StateNamespaces.global(), new TestStateTag("tag3"), new TestState("g3"), 3);
    keyCache4.put(StateNamespaces.global(), new TestStateTag("tag4"), new TestState("g4"), 4);
    keyCache1 =
        cache.forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key1"), 1, STATE_FAMILY, 0L, 1L);
    keyCache2 =
        cache.forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key2"), SHARDING_KEY, STATE_FAMILY, 0L, 1L);
    keyCache3 =
        cache.forComputation("comp2")
            .forKey(ByteString.copyFromUtf8("key1"), SHARDING_KEY, STATE_FAMILY, 0L, 1L);
    keyCache4 =
        cache.forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key1"), 2, STATE_FAMILY, 0L, 1L);
    assertEquals(
        new TestState("g1"), keyCache1.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(
        new TestState("g2"), keyCache2.get(StateNamespaces.global(), new TestStateTag("tag2")));
    assertEquals(
        new TestState("g3"), keyCache3.get(StateNamespaces.global(), new TestStateTag("tag3")));
    assertEquals(
        new TestState("g4"), keyCache4.get(StateNamespaces.global(), new TestStateTag("tag4")));

    // Invalidation of key 1 shard 1 does not affect another shard of key 1 or other keys.
    cache.forComputation("comp1").invalidate(ByteString.copyFromUtf8("key1"), 1);

    assertNull(keyCache1.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(
        new TestState("g2"), keyCache2.get(StateNamespaces.global(), new TestStateTag("tag2")));
    assertEquals(
        new TestState("g3"), keyCache3.get(StateNamespaces.global(), new TestStateTag("tag3")));
    assertEquals(
        new TestState("g4"), keyCache4.get(StateNamespaces.global(), new TestStateTag("tag4")));

    // Invalidation of an non-existing key affects nothing.
    cache.forComputation("comp1").invalidate(ByteString.copyFromUtf8("key1"), 3);

    assertEquals(
        new TestState("g2"), keyCache2.get(StateNamespaces.global(), new TestStateTag("tag2")));
    assertEquals(
        new TestState("g3"), keyCache3.get(StateNamespaces.global(), new TestStateTag("tag3")));
    assertEquals(
        new TestState("g4"), keyCache4.get(StateNamespaces.global(), new TestStateTag("tag4")));
  }

  private static class TestStateTagWithBadEquality extends TestStateTag {

    public TestStateTagWithBadEquality(String id) {
      super(id);
    }

    @Override
    public boolean equals(Object other) {
      return this == other;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }
  }

  /**
   * Verifies that caching works properly even when the StateTag does not properly implement
   * equals() and hashCode()
   */
  @Test
  public void testBadCoderEquality() throws Exception {
    WindmillStateCache.ForKey keyCache1 =
        cache.forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key1"), SHARDING_KEY, STATE_FAMILY, 0L, 0L);

    StateTag<TestState> tag = new TestStateTagWithBadEquality("tag1");
    keyCache1.put(StateNamespaces.global(), tag, new TestState("g1"), 1);

    keyCache1 =
        cache.forComputation("comp1")
            .forKey(ByteString.copyFromUtf8("key1"), SHARDING_KEY, STATE_FAMILY, 0L, 1L);
    assertEquals(new TestState("g1"), keyCache1.get(StateNamespaces.global(), tag));
    assertEquals(
        new TestState("g1"),
        keyCache1.get(StateNamespaces.global(), new TestStateTagWithBadEquality("tag1")));
  }
}
