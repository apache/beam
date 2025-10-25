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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache}. */
@RunWith(JUnit4.class)
public class WindmillStateCacheTest {

  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private static final String COMPUTATION = "computation";
  private static final long SHARDING_KEY = 123;
  private static final WindmillComputationKey COMPUTATION_KEY =
      WindmillComputationKey.create(COMPUTATION, ByteString.copyFromUtf8("key"), SHARDING_KEY);
  private static final String STATE_FAMILY = "family";
  private static final long MEGABYTES = 1024 * 1024;
  DataflowWorkerHarnessOptions options;

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
    public boolean equals(@Nullable Object other) {
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
    public boolean equals(@Nullable Object other) {
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

  private static WindmillComputationKey computationKey(
      String computationId, String key, long shardingKey) {
    return WindmillComputationKey.create(computationId, ByteString.copyFromUtf8(key), shardingKey);
  }

  private static <T extends State> Optional<T> getFromCache(
      WindmillStateCache.ForKeyAndFamily keyCache, StateNamespace namespace, StateTag<T> address) {
    return (Optional<T>)
        Optional.ofNullable(
            keyCache.get(namespace, WindmillStateTagUtil.instance().encodeKey(namespace, address)));
  }

  private static <T extends State> void putInCache(
      WindmillStateCache.ForKeyAndFamily keyCache,
      StateNamespace namespace,
      StateTag<? extends T> tag,
      T value,
      long weight) {
    keyCache.put(
        namespace, WindmillStateTagUtil.instance().encodeKey(namespace, tag), value, weight);
  }

  WindmillStateCache cache;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    cache = WindmillStateCache.builder().setSizeMb(400).build();
    assertEquals(0, cache.getWeight());
  }

  @Test
  public void conflictingUserAndSystemTags() {
    WindmillStateCache.ForKeyAndFamily keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 1L).forFamily(STATE_FAMILY);
    StateTag<ValueState<String>> userTag = StateTags.value("tag1", StringUtf8Coder.of());
    StateTag<ValueState<String>> systemTag = StateTags.makeSystemTagInternal(userTag);
    assertEquals(Optional.empty(), getFromCache(keyCache, StateNamespaces.global(), userTag));
    assertEquals(Optional.empty(), getFromCache(keyCache, StateNamespaces.global(), systemTag));
    Supplier<Closeable> closeableSupplier = () -> mock(Closeable.class);
    WindmillValue<String> userValue =
        new WindmillValue<>(
            StateNamespaces.global(),
            WindmillStateTagUtil.instance().encodeKey(StateNamespaces.global(), userTag),
            STATE_FAMILY,
            StringUtf8Coder.of(),
            false);
    WindmillValue<String> systemValue =
        new WindmillValue<>(
            StateNamespaces.global(),
            WindmillStateTagUtil.instance().encodeKey(StateNamespaces.global(), systemTag),
            STATE_FAMILY,
            StringUtf8Coder.of(),
            false);
    userValue.initializeForWorkItem(null, closeableSupplier);
    systemValue.initializeForWorkItem(null, closeableSupplier);

    userValue.write("userValue");
    systemValue.write("systemValue");
    putInCache(keyCache, StateNamespaces.global(), userTag, userValue, 1);
    putInCache(keyCache, StateNamespaces.global(), systemTag, systemValue, 1);

    assertEquals(
        Optional.of("userValue"),
        getFromCache(keyCache, StateNamespaces.global(), userTag).map(ValueState::read));
    assertEquals(
        Optional.of("systemValue"),
        getFromCache(keyCache, StateNamespaces.global(), systemTag).map(ValueState::read));
  }

  @Test
  public void testBasic() throws Exception {
    WindmillStateCache.ForKeyAndFamily keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 1L).forFamily(STATE_FAMILY);
    assertEquals(
        Optional.empty(),
        getFromCache(keyCache, StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(
        Optional.empty(), getFromCache(keyCache, windowNamespace(0), new TestStateTag("tag2")));
    assertEquals(
        Optional.empty(), getFromCache(keyCache, triggerNamespace(0, 0), new TestStateTag("tag3")));
    assertEquals(
        Optional.empty(), getFromCache(keyCache, triggerNamespace(0, 0), new TestStateTag("tag2")));
    assertEquals(0, cache.getWeight());

    putInCache(
        keyCache, StateNamespaces.global(), new TestStateTag("tag1"), new TestState("g1"), 2);
    putInCache(keyCache, windowNamespace(0), new TestStateTag("tag2"), new TestState("w2"), 2);

    assertEquals(0, cache.getWeight());
    keyCache.persist();
    assertEquals(414, cache.getWeight());

    putInCache(keyCache, triggerNamespace(0, 0), new TestStateTag("tag3"), new TestState("t3"), 2);
    putInCache(keyCache, triggerNamespace(0, 0), new TestStateTag("tag2"), new TestState("t2"), 2);

    // Observes updated weight in entries, though cache will not know about it.
    assertEquals(482, cache.getWeight());
    keyCache.persist();
    assertEquals(482, cache.getWeight());

    keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 2L).forFamily(STATE_FAMILY);
    assertEquals(
        Optional.of(new TestState("g1")),
        getFromCache(keyCache, StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(
        Optional.of(new TestState("w2")),
        getFromCache(keyCache, windowNamespace(0), new TestStateTag("tag2")));
    assertEquals(
        Optional.of(new TestState("t3")),
        getFromCache(keyCache, triggerNamespace(0, 0), new TestStateTag("tag3")));
    assertEquals(
        Optional.of(new TestState("t2")),
        getFromCache(keyCache, triggerNamespace(0, 0), new TestStateTag("tag2")));
  }

  /** Verifies that max weight is set */
  @Test
  public void testMaxWeight() throws Exception {
    assertEquals(400 * MEGABYTES, cache.getMaxWeight());
  }

  /** Verifies that values are cached in the appropriate namespaces. */
  @Test
  public void testInvalidation() throws Exception {
    WindmillStateCache.ForKeyAndFamily keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 1L).forFamily(STATE_FAMILY);
    assertEquals(
        Optional.empty(),
        getFromCache(keyCache, StateNamespaces.global(), new TestStateTag("tag1")));
    putInCache(
        keyCache, StateNamespaces.global(), new TestStateTag("tag1"), new TestState("g1"), 2);
    keyCache.persist();

    keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 2L).forFamily(STATE_FAMILY);
    assertEquals(207, cache.getWeight());
    assertEquals(
        Optional.of(new TestState("g1")),
        getFromCache(keyCache, StateNamespaces.global(), new TestStateTag("tag1")));

    keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 1L, 3L).forFamily(STATE_FAMILY);
    assertEquals(
        Optional.empty(),
        getFromCache(keyCache, StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(207, cache.getWeight());
  }

  /** Verifies that the cache is invalidated when the cache token changes. */
  @Test
  public void testEviction() throws Exception {
    WindmillStateCache.ForKeyAndFamily keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 1L).forFamily(STATE_FAMILY);
    putInCache(keyCache, windowNamespace(0), new TestStateTag("tag2"), new TestState("w2"), 2);
    putInCache(
        keyCache,
        triggerNamespace(0, 0),
        new TestStateTag("tag3"),
        new TestState("t3"),
        2000000000);
    keyCache.persist();
    assertEquals(0, cache.getWeight());

    // Eviction is atomic across the whole window.
    keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 2L).forFamily(STATE_FAMILY);
    assertEquals(
        Optional.empty(), getFromCache(keyCache, windowNamespace(0), new TestStateTag("tag2")));
    assertEquals(
        Optional.empty(), getFromCache(keyCache, triggerNamespace(0, 0), new TestStateTag("tag3")));
  }

  /** Verifies that the cache does not vend for stale work tokens. */
  @Test
  public void testStaleWorkItem() throws Exception {
    TestStateTag tag = new TestStateTag("tag2");

    WindmillStateCache.ForKeyAndFamily keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 2L).forFamily(STATE_FAMILY);
    putInCache(keyCache, windowNamespace(0), tag, new TestState("w2"), 2);

    // Same cache.
    assertEquals(Optional.of(new TestState("w2")), getFromCache(keyCache, windowNamespace(0), tag));
    assertEquals(0, cache.getWeight());
    keyCache.persist();
    assertEquals(207, cache.getWeight());
    assertEquals(Optional.of(new TestState("w2")), getFromCache(keyCache, windowNamespace(0), tag));

    // Previous work token.
    keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 1L).forFamily(STATE_FAMILY);
    assertEquals(Optional.empty(), getFromCache(keyCache, windowNamespace(0), tag));

    // Retry of work token that inserted.
    keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 2L).forFamily(STATE_FAMILY);
    assertEquals(Optional.empty(), getFromCache(keyCache, windowNamespace(0), tag));

    keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 10L).forFamily(STATE_FAMILY);
    assertEquals(Optional.empty(), getFromCache(keyCache, windowNamespace(0), tag));
    putInCache(keyCache, windowNamespace(0), tag, new TestState("w3"), 2);

    // Ensure that second put updated work token.
    keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 5L).forFamily(STATE_FAMILY);
    assertEquals(Optional.empty(), getFromCache(keyCache, windowNamespace(0), tag));

    keyCache =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 15L).forFamily(STATE_FAMILY);
    assertEquals(Optional.empty(), getFromCache(keyCache, windowNamespace(0), tag));
  }

  /** Verifies that caches are kept independently per-key. */
  @Test
  public void testMultipleKeys() throws Exception {
    TestStateTag tag = new TestStateTag("tag1");

    WindmillStateCache.ForKeyAndFamily keyCache1 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key1", SHARDING_KEY), 0L, 0L)
            .forFamily(STATE_FAMILY);
    WindmillStateCache.ForKeyAndFamily keyCache2 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key2", SHARDING_KEY), 0L, 10L)
            .forFamily(STATE_FAMILY);
    WindmillStateCache.ForKeyAndFamily keyCache3 =
        cache
            .forComputation("comp2")
            .forKey(computationKey("comp2", "key1", SHARDING_KEY), 0L, 0L)
            .forFamily(STATE_FAMILY);

    TestState state1 = new TestState("g1");
    putInCache(keyCache1, StateNamespaces.global(), tag, state1, 2);
    assertEquals(Optional.of(state1), getFromCache(keyCache1, StateNamespaces.global(), tag));
    keyCache1.persist();

    keyCache1 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key1", SHARDING_KEY), 0L, 1L)
            .forFamily(STATE_FAMILY);
    assertEquals(Optional.of(state1), getFromCache(keyCache1, StateNamespaces.global(), tag));
    assertEquals(Optional.empty(), getFromCache(keyCache2, StateNamespaces.global(), tag));
    assertEquals(Optional.empty(), getFromCache(keyCache3, StateNamespaces.global(), tag));

    TestState state2 = new TestState("g2");
    putInCache(keyCache2, StateNamespaces.global(), tag, state2, 2);
    keyCache2.persist();
    assertEquals(Optional.of(state2), getFromCache(keyCache2, StateNamespaces.global(), tag));
    keyCache2 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key2", SHARDING_KEY), 0L, 20L)
            .forFamily(STATE_FAMILY);
    assertEquals(Optional.of(state2), getFromCache(keyCache2, StateNamespaces.global(), tag));
    assertEquals(Optional.of(state1), getFromCache(keyCache1, StateNamespaces.global(), tag));
    assertEquals(Optional.empty(), getFromCache(keyCache3, StateNamespaces.global(), tag));
  }

  /** Verifies that caches are kept independently per shard of key. */
  @Test
  public void testMultipleShardsOfKey() throws Exception {
    TestStateTag tag = new TestStateTag("tag1");

    WindmillStateCache.ForKeyAndFamily key1CacheShard1 =
        cache
            .forComputation(COMPUTATION)
            .forKey(computationKey(COMPUTATION, "key1", 1), 0L, 0L)
            .forFamily(STATE_FAMILY);
    WindmillStateCache.ForKeyAndFamily key1CacheShard2 =
        cache
            .forComputation(COMPUTATION)
            .forKey(computationKey(COMPUTATION, "key1", 2), 0L, 0L)
            .forFamily(STATE_FAMILY);
    WindmillStateCache.ForKeyAndFamily key2CacheShard1 =
        cache
            .forComputation(COMPUTATION)
            .forKey(computationKey(COMPUTATION, "key2", 1), 0L, 0L)
            .forFamily(STATE_FAMILY);

    TestState state1 = new TestState("g1");
    putInCache(key1CacheShard1, StateNamespaces.global(), tag, state1, 2);
    key1CacheShard1.persist();
    assertEquals(Optional.of(state1), getFromCache(key1CacheShard1, StateNamespaces.global(), tag));
    key1CacheShard1 =
        cache
            .forComputation(COMPUTATION)
            .forKey(computationKey(COMPUTATION, "key1", 1), 0L, 1L)
            .forFamily(STATE_FAMILY);
    assertEquals(Optional.of(state1), getFromCache(key1CacheShard1, StateNamespaces.global(), tag));
    assertEquals(Optional.empty(), getFromCache(key1CacheShard2, StateNamespaces.global(), tag));
    assertEquals(Optional.empty(), getFromCache(key2CacheShard1, StateNamespaces.global(), tag));

    TestState state2 = new TestState("g2");
    putInCache(key1CacheShard2, StateNamespaces.global(), tag, state2, 2);
    assertEquals(Optional.of(state2), getFromCache(key1CacheShard2, StateNamespaces.global(), tag));
    key1CacheShard2.persist();
    key1CacheShard2 =
        cache
            .forComputation(COMPUTATION)
            .forKey(computationKey(COMPUTATION, "key1", 2), 0L, 20L)
            .forFamily(STATE_FAMILY);
    assertEquals(Optional.of(state2), getFromCache(key1CacheShard2, StateNamespaces.global(), tag));
    assertEquals(Optional.of(state1), getFromCache(key1CacheShard1, StateNamespaces.global(), tag));
    assertEquals(Optional.empty(), getFromCache(key2CacheShard1, StateNamespaces.global(), tag));
  }

  /** Verifies that caches are kept independently per-family. */
  @Test
  public void testMultipleFamilies() throws Exception {
    TestStateTag tag = new TestStateTag("tag1");

    WindmillStateCache.ForKey keyCache =
        cache.forComputation("comp1").forKey(computationKey("comp1", "key1", SHARDING_KEY), 0L, 0L);
    WindmillStateCache.ForKeyAndFamily family1 = keyCache.forFamily("family1");
    WindmillStateCache.ForKeyAndFamily family2 = keyCache.forFamily("family2");

    TestState state1 = new TestState("g1");
    putInCache(family1, StateNamespaces.global(), tag, state1, 2);
    assertEquals(Optional.of(state1), getFromCache(family1, StateNamespaces.global(), tag));
    family1.persist();

    TestState state2 = new TestState("g2");
    putInCache(family2, StateNamespaces.global(), tag, state2, 2);
    family2.persist();
    assertEquals(Optional.of(state2), getFromCache(family2, StateNamespaces.global(), tag));

    keyCache =
        cache.forComputation("comp1").forKey(computationKey("comp1", "key1", SHARDING_KEY), 0L, 1L);
    family1 = keyCache.forFamily("family1");
    family2 = keyCache.forFamily("family2");
    WindmillStateCache.ForKeyAndFamily family3 = keyCache.forFamily("family3");
    assertEquals(Optional.of(state1), getFromCache(family1, StateNamespaces.global(), tag));
    assertEquals(Optional.of(state2), getFromCache(family2, StateNamespaces.global(), tag));
    assertEquals(Optional.empty(), getFromCache(family3, StateNamespaces.global(), tag));
  }

  /** Verifies explicit invalidation does indeed invalidate the correct entries. */
  @Test
  public void testExplicitInvalidation() throws Exception {
    WindmillStateCache.ForKeyAndFamily keyCache1 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key1", 1), 0L, 0L)
            .forFamily(STATE_FAMILY);
    WindmillStateCache.ForKeyAndFamily keyCache2 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key2", SHARDING_KEY), 0L, 0L)
            .forFamily(STATE_FAMILY);
    WindmillStateCache.ForKeyAndFamily keyCache3 =
        cache
            .forComputation("comp2")
            .forKey(computationKey("comp2", "key1", SHARDING_KEY), 0L, 0L)
            .forFamily(STATE_FAMILY);
    WindmillStateCache.ForKeyAndFamily keyCache4 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key1", 2), 0L, 0L)
            .forFamily(STATE_FAMILY);

    putInCache(
        keyCache1, StateNamespaces.global(), new TestStateTag("tag1"), new TestState("g1"), 1);
    keyCache1.persist();
    putInCache(
        keyCache2, StateNamespaces.global(), new TestStateTag("tag2"), new TestState("g2"), 2);
    keyCache2.persist();
    putInCache(
        keyCache3, StateNamespaces.global(), new TestStateTag("tag3"), new TestState("g3"), 3);
    keyCache3.persist();
    putInCache(
        keyCache4, StateNamespaces.global(), new TestStateTag("tag4"), new TestState("g4"), 4);
    keyCache4.persist();
    keyCache1 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key1", 1), 0L, 1L)
            .forFamily(STATE_FAMILY);
    keyCache2 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key2", SHARDING_KEY), 0L, 1L)
            .forFamily(STATE_FAMILY);
    keyCache3 =
        cache
            .forComputation("comp2")
            .forKey(computationKey("comp2", "key1", SHARDING_KEY), 0L, 1L)
            .forFamily(STATE_FAMILY);
    keyCache4 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key1", 2), 0L, 1L)
            .forFamily(STATE_FAMILY);
    assertEquals(
        Optional.of(new TestState("g1")),
        getFromCache(keyCache1, StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(
        Optional.of(new TestState("g2")),
        getFromCache(keyCache2, StateNamespaces.global(), new TestStateTag("tag2")));
    assertEquals(
        Optional.of(new TestState("g3")),
        getFromCache(keyCache3, StateNamespaces.global(), new TestStateTag("tag3")));
    assertEquals(
        Optional.of(new TestState("g4")),
        getFromCache(keyCache4, StateNamespaces.global(), new TestStateTag("tag4")));

    // Invalidation of key 1 shard 1 does not affect another shard of key 1 or other keys.
    cache.forComputation("comp1").invalidate(ByteString.copyFromUtf8("key1"), 1);
    keyCache1 =
        cache
            .forComputation("comp1")
            .forKey(computationKey("comp1", "key1", 1), 0L, 2L)
            .forFamily(STATE_FAMILY);

    assertEquals(
        Optional.empty(),
        getFromCache(keyCache1, StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(
        Optional.of(new TestState("g2")),
        getFromCache(keyCache2, StateNamespaces.global(), new TestStateTag("tag2")));
    assertEquals(
        Optional.of(new TestState("g3")),
        getFromCache(keyCache3, StateNamespaces.global(), new TestStateTag("tag3")));
    assertEquals(
        Optional.of(new TestState("g4")),
        getFromCache(keyCache4, StateNamespaces.global(), new TestStateTag("tag4")));

    // Invalidation of an non-existing key affects nothing.
    cache.forComputation("comp1").invalidate(ByteString.copyFromUtf8("key1"), 3);

    assertEquals(
        Optional.of(new TestState("g2")),
        getFromCache(keyCache2, StateNamespaces.global(), new TestStateTag("tag2")));
    assertEquals(
        Optional.of(new TestState("g3")),
        getFromCache(keyCache3, StateNamespaces.global(), new TestStateTag("tag3")));
    assertEquals(
        Optional.of(new TestState("g4")),
        getFromCache(keyCache4, StateNamespaces.global(), new TestStateTag("tag4")));
  }

  private static class TestStateTagWithBadEquality extends TestStateTag {

    public TestStateTagWithBadEquality(String id) {
      super(id);
    }

    @Override
    public boolean equals(@Nullable Object other) {
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
    WindmillStateCache.ForKeyAndFamily keyCache1 =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 0L).forFamily(STATE_FAMILY);

    StateTag<TestState> tag = new TestStateTagWithBadEquality("tag1");
    putInCache(keyCache1, StateNamespaces.global(), tag, new TestState("g1"), 1);
    keyCache1.persist();

    keyCache1 =
        cache.forComputation(COMPUTATION).forKey(COMPUTATION_KEY, 0L, 1L).forFamily(STATE_FAMILY);
    assertEquals(
        Optional.of(new TestState("g1")), getFromCache(keyCache1, StateNamespaces.global(), tag));
    assertEquals(
        Optional.of(new TestState("g1")),
        getFromCache(keyCache1, StateNamespaces.global(), new TestStateTagWithBadEquality("tag1")));
  }
}
