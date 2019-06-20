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
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WindmillStateCache}. */
@RunWith(JUnit4.class)
public class WindmillStateCacheTest {
  private static final String COMPUTATION = "computation";
  private static final ByteString KEY = ByteString.copyFromUtf8("key");
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
    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, STATE_FAMILY, 0L);
    assertEquals(0, cache.getWeight());
  }

  @Test
  public void testBasic() throws Exception {
    assertNull(keyCache.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertNull(keyCache.get(windowNamespace(0), new TestStateTag("tag2")));
    assertNull(keyCache.get(triggerNamespace(0, 0), new TestStateTag("tag3")));
    assertNull(keyCache.get(triggerNamespace(0, 0), new TestStateTag("tag2")));

    keyCache.put(StateNamespaces.global(), new TestStateTag("tag1"), new TestState("g1"), 2);
    assertEquals(121, cache.getWeight());
    keyCache.put(windowNamespace(0), new TestStateTag("tag2"), new TestState("w2"), 2);
    assertEquals(242, cache.getWeight());
    keyCache.put(triggerNamespace(0, 0), new TestStateTag("tag3"), new TestState("t3"), 2);
    assertEquals(260, cache.getWeight());
    keyCache.put(triggerNamespace(0, 0), new TestStateTag("tag2"), new TestState("t2"), 2);
    assertEquals(278, cache.getWeight());

    assertEquals(
        new TestState("g1"), keyCache.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(new TestState("w2"), keyCache.get(windowNamespace(0), new TestStateTag("tag2")));
    assertEquals(
        new TestState("t3"), keyCache.get(triggerNamespace(0, 0), new TestStateTag("tag3")));
    assertEquals(
        new TestState("t2"), keyCache.get(triggerNamespace(0, 0), new TestStateTag("tag2")));
  }

  /** Verifies that values are cached in the appropriate namespaces. */
  @Test
  public void testInvalidation() throws Exception {
    assertNull(keyCache.get(StateNamespaces.global(), new TestStateTag("tag1")));
    keyCache.put(StateNamespaces.global(), new TestStateTag("tag1"), new TestState("g1"), 2);
    assertEquals(121, cache.getWeight());
    assertEquals(
        new TestState("g1"), keyCache.get(StateNamespaces.global(), new TestStateTag("tag1")));

    keyCache = cache.forComputation(COMPUTATION).forKey(KEY, STATE_FAMILY, 1L);
    assertEquals(121, cache.getWeight());
    assertNull(keyCache.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(0, cache.getWeight());
  }

  /** Verifies that the cache is invalidated when the cache token changes. */
  @Test
  public void testEviction() throws Exception {
    keyCache.put(windowNamespace(0), new TestStateTag("tag2"), new TestState("w2"), 2);
    assertEquals(121, cache.getWeight());
    keyCache.put(triggerNamespace(0, 0), new TestStateTag("tag3"), new TestState("t3"), 2000000000);
    assertEquals(0, cache.getWeight());
    // Eviction is atomic across the whole window.
    assertNull(keyCache.get(windowNamespace(0), new TestStateTag("tag2")));
    assertNull(keyCache.get(triggerNamespace(0, 0), new TestStateTag("tag3")));
  }

  /** Verifies that caches are kept independently per-key. */
  @Test
  public void testMultipleKeys() throws Exception {
    WindmillStateCache.ForKey keyCache1 =
        cache.forComputation("comp1").forKey(ByteString.copyFromUtf8("key1"), STATE_FAMILY, 0L);
    WindmillStateCache.ForKey keyCache2 =
        cache.forComputation("comp1").forKey(ByteString.copyFromUtf8("key2"), STATE_FAMILY, 0L);
    WindmillStateCache.ForKey keyCache3 =
        cache.forComputation("comp2").forKey(ByteString.copyFromUtf8("key1"), STATE_FAMILY, 0L);

    keyCache1.put(StateNamespaces.global(), new TestStateTag("tag1"), new TestState("g1"), 2);
    assertEquals(
        new TestState("g1"), keyCache1.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertNull(keyCache2.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertNull(keyCache3.get(StateNamespaces.global(), new TestStateTag("tag1")));
  }

  /** Verifies explicit invalidation does indeed invalidate the correct entries. */
  @Test
  public void testExplicitInvalidation() throws Exception {
    WindmillStateCache.ForKey keyCache1 =
        cache.forComputation("comp1").forKey(ByteString.copyFromUtf8("key1"), STATE_FAMILY, 0L);
    WindmillStateCache.ForKey keyCache2 =
        cache.forComputation("comp1").forKey(ByteString.copyFromUtf8("key2"), STATE_FAMILY, 0L);
    WindmillStateCache.ForKey keyCache3 =
        cache.forComputation("comp2").forKey(ByteString.copyFromUtf8("key1"), STATE_FAMILY, 0L);

    keyCache1.put(StateNamespaces.global(), new TestStateTag("tag1"), new TestState("g1"), 1);
    keyCache2.put(StateNamespaces.global(), new TestStateTag("tag2"), new TestState("g2"), 2);
    keyCache3.put(StateNamespaces.global(), new TestStateTag("tag3"), new TestState("g3"), 3);
    assertEquals(
        new TestState("g1"), keyCache1.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(
        new TestState("g2"), keyCache2.get(StateNamespaces.global(), new TestStateTag("tag2")));
    assertEquals(
        new TestState("g3"), keyCache3.get(StateNamespaces.global(), new TestStateTag("tag3")));

    cache.forComputation("comp1").invalidate(ByteString.copyFromUtf8("key1"));

    assertNull(keyCache1.get(StateNamespaces.global(), new TestStateTag("tag1")));
    assertEquals(
        new TestState("g2"), keyCache2.get(StateNamespaces.global(), new TestStateTag("tag2")));
    assertEquals(
        new TestState("g3"), keyCache3.get(StateNamespaces.global(), new TestStateTag("tag3")));
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
        cache.forComputation("comp1").forKey(ByteString.copyFromUtf8("key1"), STATE_FAMILY, 0L);

    StateTag<TestState> tag = new TestStateTagWithBadEquality("tag1");
    keyCache1.put(StateNamespaces.global(), tag, new TestState("g1"), 1);
    assertEquals(new TestState("g1"), keyCache1.get(StateNamespaces.global(), tag));
    assertEquals(
        new TestState("g1"),
        keyCache1.get(StateNamespaces.global(), new TestStateTagWithBadEquality("tag1")));
  }
}
