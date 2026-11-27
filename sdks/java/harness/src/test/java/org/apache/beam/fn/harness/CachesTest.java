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
package org.apache.beam.fn.harness;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.Cache.Shrinkable;
import org.apache.beam.fn.harness.Caches.ClearableCache;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Caches}. */
@RunWith(JUnit4.class)
public class CachesTest {
  @Test
  public void testNoopCache() throws Exception {
    Cache<String, String> cache = Caches.noop();
    cache.put("key", "value");
    assertNull(cache.peek("key"));
    assertEquals("value", cache.computeIfAbsent("key", (unused) -> "value"));
    assertNull(cache.peek("key"));
  }

  @Test
  public void testShrinkableIsShrunk() throws Exception {
    WeightedValue<String> shrinkableKey = WeightedValue.of("shrinkable", MB);
    Shrinkable<Object> shrinkable =
        new Shrinkable<Object>() {

          @Override
          public Object shrink() {
            return WeightedValue.of("wasShrunk", 1);
          }
        };

    Cache<Object, Object> cache = Caches.forMaximumBytes(3 * MB - 1);
    cache.put(shrinkableKey, WeightedValue.of(shrinkable, MB));
    // Check that we didn't evict it yet
    assertSame(shrinkable, cache.peek(shrinkableKey));

    // The next insertion should cause the value to be "shrunk"
    cache.put(WeightedValue.of("other", 1), WeightedValue.of("value", MB));
    assertEquals("wasShrunk", cache.peek(shrinkableKey));
  }

  @Test
  public void testEternalCache() throws Exception {
    testCache(Caches.eternal());
  }

  @Test
  public void testDefaultCache() throws Exception {
    testCache(Caches.fromOptions(PipelineOptionsFactory.create()));
  }

  @Test
  public void testSubCache() throws Exception {
    testCache(Caches.subCache(Caches.eternal(), "prefix"));
    testCache(Caches.subCache(Caches.eternal(), "prefixA", "prefixB"));
    testCache(Caches.subCache(Caches.eternal(), "prefixA", "prefixB", "prefixC"));
  }

  @Test
  public void testSiblingSubCaches() throws Exception {
    Cache<String, String> parent = Caches.eternal();
    Cache<String, String> cacheA = Caches.subCache(parent, "prefixA");
    Cache<String, String> cacheACopy = Caches.subCache(parent, "prefixA");
    Cache<String, String> cacheB = Caches.subCache(parent, "prefixB");

    // Test values inserted into caches with the same prefix can be found in other instances with
    // the same prefix but not in the cache with a different prefix
    cacheA.put("keyA", "valueA");
    assertEquals("valueA", cacheA.peek("keyA"));
    assertEquals("valueA", cacheACopy.peek("keyA"));
    assertNull(cacheB.peek("keyA"));
  }

  @Test
  public void testNestedSubCaches() throws Exception {
    Cache<String, String> parent = Caches.eternal();
    Cache<String, String> child = Caches.subCache(parent, "child");
    Cache<String, String> childOfChild = Caches.subCache(child, "childOfChild");

    // Test nested put
    child.put("keyA", "childA");
    childOfChild.put("keyA", "childOfChildA");
    assertEquals("childA", child.peek("keyA"));
    assertEquals("childOfChildA", childOfChild.peek("keyA"));

    // Test nested computeIfAbsent
    child.computeIfAbsent("keyB", (unused) -> "childB");
    childOfChild.computeIfAbsent("keyB", (unused) -> "childOfChildB");
    assertEquals("childB", child.peek("keyB"));
    assertEquals("childOfChildB", childOfChild.peek("keyB"));

    // Test removal doesn't impact children
    child.remove("keyA");
    assertNull(child.peek("keyA"));
    assertEquals("childOfChildA", childOfChild.peek("keyA"));

    // Test removal doesn't impact parent
    childOfChild.remove("keyB");
    assertEquals("childB", child.peek("keyB"));
    assertNull(childOfChild.peek("keyB"));
  }

  @Test
  public void testClearableCache() {
    ClearableCache<String, String> cache = new ClearableCache<>(Caches.eternal());
    testCache(cache);
    testCache(Caches.subCache(cache, "clearableChild"));
  }

  @Test
  public void testClearableCacheClearing() {
    Cache<String, String> parent = Caches.eternal();
    ClearableCache<String, String> cache = new ClearableCache<>(parent);

    parent.put("untracked", "untrackedValue");
    parent.put("tracked", "parentValue");
    cache.put("tracked", "parentValueNowTracked");
    cache.computeIfAbsent("tracked2", (unused) -> "trackedValue2");
    // Cache hit via computeIfAbsent on a pre-existing parent key should NOT track it.
    assertEquals("untrackedValue", cache.computeIfAbsent("untracked", (unused) -> "shouldNotRun"));
    cache.clear();
    assertNull(parent.peek("tracked"));
    assertNull(parent.peek("tracked2"));
    assertEquals("untrackedValue", parent.peek("untracked"));
    assertEquals(0, cache.trackedKeyCount());
    // Repeated clear on an empty cache is a safe no-op.
    cache.clear();

    // Clear-then-reuse cycle tracks newly inserted keys properly.
    cache.put("reused", "v1");
    assertEquals("v1", parent.peek("reused"));
    cache.clear();
    assertNull(parent.peek("reused"));
    assertEquals(0, cache.trackedKeyCount());
  }

  @Test
  public void testClearableCacheClearsNestedSubCaches() {
    Cache<String, String> parent = Caches.eternal();
    ClearableCache<String, String> clearable = new ClearableCache<>(parent);
    Cache<String, String> child = Caches.subCache(clearable, "child");
    Cache<String, String> grandChild = Caches.subCache(child, "grandChild");
    Cache<String, String> multiPrefixChild = Caches.subCache(clearable, "a", "b", "c");

    // Sub-caches of a ClearableCache must not expose ClearableCache#clear() directly.
    assertFalse(child instanceof ClearableCache);
    assertFalse(grandChild instanceof ClearableCache);
    assertFalse(multiPrefixChild instanceof ClearableCache);

    parent.put("parentKey", "parentVal");
    clearable.put("rootKey", "rootVal");
    child.put("childKey", "childVal");
    child.put("removedKey", "toRemove");
    grandChild.computeIfAbsent("gcKey", (unused) -> "gcVal");
    multiPrefixChild.put("mpKey", "mpVal");

    assertEquals(5, clearable.trackedKeyCount());
    child.remove("removedKey");
    assertNull(child.peek("removedKey"));
    assertEquals(4, clearable.trackedKeyCount());

    assertEquals("childVal", child.peek("childKey"));
    assertEquals("gcVal", grandChild.peek("gcKey"));
    assertEquals("mpVal", multiPrefixChild.peek("mpKey"));

    clearable.clear();

    assertNull(clearable.peek("rootKey"));
    assertNull(child.peek("childKey"));
    assertNull(grandChild.peek("gcKey"));
    assertNull(multiPrefixChild.peek("mpKey"));
    assertEquals("parentVal", parent.peek("parentKey"));
    assertEquals(0, clearable.trackedKeyCount());
  }

  @Test
  public void testNestedClearableCachePropagation() {
    Cache<String, String> parent = Caches.eternal();
    ClearableCache<String, String> outer = new ClearableCache<>(parent);
    ClearableCache<String, String> inner = new ClearableCache<>(Caches.subCache(outer, "inner"));

    outer.put("outerKey", "outerVal");
    inner.put("innerKey1", "innerVal1");
    assertEquals(2, outer.trackedKeyCount());
    assertEquals(1, inner.trackedKeyCount());

    // Clearing inner must only evict inner entries and leave outer untouched.
    inner.clear();
    assertNull(inner.peek("innerKey1"));
    assertEquals("outerVal", outer.peek("outerKey"));
    assertEquals(0, inner.trackedKeyCount());
    assertEquals(1, outer.trackedKeyCount());

    // Clearing outer must also evict entries written through inner.
    inner.put("innerKey2", "innerVal2");
    outer.clear();
    assertNull(outer.peek("outerKey"));
    assertNull(inner.peek("innerKey2"));
    assertEquals(0, outer.trackedKeyCount());
    assertEquals(0, inner.trackedKeyCount());
  }

  @Test
  public void testClearableCacheEvictionReleasesTrackedKeys() {
    Cache<Object, Object> parent = Caches.forMaximumBytes(4096L);
    ClearableCache<Object, Object> clearable = new ClearableCache<>(parent);
    Cache<Object, Object> child = Caches.subCache(clearable, "child");

    byte[] payload = new byte[1024];
    for (int i = 0; i < 200; i++) {
      child.put("k-" + i, payload);
    }
    // Evicted keys must be removed from the tracking map rather than retained indefinitely.
    assertTrue(
        "Expected bounded tracked keys, got " + clearable.trackedKeyCount(),
        clearable.trackedKeyCount() < 16);
    clearable.clear();
    assertEquals(0, clearable.trackedKeyCount());
  }

  @Test
  public void testClearableCacheShrinkableSurvivesUntilCleared() {
    WeightedValue<String> shrinkableKey = WeightedValue.of("shrinkable", MB);
    Shrinkable<Object> shrinkable = () -> WeightedValue.of("wasShrunk", 1);

    Cache<Object, Object> parent = Caches.forMaximumBytes(3 * MB - 1);
    ClearableCache<Object, Object> clearable = new ClearableCache<>(parent);
    clearable.put(shrinkableKey, WeightedValue.of(shrinkable, MB));
    clearable.put(WeightedValue.of("other", 1), WeightedValue.of("value", MB));
    assertEquals("wasShrunk", clearable.peek(shrinkableKey));

    clearable.clear();
    assertNull(parent.peek(shrinkableKey));
    assertEquals(0, clearable.trackedKeyCount());
  }

  @Test
  public void testClearableCacheNoopAndTinyCache() {
    ClearableCache<String, String> noopClearable = new ClearableCache<>(Caches.noop());
    noopClearable.put("k", "v");
    assertNull(noopClearable.peek("k"));
    assertEquals(0, noopClearable.trackedKeyCount());
    noopClearable.clear();

    // maximumBytes in [1, 63] scales down to Guava maximumWeight(0).
    ClearableCache<String, String> tinyClearable =
        new ClearableCache<>(Caches.forMaximumBytes(32L));
    tinyClearable.put("k", "v");
    assertNull(tinyClearable.peek("k"));
    assertEquals(0, tinyClearable.trackedKeyCount());
    tinyClearable.clear();
  }

  @Test
  public void testClearableCacheValidationAndFailedInserts() {
    assertThrows(IllegalArgumentException.class, () -> new ClearableCache<>(null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ClearableCache<>(
                new Cache<String, String>() {
                  @Override
                  public String peek(String key) {
                    return null;
                  }

                  @Override
                  public String computeIfAbsent(
                      String key, Function<String, String> loadingFunction) {
                    return null;
                  }

                  @Override
                  public void put(String key, String value) {}

                  @Override
                  public void remove(String key) {}

                  @Override
                  public String describeStats() {
                    return "";
                  }
                }));

    Cache<String, String> parent = Caches.eternal();
    parent.put("keep", "parentVal");
    ClearableCache<String, String> clearable = new ClearableCache<>(parent);

    // Failed put(k, null) must NOT add "keep" to tracked keys.
    assertThrows(NullPointerException.class, () -> clearable.put("keep", null));
    // Throwing / null-returning computeIfAbsent must NOT leak keys into tracked keys.
    assertThrows(
        RuntimeException.class,
        () ->
            clearable.computeIfAbsent(
                "failKey",
                unused -> {
                  throw new IllegalStateException("boom");
                }));
    assertThrows(
        RuntimeException.class, () -> clearable.computeIfAbsent("nullKey", unused -> null));
    assertEquals(0, clearable.trackedKeyCount());

    clearable.clear();
    assertEquals("parentVal", parent.peek("keep"));

    // Entry overwritten directly in parent is untracked from clearable and preserved on clear().
    clearable.put("owned", "v1");
    parent.put("owned", "v2");
    assertEquals(0, clearable.trackedKeyCount());
    clearable.clear();
    assertEquals("v2", parent.peek("owned"));
  }

  @Test
  public void testClearableCacheConcurrentPutAndClear() throws Exception {
    Cache<String, String> parent = Caches.eternal();
    ClearableCache<String, String> clearable = new ClearableCache<>(parent);
    Cache<String, String> child = Caches.subCache(clearable, "ns");
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      CountDownLatch start = new CountDownLatch(1);
      Future<?>[] futures = new Future<?>[4];
      for (int t = 0; t < 4; t++) {
        final int tid = t;
        futures[t] =
            executor.submit(
                () -> {
                  try {
                    start.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                  for (int i = 0; i < 200; i++) {
                    String k = "k-" + (i & 31);
                    if (tid == 3 && (i & 7) == 0) {
                      clearable.clear();
                    } else {
                      child.put(k, "v-" + tid + "-" + i);
                    }
                  }
                });
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
    // After quiescing, a single final clear() must remove every entry ever written by workers.
    clearable.clear();
    for (int i = 0; i < 32; i++) {
      assertNull("Orphaned entry for key k-" + i, child.peek("k-" + i));
    }
    assertEquals(0, clearable.trackedKeyCount());
  }

  @Test
  public void testCompositeKeyHashCodeEqualsNullAndLazyWeight() {
    Caches.CompositeKeyPrefix prefix = Caches.CompositeKeyPrefix.ROOT.subKey("ns");
    Caches.CompositeKey keyA = prefix.valueKey("alpha");
    Caches.CompositeKey keyB = prefix.valueKey("beta");
    assertNotEquals(keyA.hashCode(), keyB.hashCode());
    assertEquals(keyA, prefix.valueKey("alpha"));
    assertEquals(keyA.hashCode(), prefix.valueKey("alpha").hashCode());

    // Independent CompositeKeyPrefix instances with equal namespace arrays must match.
    Caches.CompositeKeyPrefix equalPrefix = Caches.CompositeKeyPrefix.ROOT.subKey("a", "b");
    Caches.CompositeKeyPrefix equalPrefix2 = Caches.CompositeKeyPrefix.ROOT.subKey("a").subKey("b");
    Caches.CompositeKey fromFirst = equalPrefix.valueKey("k");
    Caches.CompositeKey fromSecond = equalPrefix2.valueKey("k");
    assertEquals(fromFirst, fromSecond);
    assertEquals(fromFirst.hashCode(), fromSecond.hashCode());

    // Null keys are supported.
    Caches.CompositeKey nullKey1 = prefix.valueKey(null);
    Caches.CompositeKey nullKey2 = prefix.valueKey(null);
    assertEquals(nullKey1, nullKey2);
    assertEquals(nullKey1.hashCode(), nullKey2.hashCode());
    assertNotEquals(nullKey1, keyA);
    assertTrue(nullKey1.getWeight() > 0);

    // Prefix and key weights must be evaluated lazily (not on subCache / peek / remove).
    AtomicInteger prefixWeighCalls = new AtomicInteger();
    AtomicInteger keyWeighCalls = new AtomicInteger();
    Weighted countingPrefix =
        () -> {
          prefixWeighCalls.incrementAndGet();
          return 64L;
        };
    Weighted countingKey =
        () -> {
          keyWeighCalls.incrementAndGet();
          return 32L;
        };
    Cache<Object, Object> root = Caches.eternal();
    Cache<Object, Object> sub = Caches.subCache(root, countingPrefix);
    assertEquals(0, prefixWeighCalls.get());
    assertNull(sub.peek(countingKey));
    assertEquals(0, prefixWeighCalls.get());
    assertEquals(0, keyWeighCalls.get());
    sub.put(countingKey, "val");
    assertEquals(1, prefixWeighCalls.get());
    assertEquals(1, keyWeighCalls.get());
    assertEquals("val", sub.peek(countingKey));
    assertEquals(1, prefixWeighCalls.get());
    assertEquals(1, keyWeighCalls.get());

    // Saturated arithmetic clamps Long.MAX_VALUE without overflowing negative.
    Caches.CompositeKey huge =
        Caches.CompositeKeyPrefix.ROOT
            .subKey((Weighted) () -> Long.MAX_VALUE)
            .valueKey((Weighted) () -> Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, huge.getWeight());
  }

  private enum WeightedEnum implements Weighted {
    BIG;

    @Override
    public long getWeight() {
      return 1234L;
    }
  }

  @Test
  public void testWeighFastPaths() throws Exception {
    assertEquals(Caches.REFERENCE_SIZE, Caches.weigh(null));
    assertEquals(16L, Caches.weigh(new byte[0]));
    assertEquals(16L + 8L, Caches.weigh(new byte[5]));
    assertEquals(16L + 16L, Caches.weigh(new int[4]));
    assertEquals(16L + 24L, Caches.weigh(new long[3]));
    assertEquals(40L, Caches.weigh(""));
    assertEquals(40L + 8L, Caches.weigh("beam"));

    // Strings use a conservative O(1) 2 bytes/char estimate regardless of character encoding.
    assertEquals(40L + 200L, Caches.weigh("a".repeat(100)));
    assertEquals(40L + 200L, Caches.weigh("\u4e2d".repeat(100)));

    assertEquals(40L, Caches.weigh(ByteString.EMPTY));
    assertEquals(40L + 8L, Caches.weigh(ByteString.copyFromUtf8("abc")));

    // Sliced BoundedByteString (from substring / ByteStringOutputStream.toByteString()) must NOT
    // use the LiteralByteString fast path that would under-report backing array capacity.
    ByteStringOutputStream bsos = new ByteStringOutputStream();
    bsos.write(new byte[] {1, 2, 3, 4});
    ByteString bounded = bsos.toByteString();
    assertNotEquals(ByteString.EMPTY.getClass(), bounded.getClass());
    assertTrue(Caches.weigh(bounded) > 0);
    // toByteStringAndReset trims small payloads back into an exact LiteralByteString.
    ByteString trimmed = bsos.toByteStringAndReset();
    assertEquals(ByteString.EMPTY.getClass(), trimmed.getClass());

    assertEquals(24L, Caches.weigh(123L));
    assertEquals(24L, Caches.weigh(1.5d));
    assertEquals(16L, Caches.weigh(42));
    assertEquals(16L, Caches.weigh(1.5f));
    assertEquals(16L, Caches.weigh(Boolean.TRUE));
    assertEquals(16L, Caches.weigh((byte) 1));
    assertEquals(16L, Caches.weigh((short) 2));
    assertEquals(16L, Caches.weigh('x'));
    assertEquals(Caches.REFERENCE_SIZE, Caches.weigh(TimeUnit.SECONDS));

    // Weighted takes precedence over Enum<?> and saturates at Long.MAX_VALUE.
    assertEquals(1234L + Caches.REFERENCE_SIZE + 8L, Caches.weigh(WeightedEnum.BIG));
    assertEquals(Long.MAX_VALUE, Caches.weigh((Weighted) () -> Long.MAX_VALUE));
    assertTrue(Caches.weigh(new Object()) > 0);
  }

  private void testCache(Cache<String, String> cache) {
    assertNull(cache.peek("key1"));

    // Test put
    cache.put("key1", "value1");
    assertEquals("value1", cache.peek("key1"));

    // Test compute without load
    assertEquals("value1", cache.computeIfAbsent("key1", (unused) -> "anotherValue"));
    assertEquals("value1", cache.peek("key1"));

    // Test compute with load
    assertEquals("value2", cache.computeIfAbsent("key2", (unused) -> "value2"));
    assertEquals("value2", cache.peek("key2"));
  }

  private static final long MB = 1 << 20;

  private static class ShrinkableString implements Shrinkable<ShrinkableString>, Weighted {
    private final String value;
    private final long weight;

    public ShrinkableString(String value, long weight) {
      this.value = value;
      this.weight = weight;
    }

    @Override
    public ShrinkableString shrink() {
      if (weight < 800 * MB) {
        return null;
      }
      return new ShrinkableString(value, weight / 2);
    }

    @Override
    public long getWeight() {
      return weight;
    }
  }

  @Test
  public void testDescribeStats() throws Exception {
    Cache<WeightedValue<Integer>, ShrinkableString> cache = Caches.forMaximumBytes(1000 * MB);
    for (int i = 0; i < 100; ++i) {
      cache.computeIfAbsent(
          WeightedValue.of(i, MB), (key) -> new ShrinkableString("value", 2 * MB));
      cache.peek(WeightedValue.of(i, MB));
      cache.put(WeightedValue.of(100 + i, MB), new ShrinkableString("value", 2 * MB));
    }

    assertThat(cache.describeStats(), containsString("used/max 600/1000 MB"));
    assertThat(
        // Locale sensitive.
        cache.describeStats(), anyOf(containsString("hit 50.00%"), containsString("hit 50,00%")));
    assertThat(cache.describeStats(), containsString("lookups 200"));
    assertThat(cache.describeStats(), containsString("avg load time"));
    assertThat(cache.describeStats(), containsString("loads 100"));
    assertThat(cache.describeStats(), containsString("evictions 0"));

    // Test eviction, evict all the other 200 elements that were added
    cache.put(WeightedValue.of(1000, 100 * MB), new ShrinkableString("value", 899 * MB));
    assertThat(cache.describeStats(), containsString("used/max 999/1000 MB"));
    assertThat(cache.describeStats(), containsString("evictions 200"));

    // Test shrinking, 900 -> 450 + 100 + 55 + 1 = 606
    cache.put(WeightedValue.of(1001, MB), new ShrinkableString("value", 55 * MB));
    assertThat(cache.describeStats(), containsString("used/max 605/1000 MB"));
    assertThat(cache.describeStats(), containsString("evictions 201"));

    // Test composite key namespace is weighed as well.
    // 34 + 8 + 3 = 45 more then last used/max of 605 = 650
    Caches.subCache(cache, WeightedValue.of("subCache", 34 * MB))
        .put(WeightedValue.of("subCacheKey", 8 * MB), WeightedValue.of("subCacheValue", 3 * MB));
    assertThat(cache.describeStats(), containsString("used/max 650/1000 MB"));
  }

  @Test
  public void testSubCacheVarargsArrayExpansion() {
    Cache<Object, Object> root = Caches.eternal();
    Object[] extra = new Object[] {"b", "c"};
    Cache<String, String> fromObjArray = Caches.subCache(root, "a", extra);
    Cache<String, String> fromStrArray =
        Caches.subCache(root, "a", (Object[]) new String[] {"b", "c"});
    Cache<String, String> fromLiterals = Caches.subCache(root, "a", "b", "c");
    fromObjArray.put("k", "v");
    assertEquals("v", fromLiterals.peek("k"));
    assertEquals("v", fromStrArray.peek("k"));

    // Empty varargs array must resolve identically to the 1-prefix overload.
    Cache<String, String> single = Caches.subCache(root, "single");
    Cache<String, String> singleEmptyVarargs = Caches.subCache(root, "single", new Object[0]);
    single.put("x", "y");
    assertEquals("y", singleEmptyVarargs.peek("x"));
  }

  @Test
  public void testWeighLargeStringBounded() {
    int len = 16 * 1024;
    String largeAscii = "a".repeat(len);
    String largeUtf16Tail = "a".repeat(len - 1) + "\u0100";
    assertEquals(40L + 2L * len, Caches.weigh(largeAscii));
    assertEquals(40L + 2L * len, Caches.weigh(largeUtf16Tail));
  }

  @Test
  public void testDeepSubCachePrefixWeightIterative() {
    Cache<String, String> root = Caches.eternal();
    Cache<String, String> nested = root;
    final int depth = 250;
    for (int i = 0; i < depth; i++) {
      nested = Caches.subCache(nested, "p" + i);
    }
    nested.put("k", "v");
    assertEquals("v", nested.peek("k"));
    long expectedBeforeRemove = Caches.estimatedWeightInBytes(root);
    assertTrue(expectedBeforeRemove > depth * 40L);
    nested.remove("k");
    assertEquals(0L, Caches.estimatedWeightInBytes(root));
  }

  @Test
  public void testShrinkableClearedDuringShrinkDoesNotResurrect() {
    Cache<Object, Object> parent = Caches.forMaximumBytes(10 * MB);
    ClearableCache<String, Object> clearable =
        new ClearableCache<>(Caches.subCache(parent, "bundle"));
    ClearableCache<String, Object> outer = new ClearableCache<>(Caches.subCache(parent, "outer"));
    AtomicInteger shrinkCalls = new AtomicInteger();

    Shrinkable<Object> shrinkableThatClears =
        new WeightedShrinkable(
            8 * MB,
            () -> {
              shrinkCalls.incrementAndGet();
              // Simulate a concurrent bundle end clearing the cache while shrink() runs.
              clearable.clear();
              return new WeightedShrinkable(2 * MB, () -> null);
            });

    clearable.put("k", shrinkableThatClears);
    assertEquals(1, clearable.trackedKeyCount());
    // Trigger SIZE eviction of `k` inside `clearable`.
    outer.put("big", WeightedValue.of("payload", 8 * MB));

    assertEquals(1, shrinkCalls.get());
    assertNull("Shrunk entry must not resurrect after clear()", clearable.peek("k"));
    assertEquals(0, clearable.trackedKeyCount());
    outer.clear();
    assertEquals(0, outer.trackedKeyCount());
    assertEquals(0L, Caches.estimatedWeightInBytes(parent));
  }

  private static final class WeightedShrinkable implements Shrinkable<Object>, Weighted {
    private final long weight;
    private final Supplier<Object> onShrink;

    WeightedShrinkable(long weight, Supplier<Object> onShrink) {
      this.weight = weight;
      this.onShrink = onShrink;
    }

    @Override
    public Object shrink() {
      return onShrink.get();
    }

    @Override
    public long getWeight() {
      return weight;
    }
  }

  @Test
  public void testPutThrowingKeyEqualsRollsBackWeightAndTracker() {
    Cache<Object, Object> parent = Caches.eternal();
    ClearableCache<Object, Object> cache = new ClearableCache<>(Caches.subCache(parent, "ns"));

    class BrokenKey {
      final boolean throwOnEquals;

      BrokenKey(boolean throwOnEquals) {
        this.throwOnEquals = throwOnEquals;
      }

      @Override
      public int hashCode() {
        return 42;
      }

      @Override
      public boolean equals(Object other) {
        if (throwOnEquals) {
          throw new IllegalStateException("broken equals");
        }
        return this == other;
      }
    }

    BrokenKey good = new BrokenKey(false);
    cache.put(good, "v1");
    long weightAfterGood = Caches.estimatedWeightInBytes(parent);
    assertEquals(1, cache.trackedKeyCount());

    BrokenKey bad = new BrokenKey(true);
    assertThrows(IllegalStateException.class, () -> cache.put(bad, "v2"));
    assertEquals(weightAfterGood, Caches.estimatedWeightInBytes(parent));
    assertEquals(1, cache.trackedKeyCount());

    cache.clear();
    assertEquals(0, cache.trackedKeyCount());
    assertEquals(0L, Caches.estimatedWeightInBytes(parent));
  }

  @Test
  public void testByteStringKeyReplacementZeroWeightDrift() throws Exception {
    Cache<ByteString, String> cache = Caches.eternal();
    ByteString literalKey = ByteString.copyFromUtf8("abcd");
    ByteString boundedKey;
    try (ByteStringOutputStream bsos = new ByteStringOutputStream()) {
      bsos.write(new byte[] {'a', 'b', 'c', 'd'});
      boundedKey = bsos.toByteString();
    }
    assertEquals(literalKey, boundedKey);
    assertNotEquals(literalKey.getClass(), boundedKey.getClass());

    cache.put(literalKey, "v1");
    cache.put(boundedKey, "v2");
    assertEquals("v2", cache.peek(literalKey));
    cache.remove(literalKey);
    assertEquals(0L, Caches.estimatedWeightInBytes(cache));
  }

  @Test
  public void testShrinkableDoesNotClobberFreshPostClearEntry() {
    Cache<Object, Object> parent = Caches.forMaximumBytes(10 * MB);
    ClearableCache<String, Object> clearable =
        new ClearableCache<>(Caches.subCache(parent, "bundle"));
    ClearableCache<String, Object> outer = new ClearableCache<>(Caches.subCache(parent, "outer"));

    // Simulate the race between the pre-put epoch check and putIfAbsent by intercepting
    // getWeight() on the shrunk value returned by shrink().
    Weighted postClearInterceptShrunk =
        new Weighted() {
          @Override
          public long getWeight() {
            clearable.clear();
            clearable.put("k", "fresh-after-clear");
            return 1 * MB;
          }
        };
    clearable.put("k", new WeightedShrinkable(8 * MB, () -> postClearInterceptShrunk));
    // Trigger SIZE eviction of `k`.
    outer.put("big", WeightedValue.of("payload", 8 * MB));

    assertEquals("fresh-after-clear", clearable.peek("k"));
    assertEquals(1, clearable.trackedKeyCount());
    clearable.clear();
    outer.clear();
    assertEquals(0, clearable.trackedKeyCount());
    assertEquals(0, outer.trackedKeyCount());
    assertEquals(0L, Caches.estimatedWeightInBytes(parent));
  }

  @Test
  public void testShrinkableDoesNotClobberConcurrentFreshPut() {
    Cache<Object, Object> parent = Caches.forMaximumBytes(10 * MB);
    ClearableCache<String, Object> clearable =
        new ClearableCache<>(Caches.subCache(parent, "bundle"));
    ClearableCache<String, Object> outer = new ClearableCache<>(Caches.subCache(parent, "outer"));

    Weighted concurrentPutInterceptShrunk =
        new Weighted() {
          @Override
          public long getWeight() {
            clearable.put("k", "fresh-concurrent");
            return 1 * MB;
          }
        };
    clearable.put("k", new WeightedShrinkable(8 * MB, () -> concurrentPutInterceptShrunk));
    outer.put("big", WeightedValue.of("payload", 8 * MB));

    assertEquals("fresh-concurrent", clearable.peek("k"));
    assertEquals(1, clearable.trackedKeyCount());
    clearable.clear();
    outer.clear();
    assertEquals(0, clearable.trackedKeyCount());
    assertEquals(0, outer.trackedKeyCount());
    assertEquals(0L, Caches.estimatedWeightInBytes(parent));
  }

  @Test
  public void testShrinkableClearedBetweenEpochCheckAndPutDoesNotResurrect() {
    Cache<Object, Object> parent = Caches.forMaximumBytes(10 * MB);
    ClearableCache<String, Object> clearable =
        new ClearableCache<>(Caches.subCache(parent, "bundle"));
    ClearableCache<String, Object> outer = new ClearableCache<>(Caches.subCache(parent, "outer"));

    // Simulate clear() landing strictly after the pre-put epoch check and before putIfAbsent().
    Weighted clearedMidPutShrunk =
        new Weighted() {
          @Override
          public long getWeight() {
            clearable.clear();
            return 1 * MB;
          }
        };
    clearable.put("k", new WeightedShrinkable(8 * MB, () -> clearedMidPutShrunk));
    outer.put("big", WeightedValue.of("payload", 8 * MB));

    assertNull("Dekker post-check must remove shrunk entry from prior epoch", clearable.peek("k"));
    assertEquals(0, clearable.trackedKeyCount());
    outer.clear();
    assertEquals(0, outer.trackedKeyCount());
    assertEquals(0L, Caches.estimatedWeightInBytes(parent));
  }
}
