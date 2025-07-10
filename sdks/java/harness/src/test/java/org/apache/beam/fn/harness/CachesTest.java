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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.apache.beam.fn.harness.Cache.Shrinkable;
import org.apache.beam.fn.harness.Caches.ClearableCache;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.sdk.util.WeightedValue;
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

    Cache<Object, Object> cache = Caches.forMaximumBytes(2 * MB);
    cache.put(shrinkableKey, WeightedValue.of(shrinkable, MB));
    // Check that we didn't evict it yet
    assertSame(shrinkable, cache.peek(shrinkableKey));

    // The next insertion should cause the value to be "shrunk"
    cache.put(WeightedValue.of("other", 1), WeightedValue.of("value", 1));
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
    cache.clear();
    assertNull(parent.peek("tracked"));
    assertNull(parent.peek("tracked2"));
    assertEquals("untrackedValue", parent.peek("untracked"));
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
    cache.put(WeightedValue.of(1000, 100 * MB), new ShrinkableString("value", 900 * MB));
    assertThat(cache.describeStats(), containsString("used/max 1000/1000 MB"));
    assertThat(cache.describeStats(), containsString("evictions 200"));

    // Test shrinking, 900 -> 450 + 100 + 55 + 1 = 606
    cache.put(WeightedValue.of(1001, MB), new ShrinkableString("value", 55 * MB));
    assertThat(cache.describeStats(), containsString("used/max 606/1000 MB"));
    assertThat(cache.describeStats(), containsString("evictions 201"));

    // Test composite key namespace is weighed as well.
    // 33 + 8 + 3 = 44 more then last used/max of 606 = 650
    Caches.subCache(cache, WeightedValue.of("subCache", 33 * MB))
        .put(WeightedValue.of("subCacheKey", 8 * MB), WeightedValue.of("subCacheValue", 3 * MB));
    assertThat(cache.describeStats(), containsString("used/max 650/1000 MB"));
  }
}
