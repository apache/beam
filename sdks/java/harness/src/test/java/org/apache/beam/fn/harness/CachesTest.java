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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.apache.beam.fn.harness.Cache.Shrinkable;
import org.apache.beam.fn.harness.Caches.ClearableCache;
import org.apache.beam.fn.harness.Caches.ShrinkOnEviction;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
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
    Shrinkable<Object> shrinkable =
        new Shrinkable<Object>() {

          @Override
          public Object shrink() {
            return "wasShrunk";
          }
        };

    Cache<Object, Object> cache =
        Caches.forCache(new ShrinkOnEviction(CacheBuilder.newBuilder().maximumSize(1)).getCache());
    cache.put("shrinkable", shrinkable);
    // Check that we didn't evict it yet
    assertSame(shrinkable, cache.peek("shrinkable"));

    // The next insertion should cause the value to b "shrunk"
    cache.put("other", "value");
    assertEquals("wasShrunk", cache.peek("shrinkable"));
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
}
