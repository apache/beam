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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Caches}. */
@RunWith(JUnit4.class)
public class CachesTest {
  @Test
  public void testNoopCache() {
    Cache<String, String> cache = Caches.noop();
    cache.put("key", "value");
    assertNull(cache.peek("key"));
    assertEquals("value", cache.computeIfAbsent("key", (unused) -> "value"));
    assertNull(cache.peek("key"));
    assertThat(cache.keys(), is(emptyIterable()));
  }

  @Test
  public void testEternalCache() {
    testCache(Caches.eternal());
  }

  @Test
  public void testDefaultCache() {
    testCache(Caches.fromOptions(PipelineOptionsFactory.create()));
  }

  @Test
  public void testSubCache() {
    testCache(Caches.subCache(Caches.eternal(), "prefix"));
  }

  @Test
  public void testSiblingSubCaches() {
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

    // Test clearing a cache with a different prefix doesn't impact keys without the same prefix
    cacheB.clear();
    assertEquals("valueA", cacheA.peek("keyA"));
    assertEquals("valueA", cacheACopy.peek("keyA"));

    // Test clearing a cache with the same prefix impacts other instances
    cacheACopy.clear();
    assertNull(cacheA.peek("keyA"));
    assertNull(cacheACopy.peek("keyA"));
  }

  @Test
  public void testNestedSubCaches() {
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

    // Test that clearing the middle cache impacts children but not parent
    parent.put("keyA", "parentA");
    parent.put("keyB", "parentB");
    child.clear();
    assertThat(child.keys(), is(emptyIterable()));
    assertThat(childOfChild.keys(), is(emptyIterable()));
    assertEquals("parentA", parent.peek("keyA"));
    assertEquals("parentB", parent.peek("keyB"));
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

    assertThat(cache.keys(), containsInAnyOrder("key1", "key2"));

    // Test removal
    cache.remove("key1");
    assertNull(cache.peek("key1"));
    assertEquals("value2", cache.peek("key2"));
    assertThat(cache.keys(), containsInAnyOrder("key2"));

    // Test clear
    cache.clear();
    assertNull(cache.peek("key1"));
    assertNull(cache.peek("key2"));
    assertThat(cache.keys(), is(emptyIterable()));
  }
}
