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
package org.apache.beam.sdk.extensions.smb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link KeyGroupIterator}. */
public class KeyGroupIteratorTest {

  private static final Function<String, String> keyFn = s -> s.substring(0, 1);
  private static final Comparator<String> keyComparator = Comparator.naturalOrder();

  @Test
  public void testEmptyIterator() {
    KeyGroupIterator<String, String> iterator =
        new KeyGroupIterator<>(Collections.emptyIterator(), keyFn, keyComparator);
    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testSingleKeySingleValue() {
    KeyGroupIterator<String, String> iterator =
        new KeyGroupIterator<>(Iterators.forArray("a1"), keyFn, keyComparator);

    Assert.assertTrue(iterator.hasNext());
    KV<String, Iterator<String>> kv = iterator.next();
    String k = kv.getKey();
    Iterator<String> vi = kv.getValue();
    Assert.assertEquals("a", k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testSingleKeyMultiValue() {
    KeyGroupIterator<String, String> iterator =
        new KeyGroupIterator<>(Iterators.forArray("a1", "a2"), keyFn, keyComparator);

    Assert.assertTrue(iterator.hasNext());
    KV<String, Iterator<String>> kv = iterator.next();
    String k = kv.getKey();
    Iterator<String> vi = kv.getValue();
    Assert.assertEquals("a", k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1", vi.next());
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a2", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testMultiKeySingleValue() {
    KeyGroupIterator<String, String> iterator =
        new KeyGroupIterator<>(Iterators.forArray("a1", "b1"), keyFn, keyComparator);

    KV<String, Iterator<String>> kv;
    String k;
    Iterator<String> vi;

    Assert.assertTrue(iterator.hasNext());
    kv = iterator.next();
    k = kv.getKey();
    vi = kv.getValue();
    Assert.assertEquals("a", k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertTrue(iterator.hasNext());
    kv = iterator.next();
    k = kv.getKey();
    vi = kv.getValue();
    Assert.assertEquals("b", k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("b1", vi.next());
    Assert.assertFalse(vi.hasNext());
    Assert.assertThrows(NoSuchElementException.class, vi::next);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testMultiKeyMultiElement() {
    testIterator(
        Lists.newArrayList("a1", "a2", "b1", "b2"),
        Lists.newArrayList(
            KV.of("a", Lists.newArrayList("a1", "a2")),
            KV.of("b", Lists.newArrayList("b1", "b2"))));
    testIterator(
        Lists.newArrayList("a1", "b1", "b2"),
        Lists.newArrayList(
            KV.of("a", Lists.newArrayList("a1")), KV.of("b", Lists.newArrayList("b1", "b2"))));
    testIterator(
        Lists.newArrayList("a1", "a2", "b1"),
        Lists.newArrayList(
            KV.of("a", Lists.newArrayList("a1", "a2")), KV.of("b", Lists.newArrayList("b1"))));
    testIterator(
        Lists.newArrayList("a1", "b1", "b2", "c1", "c2", "c3"),
        Lists.newArrayList(
            KV.of("a", Lists.newArrayList("a1")),
            KV.of("b", Lists.newArrayList("b1", "b2")),
            KV.of("c", Lists.newArrayList("c1", "c2", "c3"))));
  }

  private void testIterator(List<String> data, List<KV<String, List<String>>> expected) {
    KeyGroupIterator<String, String> iterator =
        new KeyGroupIterator<>(data.iterator(), keyFn, keyComparator);
    List<KV<String, List<String>>> actual = new ArrayList<>();
    iterator.forEachRemaining(
        kv -> actual.add(KV.of(kv.getKey(), Lists.newArrayList(kv.getValue()))));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testIllegalStates() {
    KeyGroupIterator<String, String> iterator =
        new KeyGroupIterator<>(Iterators.forArray("a1", "a2", "b1"), keyFn, keyComparator);
    Assert.assertTrue(iterator.hasNext());
    KV<String, Iterator<String>> kv = iterator.next();
    String k = kv.getKey();
    Iterator<String> vi = kv.getValue();
    Assert.assertEquals("a", k);
    Assert.assertTrue(vi.hasNext());
    Assert.assertEquals("a1", vi.next());
    Assert.assertTrue(vi.hasNext());
    Assert.assertThrows(
        "Previous Iterator<ValueT> not fully iterated",
        IllegalStateException.class,
        iterator::hasNext);
    Assert.assertThrows(
        "Previous Iterator<ValueT> not fully iterated",
        IllegalStateException.class,
        iterator::next);
  }
}
