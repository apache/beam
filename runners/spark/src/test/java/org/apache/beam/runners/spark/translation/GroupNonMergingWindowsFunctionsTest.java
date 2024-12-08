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
package org.apache.beam.runners.spark.translation;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.GroupNonMergingWindowsFunctions.GroupByKeyIterator;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Bytes;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

/** Unit tests of {@link GroupNonMergingWindowsFunctions}. */
public class GroupNonMergingWindowsFunctionsTest {

  @Test
  public void testGroupByKeyIterator() throws Coder.NonDeterministicException {
    GroupByKeyIterator<String, Integer, GlobalWindow> iteratorUnderTest = createGbkIterator();

    Assert.assertTrue(iteratorUnderTest.hasNext());
    WindowedValue<KV<String, Iterable<Integer>>> k1Win = iteratorUnderTest.next();
    // testing that calling 2x hasNext doesn't move to next key iterator
    Assert.assertTrue(iteratorUnderTest.hasNext());
    Assert.assertTrue(iteratorUnderTest.hasNext());

    Iterator<Integer> valuesIteratorForK1 = k1Win.getValue().getValue().iterator();

    Assert.assertTrue("Now we expect first value for K1 to pop up.", valuesIteratorForK1.hasNext());
    assertEquals(1L, valuesIteratorForK1.next().longValue());
    Assert.assertTrue(valuesIteratorForK1.hasNext());
    Assert.assertTrue(valuesIteratorForK1.hasNext());
    assertEquals(2L, valuesIteratorForK1.next().longValue());

    WindowedValue<KV<String, Iterable<Integer>>> k2Win = iteratorUnderTest.next();
    Iterator<Integer> valuesIteratorForK2 = k2Win.getValue().getValue().iterator();
    assertEquals(3L, valuesIteratorForK2.next().longValue());
  }

  @Test
  public void testGroupByKeyIteratorOnNonGlobalWindows() throws Coder.NonDeterministicException {
    Instant now = Instant.now();
    GroupByKeyIterator<String, Integer, IntervalWindow> iteratorUnderTest =
        createGbkIterator(
            new IntervalWindow(now, now.plus(Duration.millis(1))),
            IntervalWindow.getCoder(),
            WindowingStrategy.of(FixedWindows.of(Duration.millis(1))));

    Assert.assertTrue(iteratorUnderTest.hasNext());
    WindowedValue<KV<String, Iterable<Integer>>> k1Win = iteratorUnderTest.next();
    // testing that calling 2x hasNext doesn't move to next key iterator
    Assert.assertTrue(iteratorUnderTest.hasNext());
    Assert.assertTrue(iteratorUnderTest.hasNext());

    Iterator<Integer> valuesIteratorForK1 = k1Win.getValue().getValue().iterator();

    Assert.assertTrue("Now we expect first value for K1 to pop up.", valuesIteratorForK1.hasNext());
    assertEquals(1L, valuesIteratorForK1.next().longValue());
    Assert.assertTrue(valuesIteratorForK1.hasNext());
    Assert.assertTrue(valuesIteratorForK1.hasNext());
    assertEquals(2L, valuesIteratorForK1.next().longValue());

    WindowedValue<KV<String, Iterable<Integer>>> k2Win = iteratorUnderTest.next();
    Iterator<Integer> valuesIteratorForK2 = k2Win.getValue().getValue().iterator();
    assertEquals(3L, valuesIteratorForK2.next().longValue());
  }

  @Test(expected = IllegalStateException.class)
  public void testGbkIteratorValuesCannotBeReiterated() throws Coder.NonDeterministicException {
    GroupByKeyIterator<String, Integer, GlobalWindow> iteratorUnderTest = createGbkIterator();
    WindowedValue<KV<String, Iterable<Integer>>> firstEl = iteratorUnderTest.next();
    Iterable<Integer> value = firstEl.getValue().getValue();
    for (Integer ignored : value) {
      // first iteration
    }
    for (Integer ignored : value) {
      // second iteration should throw IllegalStateException
    }
  }

  private GroupByKeyIterator<String, Integer, GlobalWindow> createGbkIterator()
      throws Coder.NonDeterministicException {
    return createGbkIterator(
        GlobalWindow.INSTANCE, GlobalWindow.Coder.INSTANCE, WindowingStrategy.globalDefault());
  }

  private <W extends BoundedWindow> GroupByKeyIterator<String, Integer, W> createGbkIterator(
      W window, Coder<W> winCoder, WindowingStrategy<Object, W> winStrategy)
      throws Coder.NonDeterministicException {

    StringUtf8Coder keyCoder = StringUtf8Coder.of();
    final WindowedValue.FullWindowedValueCoder<KV<String, Integer>> winValCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
            winStrategy.getWindowFn().windowCoder());

    ItemFactory<String, Integer, W> factory =
        ItemFactory.forWindow(keyCoder, winValCoder, winCoder, window);
    List<Tuple2<ByteArray, byte[]>> items =
        Arrays.asList(
            factory.create("k1", 1),
            factory.create("k1", 2),
            factory.create("k2", 3),
            factory.create("k2", 4),
            factory.create("k2", 5));
    return new GroupByKeyIterator<>(items.iterator(), keyCoder, winStrategy, winValCoder);
  }

  private static class ItemFactory<K, V, W extends BoundedWindow> {

    static <K, V> ItemFactory<K, V, GlobalWindow> forGlogalWindow(
        Coder<K> keyCoder, FullWindowedValueCoder<KV<K, V>> winValCoder) {
      return new ItemFactory<>(
          keyCoder, winValCoder, GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);
    }

    static <K, V, W extends BoundedWindow> ItemFactory<K, V, W> forWindow(
        Coder<K> keyCoder,
        FullWindowedValueCoder<KV<K, V>> winValCoder,
        Coder<W> winCoder,
        W window) {
      return new ItemFactory<>(keyCoder, winValCoder, winCoder, window);
    }

    private final Coder<K> keyCoder;
    private final WindowedValue.FullWindowedValueCoder<KV<K, V>> winValCoder;
    private final byte[] windowBytes;
    private final W window;

    ItemFactory(
        Coder<K> keyCoder,
        FullWindowedValueCoder<KV<K, V>> winValCoder,
        Coder<W> winCoder,
        W window) {
      this.keyCoder = keyCoder;
      this.winValCoder = winValCoder;
      this.windowBytes = CoderHelpers.toByteArray(window, winCoder);
      this.window = window;
    }

    private Tuple2<ByteArray, byte[]> create(K key, V value) {
      ByteArray kaw =
          new ByteArray(Bytes.concat(CoderHelpers.toByteArray(key, keyCoder), windowBytes));

      byte[] windowedValue =
          CoderHelpers.toByteArray(
              WindowedValue.of(
                  KV.of(key, value), Instant.now(), window, PaneInfo.ON_TIME_AND_ONLY_FIRING),
              winValCoder);
      return new Tuple2<>(kaw, windowedValue);
    }
  }
}
