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
import org.apache.beam.runners.spark.translation.GroupNonMergingWindowsFunctions.WindowedKey;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

/** Unit tests of {@link GroupNonMergingWindowsFunctions}. */
public class GroupNonMergingWindowsFunctionsTest {

  @Test
  public void testGroupByKeyIterator() {
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

  @Test(expected = IllegalStateException.class)
  public void testGbkIteratorValuesCannotBeReiterated() {
    GroupByKeyIterator<String, Integer, GlobalWindow> iteratorUnderTest = createGbkIterator();
    WindowedValue<KV<String, Iterable<Integer>>> firstEl = iteratorUnderTest.next();
    Iterable<Integer> value = firstEl.getValue().getValue();
    for (Integer i : value) {
      // first iteration
    }
    for (Integer i : value) {
      // second iteration should throw IllegalStateException
    }
  }

  private GroupByKeyIterator<String, Integer, GlobalWindow> createGbkIterator() {
    StringUtf8Coder keyCoder = StringUtf8Coder.of();
    BigEndianIntegerCoder valueCoder = BigEndianIntegerCoder.of();
    WindowingStrategy<Object, GlobalWindow> winStrategy = WindowingStrategy.of(new GlobalWindows());

    final WindowedValue.FullWindowedValueCoder<byte[]> winValCoder =
        WindowedValue.getFullCoder(ByteArrayCoder.of(), winStrategy.getWindowFn().windowCoder());

    ItemFactory<String, Integer> factory =
        new ItemFactory<>(
            keyCoder, valueCoder, winValCoder, winStrategy.getWindowFn().windowCoder());
    List<Tuple2<WindowedKey, byte[]>> items =
        Arrays.asList(
            factory.create("k1", 1),
            factory.create("k1", 2),
            factory.create("k2", 3),
            factory.create("k2", 4),
            factory.create("k2", 5));
    return new GroupByKeyIterator<>(
        items.iterator(), keyCoder, valueCoder, winStrategy, winValCoder);
  }

  private static class ItemFactory<K, V> {
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final WindowedValue.FullWindowedValueCoder<byte[]> winValCoder;
    private final byte[] globalWindow;

    ItemFactory(
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        FullWindowedValueCoder<byte[]> winValCoder,
        Coder<GlobalWindow> winCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.winValCoder = winValCoder;
      this.globalWindow = CoderHelpers.toByteArray(GlobalWindow.INSTANCE, winCoder);
    }

    private Tuple2<WindowedKey, byte[]> create(K key, V value) {
      WindowedKey kaw = new WindowedKey(CoderHelpers.toByteArray(key, keyCoder), globalWindow);

      byte[] valueInbytes = CoderHelpers.toByteArray(value, valueCoder);

      WindowedValue<byte[]> windowedValue =
          WindowedValue.of(
              valueInbytes, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.ON_TIME_AND_ONLY_FIRING);
      return new Tuple2<>(kaw, CoderHelpers.toByteArray(windowedValue, winValCoder));
    }
  }
}
