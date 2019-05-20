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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.PeekingIterator;
import org.apache.beam.vendor.guava.v20_0.com.google.common.primitives.UnsignedBytes;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.Instant;
import scala.Tuple2;

/** Functions for GroupByKey with Non-Merging windows translations to Spark. */
public class GroupNonMergingWindowsFunctions {

  /**
   * Creates composite key of K and W and group all values for that composite key with Spark's
   * repartitionAndSortWithinPartitions. Stream of sorted by composite key's is transformed to key
   * with iterator of all values for that key (via {@link GroupByKeyIterator}).
   *
   * <p>repartitionAndSortWithinPartitions is used because all values are not collected into memory
   * at once, but streamed with iterator unlike GroupByKey (it minimizes memory pressure).
   */
  static <K, V, W extends BoundedWindow>
      JavaRDD<WindowedValue<KV<K, Iterable<V>>>> groupByKeyAndWindow(
          JavaRDD<WindowedValue<KV<K, V>>> rdd,
          Coder<K> keyCoder,
          Coder<V> valueCoder,
          WindowingStrategy<?, W> windowingStrategy) {
    final Coder<W> windowCoder = windowingStrategy.getWindowFn().windowCoder();
    final WindowedValue.FullWindowedValueCoder<byte[]> windowedValueCoder =
        WindowedValue.getFullCoder(ByteArrayCoder.of(), windowCoder);
    return rdd.flatMapToPair(
            (WindowedValue<KV<K, V>> windowedValue) -> {
              final byte[] keyBytes =
                  CoderHelpers.toByteArray(windowedValue.getValue().getKey(), keyCoder);
              final byte[] valueBytes =
                  CoderHelpers.toByteArray(windowedValue.getValue().getValue(), valueCoder);
              return Iterators.transform(
                  windowedValue.explodeWindows().iterator(),
                  item -> {
                    Objects.requireNonNull(item, "Exploded window can not be null.");
                    @SuppressWarnings("unchecked")
                    final W window = (W) Iterables.getOnlyElement(item.getWindows());
                    final byte[] windowBytes = CoderHelpers.toByteArray(window, windowCoder);
                    final byte[] windowValueBytes =
                        CoderHelpers.toByteArray(
                            WindowedValue.of(
                                valueBytes, item.getTimestamp(), window, item.getPane()),
                            windowedValueCoder);
                    final WindowedKey windowedKey = new WindowedKey(keyBytes, windowBytes);
                    return new Tuple2<>(windowedKey, windowValueBytes);
                  });
            })
        .repartitionAndSortWithinPartitions(new HashPartitioner(rdd.getNumPartitions()))
        .mapPartitions(
            it ->
                new GroupByKeyIterator<>(
                    it, keyCoder, valueCoder, windowingStrategy, windowedValueCoder))
        .filter(Objects::nonNull); // filter last null element from GroupByKeyIterator
  }

  /**
   * Transform stream of sorted key values into stream of value iterators for each key. This
   * iterator can be iterated only once!
   *
   * <p>From Iterator<K, V> transform to <K, Iterator<V>>.
   *
   * @param <K> type of key iterator emits
   * @param <V> type of value iterator emits
   */
  static class GroupByKeyIterator<K, V, W extends BoundedWindow>
      implements Iterator<WindowedValue<KV<K, Iterable<V>>>> {

    private final PeekingIterator<Tuple2<WindowedKey, byte[]>> inner;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final WindowingStrategy<?, W> windowingStrategy;
    private final FullWindowedValueCoder<byte[]> windowedValueCoder;

    private boolean hasNext = true;
    private WindowedKey currentKey = null;

    GroupByKeyIterator(
        Iterator<Tuple2<WindowedKey, byte[]>> inner,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        WindowingStrategy<?, W> windowingStrategy,
        WindowedValue.FullWindowedValueCoder<byte[]> windowedValueCoder) {
      this.inner = Iterators.peekingIterator(inner);
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.windowingStrategy = windowingStrategy;
      this.windowedValueCoder = windowedValueCoder;
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public WindowedValue<KV<K, Iterable<V>>> next() {
      while (inner.hasNext()) {
        final WindowedKey nextKey = inner.peek()._1;
        if (nextKey.equals(currentKey)) {
          // we still did not see all values for a given key
          inner.next();
          continue;
        }
        currentKey = nextKey;
        final WindowedValue<KV<K, V>> decodedItem = decodeItem(inner.peek());
        return decodedItem.withValue(
            KV.of(decodedItem.getValue().getKey(), new ValueIterator(inner, currentKey)));
      }
      hasNext = false;
      return null;
    }

    class ValueIterator implements Iterable<V> {

      boolean usedAsIterable = false;
      private final PeekingIterator<Tuple2<WindowedKey, byte[]>> inner;
      private final WindowedKey currentKey;

      ValueIterator(PeekingIterator<Tuple2<WindowedKey, byte[]>> inner, WindowedKey currentKey) {
        this.inner = inner;
        this.currentKey = currentKey;
      }

      @Override
      public Iterator<V> iterator() {
        if (usedAsIterable) {
          throw new IllegalStateException(
              "ValueIterator can't be iterated more than once,"
                  + "otherwise there could be data lost");
        }
        usedAsIterable = true;
        return new AbstractIterator<V>() {
          @Override
          protected V computeNext() {
            if (inner.hasNext() && currentKey.equals(inner.peek()._1)) {
              return decodeValue(inner.next()._2);
            }
            return endOfData();
          }
        };
      }
    }

    private V decodeValue(byte[] windowedValueBytes) {
      final WindowedValue<byte[]> windowedValue =
          CoderHelpers.fromByteArray(windowedValueBytes, windowedValueCoder);
      return CoderHelpers.fromByteArray(windowedValue.getValue(), valueCoder);
    }

    private WindowedValue<KV<K, V>> decodeItem(Tuple2<WindowedKey, byte[]> item) {
      final K key = CoderHelpers.fromByteArray(item._1.getKey(), keyCoder);
      final WindowedValue<byte[]> windowedValue =
          CoderHelpers.fromByteArray(item._2, windowedValueCoder);
      final V value = CoderHelpers.fromByteArray(windowedValue.getValue(), valueCoder);
      @SuppressWarnings("unchecked")
      final W window = (W) Iterables.getOnlyElement(windowedValue.getWindows());
      final Instant timestamp =
          windowingStrategy
              .getTimestampCombiner()
              .assign(
                  window,
                  windowingStrategy
                      .getWindowFn()
                      .getOutputTime(windowedValue.getTimestamp(), window));
      return WindowedValue.of(
          KV.of(key, value), timestamp, window, PaneInfo.ON_TIME_AND_ONLY_FIRING);
    }
  }

  /** Composite key of key and window for groupByKey transformation. */
  public static class WindowedKey implements Comparable<WindowedKey>, Serializable {

    private final byte[] key;
    private final byte[] window;

    WindowedKey(byte[] key, byte[] window) {
      this.key = key;
      this.window = window;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WindowedKey that = (WindowedKey) o;
      return Arrays.equals(key, that.key) && Arrays.equals(window, that.window);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(key);
      result = 31 * result + Arrays.hashCode(window);
      return result;
    }

    byte[] getKey() {
      return key;
    }

    byte[] getWindow() {
      return window;
    }

    @Override
    public int compareTo(WindowedKey o) {
      int keyCompare = UnsignedBytes.lexicographicalComparator().compare(this.getKey(), o.getKey());
      if (keyCompare == 0) {
        return UnsignedBytes.lexicographicalComparator().compare(this.getWindow(), o.getWindow());
      }
      return keyCompare;
    }
  }
}
