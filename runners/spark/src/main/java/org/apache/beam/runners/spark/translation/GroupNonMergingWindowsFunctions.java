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

import static org.apache.beam.runners.spark.coders.CoderHelpers.windowedValueCoder;

import java.util.Iterator;
import java.util.Objects;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.PeekingIterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Bytes;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/** Functions for GroupByKey with Non-Merging windows translations to Spark. */
@SuppressWarnings({"keyfor", "nullness"}) // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
public class GroupNonMergingWindowsFunctions {

  private static final Logger LOG = LoggerFactory.getLogger(GroupNonMergingWindowsFunctions.class);

  /**
   * Verify if given windowing strategy and coders are suitable for group by key and window
   * optimization.
   *
   * @param windowingStrategy the windowing strategy
   * @return {@code true} if group by key and window can be used
   */
  static boolean isEligibleForGroupByWindow(WindowingStrategy<?, ?> windowingStrategy) {
    return !windowingStrategy.needsMerge()
        && windowingStrategy.getTimestampCombiner() == TimestampCombiner.END_OF_WINDOW
        && windowingStrategy.getWindowFn().windowCoder().consistentWithEquals();
  }

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
          WindowingStrategy<?, W> windowingStrategy,
          Partitioner partitioner) {
    final Coder<W> windowCoder = windowingStrategy.getWindowFn().windowCoder();
    WindowedValueCoder<KV<K, V>> windowedKvCoder =
        windowedValueCoder(KvCoder.of(keyCoder, valueCoder), windowingStrategy);
    JavaPairRDD<ByteArray, byte[]> windowInKey =
        bringWindowToKey(
            rdd, keyCoder, windowCoder, wv -> CoderHelpers.toByteArray(wv, windowedKvCoder));
    return windowInKey
        .repartitionAndSortWithinPartitions(getPartitioner(partitioner, rdd))
        .mapPartitions(
            it -> new GroupByKeyIterator<>(it, keyCoder, windowingStrategy, windowedKvCoder))
        .filter(Objects::nonNull); // filter last null element from GroupByKeyIterator
  }

  static <K, V, W extends BoundedWindow>
      JavaPairRDD<ByteArray, WindowedValue<KV<K, V>>> bringWindowToKey(
          JavaRDD<WindowedValue<KV<K, V>>> rdd, Coder<K> keyCoder, Coder<W> windowCoder) {
    return bringWindowToKey(rdd, keyCoder, windowCoder, e -> e);
  }

  /** Creates pair RDD with key being a composite of original key and window. */
  static <K, V, OutputT, W extends BoundedWindow> JavaPairRDD<ByteArray, OutputT> bringWindowToKey(
      JavaRDD<WindowedValue<KV<K, V>>> rdd,
      Coder<K> keyCoder,
      Coder<W> windowCoder,
      SerializableFunction<WindowedValue<KV<K, V>>, OutputT> mappingFn) {

    if (!isKeyAndWindowCoderConsistentWithEquals(keyCoder, windowCoder)) {
      LOG.warn(
          "Either coder {} or {} is not consistent with equals. "
              + "That might cause issues on some runners.",
          keyCoder,
          windowCoder);
    }

    return rdd.flatMapToPair(
        (WindowedValue<KV<K, V>> windowedValue) -> {
          final byte[] keyBytes =
              CoderHelpers.toByteArray(windowedValue.getValue().getKey(), keyCoder);
          return Iterators.transform(
              windowedValue.explodeWindows().iterator(),
              item -> {
                Objects.requireNonNull(item, "Exploded window can not be null.");
                @SuppressWarnings("unchecked")
                final W window = (W) Iterables.getOnlyElement(item.getWindows());
                final byte[] windowBytes = CoderHelpers.toByteArray(window, windowCoder);
                WindowedValue<KV<K, V>> valueOut =
                    WindowedValue.of(item.getValue(), item.getTimestamp(), window, item.getPane());
                final ByteArray windowedKey = new ByteArray(Bytes.concat(keyBytes, windowBytes));
                return new Tuple2<>(windowedKey, mappingFn.apply(valueOut));
              });
        });
  }

  private static boolean isKeyAndWindowCoderConsistentWithEquals(
      Coder<?> keyCoder, Coder<?> windowCoder) {
    try {
      keyCoder.verifyDeterministic();
      windowCoder.verifyDeterministic();
      return keyCoder.consistentWithEquals() && windowCoder.consistentWithEquals();
    } catch (Coder.NonDeterministicException ex) {
      throw new IllegalArgumentException(
          "Coder for both key " + keyCoder + " and " + windowCoder + " must be deterministic", ex);
    }
  }

  private static <K, V> Partitioner getPartitioner(
      Partitioner partitioner, JavaRDD<WindowedValue<KV<K, V>>> rdd) {
    return partitioner == null ? new HashPartitioner(rdd.getNumPartitions()) : partitioner;
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

    private final PeekingIterator<Tuple2<ByteArray, byte[]>> inner;
    private final Coder<K> keyCoder;
    private final WindowingStrategy<?, W> windowingStrategy;
    private final WindowedValueCoder<KV<K, V>> wvCoder;

    private boolean hasNext = true;
    private ByteArray currentKey = null;

    GroupByKeyIterator(
        Iterator<Tuple2<ByteArray, byte[]>> inner,
        Coder<K> keyCoder,
        WindowingStrategy<?, W> windowingStrategy,
        WindowedValueCoder<KV<K, V>> wvCoder) {
      this.inner = Iterators.peekingIterator(inner);
      this.keyCoder = keyCoder;
      this.windowingStrategy = windowingStrategy;
      this.wvCoder = wvCoder;
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public WindowedValue<KV<K, Iterable<V>>> next() {
      while (inner.hasNext()) {
        final ByteArray nextKey = inner.peek()._1;
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

      boolean consumed = false;
      private final PeekingIterator<Tuple2<ByteArray, byte[]>> inner;
      private final ByteArray currentKey;

      ValueIterator(PeekingIterator<Tuple2<ByteArray, byte[]>> inner, ByteArray currentKey) {
        this.inner = inner;
        this.currentKey = currentKey;
      }

      @Override
      public Iterator<V> iterator() {
        if (consumed) {
          throw new IllegalStateException(
              "ValueIterator can't be iterated more than once,"
                  + "otherwise there could be data lost");
        }
        consumed = true;
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
      final WindowedValue<KV<K, V>> windowedValue =
          CoderHelpers.fromByteArray(windowedValueBytes, wvCoder);
      return windowedValue.getValue().getValue();
    }

    private WindowedValue<KV<K, V>> decodeItem(Tuple2<ByteArray, byte[]> item) {
      final K key = CoderHelpers.fromByteArray(item._1.getValue(), keyCoder);
      final WindowedValue<KV<K, V>> windowedValue = CoderHelpers.fromByteArray(item._2, wvCoder);
      final V value = windowedValue.getValue().getValue();
      @SuppressWarnings("unchecked")
      final W window = (W) Iterables.getOnlyElement(windowedValue.getWindows());
      final Instant timestamp =
          windowingStrategy.getTimestampCombiner().assign(window, windowedValue.getTimestamp());
      // BEAM-7341: Elements produced by GbK are always ON_TIME and ONLY_FIRING
      return WindowedValue.of(
          KV.of(key, value), timestamp, window, PaneInfo.ON_TIME_AND_ONLY_FIRING);
    }
  }
}
