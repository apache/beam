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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.fn.harness.Cache.Shrinkable;
import org.apache.beam.runners.core.GlobalCombineFnRunner;
import org.apache.beam.runners.core.GlobalCombineFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

/**
 * Static utility methods that provide a grouping table implementation.
 *
 * <p>{@link NotThreadSafe} because the caller must use the bundle processing thread when invoking
 * {@link #put} and {@link #flush}. {@link #shrink} may be called from any thread.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@NotThreadSafe
public class PrecombineGroupingTable<K, InputT, AccumT>
    implements Shrinkable<PrecombineGroupingTable<K, InputT, AccumT>>, Weighted {

  /**
   * Returns a grouping table that combines inputs into an accumulator. The grouping table uses the
   * cache to defer flushing output until the cache evicts the table.
   */
  public static <K, InputT, AccumT> PrecombineGroupingTable<K, InputT, AccumT> combining(
      PipelineOptions options,
      Cache<Object, Object> cache,
      CombineFn<InputT, AccumT, ?> combineFn,
      Coder<K> keyCoder,
      boolean isGloballyWindowed) {
    return new PrecombineGroupingTable<>(
        options,
        cache,
        keyCoder,
        GlobalCombineFnRunners.create(combineFn),
        Caches::weigh,
        isGloballyWindowed);
  }

  /**
   * Returns a grouping table that combines inputs into an accumulator with sampling {@link
   * SizeEstimator SizeEstimators}. The grouping table uses the cache to defer flushing output until
   * the cache evicts the table.
   */
  public static <K, InputT, AccumT> PrecombineGroupingTable<K, InputT, AccumT> combiningAndSampling(
      PipelineOptions options,
      Cache<Object, Object> cache,
      CombineFn<InputT, AccumT, ?> combineFn,
      Coder<K> keyCoder,
      double sizeEstimatorSampleRate,
      boolean isGloballyWindowed) {
    return new PrecombineGroupingTable<>(
        options,
        cache,
        keyCoder,
        GlobalCombineFnRunners.create(combineFn),
        new SamplingSizeEstimator(Caches::weigh, sizeEstimatorSampleRate, 1.0),
        isGloballyWindowed);
  }

  @Nullable
  @Override
  public PrecombineGroupingTable<K, InputT, AccumT> shrink() {
    long currentWeight = maxWeight.updateAndGet(operand -> operand >> 1);
    // It is possible that we are shrunk multiple times until the requested max weight is too small.
    // In this case we want to effectively stop shrinking since we can't effectively cache much
    // at this time and the next insertion will likely evict all records.
    if (currentWeight <= 100L) {
      return null;
    }
    return this;
  }

  @Override
  public long getWeight() {
    return maxWeight.get();
  }

  /** Provides client-specific operations for size estimates. */
  @FunctionalInterface
  public interface SizeEstimator {
    long estimateSize(Object element);
  }

  private final Coder<K> keyCoder;
  private final GlobalCombineFnRunner<InputT, AccumT, ?> combineFn;
  private final PipelineOptions options;
  private final SizeEstimator sizer;
  private final Cache<Key, PrecombineGroupingTable<K, InputT, AccumT>> cache;
  private final LinkedHashMap<GroupingTableKey, GroupingTableEntry> lruMap;
  private final AtomicLong maxWeight;
  private long weight;
  private final boolean isGloballyWindowed;
  private long lastWeightForFlush;

  // Prevent hashmap growing too large. Improves performance for too many Unique Keys cases.
  // Keep it less than (2^14)*loadFactor=(2^14)*0.75=12288
  // Note: (2^13)*0.75=6144 looks too small to consider as limit
  private static final int DEFAULT_MAX_GROUPING_TABLE_SIZE = 12_000;

  private static final class Key implements Weighted {
    private static final Key INSTANCE = new Key();

    @Override
    public long getWeight() {
      // Ignore the actual size of this singleton because it is trivial and because
      // the weight reported here will be counted many times as it is present in
      // many different state subcaches.
      return 0;
    }
  }

  PrecombineGroupingTable(
      PipelineOptions options,
      Cache<?, ?> cache,
      Coder<K> keyCoder,
      GlobalCombineFnRunner<InputT, AccumT, ?> combineFn,
      SizeEstimator sizer,
      boolean isGloballyWindowed) {
    this.options = options;
    this.cache = (Cache<Key, PrecombineGroupingTable<K, InputT, AccumT>>) cache;
    this.keyCoder = keyCoder;
    this.combineFn = combineFn;
    this.sizer = sizer;
    this.isGloballyWindowed = isGloballyWindowed;
    this.lruMap = new LinkedHashMap<>(16, 0.75f, true);
    this.maxWeight = new AtomicLong();
    this.weight = 0L;
    this.cache.put(Key.INSTANCE, this);
  }

  private interface GroupingTableKey extends Weighted {
    Object getStructuralKey();

    Collection<? extends BoundedWindow> getWindows();

    @Override
    boolean equals(Object o);

    @Override
    int hashCode();
  }

  @VisibleForTesting
  static class WindowedGroupingTableKey implements GroupingTableKey {
    private final Object structuralKey;
    private final Collection<? extends BoundedWindow> windows;
    private final long weight;

    <K> WindowedGroupingTableKey(
        K key,
        Collection<? extends BoundedWindow> windows,
        Coder<K> keyCoder,
        SizeEstimator sizer) {
      this.structuralKey = keyCoder.structuralValue(key);
      this.windows = windows;
      this.weight = sizer.estimateSize(this);
    }

    @Override
    public Object getStructuralKey() {
      return structuralKey;
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return windows;
    }

    @Override
    public long getWeight() {
      return weight;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof WindowedGroupingTableKey)) {
        return false;
      }
      WindowedGroupingTableKey that = (WindowedGroupingTableKey) o;
      return structuralKey.equals(that.structuralKey) && windows.equals(that.windows);
    }

    @Override
    public int hashCode() {
      return structuralKey.hashCode() * 31 + windows.hashCode();
    }

    @Override
    public String toString() {
      return "GroupingTableKey{"
          + "structuralKey="
          + structuralKey
          + ", windows="
          + windows
          + ", weight="
          + weight
          + '}';
    }
  }

  @VisibleForTesting
  static class GloballyWindowedTableGroupingKey implements GroupingTableKey {
    private static final Collection<? extends BoundedWindow> GLOBAL_WINDOWS =
        Collections.singletonList(GlobalWindow.INSTANCE);

    private final Object structuralKey;
    private final long weight;

    private <K> GloballyWindowedTableGroupingKey(K key, Coder<K> keyCoder, SizeEstimator sizer) {
      this.structuralKey = keyCoder.structuralValue(key);
      this.weight = sizer.estimateSize(this);
    }

    @Override
    public Object getStructuralKey() {
      return structuralKey;
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return GLOBAL_WINDOWS;
    }

    @Override
    public long getWeight() {
      return weight;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof GloballyWindowedTableGroupingKey)) {
        return false;
      }
      GloballyWindowedTableGroupingKey that = (GloballyWindowedTableGroupingKey) o;
      return structuralKey.equals(that.structuralKey);
    }

    @Override
    public int hashCode() {
      return structuralKey.hashCode();
    }
  }

  @VisibleForTesting
  class GroupingTableEntry implements Weighted {
    private final GroupingTableKey groupingKey;
    private final K userKey;
    // The PGBK output will inherit the timestamp of one of its inputs.
    private final Instant outputTimestamp;
    private final long keySize;
    private long accumulatorSize;
    private AccumT accumulator;
    private boolean dirty;

    private GroupingTableEntry(
        GroupingTableKey groupingKey,
        Instant outputTimestamp,
        K userKey,
        InputT initialInputValue) {
      this.groupingKey = groupingKey;
      this.outputTimestamp = outputTimestamp;
      this.userKey = userKey;
      if (groupingKey.getStructuralKey() == userKey) {
        // This object is only storing references to the same objects that are being stored
        // by the cache so the accounting of the size of the key is occurring already.
        this.keySize = Caches.REFERENCE_SIZE * 2;
      } else {
        this.keySize = Caches.REFERENCE_SIZE + sizer.estimateSize(userKey);
      }
      this.accumulator =
          combineFn.createAccumulator(
              options, NullSideInputReader.empty(), groupingKey.getWindows());
      add(initialInputValue);
      this.accumulatorSize = sizer.estimateSize(accumulator);
    }

    public GroupingTableKey getGroupingKey() {
      return groupingKey;
    }

    public Instant getOutputTimestamp() {
      return outputTimestamp;
    }

    public K getKey() {
      return userKey;
    }

    public AccumT getAccumulator() {
      return accumulator;
    }

    @Override
    public long getWeight() {
      return keySize + accumulatorSize;
    }

    public void compact() {
      if (dirty) {
        accumulator =
            combineFn.compact(
                accumulator, options, NullSideInputReader.empty(), groupingKey.getWindows());
        accumulatorSize = sizer.estimateSize(accumulator);
        dirty = false;
      }
    }

    public void add(InputT value) {
      dirty = true;
      accumulator =
          combineFn.addInput(
              accumulator, value, options, NullSideInputReader.empty(), groupingKey.getWindows());
      accumulatorSize = sizer.estimateSize(accumulator);
    }

    @Override
    public String toString() {
      return "GroupingTableEntry{"
          + "groupingKey="
          + groupingKey
          + ", userKey="
          + userKey
          + ", keySize="
          + keySize
          + ", accumulatorSize="
          + accumulatorSize
          + ", accumulator="
          + accumulator
          + ", dirty="
          + dirty
          + '}';
    }
  }

  /**
   * Adds the key and value to this table, possibly flushing some entries to output if the table is
   * full.
   */
  @VisibleForTesting
  public void put(
      WindowedValue<KV<K, InputT>> value, FnDataReceiver<WindowedValue<KV<K, AccumT>>> receiver)
      throws Exception {
    // Ignore timestamp for grouping purposes.
    // The Pre-combine output will inherit the timestamp of one of its inputs.
    GroupingTableKey groupingKey =
        isGloballyWindowed
            ? new GloballyWindowedTableGroupingKey(value.getValue().getKey(), keyCoder, sizer)
            : new WindowedGroupingTableKey(
                value.getValue().getKey(), value.getWindows(), keyCoder, sizer);

    lruMap.compute(
        groupingKey,
        (key, tableEntry) -> {
          if (tableEntry == null) {
            weight += groupingKey.getWeight();
            tableEntry =
                new GroupingTableEntry(
                    groupingKey,
                    value.getTimestamp(),
                    value.getValue().getKey(),
                    value.getValue().getValue());
          } else {
            weight -= tableEntry.getWeight();
            tableEntry.add(value.getValue().getValue());
          }
          weight += tableEntry.getWeight();
          return tableEntry;
        });

    if (lruMap.size() >= DEFAULT_MAX_GROUPING_TABLE_SIZE) {
      flush(receiver);
      lastWeightForFlush = weight;
    } else if (Caches.shouldUpdateOnSizeChange(lastWeightForFlush, weight)) {
      flushIfNeeded(receiver);
      lastWeightForFlush = weight;
    }
  }

  private void flushIfNeeded(FnDataReceiver<WindowedValue<KV<K, AccumT>>> receiver)
      throws Exception {
    // Increase the maximum only if we require it
    maxWeight.accumulateAndGet(weight, (current, update) -> current < update ? update : current);

    // Update the cache to ensure that LRU is handled appropriately and for the cache to have an
    // opportunity to shrink the maxWeight if necessary.
    cache.put(Key.INSTANCE, this);

    // Get the updated weight now that the cache may have been shrunk and respect it
    long currentMax = maxWeight.get();

    // Only compact and output from the bundle processing thread that is inserting elements into the
    // grouping table. This ensures that we honor the guarantee that transforms for a single bundle
    // execute using the same thread.
    if (weight > currentMax) {
      // Try to compact as many the values as possible and only flush values if compaction wasn't
      // enough.
      for (GroupingTableEntry valueToCompact : lruMap.values()) {
        long currentWeight = valueToCompact.getWeight();
        valueToCompact.compact();
        weight += valueToCompact.getWeight() - currentWeight;
      }

      if (weight > currentMax) {
        Iterator<GroupingTableEntry> iterator = lruMap.values().iterator();
        while (iterator.hasNext()) {
          GroupingTableEntry valueToFlush = iterator.next();
          weight -= valueToFlush.getWeight() + valueToFlush.getGroupingKey().getWeight();
          iterator.remove();
          output(valueToFlush, receiver);
          if (weight <= currentMax) {
            break;
          }
        }
      }
    }
  }

  /**
   * Output the given entry. Does not actually remove it from the table or update this table's size.
   */
  private void output(
      GroupingTableEntry entry, FnDataReceiver<WindowedValue<KV<K, AccumT>>> receiver)
      throws Exception {
    entry.compact();
    receiver.accept(
        isGloballyWindowed
            ? WindowedValue.valueInGlobalWindow(KV.of(entry.getKey(), entry.getAccumulator()))
            : WindowedValue.of(
                KV.of(entry.getKey(), entry.getAccumulator()),
                entry.getOutputTimestamp(),
                entry.getGroupingKey().getWindows(),
                // The PaneInfo will always be overwritten by the GBK.
                PaneInfo.NO_FIRING));
  }

  /** Flushes all entries in this table to output. */
  public void flush(FnDataReceiver<WindowedValue<KV<K, AccumT>>> receiver) throws Exception {
    cache.remove(Key.INSTANCE);
    for (GroupingTableEntry valueToFlush : lruMap.values()) {
      output(valueToFlush, receiver);
    }
    lruMap.clear();
    weight = 0;
  }

  ////////////////////////////////////////////////////////////////////////////
  // Size sampling.

  /**
   * Implements size estimation by adaptively delegating to an underlying (potentially more
   * expensive) estimator for some elements and returning the average value for others.
   */
  @VisibleForTesting
  static class SamplingSizeEstimator implements SizeEstimator {

    /**
     * The degree of confidence required in our expected value predictions before we allow
     * under-sampling.
     *
     * <p>The value of 3.0 is a confidence interval of about 99.7% for a high-degree-of-freedom
     * t-distribution.
     */
    static final double CONFIDENCE_INTERVAL_SIGMA = 3;

    /**
     * The desired size of our confidence interval (relative to the measured expected value).
     *
     * <p>The value of 0.25 is plus or minus 25%.
     */
    static final double CONFIDENCE_INTERVAL_SIZE = 0.25;

    /** Default number of elements that must be measured before elements are skipped. */
    static final long DEFAULT_MIN_SAMPLED = 20;

    private final SizeEstimator underlying;
    private final double minSampleRate;
    private final double maxSampleRate;
    private final long minSampled;
    private final Random random;

    private long totalElements = 0;
    private long sampledElements = 0;
    private long sampledSum = 0;
    private double sampledSumSquares = 0;
    private long estimate;

    private long nextSample = 0;

    private SamplingSizeEstimator(
        SizeEstimator underlying, double minSampleRate, double maxSampleRate) {
      this(underlying, minSampleRate, maxSampleRate, DEFAULT_MIN_SAMPLED, new Random());
    }

    @VisibleForTesting
    SamplingSizeEstimator(
        SizeEstimator underlying,
        double minSampleRate,
        double maxSampleRate,
        long minSampled,
        Random random) {
      this.underlying = underlying;
      this.minSampleRate = minSampleRate;
      this.maxSampleRate = maxSampleRate;
      this.minSampled = minSampled;
      this.random = random;
    }

    @Override
    public long estimateSize(Object element) {
      if (sampleNow()) {
        return recordSample(underlying.estimateSize(element));
      } else {
        return estimate;
      }
    }

    private boolean sampleNow() {
      totalElements++;
      return --nextSample < 0;
    }

    private long recordSample(long value) {
      sampledElements += 1;
      sampledSum += value;
      sampledSumSquares += value * value;
      estimate = (long) Math.ceil((double) sampledSum / sampledElements);
      long target = desiredSampleSize();
      if (sampledElements < minSampled || sampledElements < target) {
        // Sample immediately.
        nextSample = 0;
      } else {
        double rate =
            cap(
                minSampleRate,
                maxSampleRate,
                Math.max(
                    1.0 / (totalElements - minSampled + 1), // slowly ramp down
                    target / (double) totalElements)); // "future" target
        // Uses the geometric distribution to return the likely distance between
        // successive independent trials of a fixed probability p. This gives the
        // same uniform distribution of branching on Math.random() < p, but with
        // one random number generation per success rather than one
        // per test, which can be a significant savings if p is small.
        nextSample =
            rate == 1.0 ? 0 : (long) Math.floor(Math.log(random.nextDouble()) / Math.log(1 - rate));
      }
      return value;
    }

    private static double cap(double min, double max, double value) {
      return Math.min(max, Math.max(min, value));
    }

    private long desiredSampleSize() {
      // We have no a-priori information on the actual distribution of data
      // sizes, so compute our desired sample as if it were normal.
      // Yes this formula is unstable for small stddev, but we only care about large stddev.
      double mean = sampledSum / (double) sampledElements;
      double sumSquareDiff =
          sampledSumSquares - (2 * mean * sampledSum) + (sampledElements * mean * mean);
      double stddev = Math.sqrt(sumSquareDiff / (sampledElements - 1));
      double sqrtDesiredSamples =
          (CONFIDENCE_INTERVAL_SIGMA * stddev) / (CONFIDENCE_INTERVAL_SIZE * mean);
      return (long) Math.ceil(sqrtDesiredSamples * sqrtDesiredSamples);
    }
  }
}
