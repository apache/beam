/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A partial group-by-key operation.
 */
public class PartialGroupByKeyOperation extends ReceivingOperation {
  /**
   * Provides client-specific operations for grouping keys.
   */
  public static interface GroupingKeyCreator<K> {
    public Object createGroupingKey(K key) throws Exception;
  }

  /**
   * Provides client-specific operations for size estimates.
   */
  public static interface SizeEstimator<E> {
    public long estimateSize(E element) throws Exception;
  }

  /**
   * Provides client-specific operations for working with elements
   * that are key/value or key/values pairs.
   */
  public interface PairInfo {
    public Object getKeyFromInputPair(Object pair);
    public Object getValueFromInputPair(Object pair);
    public Object makeOutputPair(Object key, Object value);
  }

  /**
   * Provides client-specific operations for combining values.
   */
  public interface Combiner<K, VI, VA, VO> {
    public VA createAccumulator(K key);
    public VA add(K key, VA accumulator, VI value);
    public VA merge(K key, Iterable<VA> accumulators);
    public VO extract(K key, VA accumulator);
  }

  /**
   * A wrapper around a byte[] that uses structural, value-based
   * equality rather than byte[]'s normal object identity.
   */
  public static class StructuralByteArray {
    byte[] value;

    public StructuralByteArray(byte[] value) {
      this.value = value;
    }

    public byte[] getValue() { return value; }

    @Override
    public boolean equals(Object o) {
      if (o instanceof StructuralByteArray) {
        StructuralByteArray that = (StructuralByteArray) o;
        return Arrays.equals(this.value, that.value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(value);
    }

    @Override
    public String toString() {
      return "Val" + Arrays.toString(value);
    }
  }

  // By default, how many bytes we allow the grouping table to consume before
  // it has to be flushed.
  static final long DEFAULT_MAX_GROUPING_TABLE_BYTES = 100_000_000L;

  // How many bytes a word in the JVM has.
  static final int BYTES_PER_JVM_WORD = getBytesPerJvmWord();

  /**
   * The number of bytes of overhead to store an entry in the
   * grouping table (a {@code HashMap<StructuralByteArray, KeyAndValues>}),
   * ignoring the actual number of bytes in the keys and values:
   *
   * - an array element (1 word),
   * - a HashMap.Entry (4 words),
   * - a StructuralByteArray (1 words),
   * - a backing array (guessed at 1 word for the length),
   * - a KeyAndValues (2 words),
   * - an ArrayList (2 words),
   * - a backing array (1 word),
   * - per-object overhead (JVM-specific, guessed at 2 words * 6 objects).
   */
  static final int PER_KEY_OVERHEAD = 24 * BYTES_PER_JVM_WORD;

  final GroupingTable<Object, Object, Object> groupingTable;

  @SuppressWarnings("unchecked")
  public PartialGroupByKeyOperation(
      String operationName,
      GroupingKeyCreator<?> groupingKeyCreator,
      SizeEstimator<?> keySizeEstimator, SizeEstimator<?> valueSizeEstimator,
      PairInfo pairInfo,
      OutputReceiver[] receivers,
      String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler) {
    super(operationName, receivers, counterPrefix, addCounterMutator, stateSampler);
    groupingTable = new BufferingGroupingTable(
        DEFAULT_MAX_GROUPING_TABLE_BYTES, groupingKeyCreator,
        pairInfo, keySizeEstimator, valueSizeEstimator);
  }

  @SuppressWarnings("unchecked")
  public PartialGroupByKeyOperation(
      String operationName,
      GroupingKeyCreator<?> groupingKeyCreator,
      SizeEstimator<?> keySizeEstimator, SizeEstimator<?> valueSizeEstimator,
      double sizeEstimatorSampleRate,
      PairInfo pairInfo,
      OutputReceiver[] receivers,
      String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) {
    this(operationName, groupingKeyCreator,
        new SamplingSizeEstimator(keySizeEstimator, sizeEstimatorSampleRate, 1.0),
        new SamplingSizeEstimator(valueSizeEstimator, sizeEstimatorSampleRate, 1.0),
        pairInfo, receivers, counterPrefix, addCounterMutator, stateSampler);
  }

  /** Invoked by tests. */
  public PartialGroupByKeyOperation(
      GroupingKeyCreator<?> groupingKeyCreator,
      SizeEstimator<?> keySizeEstimator, SizeEstimator<?> valueSizeEstimator,
      PairInfo pairInfo,
      OutputReceiver outputReceiver,
      String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler) {
    this("PartialGroupByKeyOperation", groupingKeyCreator,
         keySizeEstimator, valueSizeEstimator, pairInfo,
         new OutputReceiver[]{ outputReceiver },
         counterPrefix,
         addCounterMutator,
         stateSampler);
  }

  @Override
  public void process(Object elem) throws Exception {
    try (StateSampler.ScopedState process =
        stateSampler.scopedState(processState)) {
      if (receivers[0] != null) {
        groupingTable.put(elem, receivers[0]);
      }
    }
  }

  @Override
  public void finish() throws Exception {
    try (StateSampler.ScopedState finish =
        stateSampler.scopedState(finishState)) {
      checkStarted();
      if (receivers[0] != null) {
        groupingTable.flush(receivers[0]);
      }
      super.finish();
    }
  }

  /**
   * Sets the maximum amount of memory the grouping table is allowed to
   * consume before it has to be flushed.
   */
  // @VisibleForTesting
  public void setMaxGroupingTableBytes(long maxSize) {
    groupingTable.maxSize = maxSize;
  }

  /**
   * Returns the amount of memory the grouping table currently consumes.
   */
  // @VisibleForTesting
  public long getGroupingTableBytes() {
    return groupingTable.size;
  }

  /**
   * Returns the number of bytes in a JVM word.  In case we failed to
   * find the answer, returns 8.
   */
  static int getBytesPerJvmWord() {
    String wordSizeInBits = System.getProperty("sun.arch.data.model");
    try {
      return Integer.parseInt(wordSizeInBits) / 8;
    } catch (NumberFormatException e) {
      // The JVM word size is unknown.  Assume 64-bit.
      return 8;
    }
  }

  private abstract static class GroupingTable<K, VI, VA> {

    // Keep the table relatively full to increase the chance of collisions.
    private static final double TARGET_LOAD = 0.9;

    private long maxSize;
    private final GroupingKeyCreator<? super K> groupingKeyCreator;
    private final PairInfo pairInfo;

    private long size = 0;
    private Map<Object, GroupingTableEntry<K, VI, VA>> table;

    public GroupingTable(long maxSize,
                          GroupingKeyCreator<? super K> groupingKeyCreator,
                          PairInfo pairInfo) {
      this.maxSize = maxSize;
      this.groupingKeyCreator = groupingKeyCreator;
      this.pairInfo = pairInfo;
      this.table = new HashMap<>();
    }

    interface GroupingTableEntry<K, VI, VA> {
      public K getKey();
      public VA getValue();
      public void add(VI value) throws Exception;
      public long getSize();
    }

    public abstract GroupingTableEntry<K, VI, VA> createTableEntry(K key) throws Exception;

    /**
     * Adds a pair to this table, possibly flushing some entries to output
     * if the table is full.
     */
    @SuppressWarnings("unchecked")
    public void put(Object pair, Receiver receiver) throws Exception {
      put((K) pairInfo.getKeyFromInputPair(pair),
          (VI) pairInfo.getValueFromInputPair(pair),
          receiver);
    }

    /**
     * Adds the key and value to this table, possibly flushing some entries
     * to output if the table is full.
     */
    public void put(K key, VI value, Receiver receiver) throws Exception {
      Object groupingKey = groupingKeyCreator.createGroupingKey(key);
      GroupingTableEntry<K, VI, VA> entry = table.get(groupingKey);
      if (entry == null) {
        entry = createTableEntry(key);
        table.put(groupingKey, entry);
        size += PER_KEY_OVERHEAD;
      } else {
        size -= entry.getSize();
      }
      entry.add(value);
      size += entry.getSize();

      if (size >= maxSize) {
        long targetSize = (long) (TARGET_LOAD * maxSize);
        Iterator<GroupingTableEntry<K, VI, VA>> entries =
            table.values().iterator();
        while (size >= targetSize) {
          if (!entries.hasNext()) {
            // Should never happen, but sizes may be estimates...
            size = 0;
            break;
          }
          GroupingTableEntry<K, VI, VA> toFlush = entries.next();
          entries.remove();
          size -= toFlush.getSize() + PER_KEY_OVERHEAD;
          output(toFlush, receiver);
        }
      }
    }

    /**
     * Output the given entry. Does not actually remove it from the table or
     * update this table's size.
     */
    private void output(GroupingTableEntry<K, VI, VA> entry, Receiver receiver) throws Exception {
      receiver.process(pairInfo.makeOutputPair(entry.getKey(), entry.getValue()));
    }

    /**
     * Flushes all entries in this table to output.
     */
    public void flush(Receiver output) throws Exception {
      for (GroupingTableEntry<K, VI, VA> entry : table.values()) {
        output(entry, output);
      }
      table.clear();
      size = 0;
    }

  }

  /**
   * A grouping table that simply buffers all inserted values in a list.
   */
  public static class BufferingGroupingTable<K, V> extends GroupingTable<K, V, List<V>> {

    public final SizeEstimator<? super K> keySizer;
    public final SizeEstimator<? super V> valueSizer;

    public BufferingGroupingTable(long maxSize,
                                  GroupingKeyCreator<? super K> groupingKeyCreator,
                                  PairInfo pairInfo,
                                  SizeEstimator<? super K> keySizer,
                                  SizeEstimator<? super V> valueSizer) {
      super(maxSize, groupingKeyCreator, pairInfo);
      this.keySizer = keySizer;
      this.valueSizer = valueSizer;
    }

    @Override
    public GroupingTableEntry<K, V, List<V>> createTableEntry(final K key) throws Exception {
      return new GroupingTableEntry<K, V, List<V>>() {
        long size = keySizer.estimateSize(key);
        final List<V> values = new ArrayList<>();
        public K getKey() { return key; }
        public List<V> getValue() { return values; }
        public long getSize() { return size; }
        public void add(V value) throws Exception {
          values.add(value);
          size += BYTES_PER_JVM_WORD + valueSizer.estimateSize(value);
        }
      };
    }
  }

  /**
   * A grouping table that uses the given combiner to combine values in place.
   */
  public static class CombiningGroupingTable<K, VI, VA> extends GroupingTable<K, VI, VA> {

    private final Combiner<? super K, VI, VA, ?> combiner;
    private final SizeEstimator<? super K> keySizer;
    private final SizeEstimator<? super VA> valueSizer;

    public CombiningGroupingTable(long maxSize,
                                  GroupingKeyCreator<? super K> groupingKeyCreator,
                                  PairInfo pairInfo,
                                  Combiner<? super K, VI, VA, ?> combineFn,
                                  SizeEstimator<? super K> keySizer,
                                  SizeEstimator<? super VA> valueSizer) {
      super(maxSize, groupingKeyCreator, pairInfo);
      this.combiner =  combineFn;
      this.keySizer = keySizer;
      this.valueSizer = valueSizer;
    }

    @Override
    public GroupingTableEntry<K, VI, VA> createTableEntry(final K key) throws Exception {
      return new GroupingTableEntry<K, VI, VA>() {
        final long keySize = keySizer.estimateSize(key);
        VA accumulator = combiner.createAccumulator(key);
        long accumulatorSize = 0; // never used before a value is added...
        public K getKey() { return key; }
        public VA getValue() { return accumulator; }
        public long getSize() { return keySize + accumulatorSize; }
        public void add(VI value) throws Exception {
          accumulator = combiner.add(key, accumulator, value);
          accumulatorSize = valueSizer.estimateSize(accumulator);
        }
      };
    }
  }


  ////////////////////////////////////////////////////////////////////////////
  // Size sampling.

  /**
   * Implements size estimation by adaptively delegating to an underlying
   * (potentially more expensive) estimator for some elements and returning
   * the average value for others.
   */
  public static class SamplingSizeEstimator<E> implements SizeEstimator<E> {

    /**
     * The degree of confidence required in our expected value predictions
     * before we allow under-sampling.
     *
     * <p> The value of 3.0 is a confidence interval of about 99.7% for a
     * a high-degree-of-freedom t-distribution.
     */
    public static final double CONFIDENCE_INTERVAL_SIGMA = 3;

    /**
     * The desired size of our confidence interval (relative to the measured
     * expected value).
     *
     * <p> The value of 0.25 is plus or minus 25%.
     */
    public static final double CONFIDENCE_INTERVAL_SIZE = 0.25;

    /**
     * Default number of elements that must be measured before elements are skipped.
     */
    public static final long DEFAULT_MIN_SAMPLED = 20;

    private final SizeEstimator<E> underlying;
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

    public SamplingSizeEstimator(
        SizeEstimator<E> underlying,
        double minSampleRate,
        double maxSampleRate) {
      this(underlying, minSampleRate, maxSampleRate, DEFAULT_MIN_SAMPLED, new Random());
    }

    public SamplingSizeEstimator(SizeEstimator<E> underlying,
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
    public long estimateSize(E element) throws Exception {
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
      estimate = (long) Math.ceil(sampledSum / sampledElements);
      long target = desiredSampleSize();
      if (sampledElements < minSampled || sampledElements < target) {
        // Sample immediately.
        nextSample = 0;
      } else {
        double rate = cap(
            minSampleRate,
            maxSampleRate,
            Math.max(1.0 / (totalElements - minSampled + 1), // slowly ramp down
                     target / (double) totalElements));      // "future" target
        // Uses the geometric distribution to return the likely distance between
        // successive independent trials of a fixed probability p. This gives the
        // same uniform distribution of branching on Math.random() < p, but with
        // one random number generation per success rather than one per test,
        // which can be a significant savings if p is small.
        nextSample = rate == 1.0
            ? 0
            : (long) Math.floor(Math.log(random.nextDouble()) / Math.log(1 - rate));
      }
      return value;
    }

    private static final double cap(double min, double max, double value) {
      return Math.min(max, Math.max(min, value));
    }

    private long desiredSampleSize() {
      // We have no a-priori information on the actual distribution of data
      // sizes, so compute our desired sample as if it were normal.
      // Yes this formula is unstable for small stddev, but we only care about large stddev.
      double mean = sampledSum / (double) sampledElements;
      double sumSquareDiff =
          (sampledSumSquares - (2 * mean * sampledSum) + (sampledElements * mean * mean));
      double stddev = Math.sqrt(sumSquareDiff / (sampledElements - 1));
      double sqrtDesiredSamples =
          (CONFIDENCE_INTERVAL_SIGMA * stddev) / (CONFIDENCE_INTERVAL_SIZE * mean);
      return (long) Math.ceil(sqrtDesiredSamples * sqrtDesiredSamples);
    }
  }
}
