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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/** Static utility methods that provide {@link GroupingTable} implementations. */
public class GroupingTables {
  /** Returns a {@link GroupingTable} that groups inputs into a {@link List}. */
  public static <K, V> GroupingTable<K, V, List<V>> buffering(
      GroupingKeyCreator<? super K> groupingKeyCreator,
      PairInfo pairInfo,
      SizeEstimator<? super K> keySizer,
      SizeEstimator<? super V> valueSizer) {
    return new BufferingGroupingTable<>(
        DEFAULT_MAX_GROUPING_TABLE_BYTES, groupingKeyCreator, pairInfo, keySizer, valueSizer);
  }

  /**
   * Returns a {@link GroupingTable} with a given max size that groups inputs into a {@link List}.
   */
  public static <K, V> GroupingTable<K, V, List<V>> buffering(
      Long maxTableSizeBytes,
      GroupingKeyCreator<? super K> groupingKeyCreator,
      PairInfo pairInfo,
      SizeEstimator<? super K> keySizer,
      SizeEstimator<? super V> valueSizer) {
    return new BufferingGroupingTable<>(
        maxTableSizeBytes, groupingKeyCreator, pairInfo, keySizer, valueSizer);
  }

  /**
   * Returns a {@link GroupingTable} that groups inputs into a {@link List} with sampling {@link
   * SizeEstimator SizeEstimators}.
   */
  public static <K, V> GroupingTable<K, V, List<V>> bufferingAndSampling(
      GroupingKeyCreator<? super K> groupingKeyCreator,
      PairInfo pairInfo,
      SizeEstimator<? super K> keySizer,
      SizeEstimator<? super V> valueSizer,
      double sizeEstimatorSampleRate,
      long maxSizeBytes) {
    return new BufferingGroupingTable<>(
        maxSizeBytes,
        groupingKeyCreator,
        pairInfo,
        new SamplingSizeEstimator<>(keySizer, sizeEstimatorSampleRate, 1.0),
        new SamplingSizeEstimator<>(valueSizer, sizeEstimatorSampleRate, 1.0));
  }

  /** Returns a {@link GroupingTable} that combines inputs into a accumulator. */
  public static <K, InputT, AccumT> GroupingTable<K, InputT, AccumT> combining(
      GroupingKeyCreator<? super K> groupingKeyCreator,
      PairInfo pairInfo,
      Combiner<? super K, InputT, AccumT, ?> combineFn,
      SizeEstimator<? super K> keySizer,
      SizeEstimator<? super AccumT> accumulatorSizer) {
    return new CombiningGroupingTable<>(
        DEFAULT_MAX_GROUPING_TABLE_BYTES,
        groupingKeyCreator,
        pairInfo,
        combineFn,
        keySizer,
        accumulatorSizer);
  }

  /**
   * Returns a {@link GroupingTable} that combines inputs into a accumulator with sampling {@link
   * SizeEstimator SizeEstimators}.
   */
  public static <K, InputT, AccumT> GroupingTable<K, InputT, AccumT> combiningAndSampling(
      GroupingKeyCreator<? super K> groupingKeyCreator,
      PairInfo pairInfo,
      Combiner<? super K, InputT, AccumT, ?> combineFn,
      SizeEstimator<? super K> keySizer,
      SizeEstimator<? super AccumT> accumulatorSizer,
      double sizeEstimatorSampleRate,
      long maxSizeBytes) {
    return new CombiningGroupingTable<>(
        maxSizeBytes,
        groupingKeyCreator,
        pairInfo,
        combineFn,
        new SamplingSizeEstimator<>(keySizer, sizeEstimatorSampleRate, 1.0),
        new SamplingSizeEstimator<>(accumulatorSizer, sizeEstimatorSampleRate, 1.0));
  }

  /** Provides client-specific operations for grouping keys. */
  public static interface GroupingKeyCreator<K> {
    public Object createGroupingKey(K key) throws Exception;
  }

  /** Provides client-specific operations for size estimates. */
  public static interface SizeEstimator<T> {
    public long estimateSize(T element) throws Exception;
  }

  /**
   * Provides client-specific operations for working with elements that are key/value or key/values
   * pairs.
   */
  public interface PairInfo {
    public Object getKeyFromInputPair(Object pair);

    public Object getValueFromInputPair(Object pair);

    public Object makeOutputPair(Object key, Object value);
  }

  /** Provides client-specific operations for combining values. */
  public interface Combiner<K, InputT, AccumT, OutputT> {
    public AccumT createAccumulator(K key);

    public AccumT add(K key, AccumT accumulator, InputT value);

    public AccumT merge(K key, Iterable<AccumT> accumulators);

    public AccumT compact(K key, AccumT accumulator);

    public OutputT extract(K key, AccumT accumulator);
  }

  // By default, how many bytes we allow the grouping table to consume before
  // it has to be flushed.
  static final long DEFAULT_MAX_GROUPING_TABLE_BYTES = 100_000_000L;

  // How many bytes a word in the JVM has.
  static final int BYTES_PER_JVM_WORD = getBytesPerJvmWord();
  /**
   * The number of bytes of overhead to store an entry in the grouping table (a {@code
   * HashMap<StructuralByteArray, KeyAndValues>}), ignoring the actual number of bytes in the keys
   * and values:
   *
   * <ul>
   *   <li>an array element (1 word),
   *   <li>a HashMap.Entry (4 words),
   *   <li>a StructuralByteArray (1 words),
   *   <li>a backing array (guessed at 1 word for the length),
   *   <li>a KeyAndValues (2 words),
   *   <li>an ArrayList (2 words),
   *   <li>a backing array (1 word),
   *   <li>per-object overhead (JVM-specific, guessed at 2 words * 6 objects).
   * </ul>
   */
  static final int PER_KEY_OVERHEAD = 24 * BYTES_PER_JVM_WORD;

  /**
   * A base class of {@link GroupingTable} that provides the implementation of {@link #put} and
   * {@link #flush}.
   *
   * <p>Subclasses override {@link #createTableEntry}.
   */
  @VisibleForTesting
  public abstract static class GroupingTableBase<K, InputT, AccumT>
      implements GroupingTable<K, InputT, AccumT> {
    // Keep the table relatively full to increase the chance of collisions.
    private static final double TARGET_LOAD = 0.9;

    private long maxSize;
    private final GroupingKeyCreator<? super K> groupingKeyCreator;
    private final PairInfo pairInfo;

    private long size = 0;
    private Map<Object, GroupingTableEntry<K, InputT, AccumT>> table;

    private GroupingTableBase(
        long maxSize, GroupingKeyCreator<? super K> groupingKeyCreator, PairInfo pairInfo) {
      this.maxSize = maxSize;
      this.groupingKeyCreator = groupingKeyCreator;
      this.pairInfo = pairInfo;
      this.table = new HashMap<>();
    }

    interface GroupingTableEntry<K, InputT, AccumT> {
      public K getKey();

      public AccumT getValue();

      public void add(InputT value) throws Exception;

      public long getSize();

      public void compact() throws Exception;
    }

    public abstract GroupingTableEntry<K, InputT, AccumT> createTableEntry(K key) throws Exception;

    /** Adds a pair to this table, possibly flushing some entries to output if the table is full. */
    @SuppressWarnings("unchecked")
    @Override
    public void put(Object pair, Receiver receiver) throws Exception {
      put(
          (K) pairInfo.getKeyFromInputPair(pair),
          (InputT) pairInfo.getValueFromInputPair(pair),
          receiver);
    }

    /**
     * Adds the key and value to this table, possibly flushing some entries to output if the table
     * is full.
     */
    public void put(K key, InputT value, Receiver receiver) throws Exception {
      Object groupingKey = groupingKeyCreator.createGroupingKey(key);
      GroupingTableEntry<K, InputT, AccumT> entry = table.get(groupingKey);
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
        Iterator<GroupingTableEntry<K, InputT, AccumT>> entries = table.values().iterator();
        while (size >= targetSize) {
          if (!entries.hasNext()) {
            // Should never happen, but sizes may be estimates...
            size = 0;
            break;
          }
          GroupingTableEntry<K, InputT, AccumT> toFlush = entries.next();
          entries.remove();
          size -= toFlush.getSize() + PER_KEY_OVERHEAD;
          output(toFlush, receiver);
        }
      }
    }

    /**
     * Output the given entry. Does not actually remove it from the table or update this table's
     * size.
     */
    private void output(GroupingTableEntry<K, InputT, AccumT> entry, Receiver receiver)
        throws Exception {
      entry.compact();
      receiver.process(pairInfo.makeOutputPair(entry.getKey(), entry.getValue()));
    }

    /** Flushes all entries in this table to output. */
    @Override
    public void flush(Receiver output) throws Exception {
      for (GroupingTableEntry<K, InputT, AccumT> entry : table.values()) {
        output(entry, output);
      }
      table.clear();
      size = 0;
    }

    @VisibleForTesting
    public void setMaxSize(long maxSize) {
      this.maxSize = maxSize;
    }

    @VisibleForTesting
    public long size() {
      return size;
    }
  }

  /** A grouping table that simply buffers all inserted values in a list. */
  private static class BufferingGroupingTable<K, V> extends GroupingTableBase<K, V, List<V>> {

    public final SizeEstimator<? super K> keySizer;
    public final SizeEstimator<? super V> valueSizer;

    private BufferingGroupingTable(
        long maxSize,
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

        @Override
        public K getKey() {
          return key;
        }

        @Override
        public List<V> getValue() {
          return values;
        }

        @Override
        public long getSize() {
          return size;
        }

        @Override
        public void compact() {}

        @Override
        public void add(V value) throws Exception {
          values.add(value);
          size += BYTES_PER_JVM_WORD + valueSizer.estimateSize(value);
        }
      };
    }
  }

  /** A grouping table that uses the given combiner to combine values in place. */
  private static class CombiningGroupingTable<K, InputT, AccumT>
      extends GroupingTableBase<K, InputT, AccumT> {

    private final Combiner<? super K, InputT, AccumT, ?> combiner;
    private final SizeEstimator<? super K> keySizer;
    private final SizeEstimator<? super AccumT> accumulatorSizer;

    private CombiningGroupingTable(
        long maxSize,
        GroupingKeyCreator<? super K> groupingKeyCreator,
        PairInfo pairInfo,
        Combiner<? super K, InputT, AccumT, ?> combineFn,
        SizeEstimator<? super K> keySizer,
        SizeEstimator<? super AccumT> accumulatorSizer) {
      super(maxSize, groupingKeyCreator, pairInfo);
      this.combiner = combineFn;
      this.keySizer = keySizer;
      this.accumulatorSizer = accumulatorSizer;
    }

    @Override
    public GroupingTableEntry<K, InputT, AccumT> createTableEntry(final K key) throws Exception {
      return new GroupingTableEntry<K, InputT, AccumT>() {
        final long keySize = keySizer.estimateSize(key);
        AccumT accumulator = combiner.createAccumulator(key);
        long accumulatorSize = 0; // never used before a value is added...

        @Override
        public K getKey() {
          return key;
        }

        @Override
        public AccumT getValue() {
          return accumulator;
        }

        @Override
        public long getSize() {
          return keySize + accumulatorSize;
        }

        @Override
        public void compact() throws Exception {
          AccumT newAccumulator = combiner.compact(key, accumulator);
          if (newAccumulator != accumulator) {
            accumulator = newAccumulator;
            accumulatorSize = accumulatorSizer.estimateSize(newAccumulator);
          }
        }

        @Override
        public void add(InputT value) throws Exception {
          accumulator = combiner.add(key, accumulator, value);
          accumulatorSize = accumulatorSizer.estimateSize(accumulator);
        }
      };
    }
  }

  /** Returns the number of bytes in a JVM word. In case we failed to find the answer, returns 8. */
  private static int getBytesPerJvmWord() {
    String wordSizeInBits = System.getProperty("sun.arch.data.model");
    try {
      return Integer.parseInt(wordSizeInBits) / 8;
    } catch (NumberFormatException e) {
      // The JVM word size is unknown.  Assume 64-bit.
      return 8;
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  // Size sampling.

  /**
   * Implements size estimation by adaptively delegating to an underlying (potentially more
   * expensive) estimator for some elements and returning the average value for others.
   */
  @VisibleForTesting
  static class SamplingSizeEstimator<T> implements SizeEstimator<T> {

    /**
     * The degree of confidence required in our expected value predictions before we allow
     * under-sampling.
     *
     * <p>The value of 3.0 is a confidence interval of about 99.7% for a a high-degree-of-freedom
     * t-distribution.
     */
    public static final double CONFIDENCE_INTERVAL_SIGMA = 3;

    /**
     * The desired size of our confidence interval (relative to the measured expected value).
     *
     * <p>The value of 0.25 is plus or minus 25%.
     */
    public static final double CONFIDENCE_INTERVAL_SIZE = 0.25;

    /** Default number of elements that must be measured before elements are skipped. */
    public static final long DEFAULT_MIN_SAMPLED = 20;

    private final SizeEstimator<T> underlying;
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
        SizeEstimator<T> underlying, double minSampleRate, double maxSampleRate) {
      this(underlying, minSampleRate, maxSampleRate, DEFAULT_MIN_SAMPLED, new Random());
    }

    @VisibleForTesting
    SamplingSizeEstimator(
        SizeEstimator<T> underlying,
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
    public long estimateSize(T element) throws Exception {
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
      estimate = (long) Math.ceil(sampledSum / (double) sampledElements);
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
