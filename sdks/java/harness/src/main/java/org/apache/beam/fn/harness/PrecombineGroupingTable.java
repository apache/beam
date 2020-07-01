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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.apache.beam.runners.core.GlobalCombineFnRunner;
import org.apache.beam.runners.core.GlobalCombineFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CountingOutputStream;
import org.joda.time.Instant;

/** Static utility methods that provide {@link GroupingTable} implementations. */
public class PrecombineGroupingTable<K, InputT, AccumT>
    implements GroupingTable<K, InputT, AccumT> {
  private static long getGroupingTableSizeBytes(PipelineOptions options) {
    return options.as(SdkHarnessOptions.class).getGroupingTableMaxSizeMb() * 1024L * 1024L;
  }

  /** Returns a {@link GroupingTable} that combines inputs into a accumulator. */
  public static <K, InputT, AccumT> GroupingTable<WindowedValue<K>, InputT, AccumT> combining(
      PipelineOptions options,
      CombineFn<InputT, AccumT, ?> combineFn,
      Coder<K> keyCoder,
      Coder<? super AccumT> accumulatorCoder) {
    Combiner<WindowedValue<K>, InputT, AccumT, ?> valueCombiner =
        new ValueCombiner<>(
            GlobalCombineFnRunners.create(combineFn), NullSideInputReader.empty(), options);
    return new PrecombineGroupingTable<>(
        getGroupingTableSizeBytes(options),
        new WindowingCoderGroupingKeyCreator<>(keyCoder),
        WindowedPairInfo.create(),
        valueCombiner,
        new CoderSizeEstimator<>(WindowedValue.getValueOnlyCoder(keyCoder)),
        new CoderSizeEstimator<>(accumulatorCoder));
  }

  /**
   * Returns a {@link GroupingTable} that combines inputs into a accumulator with sampling {@link
   * SizeEstimator SizeEstimators}.
   */
  public static <K, InputT, AccumT>
      GroupingTable<WindowedValue<K>, InputT, AccumT> combiningAndSampling(
          PipelineOptions options,
          CombineFn<InputT, AccumT, ?> combineFn,
          Coder<K> keyCoder,
          Coder<? super AccumT> accumulatorCoder,
          double sizeEstimatorSampleRate) {
    Combiner<WindowedValue<K>, InputT, AccumT, ?> valueCombiner =
        new ValueCombiner<>(
            GlobalCombineFnRunners.create(combineFn), NullSideInputReader.empty(), options);
    return new PrecombineGroupingTable<>(
        getGroupingTableSizeBytes(options),
        new WindowingCoderGroupingKeyCreator<>(keyCoder),
        WindowedPairInfo.create(),
        valueCombiner,
        new SamplingSizeEstimator<>(
            new CoderSizeEstimator<>(WindowedValue.getValueOnlyCoder(keyCoder)),
            sizeEstimatorSampleRate,
            1.0),
        new SamplingSizeEstimator<>(
            new CoderSizeEstimator<>(accumulatorCoder), sizeEstimatorSampleRate, 1.0));
  }

  /** Provides client-specific operations for grouping keys. */
  public interface GroupingKeyCreator<K> {
    Object createGroupingKey(K key) throws Exception;
  }

  /** Implements Precombine GroupingKeyCreator via Coder. */
  public static class WindowingCoderGroupingKeyCreator<K>
      implements GroupingKeyCreator<WindowedValue<K>> {

    private static final Instant ignored = BoundedWindow.TIMESTAMP_MIN_VALUE;

    private final Coder<K> coder;

    WindowingCoderGroupingKeyCreator(Coder<K> coder) {
      this.coder = coder;
    }

    @Override
    public Object createGroupingKey(WindowedValue<K> key) {
      // Ignore timestamp for grouping purposes.
      // The Precombine output will inherit the timestamp of one of its inputs.
      return WindowedValue.of(
          coder.structuralValue(key.getValue()), ignored, key.getWindows(), key.getPane());
    }
  }

  /** Provides client-specific operations for size estimates. */
  public interface SizeEstimator<T> {
    long estimateSize(T element) throws Exception;
  }

  /** Implements SizeEstimator via Coder. */
  public static class CoderSizeEstimator<T> implements SizeEstimator<T> {
    /** Basic implementation of {@link ElementByteSizeObserver} for use in size estimation. */
    private static class Observer extends ElementByteSizeObserver {
      private long observedSize = 0;

      @Override
      protected void reportElementSize(long elementSize) {
        observedSize += elementSize;
      }
    }

    final Coder<T> coder;

    CoderSizeEstimator(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public long estimateSize(T value) throws Exception {
      // First try using byte size observer
      CoderSizeEstimator.Observer observer = new CoderSizeEstimator.Observer();
      coder.registerByteSizeObserver(value, observer);

      if (!observer.getIsLazy()) {
        observer.advance();
        return observer.observedSize;
      } else {
        // Coder byte size observation is lazy (requires iteration for observation) so fall back to
        // counting output stream
        CountingOutputStream os = new CountingOutputStream(ByteStreams.nullOutputStream());
        coder.encode(value, os);
        return os.getCount();
      }
    }
  }

  /**
   * Provides client-specific operations for working with elements that are key/value or key/values
   * pairs.
   */
  public interface PairInfo {
    Object getKeyFromInputPair(Object pair);

    Object getValueFromInputPair(Object pair);

    Object makeOutputPair(Object key, Object value);
  }

  /** Implements Precombine PairInfo via KVs. */
  public static class WindowedPairInfo implements PairInfo {
    private static WindowedPairInfo theInstance = new WindowedPairInfo();

    public static WindowedPairInfo create() {
      return theInstance;
    }

    private WindowedPairInfo() {}

    @Override
    public Object getKeyFromInputPair(Object pair) {
      @SuppressWarnings("unchecked")
      WindowedValue<KV<?, ?>> windowedKv = (WindowedValue<KV<?, ?>>) pair;
      return windowedKv.withValue(windowedKv.getValue().getKey());
    }

    @Override
    public Object getValueFromInputPair(Object pair) {
      @SuppressWarnings("unchecked")
      WindowedValue<KV<?, ?>> windowedKv = (WindowedValue<KV<?, ?>>) pair;
      return windowedKv.getValue().getValue();
    }

    @Override
    public Object makeOutputPair(Object key, Object values) {
      WindowedValue<?> windowedKey = (WindowedValue<?>) key;
      return windowedKey.withValue(KV.of(windowedKey.getValue(), values));
    }
  }

  /** Provides client-specific operations for combining values. */
  public interface Combiner<K, InputT, AccumT, OutputT> {
    AccumT createAccumulator(K key);

    AccumT add(K key, AccumT accumulator, InputT value);

    AccumT merge(K key, Iterable<AccumT> accumulators);

    AccumT compact(K key, AccumT accumulator);

    OutputT extract(K key, AccumT accumulator);
  }

  /** Implements Precombine Combiner via Combine.KeyedCombineFn. */
  public static class ValueCombiner<K, InputT, AccumT, OutputT>
      implements Combiner<WindowedValue<K>, InputT, AccumT, OutputT> {
    private final GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFn;
    private final SideInputReader sideInputReader;
    private final PipelineOptions options;

    private ValueCombiner(
        GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFn,
        SideInputReader sideInputReader,
        PipelineOptions options) {
      this.combineFn = combineFn;
      this.sideInputReader = sideInputReader;
      this.options = options;
    }

    @Override
    public AccumT createAccumulator(WindowedValue<K> windowedKey) {
      return this.combineFn.createAccumulator(options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public AccumT add(WindowedValue<K> windowedKey, AccumT accumulator, InputT value) {
      return this.combineFn.addInput(
          accumulator, value, options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public AccumT merge(WindowedValue<K> windowedKey, Iterable<AccumT> accumulators) {
      return this.combineFn.mergeAccumulators(
          accumulators, options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public AccumT compact(WindowedValue<K> windowedKey, AccumT accumulator) {
      return this.combineFn.compact(
          accumulator, options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public OutputT extract(WindowedValue<K> windowedKey, AccumT accumulator) {
      return this.combineFn.extractOutput(
          accumulator, options, sideInputReader, windowedKey.getWindows());
    }
  }

  // How many bytes a word in the JVM has.
  private static final int BYTES_PER_JVM_WORD = getBytesPerJvmWord();
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
  private static final int PER_KEY_OVERHEAD = 24 * BYTES_PER_JVM_WORD;

  /** A {@link GroupingTable} that uses the given combiner to combine values in place. */
  // Keep the table relatively full to increase the chance of collisions.
  private static final double TARGET_LOAD = 0.9;

  private long maxSize;
  private final GroupingKeyCreator<? super K> groupingKeyCreator;
  private final PairInfo pairInfo;
  private final Combiner<? super K, InputT, AccumT, ?> combiner;
  private final SizeEstimator<? super K> keySizer;
  private final SizeEstimator<? super AccumT> accumulatorSizer;

  private long size = 0;
  private Map<Object, GroupingTableEntry<K, InputT, AccumT>> table;

  PrecombineGroupingTable(
      long maxSize,
      GroupingKeyCreator<? super K> groupingKeyCreator,
      PairInfo pairInfo,
      Combiner<? super K, InputT, AccumT, ?> combineFn,
      SizeEstimator<? super K> keySizer,
      SizeEstimator<? super AccumT> accumulatorSizer) {
    this.maxSize = maxSize;
    this.groupingKeyCreator = groupingKeyCreator;
    this.pairInfo = pairInfo;
    this.combiner = combineFn;
    this.keySizer = keySizer;
    this.accumulatorSizer = accumulatorSizer;
    this.table = new HashMap<>();
  }

  interface GroupingTableEntry<K, InputT, AccumT> {
    K getKey();

    AccumT getValue();

    void add(InputT value) throws Exception;

    long getSize();

    void compact() throws Exception;
  }

  private GroupingTableEntry<K, InputT, AccumT> createTableEntry(final K key) throws Exception {
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
   * Adds the key and value to this table, possibly flushing some entries to output if the table is
   * full.
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
   * Output the given entry. Does not actually remove it from the table or update this table's size.
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
    static final double CONFIDENCE_INTERVAL_SIGMA = 3;

    /**
     * The desired size of our confidence interval (relative to the measured expected value).
     *
     * <p>The value of 0.25 is plus or minus 25%.
     */
    static final double CONFIDENCE_INTERVAL_SIZE = 0.25;

    /** Default number of elements that must be measured before elements are skipped. */
    static final long DEFAULT_MIN_SAMPLED = 20;

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
