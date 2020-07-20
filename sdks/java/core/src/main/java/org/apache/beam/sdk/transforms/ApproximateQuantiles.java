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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.UnmodifiableIterator;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code PTransform}s for getting an idea of a {@code PCollection}'s data distribution using
 * approximate {@code N}-tiles (e.g. quartiles, percentiles, etc.), either globally or per-key.
 */
public class ApproximateQuantiles {
  private ApproximateQuantiles() {
    // do not instantiate
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<T>} and returns a {@code
   * PCollection<List<T>>} whose single value is a {@code List} of the approximate {@code N}-tiles
   * of the elements of the input {@code PCollection}. This gives an idea of the distribution of the
   * input elements.
   *
   * <p>The computed {@code List} is of size {@code numQuantiles}, and contains the input elements'
   * minimum value, {@code numQuantiles-2} intermediate values, and maximum value, in sorted order,
   * using the given {@code Comparator} to order values. To compute traditional {@code N}-tiles, one
   * should use {@code ApproximateQuantiles.globally(N+1, compareFn)}.
   *
   * <p>If there are fewer input elements than {@code numQuantiles}, then the result {@code List}
   * will contain all the input elements, in sorted order.
   *
   * <p>The argument {@code Comparator} must be {@code Serializable}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> pc = ...;
   * PCollection<List<String>> quantiles =
   *     pc.apply(ApproximateQuantiles.globally(11, stringCompareFn));
   * }</pre>
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   * @param numQuantiles the number of elements in the resulting quantile values {@code List}
   * @param compareFn the function to use to order the elements
   */
  public static <T, ComparatorT extends Comparator<T> & Serializable>
      PTransform<PCollection<T>, PCollection<List<T>>> globally(
          int numQuantiles, ComparatorT compareFn) {
    return Combine.globally(ApproximateQuantilesCombineFn.create(numQuantiles, compareFn));
  }

  /**
   * Like {@link #globally(int, Comparator)}, but sorts using the elements' natural ordering.
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   * @param numQuantiles the number of elements in the resulting quantile values {@code List}
   */
  public static <T extends Comparable<T>> PTransform<PCollection<T>, PCollection<List<T>>> globally(
      int numQuantiles) {
    return Combine.globally(ApproximateQuantilesCombineFn.<T>create(numQuantiles));
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<KV<K, V>>} and returns a {@code
   * PCollection<KV<K, List<V>>>} that contains an output element mapping each distinct key in the
   * input {@code PCollection} to a {@code List} of the approximate {@code N}-tiles of the values
   * associated with that key in the input {@code PCollection}. This gives an idea of the
   * distribution of the input values for each key.
   *
   * <p>Each of the computed {@code List}s is of size {@code numQuantiles}, and contains the input
   * values' minimum value, {@code numQuantiles-2} intermediate values, and maximum value, in sorted
   * order, using the given {@code Comparator} to order values. To compute traditional {@code
   * N}-tiles, one should use {@code ApproximateQuantiles.perKey(compareFn, N+1)}.
   *
   * <p>If a key has fewer than {@code numQuantiles} values associated with it, then that key's
   * output {@code List} will contain all the key's input values, in sorted order.
   *
   * <p>The argument {@code Comparator} must be {@code Serializable}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<KV<Integer, String>> pc = ...;
   * PCollection<KV<Integer, List<String>>> quantilesPerKey =
   *     pc.apply(ApproximateQuantiles.<Integer, String>perKey(stringCompareFn, 11));
   * }</pre>
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   *
   * @param <K> the type of the keys in the input and output {@code PCollection}s
   * @param <V> the type of the values in the input {@code PCollection}
   * @param numQuantiles the number of elements in the resulting quantile values {@code List}
   * @param compareFn the function to use to order the elements
   */
  public static <K, V, ComparatorT extends Comparator<V> & Serializable>
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, List<V>>>> perKey(
          int numQuantiles, ComparatorT compareFn) {
    return Combine.perKey(ApproximateQuantilesCombineFn.create(numQuantiles, compareFn));
  }

  /**
   * Like {@link #perKey(int, Comparator)}, but sorts values using the their natural ordering.
   *
   * @param <K> the type of the keys in the input and output {@code PCollection}s
   * @param <V> the type of the values in the input {@code PCollection}
   * @param numQuantiles the number of elements in the resulting quantile values {@code List}
   */
  public static <K, V extends Comparable<V>>
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, List<V>>>> perKey(int numQuantiles) {
    return Combine.perKey(ApproximateQuantilesCombineFn.<V>create(numQuantiles));
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * The {@code ApproximateQuantilesCombineFn} combiner gives an idea of the distribution of a
   * collection of values using approximate {@code N}-tiles. The output of this combiner is a {@code
   * List} of size {@code numQuantiles}, containing the input values' minimum value, {@code
   * numQuantiles-2} intermediate values, and maximum value, in sorted order, so for traditional
   * {@code N}-tiles, one should use {@code ApproximateQuantilesCombineFn#create(N+1)}.
   *
   * <p>If there are fewer values to combine than {@code numQuantiles}, then the result {@code List}
   * will contain all the values being combined, in sorted order.
   *
   * <p>Values are ordered using either a specified {@code Comparator} or the values' natural
   * ordering.
   *
   * <p>To evaluate the quantiles we use the "New Algorithm" described here:
   *
   * <pre>
   *   [MRL98] Manku, Rajagopalan &amp; Lindsay, "Approximate Medians and other
   *   Quantiles in One Pass and with Limited Memory", Proc. 1998 ACM
   *   SIGMOD, Vol 27, No 2, p 426-435, June 1998.
   *   http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.6.6513&amp;rep=rep1&amp;type=pdf
   * </pre>
   *
   * <p>The default error bound is {@code 1 / N}, though in practice the accuracy tends to be much
   * better.
   *
   * <p>See {@link #create(int, Comparator, long, double)} for more information about the meaning of
   * {@code epsilon}, and {@link #withEpsilon} for a convenient way to adjust it.
   *
   * @param <T> the type of the values being combined
   */
  public static class ApproximateQuantilesCombineFn<
          T, ComparatorT extends Comparator<T> & Serializable>
      extends AccumulatingCombineFn<T, QuantileState<T, ComparatorT>, List<T>> {

    /**
     * The cost (in time and space) to compute quantiles to a given accuracy is a function of the
     * total number of elements in the data set. If an estimate is not known or specified, we use
     * this as an upper bound. If this is too low, errors may exceed the requested tolerance; if too
     * high, efficiency may be non-optimal. The impact is logarithmic with respect to this value, so
     * this default should be fine for most uses.
     */
    public static final long DEFAULT_MAX_NUM_ELEMENTS = (long) 1e9;

    /** The comparison function to use. */
    private final ComparatorT compareFn;

    /**
     * Number of quantiles to produce. The size of the final output list, including the minimum and
     * maximum, is numQuantiles.
     */
    private final int numQuantiles;

    /** The size of the buffers, corresponding to k in the referenced paper. */
    private final int bufferSize;

    /** The number of buffers, corresponding to b in the referenced paper. */
    private final int numBuffers;

    private final long maxNumElements;

    private ApproximateQuantilesCombineFn(
        int numQuantiles,
        ComparatorT compareFn,
        int bufferSize,
        int numBuffers,
        long maxNumElements) {
      checkArgument(numQuantiles >= 2);
      checkArgument(bufferSize >= 2);
      checkArgument(numBuffers >= 2);
      this.numQuantiles = numQuantiles;
      this.compareFn = compareFn;
      this.bufferSize = bufferSize;
      this.numBuffers = numBuffers;
      this.maxNumElements = maxNumElements;
    }

    /**
     * Returns an approximate quantiles combiner with the given {@code compareFn} and desired number
     * of quantiles. A total of {@code numQuantiles} elements will appear in the output list,
     * including the minimum and maximum.
     *
     * <p>The {@code Comparator} must be {@code Serializable}.
     *
     * <p>The default error bound is {@code 1 / numQuantiles}, which holds as long as the number of
     * elements is less than {@link #DEFAULT_MAX_NUM_ELEMENTS}.
     */
    public static <T, ComparatorT extends Comparator<T> & Serializable>
        ApproximateQuantilesCombineFn<T, ComparatorT> create(
            int numQuantiles, ComparatorT compareFn) {
      return create(numQuantiles, compareFn, DEFAULT_MAX_NUM_ELEMENTS, 1.0 / numQuantiles);
    }

    /** Like {@link #create(int, Comparator)}, but sorts values using their natural ordering. */
    public static <T extends Comparable<T>> ApproximateQuantilesCombineFn<T, Top.Natural<T>> create(
        int numQuantiles) {
      return create(numQuantiles, new Top.Natural<T>());
    }

    /**
     * Returns an {@code ApproximateQuantilesCombineFn} that's like this one except that it uses the
     * specified {@code epsilon} value. Does not modify this combiner.
     *
     * <p>See {@link #create(int, Comparator, long, double)} for more information about the meaning
     * of {@code epsilon}.
     */
    public ApproximateQuantilesCombineFn<T, ComparatorT> withEpsilon(double epsilon) {
      return create(numQuantiles, compareFn, maxNumElements, epsilon);
    }

    /**
     * Returns an {@code ApproximateQuantilesCombineFn} that's like this one except that it uses the
     * specified {@code maxNumElements} value. Does not modify this combiner.
     *
     * <p>See {@link #create(int, Comparator, long, double)} for more information about the meaning
     * of {@code maxNumElements}.
     */
    public ApproximateQuantilesCombineFn<T, ComparatorT> withMaxInputSize(long maxNumElements) {
      return create(numQuantiles, compareFn, maxNumElements, maxNumElements);
    }

    /**
     * Creates an approximate quantiles combiner with the given {@code compareFn} and desired number
     * of quantiles. A total of {@code numQuantiles} elements will appear in the output list,
     * including the minimum and maximum.
     *
     * <p>The {@code Comparator} must be {@code Serializable}.
     *
     * <p>The default error bound is {@code epsilon}, which holds as long as the number of elements
     * is less than {@code maxNumElements}. Specifically, if one considers the input as a sorted
     * list x_1, ..., x_N, then the distance between the each exact quantile x_c and its
     * approximation x_c' is bounded by {@code |c - c'| < epsilon * N}. Note that these errors are
     * worst-case scenarios; in practice the accuracy tends to be much better.
     */
    public static <T, ComparatorT extends Comparator<T> & Serializable>
        ApproximateQuantilesCombineFn<T, ComparatorT> create(
            int numQuantiles, ComparatorT compareFn, long maxNumElements, double epsilon) {
      // Compute optimal b and k.
      int b = 2;
      while ((b - 2) * (1 << (b - 2)) < epsilon * maxNumElements) {
        b++;
      }
      b--;
      int k = Math.max(2, (int) Math.ceil(maxNumElements / (float) (1 << (b - 1))));
      return new ApproximateQuantilesCombineFn<>(numQuantiles, compareFn, k, b, maxNumElements);
    }

    @Override
    public QuantileState<T, ComparatorT> createAccumulator() {
      return QuantileState.empty(compareFn, numQuantiles, numBuffers, bufferSize);
    }

    @Override
    public Coder<QuantileState<T, ComparatorT>> getAccumulatorCoder(
        CoderRegistry registry, Coder<T> elementCoder) {
      return new QuantileStateCoder<>(compareFn, elementCoder);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("numQuantiles", numQuantiles).withLabel("Quantile Count"))
          .add(DisplayData.item("comparer", compareFn.getClass()).withLabel("Record Comparer"));
    }

    int getNumBuffers() {
      return numBuffers;
    }

    int getBufferSize() {
      return bufferSize;
    }
  }

  /** Compact summarization of a collection on which quantiles can be estimated. */
  static class QuantileState<T, ComparatorT extends Comparator<T> & Serializable>
      implements Accumulator<T, QuantileState<T, ComparatorT>, List<T>> {

    private ComparatorT compareFn;
    private int numQuantiles;
    private int numBuffers;
    private int bufferSize;

    private @Nullable T min;

    private @Nullable T max;

    /** The set of buffers, ordered by level from smallest to largest. */
    private PriorityQueue<QuantileBuffer<T>> buffers;

    /**
     * The algorithm requires that the manipulated buffers always be filled to capacity to perform
     * the collapse operation. This operation can be extended to buffers of varying sizes by
     * introducing the notion of fractional weights, but it's easier to simply combine the
     * remainders from all shards into new, full buffers and then take them into account when
     * computing the final output.
     */
    private List<T> unbufferedElements = Lists.newArrayList();

    private QuantileState(
        ComparatorT compareFn,
        int numQuantiles,
        @Nullable T min,
        @Nullable T max,
        int numBuffers,
        int bufferSize,
        Collection<T> unbufferedElements,
        Collection<QuantileBuffer<T>> buffers) {
      this.compareFn = compareFn;
      this.numQuantiles = numQuantiles;
      this.numBuffers = numBuffers;
      this.bufferSize = bufferSize;
      this.buffers =
          new PriorityQueue<>(numBuffers + 1, (q1, q2) -> Integer.compare(q1.level, q2.level));
      this.min = min;
      this.max = max;
      this.unbufferedElements.addAll(unbufferedElements);
      this.buffers.addAll(buffers);
    }

    public static <T, ComparatorT extends Comparator<T> & Serializable>
        QuantileState<T, ComparatorT> empty(
            ComparatorT compareFn, int numQuantiles, int numBuffers, int bufferSize) {
      return new QuantileState<>(
          compareFn,
          numQuantiles,
          null, /* min */
          null, /* max */
          numBuffers,
          bufferSize,
          Collections.emptyList(),
          Collections.emptyList());
    }

    public static <T, ComparatorT extends Comparator<T> & Serializable>
        QuantileState<T, ComparatorT> singleton(
            ComparatorT compareFn, int numQuantiles, T elem, int numBuffers, int bufferSize) {
      return new QuantileState<>(
          compareFn,
          numQuantiles,
          elem, /* min */
          elem, /* max */
          numBuffers,
          bufferSize,
          Collections.singletonList(elem),
          Collections.emptyList());
    }

    /** Add a new element to the collection being summarized by this state. */
    @Override
    public void addInput(T elem) {
      if (isEmpty()) {
        min = max = elem;
      } else if (compareFn.compare(elem, min) < 0) {
        min = elem;
      } else if (compareFn.compare(elem, max) > 0) {
        max = elem;
      }
      addUnbuffered(elem);
    }

    /** Add a new buffer to the unbuffered list, creating a new buffer and collapsing if needed. */
    private void addUnbuffered(T elem) {
      unbufferedElements.add(elem);
      if (unbufferedElements.size() == bufferSize) {
        unbufferedElements.sort(compareFn);
        buffers.add(new QuantileBuffer<>(unbufferedElements));
        unbufferedElements = Lists.newArrayListWithCapacity(bufferSize);
        collapseIfNeeded();
      }
    }

    /**
     * Updates this as if adding all elements seen by other.
     *
     * <p>Note that this ignores the {@code Comparator} of the other {@link QuantileState}. In
     * practice, they should generally be equal, but this method tolerates a mismatch.
     */
    @Override
    public void mergeAccumulator(QuantileState<T, ComparatorT> other) {
      if (other.isEmpty()) {
        return;
      }
      if (min == null || compareFn.compare(other.min, min) < 0) {
        min = other.min;
      }
      if (max == null || compareFn.compare(other.max, max) > 0) {
        max = other.max;
      }
      for (T elem : other.unbufferedElements) {
        addUnbuffered(elem);
      }
      buffers.addAll(other.buffers);
      collapseIfNeeded();
    }

    public boolean isEmpty() {
      return unbufferedElements.isEmpty() && buffers.isEmpty();
    }

    private void collapseIfNeeded() {
      while (buffers.size() > numBuffers) {
        List<QuantileBuffer<T>> toCollapse = Lists.newArrayList();
        toCollapse.add(buffers.poll());
        toCollapse.add(buffers.poll());
        int minLevel = toCollapse.get(1).level;
        while (!buffers.isEmpty() && buffers.peek().level == minLevel) {
          toCollapse.add(buffers.poll());
        }
        buffers.add(collapse(toCollapse));
      }
    }

    private QuantileBuffer<T> collapse(Iterable<QuantileBuffer<T>> buffers) {
      int newLevel = 0;
      long newWeight = 0;
      for (QuantileBuffer<T> buffer : buffers) {
        // As presented in the paper, there should always be at least two
        // buffers of the same (minimal) level to collapse, but it is possible
        // to violate this condition when combining buffers from independently
        // computed shards.  If they differ we take the max.
        newLevel = Math.max(newLevel, buffer.level + 1);
        newWeight += buffer.weight;
      }
      List<T> newElements = interpolate(buffers, bufferSize, newWeight, offset(newWeight));
      return new QuantileBuffer<>(newLevel, newWeight, newElements);
    }

    /**
     * If the weight is even, we must round up or down. Alternate between these two options to avoid
     * a bias.
     */
    private long offset(long newWeight) {
      if (newWeight % 2 == 1) {
        return (newWeight + 1) / 2;
      } else {
        offsetJitter = 2 - offsetJitter;
        return (newWeight + offsetJitter) / 2;
      }
    }

    /** For alternating between biasing up and down in the above even weight collapse operation. */
    private int offsetJitter = 0;

    /**
     * Emulates taking the ordered union of all elements in buffers, repeated according to their
     * weight, and picking out the (k * step + offset)-th elements of this list for {@code 0 &lt;= k
     * &lt; count}.
     */
    private List<T> interpolate(
        Iterable<QuantileBuffer<T>> buffers, int count, double step, double offset) {
      List<Iterator<WeightedValue<T>>> iterators = Lists.newArrayList();
      for (QuantileBuffer<T> buffer : buffers) {
        iterators.add(buffer.sizedIterator());
      }
      // Each of the buffers is already sorted by element.
      Iterator<WeightedValue<T>> sorted =
          Iterators.mergeSorted(iterators, (a, b) -> compareFn.compare(a.getValue(), b.getValue()));

      List<T> newElements = Lists.newArrayListWithCapacity(count);
      WeightedValue<T> weightedElement = sorted.next();
      double current = weightedElement.getWeight();
      for (int j = 0; j < count; j++) {
        double target = j * step + offset;
        while (current <= target && sorted.hasNext()) {
          weightedElement = sorted.next();
          current += weightedElement.getWeight();
        }
        newElements.add(weightedElement.getValue());
      }
      return newElements;
    }

    /**
     * Outputs numQuantiles elements consisting of the minimum, maximum, and numQuantiles - 2 evenly
     * spaced intermediate elements.
     *
     * <p>Returns the empty list if no elements have been added.
     */
    @Override
    public List<T> extractOutput() {
      if (isEmpty()) {
        return Lists.newArrayList();
      }
      long totalCount = unbufferedElements.size();
      for (QuantileBuffer<T> buffer : buffers) {
        totalCount += bufferSize * buffer.weight;
      }
      List<QuantileBuffer<T>> all = Lists.newArrayList(buffers);
      if (!unbufferedElements.isEmpty()) {
        unbufferedElements.sort(compareFn);
        all.add(new QuantileBuffer<>(unbufferedElements));
      }
      double step = 1.0 * totalCount / (numQuantiles - 1);
      double offset = (1.0 * totalCount - 1) / (numQuantiles - 1);
      List<T> quantiles = interpolate(all, numQuantiles - 2, step, offset);
      quantiles.add(0, min);
      quantiles.add(max);
      return quantiles;
    }
  }

  /** A single buffer in the sense of the referenced algorithm. */
  private static class QuantileBuffer<T> {
    private int level;
    private long weight;
    private List<T> elements;

    public QuantileBuffer(List<T> elements) {
      this(0, 1, elements);
    }

    public QuantileBuffer(int level, long weight, List<T> elements) {
      this.level = level;
      this.weight = weight;
      this.elements = elements;
    }

    @Override
    public String toString() {
      return "QuantileBuffer["
          + "level="
          + level
          + ", weight="
          + weight
          + ", elements="
          + elements
          + "]";
    }

    public Iterator<WeightedValue<T>> sizedIterator() {
      return new UnmodifiableIterator<WeightedValue<T>>() {
        Iterator<T> iter = elements.iterator();

        @Override
        public boolean hasNext() {
          return iter.hasNext();
        }

        @Override
        public WeightedValue<T> next() {
          return WeightedValue.of(iter.next(), weight);
        }
      };
    }
  }

  /** Coder for QuantileState. */
  private static class QuantileStateCoder<T, ComparatorT extends Comparator<T> & Serializable>
      extends CustomCoder<QuantileState<T, ComparatorT>> {
    private final ComparatorT compareFn;
    private final Coder<T> elementCoder;
    private final Coder<List<T>> elementListCoder;
    private final Coder<Integer> intCoder = BigEndianIntegerCoder.of();

    public QuantileStateCoder(ComparatorT compareFn, Coder<T> elementCoder) {
      this.compareFn = compareFn;
      this.elementCoder = elementCoder;
      this.elementListCoder = ListCoder.of(elementCoder);
    }

    @Override
    public void encode(QuantileState<T, ComparatorT> state, OutputStream outStream)
        throws CoderException, IOException {
      intCoder.encode(state.numQuantiles, outStream);
      intCoder.encode(state.bufferSize, outStream);
      elementCoder.encode(state.min, outStream);
      elementCoder.encode(state.max, outStream);
      elementListCoder.encode(state.unbufferedElements, outStream);
      BigEndianIntegerCoder.of().encode(state.buffers.size(), outStream);
      for (QuantileBuffer<T> buffer : state.buffers) {
        encodeBuffer(buffer, outStream);
      }
    }

    @Override
    public QuantileState<T, ComparatorT> decode(InputStream inStream)
        throws CoderException, IOException {
      int numQuantiles = intCoder.decode(inStream);
      int bufferSize = intCoder.decode(inStream);
      T min = elementCoder.decode(inStream);
      T max = elementCoder.decode(inStream);
      List<T> unbufferedElements = elementListCoder.decode(inStream);
      int numBuffers = BigEndianIntegerCoder.of().decode(inStream);
      List<QuantileBuffer<T>> buffers = new ArrayList<>(numBuffers);
      for (int i = 0; i < numBuffers; i++) {
        buffers.add(decodeBuffer(inStream));
      }
      return new QuantileState<>(
          compareFn, numQuantiles, min, max, numBuffers, bufferSize, unbufferedElements, buffers);
    }

    private void encodeBuffer(QuantileBuffer<T> buffer, OutputStream outStream)
        throws CoderException, IOException {
      DataOutputStream outData = new DataOutputStream(outStream);
      outData.writeInt(buffer.level);
      outData.writeLong(buffer.weight);
      elementListCoder.encode(buffer.elements, outStream);
    }

    private QuantileBuffer<T> decodeBuffer(InputStream inStream)
        throws IOException, CoderException {
      DataInputStream inData = new DataInputStream(inStream);
      return new QuantileBuffer<>(
          inData.readInt(), inData.readLong(), elementListCoder.decode(inStream));
    }

    /**
     * Notifies ElementByteSizeObserver about the byte size of the encoded value using this coder.
     */
    @Override
    public void registerByteSizeObserver(
        QuantileState<T, ComparatorT> state, ElementByteSizeObserver observer) throws Exception {
      elementCoder.registerByteSizeObserver(state.min, observer);
      elementCoder.registerByteSizeObserver(state.max, observer);
      elementListCoder.registerByteSizeObserver(state.unbufferedElements, observer);

      BigEndianIntegerCoder.of().registerByteSizeObserver(state.buffers.size(), observer);
      for (QuantileBuffer<T> buffer : state.buffers) {
        observer.update(4L + 8);
        elementListCoder.registerByteSizeObserver(buffer.elements, observer);
      }
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (!(other instanceof QuantileStateCoder)) {
        return false;
      }
      QuantileStateCoder<?, ?> that = (QuantileStateCoder<?, ?>) other;
      return Objects.equals(this.elementCoder, that.elementCoder)
          && Objects.equals(this.compareFn, that.compareFn);
    }

    @Override
    public int hashCode() {
      return Objects.hash(elementCoder, compareFn);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(this, "QuantileState.ElementCoder must be deterministic", elementCoder);
      verifyDeterministic(
          this, "QuantileState.ElementListCoder must be deterministic", elementListCoder);
    }
  }
}
