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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;
import org.apache.beam.sdk.transforms.Combine.PerKey;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.NameUtils.NameOverride;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code PTransform}s for finding the largest (or smallest) set of elements in a {@code
 * PCollection}, or the largest (or smallest) set of values associated with each key in a {@code
 * PCollection} of {@code KV}s.
 */
public class Top {

  private Top() {
    // do not instantiate
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<List<T>>} with a single element containing the largest {@code count} elements of
   * the input {@code PCollection<T>}, in decreasing order, sorted using the given {@code
   * Comparator<T>}. The {@code Comparator<T>} must also be {@code Serializable}.
   *
   * <p>If {@code count} {@code >} the number of elements in the input {@code PCollection}, then all
   * the elements of the input {@code PCollection} will be in the resulting {@code List}, albeit in
   * sorted order.
   *
   * <p>All the elements of the result's {@code List} must fit into the memory of a single machine.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Student> students = ...;
   * PCollection<List<Student>> top10Students =
   *     students.apply(Top.of(10, new CompareStudentsByAvgGrade()));
   * }</pre>
   *
   * <p>By default, the {@code Coder} of the output {@code PCollection} is a {@code ListCoder} of
   * the {@code Coder} of the elements of the input {@code PCollection}.
   *
   * <p>If the input {@code PCollection} is windowed into {@link GlobalWindows}, an empty {@code
   * List<T>} in the {@link GlobalWindow} will be output if the input {@code PCollection} is empty.
   * To use this with inputs with other windowing, either {@link Combine.Globally#withoutDefaults
   * withoutDefaults} or {@link Combine.Globally#asSingletonView asSingletonView} must be called.
   *
   * <p>See also {@link #smallest} and {@link #largest}, which sort {@code Comparable} elements
   * using their natural ordering.
   *
   * <p>See also {@link #perKey}, {@link #smallestPerKey}, and {@link #largestPerKey}, which take a
   * {@code PCollection} of {@code KV}s and return the top values associated with each key.
   */
  public static <T, ComparatorT extends Comparator<T> & Serializable>
      Combine.Globally<T, List<T>> of(int count, ComparatorT compareFn) {
    return Combine.globally(new TopCombineFn<>(count, compareFn));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<List<T>>} with a single element containing the smallest {@code count} elements of
   * the input {@code PCollection<T>}, in increasing order, sorted according to their natural order.
   *
   * <p>If {@code count} {@code >} the number of elements in the input {@code PCollection}, then all
   * the elements of the input {@code PCollection} will be in the resulting {@code PCollection}'s
   * {@code List}, albeit in sorted order.
   *
   * <p>All the elements of the result {@code List} must fit into the memory of a single machine.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Integer> values = ...;
   * PCollection<List<Integer>> smallest10Values = values.apply(Top.smallest(10));
   * }</pre>
   *
   * <p>By default, the {@code Coder} of the output {@code PCollection} is a {@code ListCoder} of
   * the {@code Coder} of the elements of the input {@code PCollection}.
   *
   * <p>If the input {@code PCollection} is windowed into {@link GlobalWindows}, an empty {@code
   * List<T>} in the {@link GlobalWindow} will be output if the input {@code PCollection} is empty.
   * To use this with inputs with other windowing, either {@link Combine.Globally#withoutDefaults
   * withoutDefaults} or {@link Combine.Globally#asSingletonView asSingletonView} must be called.
   *
   * <p>See also {@link #largest}.
   *
   * <p>See also {@link #of}, which sorts using a user-specified {@code Comparator} function.
   *
   * <p>See also {@link #perKey}, {@link #smallestPerKey}, and {@link #largestPerKey}, which take a
   * {@code PCollection} of {@code KV}s and return the top values associated with each key.
   */
  public static <T extends Comparable<T>> Combine.Globally<T, List<T>> smallest(int count) {
    return Combine.globally(new TopCombineFn<>(count, new Reversed<T>()));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<List<T>>} with a single element containing the largest {@code count} elements of
   * the input {@code PCollection<T>}, in decreasing order, sorted according to their natural order.
   *
   * <p>If {@code count} {@code >} the number of elements in the input {@code PCollection}, then all
   * the elements of the input {@code PCollection} will be in the resulting {@code PCollection}'s
   * {@code List}, albeit in sorted order.
   *
   * <p>All the elements of the result's {@code List} must fit into the memory of a single machine.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Integer> values = ...;
   * PCollection<List<Integer>> largest10Values = values.apply(Top.largest(10));
   * }</pre>
   *
   * <p>By default, the {@code Coder} of the output {@code PCollection} is a {@code ListCoder} of
   * the {@code Coder} of the elements of the input {@code PCollection}.
   *
   * <p>If the input {@code PCollection} is windowed into {@link GlobalWindows}, an empty {@code
   * List<T>} in the {@link GlobalWindow} will be output if the input {@code PCollection} is empty.
   * To use this with inputs with other windowing, either {@link Combine.Globally#withoutDefaults
   * withoutDefaults} or {@link Combine.Globally#asSingletonView asSingletonView} must be called.
   *
   * <p>See also {@link #smallest}.
   *
   * <p>See also {@link #of}, which sorts using a user-specified {@code Comparator} function.
   *
   * <p>See also {@link #perKey}, {@link #smallestPerKey}, and {@link #largestPerKey}, which take a
   * {@code PCollection} of {@code KV}s and return the top values associated with each key.
   */
  public static <T extends Comparable<T>> Combine.Globally<T, List<T>> largest(int count) {
    return Combine.globally(largestFn(count));
  }

  /** Returns a {@link TopCombineFn} that aggregates the largest count values. */
  public static <T extends Comparable<T>> TopCombineFn<T, Natural<T>> largestFn(int count) {
    return new TopCombineFn<T, Natural<T>>(count, new Natural<T>()) {};
  }
  /** Returns a {@link TopCombineFn} that aggregates the largest count long values. */
  public static TopCombineFn<Long, Natural<Long>> largestLongsFn(int count) {
    return new TopCombineFn<Long, Natural<Long>>(count, new Natural<Long>()) {};
  }
  /** Returns a {@link TopCombineFn} that aggregates the largest count int values. */
  public static TopCombineFn<Integer, Natural<Integer>> largestIntsFn(int count) {
    return new TopCombineFn<Integer, Natural<Integer>>(count, new Natural<>()) {};
  }
  /** Returns a {@link TopCombineFn} that aggregates the largest count double values. */
  public static TopCombineFn<Double, Natural<Double>> largestDoublesFn(int count) {
    return new TopCombineFn<Double, Natural<Double>>(count, new Natural<>()) {};
  }
  /** Returns a {@link TopCombineFn} that aggregates the smallest count values. */
  public static <T extends Comparable<T>> TopCombineFn<T, Reversed<T>> smallestFn(int count) {
    return new TopCombineFn<T, Reversed<T>>(count, new Reversed<>()) {};
  }
  /** Returns a {@link TopCombineFn} that aggregates the smallest count long values. */
  public static TopCombineFn<Long, Reversed<Long>> smallestLongsFn(int count) {
    return new TopCombineFn<Long, Reversed<Long>>(count, new Reversed<>()) {};
  }
  /** Returns a {@link TopCombineFn} that aggregates the smallest count int values. */
  public static TopCombineFn<Integer, Reversed<Integer>> smallestIntsFn(int count) {
    return new TopCombineFn<Integer, Reversed<Integer>>(count, new Reversed<>()) {};
  }
  /** Returns a {@link TopCombineFn} that aggregates the smallest count double values. */
  public static TopCombineFn<Double, Reversed<Double>> smallestDoublesFn(int count) {
    return new TopCombineFn<Double, Reversed<Double>>(count, new Reversed<>()) {};
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, List<V>>>} that contains an output element mapping each distinct key
   * in the input {@code PCollection} to the largest {@code count} values associated with that key
   * in the input {@code PCollection<KV<K, V>>}, in decreasing order, sorted using the given {@code
   * Comparator<V>}. The {@code Comparator<V>} must also be {@code Serializable}.
   *
   * <p>If there are fewer than {@code count} values associated with a particular key, then all
   * those values will be in the result mapping for that key, albeit in sorted order.
   *
   * <p>All the values associated with a single key must fit into the memory of a single machine,
   * but there can be many more {@code KV}s in the resulting {@code PCollection} than can fit into
   * the memory of a single machine.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<KV<School, Student>> studentsBySchool = ...;
   * PCollection<KV<School, List<Student>>> top10StudentsBySchool =
   *     studentsBySchool.apply(
   *         Top.perKey(10, new CompareStudentsByAvgGrade()));
   * }</pre>
   *
   * <p>By default, the {@code Coder} of the keys of the output {@code PCollection} is the same as
   * that of the keys of the input {@code PCollection}, and the {@code Coder} of the values of the
   * output {@code PCollection} is a {@code ListCoder} of the {@code Coder} of the values of the
   * input {@code PCollection}.
   *
   * <p>See also {@link #smallestPerKey} and {@link #largestPerKey}, which sort {@code
   * Comparable<V>} values using their natural ordering.
   *
   * <p>See also {@link #of}, {@link #smallest}, and {@link #largest}, which take a {@code
   * PCollection} and return the top elements.
   */
  public static <K, V, ComparatorT extends Comparator<V> & Serializable>
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, List<V>>>> perKey(
          int count, ComparatorT compareFn) {
    return Combine.perKey(new TopCombineFn<>(count, compareFn));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, List<V>>>} that contains an output element mapping each distinct key
   * in the input {@code PCollection} to the smallest {@code count} values associated with that key
   * in the input {@code PCollection<KV<K, V>>}, in increasing order, sorted according to their
   * natural order.
   *
   * <p>If there are fewer than {@code count} values associated with a particular key, then all
   * those values will be in the result mapping for that key, albeit in sorted order.
   *
   * <p>All the values associated with a single key must fit into the memory of a single machine,
   * but there can be many more {@code KV}s in the resulting {@code PCollection} than can fit into
   * the memory of a single machine.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<KV<String, Integer>> keyedValues = ...;
   * PCollection<KV<String, List<Integer>>> smallest10ValuesPerKey =
   *     keyedValues.apply(Top.smallestPerKey(10));
   * }</pre>
   *
   * <p>By default, the {@code Coder} of the keys of the output {@code PCollection} is the same as
   * that of the keys of the input {@code PCollection}, and the {@code Coder} of the values of the
   * output {@code PCollection} is a {@code ListCoder} of the {@code Coder} of the values of the
   * input {@code PCollection}.
   *
   * <p>See also {@link #largestPerKey}.
   *
   * <p>See also {@link #perKey}, which sorts values using a user-specified {@code Comparator}
   * function.
   *
   * <p>See also {@link #of}, {@link #smallest}, and {@link #largest}, which take a {@code
   * PCollection} and return the top elements.
   */
  public static <K, V extends Comparable<V>>
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, List<V>>>> smallestPerKey(int count) {
    return Combine.perKey(smallestFn(count));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, List<V>>>} that contains an output element mapping each distinct key
   * in the input {@code PCollection} to the largest {@code count} values associated with that key
   * in the input {@code PCollection<KV<K, V>>}, in decreasing order, sorted according to their
   * natural order.
   *
   * <p>If there are fewer than {@code count} values associated with a particular key, then all
   * those values will be in the result mapping for that key, albeit in sorted order.
   *
   * <p>All the values associated with a single key must fit into the memory of a single machine,
   * but there can be many more {@code KV}s in the resulting {@code PCollection} than can fit into
   * the memory of a single machine.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<KV<String, Integer>> keyedValues = ...;
   * PCollection<KV<String, List<Integer>>> largest10ValuesPerKey =
   *     keyedValues.apply(Top.largestPerKey(10));
   * }</pre>
   *
   * <p>By default, the {@code Coder} of the keys of the output {@code PCollection} is the same as
   * that of the keys of the input {@code PCollection}, and the {@code Coder} of the values of the
   * output {@code PCollection} is a {@code ListCoder} of the {@code Coder} of the values of the
   * input {@code PCollection}.
   *
   * <p>See also {@link #smallestPerKey}.
   *
   * <p>See also {@link #perKey}, which sorts values using a user-specified {@code Comparator}
   * function.
   *
   * <p>See also {@link #of}, {@link #smallest}, and {@link #largest}, which take a {@code
   * PCollection} and return the top elements.
   */
  public static <K, V extends Comparable<V>> PerKey<K, V, List<V>> largestPerKey(int count) {
    return Combine.perKey(largestFn(count));
  }

  /** @deprecated use {@link Natural} instead */
  @Deprecated
  public static class Largest<T extends Comparable<? super T>>
      implements Comparator<T>, Serializable {
    @Override
    public int compare(T a, T b) {
      return a.compareTo(b);
    }
  }

  /**
   * A {@code Serializable} {@code Comparator} that that uses the compared elements' natural
   * ordering.
   */
  public static class Natural<T extends Comparable<? super T>>
      implements Comparator<T>, Serializable {
    @Override
    public int compare(T a, T b) {
      return a.compareTo(b);
    }
  }

  /** @deprecated use {@link Reversed} instead */
  @Deprecated
  public static class Smallest<T extends Comparable<? super T>>
      implements Comparator<T>, Serializable {
    @Override
    public int compare(T a, T b) {
      return b.compareTo(a);
    }
  }

  /**
   * {@code Serializable} {@code Comparator} that that uses the reverse of the compared elements'
   * natural ordering.
   */
  public static class Reversed<T extends Comparable<? super T>>
      implements Comparator<T>, Serializable {
    @Override
    public int compare(T a, T b) {
      return b.compareTo(a);
    }
  }

  ////////////////////////////////////////////////////////////////////////////

  /**
   * {@code CombineFn} for {@code Top} transforms that combines a bunch of {@code T}s into a single
   * {@code count}-long {@code List<T>}, using {@code compareFn} to choose the largest {@code T}s.
   *
   * @param <T> type of element being compared
   */
  public static class TopCombineFn<T, ComparatorT extends Comparator<T> & Serializable>
      extends AccumulatingCombineFn<T, BoundedHeap<T, ComparatorT>, List<T>>
      implements NameOverride {

    private final int count;
    private final ComparatorT compareFn;

    public TopCombineFn(int count, ComparatorT compareFn) {
      checkArgument(count >= 0, "count must be >= 0 (not %s)", count);
      this.count = count;
      this.compareFn = compareFn;
    }

    @Override
    public String getNameOverride() {
      return String.format("Top(%s)", NameUtils.approximateSimpleName(compareFn));
    }

    @Override
    public BoundedHeap<T, ComparatorT> createAccumulator() {
      return new BoundedHeap<>(count, compareFn, new ArrayList<>());
    }

    @Override
    public Coder<BoundedHeap<T, ComparatorT>> getAccumulatorCoder(
        CoderRegistry registry, Coder<T> inputCoder) {
      return new BoundedHeapCoder<>(count, compareFn, inputCoder);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("count", count).withLabel("Top Count"))
          .add(DisplayData.item("comparer", compareFn.getClass()).withLabel("Record Comparer"));
    }

    @Override
    public String getIncompatibleGlobalWindowErrorMessage() {
      return "Default values are not supported in Top.[of, smallest, largest]() if the input "
          + "PCollection is not windowed by GlobalWindows. Instead, use "
          + "Top.[of, smallest, largest]().withoutDefaults() to output an empty PCollection if the "
          + "input PCollection is empty, or Top.[of, smallest, largest]().asSingletonView() to "
          + "get a PCollection containing the empty list if the input PCollection is empty.";
    }
  }

  /**
   * A heap that stores only a finite number of top elements according to its provided {@code
   * Comparator}. Implemented as an {@link Accumulator} to facilitate implementation of {@link Top}.
   *
   * <p>This class is <i>not</i> safe for multithreaded use, except read-only.
   */
  static class BoundedHeap<T, ComparatorT extends Comparator<T> & Serializable>
      implements Accumulator<T, BoundedHeap<T, ComparatorT>, List<T>> {

    /**
     * A queue with smallest at the head, for quick adds.
     *
     * <p>Only one of asList and asQueue may be non-null.
     */
    private @Nullable PriorityQueue<T> asQueue;

    /**
     * A list in with largest first, the form of extractOutput().
     *
     * <p>Only one of asList and asQueue may be non-null.
     */
    private @Nullable List<T> asList;

    /** The user-provided Comparator. */
    private final ComparatorT compareFn;

    /** The maximum size of the heap. */
    private final int maximumSize;

    /** Creates a new heap with the provided size, comparator, and initial elements. */
    private BoundedHeap(int maximumSize, ComparatorT compareFn, List<T> asList) {
      this.maximumSize = maximumSize;
      this.asList = asList;
      this.compareFn = compareFn;
    }

    @Override
    public void addInput(T value) {
      maybeAddInput(value);
    }

    /**
     * Adds {@code value} to this heap if it is larger than any of the current elements. Returns
     * {@code true} if {@code value} was added.
     */
    private boolean maybeAddInput(T value) {
      if (maximumSize == 0) {
        // Don't add anything.
        return false;
      }

      // If asQueue == null, then this is the first add after the latest call to the
      // constructor or asList().
      if (asQueue == null) {
        asQueue = new PriorityQueue<>(maximumSize, compareFn);
        for (T item : asList) {
          asQueue.add(item);
        }
        asList = null;
      }

      if (asQueue.size() < maximumSize) {
        asQueue.add(value);
        return true;
      } else if (compareFn.compare(value, asQueue.peek()) > 0) {
        asQueue.poll();
        asQueue.add(value);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void mergeAccumulator(BoundedHeap<T, ComparatorT> accumulator) {
      for (T value : accumulator.asList()) {
        if (!maybeAddInput(value)) {
          // If this element of accumulator does not make the top N, neither
          // will the rest, which are all smaller.
          break;
        }
      }
    }

    @Override
    public List<T> extractOutput() {
      return asList();
    }

    /** Returns the contents of this Heap as a List sorted largest-to-smallest. */
    private List<T> asList() {
      if (asList == null) {
        List<T> smallestFirstList = Lists.newArrayListWithCapacity(asQueue.size());
        while (!asQueue.isEmpty()) {
          smallestFirstList.add(asQueue.poll());
        }
        asList = Lists.reverse(smallestFirstList);
        asQueue = null;
      }
      return asList;
    }
  }

  /** A {@link Coder} for {@link BoundedHeap}, using Java serialization via {@link CustomCoder}. */
  private static class BoundedHeapCoder<T, ComparatorT extends Comparator<T> & Serializable>
      extends CustomCoder<BoundedHeap<T, ComparatorT>> {
    private final Coder<List<T>> listCoder;
    private final ComparatorT compareFn;
    private final int maximumSize;

    public BoundedHeapCoder(int maximumSize, ComparatorT compareFn, Coder<T> elementCoder) {
      listCoder = ListCoder.of(elementCoder);
      this.compareFn = compareFn;
      this.maximumSize = maximumSize;
    }

    @Override
    public void encode(BoundedHeap<T, ComparatorT> value, OutputStream outStream)
        throws CoderException, IOException {
      listCoder.encode(value.asList(), outStream);
    }

    @Override
    public BoundedHeap<T, ComparatorT> decode(InputStream inStream)
        throws CoderException, IOException {
      return new BoundedHeap<>(maximumSize, compareFn, listCoder.decode(inStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(this, "HeapCoder requires a deterministic list coder", listCoder);
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(BoundedHeap<T, ComparatorT> value) {
      return listCoder.isRegisterByteSizeObserverCheap(value.asList());
    }

    @Override
    public void registerByteSizeObserver(
        BoundedHeap<T, ComparatorT> value, ElementByteSizeObserver observer) throws Exception {
      listCoder.registerByteSizeObserver(value.asList(), observer);
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (!(other instanceof BoundedHeapCoder)) {
        return false;
      }
      BoundedHeapCoder<?, ?> that = (BoundedHeapCoder<?, ?>) other;
      return Objects.equals(this.compareFn, that.compareFn)
          && Objects.equals(this.listCoder, that.listCoder)
          && this.maximumSize == that.maximumSize;
    }

    @Override
    public int hashCode() {
      return Objects.hash(compareFn, listCoder, maximumSize);
    }
  }
}
