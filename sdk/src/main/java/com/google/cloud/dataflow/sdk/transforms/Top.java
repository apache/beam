/*
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
 */

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.CustomCoder;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine.AccumulatingCombineFn;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * {@code PTransform}s for finding the largest (or smallest) set
 * of elements in a {@code PCollection}, or the largest (or smallest)
 * set of values associated with each key in a {@code PCollection} of
 * {@code KV}s.
 */
public class Top {

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<T>} and returns a {@code PCollection<List<T>>} with a
   * single element containing the largest {@code count} elements of the input
   * {@code PCollection<T>}, in decreasing order, sorted using the
   * given {@code Comparator<T>}.  The {@code Comparator<T>} must also
   * be {@code Serializable}.
   *
   * <p> If {@code count} {@code <} the number of elements in the
   * input {@code PCollection}, then all the elements of the input
   * {@code PCollection} will be in the resulting
   * {@code List}, albeit in sorted order.
   *
   * <p> All the elements of the result's {@code List}
   * must fit into the memory of a single machine.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<Student> students = ...;
   * PCollection<List<Student>> top10Students =
   *     students.apply(Top.of(10, new CompareStudentsByAvgGrade()));
   * } </pre>
   *
   * <p> By default, the {@code Coder} of the output {@code PCollection}
   * is a {@code ListCoder} of the {@code Coder} of the elements of
   * the input {@code PCollection}.
   *
   * <p> See also {@link #smallest} and {@link #largest}, which sort
   * {@code Comparable} elements using their natural ordering.
   *
   * <p> See also {@link #perKey}, {@link #smallestPerKey}, and
   * {@link #largestPerKey} which take a {@code PCollection} of
   * {@code KV}s and return the top values associated with each key.
   */
  public static <T, C extends Comparator<T> & Serializable>
      PTransform<PCollection<T>, PCollection<List<T>>> of(int count, C compareFn) {
    return Combine.globally(new TopCombineFn<>(count, compareFn))
        .withName("Top");

  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<T>} and returns a {@code PCollection<List<T>>} with a
   * single element containing the smallest {@code count} elements of the input
   * {@code PCollection<T>}, in increasing order, sorted according to
   * their natural order.
   *
   * <p> If {@code count} {@code <} the number of elements in the
   * input {@code PCollection}, then all the elements of the input
   * {@code PCollection} will be in the resulting {@code PCollection}'s
   * {@code List}, albeit in sorted order.
   *
   * <p> All the elements of the result {@code List}
   * must fit into the memory of a single machine.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<Integer> values = ...;
   * PCollection<List<Integer>> smallest10Values = values.apply(Top.smallest(10));
   * } </pre>
   *
   * <p> By default, the {@code Coder} of the output {@code PCollection}
   * is a {@code ListCoder} of the {@code Coder} of the elements of
   * the input {@code PCollection}.
   *
   * <p> See also {@link #largest}.
   *
   * <p> See also {@link #of}, which sorts using a user-specified
   * {@code Comparator} function.
   *
   * <p> See also {@link #perKey}, {@link #smallestPerKey}, and
   * {@link #largestPerKey} which take a {@code PCollection} of
   * {@code KV}s and return the top values associated with each key.
   */
  public static <T extends Comparable<T>>
      PTransform<PCollection<T>, PCollection<List<T>>> smallest(int count) {
    return Combine.globally(new TopCombineFn<>(count, new Smallest<T>()))
        .withName("Top.Smallest");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<T>} and returns a {@code PCollection<List<T>>} with a
   * single element containing the largest {@code count} elements of the input
   * {@code PCollection<T>}, in decreasing order, sorted according to
   * their natural order.
   *
   * <p> If {@code count} {@code <} the number of elements in the
   * input {@code PCollection}, then all the elements of the input
   * {@code PCollection} will be in the resulting {@code PCollection}'s
   * {@code List}, albeit in sorted order.
   *
   * <p> All the elements of the result's {@code List}
   * must fit into the memory of a single machine.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<Integer> values = ...;
   * PCollection<List<Integer>> largest10Values = values.apply(Top.largest(10));
   * } </pre>
   *
   * <p> By default, the {@code Coder} of the output {@code PCollection}
   * is a {@code ListCoder} of the {@code Coder} of the elements of
   * the input {@code PCollection}.
   *
   * <p> See also {@link #smallest}.
   *
   * <p> See also {@link #of}, which sorts using a user-specified
   * {@code Comparator} function.
   *
   * <p> See also {@link #perKey}, {@link #smallestPerKey}, and
   * {@link #largestPerKey} which take a {@code PCollection} of
   * {@code KV}s and return the top values associated with each key.
   */
  public static <T extends Comparable<T>>
      PTransform<PCollection<T>, PCollection<List<T>>> largest(int count) {
    return Combine.globally(new TopCombineFn<>(count, new Largest<T>()))
        .withName("Top.Largest");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, List<V>>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the largest {@code count} values
   * associated with that key in the input
   * {@code PCollection<KV<K, V>>}, in decreasing order, sorted using
   * the given {@code Comparator<V>}.  The
   * {@code Comparator<V>} must also be {@code Serializable}.
   *
   * <p> If there are fewer than {@code count} values associated with
   * a particular key, then all those values will be in the result
   * mapping for that key, albeit in sorted order.
   *
   * <p> All the values associated with a single key must fit into the
   * memory of a single machine, but there can be many more
   * {@code KV}s in the resulting {@code PCollection} than can fit
   * into the memory of a single machine.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<KV<School, Student>> studentsBySchool = ...;
   * PCollection<KV<School, List<Student>>> top10StudentsBySchool =
   *     studentsBySchool.apply(
   *         Top.perKey(10, new CompareStudentsByAvgGrade()));
   * } </pre>
   *
   * <p> By default, the {@code Coder} of the keys of the output
   * {@code PCollection} is the same as that of the keys of the input
   * {@code PCollection}, and the {@code Coder} of the values of the
   * output {@code PCollection} is a {@code ListCoder} of the
   * {@code Coder} of the values of the input {@code PCollection}.
   *
   * <p> See also {@link #smallestPerKey} and {@link #largestPerKey},
   * which sort {@code Comparable<V>} values using their natural
   * ordering.
   *
   * <p> See also {@link #of}, {@link #smallest}, and {@link #largest}
   * which take a {@code PCollection} and return the top elements.
   */
  public static <K, V, C extends Comparator<V> & Serializable>
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, List<V>>>>
      perKey(int count, C compareFn) {
    return Combine.perKey(
        new TopCombineFn<>(count, compareFn).<K>asKeyedFn())
        .withName("Top.PerKey");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, List<V>>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the smallest {@code count} values
   * associated with that key in the input
   * {@code PCollection<KV<K, V>>}, in increasing order, sorted
   * according to their natural order.
   *
   * <p> If there are fewer than {@code count} values associated with
   * a particular key, then all those values will be in the result
   * mapping for that key, albeit in sorted order.
   *
   * <p> All the values associated with a single key must fit into the
   * memory of a single machine, but there can be many more
   * {@code KV}s in the resulting {@code PCollection} than can fit
   * into the memory of a single machine.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<KV<String, Integer>> keyedValues = ...;
   * PCollection<KV<String, List<Integer>>> smallest10ValuesPerKey =
   *     keyedValues.apply(Top.smallestPerKey(10));
   * } </pre>
   *
   * <p> By default, the {@code Coder} of the keys of the output
   * {@code PCollection} is the same as that of the keys of the input
   * {@code PCollection}, and the {@code Coder} of the values of the
   * output {@code PCollection} is a {@code ListCoder} of the
   * {@code Coder} of the values of the input {@code PCollection}.
   *
   * <p> See also {@link #largestPerKey}.
   *
   * <p> See also {@link #perKey}, which sorts values using a user-specified
   * {@code Comparator} function.
   *
   * <p> See also {@link #of}, {@link #smallest}, and {@link #largest}
   * which take a {@code PCollection} and return the top elements.
   */
  public static <K, V extends Comparable<V>>
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, List<V>>>>
      smallestPerKey(int count) {
    return Combine.perKey(
        new TopCombineFn<>(count, new Smallest<V>()).<K>asKeyedFn())
        .withName("Top.SmallestPerKey");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, List<V>>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the largest {@code count} values
   * associated with that key in the input
   * {@code PCollection<KV<K, V>>}, in decreasing order, sorted
   * according to their natural order.
   *
   * <p> If there are fewer than {@code count} values associated with
   * a particular key, then all those values will be in the result
   * mapping for that key, albeit in sorted order.
   *
   * <p> All the values associated with a single key must fit into the
   * memory of a single machine, but there can be many more
   * {@code KV}s in the resulting {@code PCollection} than can fit
   * into the memory of a single machine.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<KV<String, Integer>> keyedValues = ...;
   * PCollection<KV<String, List<Integer>>> largest10ValuesPerKey =
   *     keyedValues.apply(Top.largestPerKey(10));
   * } </pre>
   *
   * <p> By default, the {@code Coder} of the keys of the output
   * {@code PCollection} is the same as that of the keys of the input
   * {@code PCollection}, and the {@code Coder} of the values of the
   * output {@code PCollection} is a {@code ListCoder} of the
   * {@code Coder} of the values of the input {@code PCollection}.
   *
   * <p> See also {@link #smallestPerKey}.
   *
   * <p> See also {@link #perKey}, which sorts values using a user-specified
   * {@code Comparator} function.
   *
   * <p> See also {@link #of}, {@link #smallest}, and {@link #largest}
   * which take a {@code PCollection} and return the top elements.
   */
  public static <K, V extends Comparable<V>>
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, List<V>>>>
      largestPerKey(int count) {
    return Combine.perKey(
        new TopCombineFn<>(count, new Largest<V>()).<K>asKeyedFn())
        .withName("Top.LargestPerKey");
  }


  ////////////////////////////////////////////////////////////////////////////

  /**
   * {@code CombineFn} for {@code Top} transforms that combines a
   * bunch of {@code T}s into a single {@code count}-long
   * {@code List<T>}, using {@code compareFn} to choose the largest
   * {@code T}s.
   *
   * @param <T> type of element being compared
   */
  @SuppressWarnings("serial")
  public static class TopCombineFn<T>
      extends AccumulatingCombineFn<T, TopCombineFn<T>.Heap, List<T>> {

    private final int count;
    private final Comparator<T> compareFn;

    public <C extends Comparator<T> & Serializable> TopCombineFn(
        int count, C compareFn) {
      if (count < 0) {
        throw new IllegalArgumentException("count must be >= 0");
      }
      this.count = count;
      this.compareFn = compareFn;
    }

    class Heap implements AccumulatingCombineFn.Accumulator<T, TopCombineFn<T>.Heap, List<T>> {

      // Exactly one of these should be set.
      private List<T> asList;            // ordered largest first
      private PriorityQueue<T> asQueue;  // head is smallest

      private Heap(List<T> asList) {
        this.asList = asList;
      }

      @Override
      public void addInput(T value) {
        addInputInternal(value);
      }

      private boolean addInputInternal(T value) {
        if (count == 0) {
          // Don't add anything.
          return false;
        }

        if (asQueue == null) {
          asQueue = new PriorityQueue<>(count, compareFn);
          for (T item : asList) {
            asQueue.add(item);
          }
          asList = null;
        }

        if (asQueue.size() < count) {
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
      public void mergeAccumulator(Heap accumulator) {
        for (T value : accumulator.asList()) {
          if (!addInputInternal(value)) {
            // The list is ordered, remainder will also all be smaller.
            break;
          }
        }
      }

      @Override
      public List<T> extractOutput() {
        return asList();
      }

      private List<T> asList() {
        if (asList == null) {
          int index = asQueue.size();
          @SuppressWarnings("unchecked")
          T[] ordered = (T[]) new Object[index];
          while (!asQueue.isEmpty()) {
            index--;
            ordered[index] = asQueue.poll();
          }
          asList = Arrays.asList(ordered);
          asQueue = null;
        }
        return asList;
      }
    }

    @Override
    public Heap createAccumulator() {
      return new Heap(new ArrayList<T>());
    }

    @Override
    public Coder<Heap> getAccumulatorCoder(
        CoderRegistry registry, Coder<T> inputCoder) {
      return new HeapCoder(inputCoder);
    }

    @SuppressWarnings("serial")
    private class HeapCoder extends CustomCoder<Heap> {
      private final Coder<List<T>> listCoder;

      public HeapCoder(Coder<T> inputCoder) {
        listCoder = ListCoder.of(inputCoder);
      }

      @Override
      public void encode(Heap value, OutputStream outStream,
          Context context) throws CoderException, IOException {
        listCoder.encode(value.asList(), outStream, context);
      }

      @Override
      public Heap decode(InputStream inStream, Coder.Context context)
          throws CoderException, IOException {
        return new Heap(listCoder.decode(inStream, context));
      }

      @Override
      public boolean isDeterministic() {
        return listCoder.isDeterministic();
      }

      @Override
      public boolean isRegisterByteSizeObserverCheap(
          Heap value, Context context) {
        return listCoder.isRegisterByteSizeObserverCheap(
            value.asList(), context);
      }

      @Override
      public void registerByteSizeObserver(
          Heap value, ElementByteSizeObserver observer, Context context)
          throws Exception {
        listCoder.registerByteSizeObserver(value.asList(), observer, context);
      }
    }
  }

  /**
   * {@code Serializable} {@code Comparator} that that uses the
   * compared elements' natural ordering.
   */
  @SuppressWarnings("serial")
  public static class Largest<T extends Comparable<T>>
      implements Comparator<T>, Serializable {
    @Override
    public int compare(T a, T b) {
      return a.compareTo(b);
    }
  }

  /**
   * {@code Serializable} {@code Comparator} that that uses the
   * reverse of the compared elements' natural ordering.
   */
  @SuppressWarnings("serial")
  public static class Smallest<T extends Comparable<T>>
      implements Comparator<T>, Serializable {
    @Override
    public int compare(T a, T b) {
      return b.compareTo(a);
    }
  }
}
