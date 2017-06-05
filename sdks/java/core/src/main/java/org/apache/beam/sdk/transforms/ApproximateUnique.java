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

import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code PTransform}s for estimating the number of distinct elements
 * in a {@code PCollection}, or the number of distinct values
 * associated with each key in a {@code PCollection} of {@code KV}s.
 */
public class ApproximateUnique {

  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<T>}
   * and returns a {@code PCollection<Long>} containing a single value
   * that is an estimate of the number of distinct elements in the
   * input {@code PCollection}.
   *
   * <p>The {@code sampleSize} parameter controls the estimation
   * error.  The error is about {@code 2 / sqrt(sampleSize)}, so for
   * {@code ApproximateUnique.globally(10000)} the estimation error is
   * about 2%.  Similarly, for {@code ApproximateUnique.of(16)} the
   * estimation error is about 50%.  If there are fewer than
   * {@code sampleSize} distinct elements then the returned result
   * will be exact with extremely high probability (the chance of a
   * hash collision is about {@code sampleSize^2 / 2^65}).
   *
   * <p>This transform approximates the number of elements in a set
   * by computing the top {@code sampleSize} hash values, and using
   * that to extrapolate the size of the entire set of hash values by
   * assuming the rest of the hash values are as densely distributed
   * as the top {@code sampleSize}.
   *
   * <p>See also {@link #globally(double)}.
   *
   * <p>Example of use:
   * <pre> {@code
   * PCollection<String> pc = ...;
   * PCollection<Long> approxNumDistinct =
   *     pc.apply(ApproximateUnique.<String>globally(1000));
   * } </pre>
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   * @param sampleSize the number of entries in the statistical
   *        sample; the higher this number, the more accurate the
   *        estimate will be; should be {@code >= 16}
   * @throws IllegalArgumentException if the {@code sampleSize}
   *         argument is too small
   */
  public static <T> Globally<T> globally(int sampleSize) {
    return new Globally<>(sampleSize);
  }

  /**
   * Like {@link #globally(int)}, but specifies the desired maximum
   * estimation error instead of the sample size.
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   * @param maximumEstimationError the maximum estimation error, which
   *        should be in the range {@code [0.01, 0.5]}
   * @throws IllegalArgumentException if the
   *         {@code maximumEstimationError} argument is out of range
   */
  public static <T> Globally<T> globally(double maximumEstimationError) {
    return new Globally<>(maximumEstimationError);
  }

  /**
   * Returns a {@code PTransform} that takes a
   * {@code PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, Long>>} that contains an output element
   * mapping each distinct key in the input {@code PCollection} to an
   * estimate of the number of distinct values associated with that
   * key in the input {@code PCollection}.
   *
   * <p>See {@link #globally(int)} for an explanation of the
   * {@code sampleSize} parameter.  A separate sampling is computed
   * for each distinct key of the input.
   *
   * <p>See also {@link #perKey(double)}.
   *
   * <p>Example of use:
   * <pre> {@code
   * PCollection<KV<Integer, String>> pc = ...;
   * PCollection<KV<Integer, Long>> approxNumDistinctPerKey =
   *     pc.apply(ApproximateUnique.<Integer, String>perKey(1000));
   * } </pre>
   *
   * @param <K> the type of the keys in the input and output
   *        {@code PCollection}s
   * @param <V> the type of the values in the input {@code PCollection}
   * @param sampleSize the number of entries in the statistical
   *        sample; the higher this number, the more accurate the
   *        estimate will be; should be {@code >= 16}
   * @throws IllegalArgumentException if the {@code sampleSize}
   *         argument is too small
   */
  public static <K, V> PerKey<K, V> perKey(int sampleSize) {
    return new PerKey<>(sampleSize);
  }

  /**
   * Like {@link #perKey(int)}, but specifies the desired maximum
   * estimation error instead of the sample size.
   *
   * @param <K> the type of the keys in the input and output
   *        {@code PCollection}s
   * @param <V> the type of the values in the input {@code PCollection}
   * @param maximumEstimationError the maximum estimation error, which
   *        should be in the range {@code [0.01, 0.5]}
   * @throws IllegalArgumentException if the
   *         {@code maximumEstimationError} argument is out of range
   */
  public static <K, V> PerKey<K, V> perKey(double maximumEstimationError) {
    return new PerKey<>(maximumEstimationError);
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code PTransform} for estimating the number of distinct elements
   * in a {@code PCollection}.
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   */
  static class Globally<T> extends PTransform<PCollection<T>, PCollection<Long>> {

    /**
     * The number of entries in the statistical sample; the higher this number,
     * the more accurate the estimate will be.
     */
    private final long sampleSize;

    /**
     * The desired maximum estimation error or null if not specified.
     */
    @Nullable
    private final Double maximumEstimationError;

    /**
     * @see ApproximateUnique#globally(int)
     */
    public Globally(int sampleSize) {
      if (sampleSize < 16) {
        throw new IllegalArgumentException(
            "ApproximateUnique needs a sampleSize "
            + ">= 16 for an estimation error <= 50%.  "
            + "In general, the estimation "
            + "error is about 2 / sqrt(sampleSize).");
      }

      this.sampleSize = sampleSize;
      this.maximumEstimationError = null;
    }

    /**
     * @see ApproximateUnique#globally(double)
     */
    public Globally(double maximumEstimationError) {
      if (maximumEstimationError < 0.01 || maximumEstimationError > 0.5) {
        throw new IllegalArgumentException(
            "ApproximateUnique needs an "
            + "estimation error between 1% (0.01) and 50% (0.5).");
      }

      this.sampleSize = sampleSizeFromEstimationError(maximumEstimationError);
      this.maximumEstimationError = maximumEstimationError;
    }

    @Override
    public PCollection<Long> expand(PCollection<T> input) {
      Coder<T> coder = input.getCoder();
      return input.apply(
          Combine.globally(
              new ApproximateUniqueCombineFn<>(sampleSize, coder)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      ApproximateUnique.populateDisplayData(builder, sampleSize, maximumEstimationError);
    }
  }

  /**
   * {@code PTransform} for estimating the number of distinct values
   * associated with each key in a {@code PCollection} of {@code KV}s.
   *
   * @param <K> the type of the keys in the input and output
   *        {@code PCollection}s
   * @param <V> the type of the values in the input {@code PCollection}
   */
  static class PerKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Long>>> {

    /**
     * The number of entries in the statistical sample; the higher this number,
     * the more accurate the estimate will be.
     */
    private final long sampleSize;

    /**
     * The the desired maximum estimation error or null if not specified.
     */
    @Nullable
    private final Double maximumEstimationError;

    /**
     * @see ApproximateUnique#perKey(int)
     */
    public PerKey(int sampleSize) {
      if (sampleSize < 16) {
        throw new IllegalArgumentException(
            "ApproximateUnique needs a "
            + "sampleSize >= 16 for an estimation error <= 50%.  In general, "
            + "the estimation error is about 2 / sqrt(sampleSize).");
      }

      this.sampleSize = sampleSize;
      this.maximumEstimationError = null;
    }

    /**
     * @see ApproximateUnique#perKey(double)
     */
    public PerKey(double estimationError) {
      if (estimationError < 0.01 || estimationError > 0.5) {
        throw new IllegalArgumentException(
            "ApproximateUnique.PerKey needs an "
            + "estimation error between 1% (0.01) and 50% (0.5).");
      }

      this.sampleSize = sampleSizeFromEstimationError(estimationError);
      this.maximumEstimationError = estimationError;
    }

    @Override
    public PCollection<KV<K, Long>> expand(PCollection<KV<K, V>> input) {
      Coder<KV<K, V>> inputCoder = input.getCoder();
      if (!(inputCoder instanceof KvCoder)) {
        throw new IllegalStateException(
            "ApproximateUnique.PerKey requires its input to use KvCoder");
      }
      @SuppressWarnings("unchecked")
      final Coder<V> coder = ((KvCoder<K, V>) inputCoder).getValueCoder();

      return input.apply(
          Combine.<K, V, Long>perKey(new ApproximateUniqueCombineFn<>(sampleSize, coder)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      ApproximateUnique.populateDisplayData(builder, sampleSize, maximumEstimationError);
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code CombineFn} that computes an estimate of the number of
   * distinct values that were combined.
   *
   * <p>Hashes input elements, computes the top {@code sampleSize}
   * hash values, and uses those to extrapolate the size of the entire
   * set of hash values by assuming the rest of the hash values are as
   * densely distributed as the top {@code sampleSize}.
   *
   * <p>Used to implement
   * {@link #globally(int) ApproximatUnique.globally(...)} and
   * {@link #perKey(int) ApproximatUnique.perKey(...)}.
   *
   * @param <T> the type of the values being combined
   */
  public static class ApproximateUniqueCombineFn<T> extends
      CombineFn<T, ApproximateUniqueCombineFn.LargestUnique, Long> {

    /**
     * The size of the space of hashes returned by the hash function.
     */
    static final double HASH_SPACE_SIZE =
        Long.MAX_VALUE - (double) Long.MIN_VALUE;

    /**
     * A heap utility class to efficiently track the largest added elements.
     */
    public static class LargestUnique implements Serializable {
      private PriorityQueue<Long> heap = new PriorityQueue<>();
      private final long sampleSize;

      /**
       * Creates a heap to track the largest {@code sampleSize} elements.
       *
       * @param sampleSize the size of the heap
       */
      public LargestUnique(long sampleSize) {
        this.sampleSize = sampleSize;
      }

      /**
       * Adds a value to the heap, returning whether the value is (large enough
       * to be) in the heap.
       */
      public boolean add(Long value) {
        if (heap.contains(value)) {
          return true;
        } else if (heap.size() < sampleSize) {
          heap.add(value);
          return true;
        } else if (value > heap.element()) {
          heap.remove();
          heap.add(value);
          return true;
        } else {
          return false;
        }
      }

      /**
       * Returns the values in the heap, ordered largest to smallest.
       */
      public List<Long> extractOrderedList() {
        // The only way to extract the order from the heap is element-by-element
        // from smallest to largest.
        Long[] array = new Long[heap.size()];
        for (int i = heap.size() - 1; i >= 0; i--) {
          array[i] = heap.remove();
        }
        return Arrays.asList(array);
      }
    }

    private final long sampleSize;
    private final Coder<T> coder;

    public ApproximateUniqueCombineFn(long sampleSize, Coder<T> coder) {
      this.sampleSize = sampleSize;
      this.coder = coder;
    }

    @Override
    public LargestUnique createAccumulator() {
      return new LargestUnique(sampleSize);
    }

    @Override
    public LargestUnique addInput(LargestUnique heap, T input) {
      try {
        heap.add(hash(input, coder));
        return heap;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public LargestUnique mergeAccumulators(Iterable<LargestUnique> heaps) {
      Iterator<LargestUnique> iterator = heaps.iterator();
      LargestUnique heap = iterator.next();
      while (iterator.hasNext()) {
        List<Long> largestHashes = iterator.next().extractOrderedList();
        for (long hash : largestHashes) {
          if (!heap.add(hash)) {
            break; // The remainder of this list is all smaller.
          }
        }
      }
      return heap;
    }

    @Override
    public Long extractOutput(LargestUnique heap) {
      List<Long> largestHashes = heap.extractOrderedList();
      if (largestHashes.size() < sampleSize) {
        return (long) largestHashes.size();
      } else {
        long smallestSampleHash = largestHashes.get(largestHashes.size() - 1);
        double sampleSpaceSize = Long.MAX_VALUE - (double) smallestSampleHash;
        // This formula takes into account the possibility of hash collisions,
        // which become more likely than not for 2^32 distinct elements.
        // Note that log(1+x) ~ x for small x, so for sampleSize << maxHash
        // log(1 - sampleSize/sampleSpace) / log(1 - 1/sampleSpace) ~ sampleSize
        // and hence estimate ~ sampleSize * HASH_SPACE_SIZE / sampleSpace
        // as one would expect.
        double estimate = Math.log1p(-sampleSize / sampleSpaceSize)
            / Math.log1p(-1 / sampleSpaceSize)
            * HASH_SPACE_SIZE / sampleSpaceSize;
        return Math.round(estimate);
      }
    }

    @Override
    public Coder<LargestUnique> getAccumulatorCoder(CoderRegistry registry,
        Coder<T> inputCoder) {
      return SerializableCoder.of(LargestUnique.class);
    }

    /**
     * Encodes the given element using the given coder and hashes the encoding.
     */
    static <T> long hash(T element, Coder<T> coder) throws CoderException, IOException {
      try (HashingOutputStream stream =
              new HashingOutputStream(Hashing.murmur3_128(), ByteStreams.nullOutputStream())) {
        coder.encode(element, stream, Context.OUTER);
        return stream.hash().asLong();
      }
    }
  }

  /**
   * Computes the sampleSize based on the desired estimation error.
   *
   * @param estimationError should be bounded by [0.01, 0.5]
   * @return the sample size needed for the desired estimation error
   */
  static long sampleSizeFromEstimationError(double estimationError) {
    return Math.round(Math.ceil(4.0 / Math.pow(estimationError, 2.0)));
  }

  private static void populateDisplayData(
      DisplayData.Builder builder, long sampleSize, Double maxEstimationError) {
    builder
        .add(DisplayData.item("sampleSize", sampleSize)
          .withLabel("Sample Size"))
        .addIfNotNull(DisplayData.item("maximumEstimationError", maxEstimationError)
          .withLabel("Maximum Estimation Error"));
  }
}
