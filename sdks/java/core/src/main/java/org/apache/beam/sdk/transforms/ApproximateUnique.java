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
import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.HashingOutputStream;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code PTransform}s for estimating the number of distinct elements in a {@code PCollection}, or
 * the number of distinct values associated with each key in a {@code PCollection} of {@code KV}s.
 *
 * <p>Consider using {@code HllCount} in the {@code zetasketch} extension module if you need better
 * performance or need to save intermediate aggregation result into a sketch for later processing.
 *
 * <p>For example, to estimate the number of distinct elements in a {@code PCollection<String>}:
 *
 * <pre>{@code
 * PCollection<String> input = ...;
 * PCollection<Long> countDistinct =
 *     input.apply(HllCount.Init.forStrings().globally()).apply(HllCount.Extract.globally());
 * }</pre>
 *
 * <p>{@link #combineFn} can also be used manually, with the {@link Combine} transform.
 *
 * <p>For more details about using {@code HllCount} and the {@code zetasketch} extension module, see
 * https://s.apache.org/hll-in-beam#bookmark=id.v6chsij1ixo7.
 */
public class ApproximateUnique {

  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<T>} and returns a {@code
   * PCollection<Long>} containing a single value that is an estimate of the number of distinct
   * elements in the input {@code PCollection}.
   *
   * <p>The {@code sampleSize} parameter controls the estimation error. The error is about {@code 2
   * / sqrt(sampleSize)}, so for {@code ApproximateUnique.globally(10000)} the estimation error is
   * about 2%. Similarly, for {@code ApproximateUnique.of(16)} the estimation error is about 50%. If
   * there are fewer than {@code sampleSize} distinct elements then the returned result will be
   * exact with extremely high probability (the chance of a hash collision is about {@code
   * sampleSize^2 / 2^65}).
   *
   * <p>This transform approximates the number of elements in a set by computing the top {@code
   * sampleSize} hash values, and using that to extrapolate the size of the entire set of hash
   * values by assuming the rest of the hash values are as densely distributed as the top {@code
   * sampleSize}.
   *
   * <p>See also {@link #globally(double)}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> pc = ...;
   * PCollection<Long> approxNumDistinct =
   *     pc.apply(ApproximateUnique.<String>globally(1000));
   * }</pre>
   *
   * <p>Note: if the input collection uses a windowing strategy other than {@link GlobalWindows},
   * use {@code ApproximateUnique.<T>combineFn(sampleSize, inputCoder).withoutDefaults()} instead.
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   * @param sampleSize the number of entries in the statistical sample; the higher this number, the
   *     more accurate the estimate will be; should be {@code >= 16}
   * @throws IllegalArgumentException if the {@code sampleSize} argument is too small
   */
  public static <T> Globally<T> globally(int sampleSize) {
    return new Globally<>(sampleSize);
  }

  /**
   * Like {@link #globally(int)}, but specifies the desired maximum estimation error instead of the
   * sample size.
   *
   * <p>Note: if the input collection uses a windowing strategy other than {@link GlobalWindows},
   * use {@code ApproximateUnique.<T>combineFn(maximumEstimationError,
   * inputCoder).withoutDefaults()} instead.
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   * @param maximumEstimationError the maximum estimation error, which should be in the range {@code
   *     [0.01, 0.5]}
   * @throws IllegalArgumentException if the {@code maximumEstimationError} argument is out of range
   */
  public static <T> Globally<T> globally(double maximumEstimationError) {
    return new Globally<>(maximumEstimationError);
  }

  /**
   * Returns a {@link Combine.Globally} that gives a single value that is an estimate of the number
   * of distinct elements in the input {@code PCollection}.
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   * @param sampleSize the number of entries in the statistical sample; the higher this number, the
   *     more accurate the estimate will be; should be {@code >= 16}
   * @param inputCoder the coder for {@code PCollection<T>} where the combineFn will be applied on.
   * @throws IllegalArgumentException if the {@code sampleSize} argument is too small
   */
  public static <T> Combine.Globally<T, Long> combineFn(int sampleSize, Coder<T> inputCoder) {
    checkArgument(inputCoder != null, "inputCoder should not be null");
    return new Globally<T>(sampleSize).combineFn(inputCoder);
  }

  /**
   * Returns a {@link Combine.Globally} that gives a single value that is an estimate of the number
   * of distinct elements in the input {@code PCollection}, but specifies the desired maximum
   * estimation error instead of the sample size.
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   * @param maximumEstimationError the maximum estimation error, which should be in the range {@code
   *     [0.01, 0.5]}
   * @param inputCoder the coder for {@code PCollection<T>} where the combineFn will be applied on.
   * @throws IllegalArgumentException if the {@code maximumEstimationError} argument is out of range
   */
  public static <T> Combine.Globally<T, Long> combineFn(
      double maximumEstimationError, Coder<T> inputCoder) {
    checkArgument(inputCoder != null, "inputCoder should not be null");
    return new Globally<T>(maximumEstimationError).combineFn(inputCoder);
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<KV<K, V>>} and returns a {@code
   * PCollection<KV<K, Long>>} that contains an output element mapping each distinct key in the
   * input {@code PCollection} to an estimate of the number of distinct values associated with that
   * key in the input {@code PCollection}.
   *
   * <p>See {@link #globally(int)} for an explanation of the {@code sampleSize} parameter. A
   * separate sampling is computed for each distinct key of the input.
   *
   * <p>See also {@link #perKey(double)}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<KV<Integer, String>> pc = ...;
   * PCollection<KV<Integer, Long>> approxNumDistinctPerKey =
   *     pc.apply(ApproximateUnique.<Integer, String>perKey(1000));
   * }</pre>
   *
   * @param <K> the type of the keys in the input and output {@code PCollection}s
   * @param <V> the type of the values in the input {@code PCollection}
   * @param sampleSize the number of entries in the statistical sample; the higher this number, the
   *     more accurate the estimate will be; should be {@code >= 16}
   * @throws IllegalArgumentException if the {@code sampleSize} argument is too small
   */
  public static <K, V> PerKey<K, V> perKey(int sampleSize) {
    return new PerKey<>(sampleSize);
  }

  /**
   * Like {@link #perKey(int)}, but specifies the desired maximum estimation error instead of the
   * sample size.
   *
   * @param <K> the type of the keys in the input and output {@code PCollection}s
   * @param <V> the type of the values in the input {@code PCollection}
   * @param maximumEstimationError the maximum estimation error, which should be in the range {@code
   *     [0.01, 0.5]}
   * @throws IllegalArgumentException if the {@code maximumEstimationError} argument is out of range
   */
  public static <K, V> PerKey<K, V> perKey(double maximumEstimationError) {
    return new PerKey<>(maximumEstimationError);
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code PTransform} for estimating the number of distinct elements in a {@code PCollection}.
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   */
  public static final class Globally<T> extends PTransform<PCollection<T>, PCollection<Long>> {

    /**
     * The number of entries in the statistical sample; the higher this number, the more accurate
     * the estimate will be.
     */
    private final long sampleSize;

    /** The desired maximum estimation error or null if not specified. */
    private final @Nullable Double maximumEstimationError;

    private Combine.Globally<T, Long> combineFn(Coder<T> coder) {
      return Combine.globally(new ApproximateUniqueCombineFn<>(sampleSize, coder));
    }

    /** @see ApproximateUnique#globally(int) */
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

    /** @see ApproximateUnique#globally(double) */
    public Globally(double maximumEstimationError) {
      if (maximumEstimationError < 0.01 || maximumEstimationError > 0.5) {
        throw new IllegalArgumentException(
            "ApproximateUnique needs an " + "estimation error between 1% (0.01) and 50% (0.5).");
      }

      this.sampleSize = sampleSizeFromEstimationError(maximumEstimationError);
      this.maximumEstimationError = maximumEstimationError;
    }

    @Override
    public PCollection<Long> expand(PCollection<T> input) {
      Coder<T> coder = input.getCoder();
      return input.apply(combineFn(coder));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      ApproximateUnique.populateDisplayData(builder, sampleSize, maximumEstimationError);
    }
  }

  /**
   * {@code PTransform} for estimating the number of distinct values associated with each key in a
   * {@code PCollection} of {@code KV}s.
   *
   * @param <K> the type of the keys in the input and output {@code PCollection}s
   * @param <V> the type of the values in the input {@code PCollection}
   */
  public static final class PerKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Long>>> {

    /**
     * The number of entries in the statistical sample; the higher this number, the more accurate
     * the estimate will be.
     */
    private final long sampleSize;

    /** The the desired maximum estimation error or null if not specified. */
    private final @Nullable Double maximumEstimationError;

    /** @see ApproximateUnique#perKey(int) */
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

    /** @see ApproximateUnique#perKey(double) */
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

      return input.apply(Combine.perKey(new ApproximateUniqueCombineFn<>(sampleSize, coder)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      ApproximateUnique.populateDisplayData(builder, sampleSize, maximumEstimationError);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code CombineFn} that computes an estimate of the number of distinct values that were
   * combined.
   *
   * <p>Hashes input elements, computes the top {@code sampleSize} hash values, and uses those to
   * extrapolate the size of the entire set of hash values by assuming the rest of the hash values
   * are as densely distributed as the top {@code sampleSize}.
   *
   * <p>Used to implement {@link #globally(int) ApproximatUnique.globally(...)} and {@link
   * #perKey(int) ApproximatUnique.perKey(...)}.
   *
   * @param <T> the type of the values being combined
   */
  public static class ApproximateUniqueCombineFn<T>
      extends CombineFn<T, ApproximateUniqueCombineFn.LargestUnique, Long> {

    /** The size of the space of hashes returned by the hash function. */
    static final double HASH_SPACE_SIZE = Long.MAX_VALUE - (double) Long.MIN_VALUE;

    /** A heap utility class to efficiently track the largest added elements. */
    public static class LargestUnique implements Serializable {
      private TreeSet<Long> heap = new TreeSet<>();
      private long minHash = Long.MAX_VALUE;
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
       * Adds a value to the heap, returning whether the value is (large enough to be) in the heap.
       */
      public boolean add(long value) {
        if (heap.size() >= sampleSize && value < minHash) {
          return false; // Common case as input size increases.
        }
        if (heap.add(value)) {
          if (heap.size() > sampleSize) {
            heap.remove(minHash);
            minHash = heap.first();
          } else if (value < minHash) {
            minHash = value;
          }
        }
        return true;
      }

      long getEstimate() {
        if (heap.size() < sampleSize) {
          return (long) heap.size();
        } else {
          double sampleSpaceSize = Long.MAX_VALUE - (double) minHash;
          // This formula takes into account the possibility of hash collisions,
          // which become more likely than not for 2^32 distinct elements.
          // Note that log(1+x) ~ x for small x, so for sampleSize << maxHash
          // log(1 - sampleSize/sampleSpace) / log(1 - 1/sampleSpace) ~ sampleSize
          // and hence estimate ~ sampleSize * HASH_SPACE_SIZE / sampleSpace
          // as one would expect.
          double estimate =
              Math.log1p(-sampleSize / sampleSpaceSize)
                  / Math.log1p(-1 / sampleSpaceSize)
                  * HASH_SPACE_SIZE
                  / sampleSpaceSize;
          return Math.round(estimate);
        }
      }

      @Override
      public boolean equals(@Nullable Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        LargestUnique that = (LargestUnique) o;

        return sampleSize == that.sampleSize && Iterables.elementsEqual(heap, that.heap);
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(Lists.newArrayList(heap), sampleSize);
      }
    }

    private final long sampleSize;
    private final Coder<T> coder;

    public ApproximateUniqueCombineFn(long sampleSize, Coder<T> coder) {
      this.sampleSize = sampleSize;
      this.coder = coder;
    }

    @Override
    public String getIncompatibleGlobalWindowErrorMessage() {
      return "If the input collection uses a windowing strategy other than GlobalWindows, "
          + "use ApproximateUnique.<T>combineFn(sampleSize, inputCoder).withoutDefaults() "
          + "or ApproximateUnique.<T>combineFn(maximumEstimationError, inputCoder).withoutDefaults() instead.";
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
      LargestUnique accumulator = iterator.next();
      while (iterator.hasNext()) {
        iterator.next().heap.forEach(h -> accumulator.add(h));
      }
      return accumulator;
    }

    @Override
    public Long extractOutput(LargestUnique heap) {
      return heap.getEstimate();
    }

    @Override
    public Coder<LargestUnique> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return SerializableCoder.of(LargestUnique.class);
    }

    /** Encodes the given element using the given coder and hashes the encoding. */
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
      DisplayData.Builder builder, long sampleSize, @Nullable Double maxEstimationError) {
    builder
        .add(DisplayData.item("sampleSize", sampleSize).withLabel("Sample Size"))
        .addIfNotNull(
            DisplayData.item("maximumEstimationError", maxEstimationError)
                .withLabel("Maximum Estimation Error"));
  }
}
