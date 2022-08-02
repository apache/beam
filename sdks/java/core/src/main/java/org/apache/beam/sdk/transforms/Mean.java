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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code PTransform}s for computing the arithmetic mean (a.k.a. average) of the elements in a
 * {@code PCollection}, or the mean of the values associated with each key in a {@code PCollection}
 * of {@code KV}s.
 *
 * <p>Example 1: get the mean of a {@code PCollection} of {@code Long}s.
 *
 * <pre>{@code
 * PCollection<Long> input = ...;
 * PCollection<Double> mean = input.apply(Mean.<Long>globally());
 * }</pre>
 *
 * <p>Example 2: calculate the mean of the {@code Integer}s associated with each unique key (which
 * is of type {@code String}).
 *
 * <pre>{@code
 * PCollection<KV<String, Integer>> input = ...;
 * PCollection<KV<String, Double>> meanPerKey =
 *     input.apply(Mean.<String, Integer>perKey());
 * }</pre>
 */
public class Mean {

  private Mean() {} // Namespace only

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<NumT>} and returns a {@code
   * PCollection<Double>} whose contents is the mean of the input {@code PCollection}'s elements, or
   * {@code 0} if there are no elements.
   *
   * @param <NumT> the type of the {@code Number}s being combined
   */
  public static <NumT extends Number> Combine.Globally<NumT, Double> globally() {
    return Combine.globally(Mean.of());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, N>>} and returns a
   * {@code PCollection<KV<K, Double>>} that contains an output element mapping each distinct key in
   * the input {@code PCollection} to the mean of the values associated with that key in the input
   * {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and bucketing.
   *
   * @param <K> the type of the keys
   * @param <NumT> the type of the {@code Number}s being combined
   */
  public static <K, NumT extends Number> Combine.PerKey<K, NumT, Double> perKey() {
    return Combine.perKey(Mean.of());
  }

  /**
   * A {@code Combine.CombineFn} that computes the arithmetic mean (a.k.a. average) of an {@code
   * Iterable} of numbers of type {@code N}, useful as an argument to {@link Combine#globally} or
   * {@link Combine#perKey}.
   *
   * <p>Returns {@code Double.NaN} if combining zero elements.
   *
   * @param <NumT> the type of the {@code Number}s being combined
   */
  public static <NumT extends Number>
      Combine.AccumulatingCombineFn<NumT, CountSum<NumT>, Double> of() {
    return new MeanFn<>();
  }

  /////////////////////////////////////////////////////////////////////////////

  private static class MeanFn<NumT extends Number>
      extends Combine.AccumulatingCombineFn<NumT, CountSum<NumT>, Double> {
    /**
     * Constructs a combining function that computes the mean over a collection of values of type
     * {@code N}.
     */
    @Override
    public CountSum<NumT> createAccumulator() {
      return new CountSum<>();
    }

    @Override
    public Coder<CountSum<NumT>> getAccumulatorCoder(
        CoderRegistry registry, Coder<NumT> inputCoder) {
      return new CountSumCoder<>();
    }
  }

  /** Accumulator class for {@link MeanFn}. */
  static class CountSum<NumT extends Number> implements Accumulator<NumT, CountSum<NumT>, Double> {

    long count = 0;
    double sum = 0.0;

    public CountSum() {
      this(0, 0);
    }

    public CountSum(long count, double sum) {
      this.count = count;
      this.sum = sum;
    }

    @Override
    public void addInput(NumT element) {
      count++;
      sum += element.doubleValue();
    }

    @Override
    public void mergeAccumulator(CountSum<NumT> accumulator) {
      count += accumulator.count;
      sum += accumulator.sum;
    }

    @Override
    public Double extractOutput() {
      return count == 0 ? Double.NaN : sum / count;
    }

    @Override
    // Comparing doubles directly since class is package private and equals method is only used in
    // coder test.
    @SuppressFBWarnings("FE_FLOATING_POINT_EQUALITY")
    public boolean equals(@Nullable Object other) {
      if (!(other instanceof CountSum)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      CountSum<?> otherCountSum = (CountSum<?>) other;
      return (count == otherCountSum.count) && (sum == otherCountSum.sum);
    }

    @Override
    public int hashCode() {
      return Objects.hash(count, sum);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("count", count).add("sum", sum).toString();
    }
  }

  static class CountSumCoder<NumT extends Number> extends AtomicCoder<CountSum<NumT>> {
    private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
    private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

    @Override
    public void encode(CountSum<NumT> value, OutputStream outStream)
        throws CoderException, IOException {
      LONG_CODER.encode(value.count, outStream);
      DOUBLE_CODER.encode(value.sum, outStream);
    }

    @Override
    public CountSum<NumT> decode(InputStream inStream) throws CoderException, IOException {
      return new CountSum<>(LONG_CODER.decode(inStream), DOUBLE_CODER.decode(inStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      LONG_CODER.verifyDeterministic();
      DOUBLE_CODER.verifyDeterministic();
    }
  }
}
