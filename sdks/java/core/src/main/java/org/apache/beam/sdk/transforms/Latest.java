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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link PTransform} and {@link Combine.CombineFn} for computing the latest element in a {@link
 * PCollection}.
 *
 * <p>Example: compute the latest value for each session:
 *
 * <pre>{@code
 * PCollection<Long> input = ...;
 * PCollection<Long> sessioned = input
 *    .apply(Window.<Long>into(Sessions.withGapDuration(Duration.standardMinutes(5)));
 * PCollection<Long> latestValues = sessioned.apply(Latest.<Long>globally());
 * }</pre>
 *
 * <p>{@link #combineFn} can also be used manually, in combination with state and with the {@link
 * Combine} transform.
 *
 * <p>For elements with the same timestamp, the element chosen for output is arbitrary.
 */
public class Latest {
  // Do not instantiate
  private Latest() {}

  /** Returns a {@link Combine.CombineFn} that selects the latest element among its inputs. */
  public static <T> Combine.CombineFn<TimestampedValue<T>, ?, T> combineFn() {
    return new LatestFn<>();
  }

  /**
   * Returns a {@link PTransform} that takes as input a {@code PCollection<T>} and returns a {@code
   * PCollection<T>} whose contents is the latest element according to its event time, or {@literal
   * null} if there are no elements.
   *
   * @param <T> The type of the elements being combined.
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> globally() {
    return new Globally<>();
  }

  /**
   * Returns a {@link PTransform} that takes as input a {@code PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, V>>} whose contents is the latest element per-key according to its
   * event time.
   *
   * @param <K> The key type of the elements being combined.
   * @param <V> The value type of the elements being combined.
   */
  public static <K, V> PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> perKey() {
    return new PerKey<>();
  }

  /**
   * A {@link Combine.CombineFn} that computes the latest element from a set of inputs.
   *
   * <p>The accumulator holds a {@literal null} value until the first input arrives, so combining an
   * empty input yields {@literal null}. {@code T} must therefore be instantiated at a nullable type
   * for the output to be sound, which the declaration cannot say without changing the public
   * signature of {@link Latest#combineFn()}.
   *
   * @param <T> Type of input element.
   * @see Latest
   */
  @VisibleForTesting
  static class LatestFn<T>
      extends Combine.CombineFn<TimestampedValue<T>, TimestampedValue<@Nullable T>, T> {
    /** Construct a new {@link LatestFn} instance. */
    public LatestFn() {}

    @Override
    public TimestampedValue<@Nullable T> createAccumulator() {
      return TimestampedValue.atMinimumTimestamp(null);
    }

    @Override
    public TimestampedValue<@Nullable T> addInput(
        TimestampedValue<@Nullable T> accumulator, TimestampedValue<T> input) {
      checkNotNull(accumulator, "accumulator must be non-null");
      checkNotNull(input, "input must be non-null");

      return latest(accumulator, input);
    }

    /** Returns whichever argument is later, preferring a non-null value when they are equal. */
    private static <T> TimestampedValue<@Nullable T> latest(
        TimestampedValue<@Nullable T> accumulator, TimestampedValue<@Nullable T> input) {
      if (input.getTimestamp().isBefore(accumulator.getTimestamp())) {
        return accumulator;
      } else if (input.getTimestamp().isAfter(accumulator.getTimestamp())) {
        return input;
      } else {
        return accumulator.getValue() != null ? accumulator : input;
      }
    }

    @Override
    @SuppressWarnings("nullness") // accumulated values may be null
    public Coder<TimestampedValue<@Nullable T>> getAccumulatorCoder(
        CoderRegistry registry, Coder<TimestampedValue<T>> inputCoder)
        throws CannotProvideCoderException {
      return NullableCoder.of(inputCoder);
    }

    @Override
    @SuppressWarnings("nullness") // the output is null when the input is empty
    public Coder<T> getDefaultOutputCoder(
        CoderRegistry registry, Coder<TimestampedValue<T>> inputCoder)
        throws CannotProvideCoderException {
      checkState(
          inputCoder instanceof TimestampedValue.TimestampedValueCoder,
          "inputCoder must be a TimestampedValueCoder, but was %s",
          inputCoder);

      TimestampedValue.TimestampedValueCoder<T> inputTVCoder =
          (TimestampedValue.TimestampedValueCoder<T>) inputCoder;
      return NullableCoder.of(inputTVCoder.getValueCoder());
    }

    @Override
    public TimestampedValue<@Nullable T> mergeAccumulators(
        Iterable<TimestampedValue<@Nullable T>> accumulators) {
      checkNotNull(accumulators, "accumulators must be non-null");

      Iterator<TimestampedValue<@Nullable T>> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      }

      TimestampedValue<@Nullable T> merged = iter.next();
      while (iter.hasNext()) {
        TimestampedValue<@Nullable T> next = iter.next();
        checkNotNull(next, "input must be non-null");
        merged = latest(merged, next);
      }

      return merged;
    }

    @Override
    @SuppressWarnings("nullness") // the output is null until the first input arrives
    public T extractOutput(TimestampedValue<@Nullable T> accumulator) {
      return accumulator.getValue();
    }
  }

  /** Implementation of {@link #globally()}. */
  private static class Globally<T> extends PTransform<PCollection<T>, PCollection<T>> {
    @Override
    public PCollection<T> expand(PCollection<T> input) {
      Coder<T> inputCoder = input.getCoder();

      return input
          .apply("Reify Timestamps", Reify.timestamps())
          .apply("Latest Value", Combine.globally(new LatestFn<>()))
          .setCoder(NullableCoder.of(inputCoder));
    }
  }

  /** Implementation of {@link #perKey()}. */
  private static class PerKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {
    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
      checkNotNull(input);
      checkArgument(
          input.getCoder() instanceof KvCoder,
          "Input specifiedCoder must be an instance of KvCoder, but was %s",
          input.getCoder());

      @SuppressWarnings("unchecked")
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      return input
          .apply("Reify Timestamps", Reify.timestampsInValue())
          .apply("Latest Value", Combine.perKey(new LatestFn<>()))
          .setCoder(inputCoder);
    }
  }
}
