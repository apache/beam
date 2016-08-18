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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

import org.joda.time.Instant;

import java.io.Serializable;

/**
 * {@link PTransform} and {@link Combine.CombineFn} for computing the latest element
 * in a {@link PCollection}. This is particularly useful as an {@link Aggregator}:
 *
 * <pre><code>
 * class MyDoFn extends DoFn<String, String> {
 *  private Aggregator<TimestampedValue<Double>, Double> latestValue =
 *    createAggregator("latestValue", new Latest.LatestFn<Double>());
 *
 *  {@literal @}ProcessElement
 *  public void processElement(ProcessContext c) {
 *    double val = // ..
 *    latestValue.addValue(TimestampedValue.of(val, c.timestamp()));
 *    // ..
 *  }
 * }
 * </code></pre>
 */
public class Latest {
  // Do not instantiate
  private Latest() {}

  /**
   * A {@link Combine.CombineFn} that computes the latest element from a set of inputs. This is
   * particularly useful as an {@link Aggregator}.
   *
   * @param <T> Type of input element.
   * @see {@link Latest}
   */
  public static class LatestFn<T>
      extends Combine.CombineFn<TimestampedValue<T>, TimestampedValue<T>, T> {
    @Override
    public TimestampedValue<T> createAccumulator() {
      return null;
    }

    @Override
    public TimestampedValue<T> addInput(TimestampedValue<T> accumulator,
        TimestampedValue<T> input) {
      checkNotNull(input, "input must be non-null");

      if (accumulator == null || input.getTimestamp().isAfter(accumulator.getTimestamp())) {
        return input;
      } else {
        return accumulator;
      }
    }

    @Override
    public TimestampedValue<T> mergeAccumulators(Iterable<TimestampedValue<T>> accumulators) {
      checkNotNull(accumulators, "accumulators must be non-null");
      TimestampedValue<T> merged = null;
      for (TimestampedValue<T> accum : accumulators) {
        if (accum != null) {
          merged = addInput(merged, accum);
        }
      }

      return merged;
    }

    @Override
    public T extractOutput(TimestampedValue<T> accumulator) {
      if (accumulator == null) {
        return null;
      }

      return accumulator.getValue();
    }
  }

  /**
   * Returns a {@link PTransform} that takes as input a {@link PCollection<T>} and returns a
   * {@link PCollection<T>} whose contents is the latest element according to its event time, or
   * {@literal null} if there are no elements.
   *
   * @param <T> The type of the elements being combined.
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> globally() {
    return new Globally<>(new TimestampFn<T>() {
      @Override
      public Instant extractTimestamp(DoFn<T, ?>.ProcessContext c) {
        return c.timestamp();
      }
    });
  }

  /**
   * Returns a {@link PTransform} that takes as input a {@link PCollection} and a timestamp
   * mapping function, and returns a {@link PCollection} whose contents is the latest element
   * according to the timestamp mapping function, or {@literal null} if there are no elements.
   *
   * @param timestampFn Mapping function to extract a timestamp from the element.
   * @param <T> The type of the elements being combined.
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> globally(
      final SerializableFunction<T, Instant> timestampFn) {
    checkNotNull(timestampFn, "timestampFn must be non-null");

    return new Globally<>(new TimestampFn<T>() {
      @Override
      public Instant extractTimestamp(DoFn<T, ?>.ProcessContext c) {
        return timestampFn.apply(c.element());
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        Latest.populateDisplayData(builder, timestampFn);
      }
    });
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
    return new PerKey<>(new TimestampFn<KV<K, V>>() {
      @Override
      public Instant extractTimestamp(DoFn<KV<K, V>, ?>.ProcessContext c) {
        return c.timestamp();
      }
    });
  }

  /**
   * Returns a {@link PTransform} that takes as input a {@code PCollection<KV<K, V>>} and a
   * timestamp mapping function, and returns a {@code PCollection<KV<K, V>>} whose contents is the
   * latest element per-key according to the timestamp mapping function.
   *
   * @param timestampFn Mapping function to extract a timestamp from the element.
   * @param <K> The key type of the elements being combined.
   * @param <V> The value type of the elements being combined.
   */
  public static <K, V> PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> perKey(
      final SerializableFunction<KV<K, V>, Instant> timestampFn) {
    checkNotNull(timestampFn, "timestampFn must be non-null");

    return new PerKey<>(new TimestampFn<KV<K, V>>() {
      @Override
      public Instant extractTimestamp(DoFn<KV<K, V>, ?>.ProcessContext c) {
        return timestampFn.apply(c.element());
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        Latest.populateDisplayData(builder, timestampFn);
      }
    });
  }

  private abstract static class TimestampFn<T> implements Serializable, HasDisplayData {
    abstract Instant extractTimestamp(DoFn<T, ?>.ProcessContext c);
    @Override public void populateDisplayData(DisplayData.Builder builder) { }
  }

  private static class Globally<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final TimestampFn<T> timestampFn;
    public Globally(TimestampFn<T> timestampFn) {
      this.timestampFn = timestampFn;
    }

    @Override
    public PCollection<T> apply(PCollection<T> input) {
      return input
        .apply("Reify Timestamps", ParDo.of(new DoFn<T, TimestampedValue<T>>() {
          @ProcessElement
          public void processElement(DoFn<T, TimestampedValue<T>>.ProcessContext c) {
            c.output(TimestampedValue.of(c.element(), timestampFn.extractTimestamp(c)));
          }

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.include(timestampFn);
          }
        }))
        .apply("Latest Value", Combine.globally(new LatestFn<T>()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.include(timestampFn);
    }
  }

  private static class PerKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {
    private final TimestampFn<KV<K, V>> timestampFn;
    public PerKey(TimestampFn<KV<K, V>> timestampFn) {
      this.timestampFn = timestampFn;
    }

    @Override
    public PCollection<KV<K, V>> apply(PCollection<KV<K, V>> input) {
      return input.apply("Reify Timestamps", ParDo.of(
          new DoFn<KV<K, V>, KV<K, TimestampedValue<V>>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element().getKey(), TimestampedValue.of(c.element().getValue(),
                  timestampFn.extractTimestamp(c))));
            }

            @Override
            public void populateDisplayData(DisplayData.Builder builder) {
              super.populateDisplayData(builder);
              builder.include(timestampFn);
            }
          }))
          .apply("Latest Value", Combine.<K, TimestampedValue<V>, V>perKey(new LatestFn<V>()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.include(timestampFn);
    }
  }

  private static void populateDisplayData(DisplayData.Builder builder,
      SerializableFunction<?, Instant> timestampFn) {
    builder.add(DisplayData.item("timestampFn", timestampFn.getClass())
        .withLabel("Timestamp Function"));
  }
}
