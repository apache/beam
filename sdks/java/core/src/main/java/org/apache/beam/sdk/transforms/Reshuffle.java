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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.ReshuffleTrigger;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Comparators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedInteger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * <b>For internal use only; no backwards compatibility guarantees.</b>
 *
 * <p>A {@link PTransform} that returns a {@link PCollection} equivalent to its input but
 * operationally provides some of the side effects of a {@link GroupByKey}, in particular
 * redistribution of elements between workers, checkpointing, and preventing fusion of the
 * surrounding transforms. Some of these side effects (e.g. checkpointing) are not portable and are
 * not guaranteed to occur on all runners.
 *
 * <p>Performs a {@link GroupByKey} so that the data is key-partitioned. Configures the {@link
 * WindowingStrategy} so that no data is dropped, but doesn't affect the need for the user to
 * specify allowed lateness and accumulation mode before a user-inserted GroupByKey.
 *
 * @param <K> The type of key being reshuffled on.
 * @param <V> The type of value being reshuffled.
 */
@Internal
public class Reshuffle<K, V> extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {

  private Reshuffle() {}

  public static <K, V> Reshuffle<K, V> of() {
    return new Reshuffle<>();
  }

  /**
   * Encapsulates the sequence "pair input with unique key, apply {@link Reshuffle#of}, drop the
   * key" commonly used to break fusion.
   */
  public static <T> ViaRandomKey<T> viaRandomKey() {
    return new ViaRandomKey<>();
  }

  @Override
  public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
    String requestedVersionString =
        input.getPipeline().getOptions().as(StreamingOptions.class).getUpdateCompatibilityVersion();

    if (requestedVersionString != null) {
      List<String> requestedVersion = Arrays.asList(requestedVersionString.split("\\."));
      List<String> targetVersion = Arrays.asList("2", "53", "0");

      if (Comparators.lexicographical(Comparator.<String>naturalOrder())
              .compare(requestedVersion, targetVersion)
          <= 0) {
        return expand_2_53_0(input);
      }
    }

    WindowingStrategy<?, ?> originalStrategy = input.getWindowingStrategy();
    // If the input has already had its windows merged, then the GBK that performed the merge
    // will have set originalStrategy.getWindowFn() to InvalidWindows, causing the GBK contained
    // here to fail. Instead, we install a valid WindowFn that leaves all windows unchanged.
    // The TimestampCombiner is set to ensure the GroupByKey does not shift elements forwards in
    // time.
    // Because this outputs as fast as possible, this should not hold the watermark.
    Window<KV<K, V>> rewindow =
        Window.<KV<K, V>>into(new IdentityWindowFn<>(originalStrategy.getWindowFn().windowCoder()))
            .triggering(new ReshuffleTrigger<>())
            .discardingFiredPanes()
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withAllowedLateness(Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

    PCollection<KV<K, ValueInSingleWindow<V>>> reified =
        input
            .apply("SetIdentityWindow", rewindow)
            .apply("ReifyOriginalMetadata", Reify.windowsInValue());

    PCollection<KV<K, Iterable<ValueInSingleWindow<V>>>> grouped =
        reified.apply(GroupByKey.create());
    return grouped
        .apply(
            "ExpandIterable",
            ParDo.of(
                new DoFn<KV<K, Iterable<ValueInSingleWindow<V>>>, KV<K, ValueInSingleWindow<V>>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<K, Iterable<ValueInSingleWindow<V>>> element,
                      OutputReceiver<KV<K, ValueInSingleWindow<V>>> r) {
                    K key = element.getKey();
                    for (ValueInSingleWindow<V> value : element.getValue()) {
                      r.output(KV.of(key, value));
                    }
                  }
                }))
        .apply("RestoreMetadata", new RestoreMetadata<>())
        // Set the windowing strategy directly, so that it doesn't get counted as the user having
        // set allowed lateness.
        .setWindowingStrategyInternal(originalStrategy);
  }

  private PCollection<KV<K, V>> expand_2_53_0(PCollection<KV<K, V>> input) {
    WindowingStrategy<?, ?> originalStrategy = input.getWindowingStrategy();
    // If the input has already had its windows merged, then the GBK that performed the merge
    // will have set originalStrategy.getWindowFn() to InvalidWindows, causing the GBK contained
    // here to fail. Instead, we install a valid WindowFn that leaves all windows unchanged.
    // The TimestampCombiner is set to ensure the GroupByKey does not shift elements forwards in
    // time.
    // Because this outputs as fast as possible, this should not hold the watermark.
    Window<KV<K, V>> rewindow =
        Window.<KV<K, V>>into(new IdentityWindowFn<>(originalStrategy.getWindowFn().windowCoder()))
            .triggering(new ReshuffleTrigger<>())
            .discardingFiredPanes()
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withAllowedLateness(Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

    return input
        .apply(rewindow)
        .apply("ReifyOriginalTimestamps", Reify.timestampsInValue())
        .apply(GroupByKey.create())
        // Set the windowing strategy directly, so that it doesn't get counted as the user having
        // set allowed lateness.
        .setWindowingStrategyInternal(originalStrategy)
        .apply(
            "ExpandIterable",
            ParDo.of(
                new DoFn<KV<K, Iterable<TimestampedValue<V>>>, KV<K, TimestampedValue<V>>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<K, Iterable<TimestampedValue<V>>> element,
                      OutputReceiver<KV<K, TimestampedValue<V>>> r) {
                    K key = element.getKey();
                    for (TimestampedValue<V> value : element.getValue()) {
                      r.output(KV.of(key, value));
                    }
                  }
                }))
        .apply("RestoreOriginalTimestamps", ReifyTimestamps.extractFromValues());
  }

  private static class RestoreMetadata<K, V>
      extends PTransform<PCollection<KV<K, ValueInSingleWindow<V>>>, PCollection<KV<K, V>>> {
    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, ValueInSingleWindow<V>>> input) {
      return input.apply(
          ParDo.of(
              new DoFn<KV<K, ValueInSingleWindow<V>>, KV<K, V>>() {
                @Override
                public Duration getAllowedTimestampSkew() {
                  return Duration.millis(Long.MAX_VALUE);
                }

                @ProcessElement
                public void processElement(
                    @Element KV<K, ValueInSingleWindow<V>> kv, OutputReceiver<KV<K, V>> r) {
                  r.outputWindowedValue(
                      KV.of(kv.getKey(), kv.getValue().getValue()),
                      kv.getValue().getTimestamp(),
                      Collections.singleton(kv.getValue().getWindow()),
                      kv.getValue().getPane());
                }
              }));
    }
  }

  /** Implementation of {@link #viaRandomKey()}. */
  public static class ViaRandomKey<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private ViaRandomKey() {}

    private ViaRandomKey(@Nullable Integer numBuckets) {
      this.numBuckets = numBuckets;
    }

    // The number of buckets to shard into. This is a performance optimization to prevent having
    // unit sized bundles on the output. If unset, uses a random integer key.
    private @Nullable Integer numBuckets;

    public ViaRandomKey<T> withNumBuckets(@Nullable Integer numBuckets) {
      return new ViaRandomKey<>(numBuckets);
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input
          .apply("Pair with random key", ParDo.of(new AssignShardFn<>(numBuckets)))
          .apply(Reshuffle.of())
          .apply(Values.create());
    }
  }

  public static class AssignShardFn<T> extends DoFn<T, KV<Integer, T>> {
    private int shard;
    private @Nullable Integer numBuckets;

    public AssignShardFn(@Nullable Integer numBuckets) {
      this.numBuckets = numBuckets;
    }

    @Setup
    public void setup() {
      shard = ThreadLocalRandom.current().nextInt();
    }

    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<KV<Integer, T>> r) {
      ++shard;
      // Smear the shard into something more random-looking, to avoid issues
      // with runners that don't properly hash the key being shuffled, but rely
      // on it being random-looking. E.g. Spark takes the Java hashCode() of keys,
      // which for Integer is a no-op and it is an issue:
      // http://hydronitrogen.com/poor-hash-partitioning-of-timestamps-integers-and-longs-in-
      // spark.html
      // This hashing strategy is copied from
      // org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Hashing.smear().
      int hashOfShard = 0x1b873593 * Integer.rotateLeft(shard * 0xcc9e2d51, 15);
      if (numBuckets != null) {
        UnsignedInteger unsignedNumBuckets = UnsignedInteger.fromIntBits(numBuckets);
        hashOfShard = UnsignedInteger.fromIntBits(hashOfShard).mod(unsignedNumBuckets).intValue();
      }
      r.output(KV.of(hashOfShard, element));
    }
  }
}
