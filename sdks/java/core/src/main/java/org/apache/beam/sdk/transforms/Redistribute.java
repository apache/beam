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

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.ReshuffleTrigger;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedInteger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * A family of {@link PTransform PTransforms} that returns a {@link PCollection} equivalent to its
 * input but functions as an operational hint to a runner that redistributing the data in some way
 * is likely useful.
 */
public class Redistribute {
  /** @return a {@link RedistributeArbitrarily} transform with default configuration. */
  public static <T> RedistributeArbitrarily<T> arbitrarily() {
    return new RedistributeArbitrarily<>(null, false);
  }

  /** @return a {@link RedistributeByKey} transform with default configuration. */
  public static <K, V> RedistributeByKey<K, V> byKey() {
    return new RedistributeByKey<>(false);
  }

  /**
   * @param <K> The type of key being reshuffled on.
   * @param <V> The type of value being reshuffled.
   */
  public static class RedistributeByKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {

    private final boolean allowDuplicates;

    private RedistributeByKey(boolean allowDuplicates) {
      this.allowDuplicates = allowDuplicates;
    }

    public RedistributeByKey<K, V> withAllowDuplicates(boolean newAllowDuplicates) {
      return new RedistributeByKey<>(newAllowDuplicates);
    }

    public boolean getAllowDuplicates() {
      return allowDuplicates;
    }

    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
      WindowingStrategy<?, ?> originalStrategy = input.getWindowingStrategy();
      // If the input has already had its windows merged, then the GBK that performed the merge
      // will have set originalStrategy.getWindowFn() to InvalidWindows, causing the GBK contained
      // here to fail. Instead, we install a valid WindowFn that leaves all windows unchanged.
      // The TimestampCombiner is set to ensure the GroupByKey does not shift elements forwards in
      // time.
      // Because this outputs as fast as possible, this should not hold the watermark.
      Window<KV<K, V>> rewindow =
          Window.<KV<K, V>>into(
                  new IdentityWindowFn<>(originalStrategy.getWindowFn().windowCoder()))
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
                  new DoFn<
                      KV<K, Iterable<ValueInSingleWindow<V>>>, KV<K, ValueInSingleWindow<V>>>() {
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
  }

  /**
   * Noop transform that hints to the runner to try to redistribute the work evenly, or via whatever
   * clever strategy the runner comes up with.
   */
  public static class RedistributeArbitrarily<T>
      extends PTransform<PCollection<T>, PCollection<T>> {
    // The number of buckets to shard into.
    // A runner is free to ignore this (a runner may ignore the transorm
    // entirely!) This is a performance optimization to prevent having
    // unit sized bundles on the output. If unset, uses a random integer key.
    private @Nullable Integer numBuckets = null;
    private boolean allowDuplicates = false;

    private RedistributeArbitrarily(@Nullable Integer numBuckets, boolean allowDuplicates) {
      this.numBuckets = numBuckets;
      this.allowDuplicates = allowDuplicates;
    }

    public RedistributeArbitrarily<T> withNumBuckets(@Nullable Integer numBuckets) {
      return new RedistributeArbitrarily<>(numBuckets, this.allowDuplicates);
    }

    public RedistributeArbitrarily<T> withAllowDuplicates(boolean allowDuplicates) {
      return new RedistributeArbitrarily<>(this.numBuckets, allowDuplicates);
    }

    public boolean getAllowDuplicates() {
      return allowDuplicates;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input
          .apply("Pair with random key", ParDo.of(new AssignShardFn<>(numBuckets)))
          .apply(Redistribute.<Integer, T>byKey().withAllowDuplicates(this.allowDuplicates))
          .apply(Values.create());
    }
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

  static class AssignShardFn<T> extends DoFn<T, KV<Integer, T>> {
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

  static class RedistributeByKeyTranslator
      implements PTransformTranslation.TransformPayloadTranslator<RedistributeByKey<?, ?>> {
    @Override
    public String getUrn() {
      return PTransformTranslation.REDISTRIBUTE_BY_KEY_URN;
    }

    @Override
    @SuppressWarnings("nullness") // Cannot figure out how to make this typecheck
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, RedistributeByKey<?, ?>> transform, SdkComponents components) {
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(
              RunnerApi.RedistributePayload.newBuilder()
                  .setAllowDuplicates(transform.getTransform().getAllowDuplicates())
                  .build()
                  .toByteString())
          .build();
    }
  }

  static class RedistributeArbitrarilyTranslator
      implements PTransformTranslation.TransformPayloadTranslator<RedistributeArbitrarily<?>> {
    @Override
    public String getUrn() {
      return PTransformTranslation.REDISTRIBUTE_ARBITRARILY_URN;
    }

    @Override
    @SuppressWarnings("nullness") // Cannot figure out how to make this typecheck
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, RedistributeArbitrarily<?>> transform, SdkComponents components) {
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(
              RunnerApi.RedistributePayload.newBuilder()
                  .setAllowDuplicates(transform.getTransform().getAllowDuplicates())
                  .build()
                  .toByteString())
          .build();
    }
  }

  /** Registers translators for the Redistribute family of transforms. */
  @Internal
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {

    @Override
    @SuppressWarnings("rawtypes")
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap
          .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
          .put(RedistributeByKey.class, new RedistributeByKeyTranslator())
          .put(RedistributeArbitrarily.class, new RedistributeArbitrarilyTranslator())
          .build();
    }
  }
}
