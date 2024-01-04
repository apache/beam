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

import org.apache.beam.sdk.transforms.WithKeys;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.ReshuffleTrigger;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedInteger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.apache.beam.sdk.util.ShardedKey;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * <b>For internal use only; no backwards compatibility guarantees.</b>
 *
 * <p>A {@link PTransform} that returns a {@link PCollection} equivalent to its input but
 * operationally provides some of the side effects of a {@link GroupByKey}, in particular
 * checkpointing, and preventing fusion of the surrounding transforms.
 *
 * <p>Performs a {@link GroupByKey} so that the data is key-partitioned. Configures the {@link
 * WindowingStrategy} so that no data is dropped, but doesn't affect the need for the user to
 * specify allowed lateness and accumulation mode before a user-inserted GroupByKey.
 *
 * @param <K> The type of key being reshuffled on.
 * @param <V> The type of value being reshuffled.
 */
public class AutoshardedKeyReshuffle<K, V> extends PTransform<
    PCollection<KV<ShardedKey<K>, V>>, PCollection<KV<ShardedKey<K>, V>>> {

  private AutoshardedKeyReshuffle() {}

  public static <K, V>  AutoshardedKeyReshuffle<K, V>  of() {
    return new AutoshardedKeyReshuffle<>();
  }

  /**
   * Encapsulates the sequence "take keyed value, return the same as autosharded key with single autoshardable key, apply {@link Reshuffle#of}, DONT drop the
   * key" commonly used to break fusion.
   */
  public static <K, V> ViaRandomKey <K, V> viaRandomKey() {
    return new ViaRandomKey<>();
  }

  @Override
  public PCollection<KV<ShardedKey<K>, V>>  expand(PCollection<KV<ShardedKey<K>, V>> input) {
    WindowingStrategy<?, ?> originalStrategy = input.getWindowingStrategy();
    // If the input has already had its windows merged, then the GBK that performed the merge
    // will have set originalStrategy.getWindowFn() to InvalidWindows, causing the GBK contained
    // here to fail. Instead, we install a valid WindowFn that leaves all windows unchanged.
    // The TimestampCombiner is set to ensure the GroupByKey does not shift elements forwards in
    // time.
    // Because this outputs as fast as possible, this should not hold the watermark.
    Window<KV<ShardedKey<K>, V>> rewindow =
        Window.<KV<ShardedKey<K>, V>>into(new IdentityWindowFn<>(originalStrategy.getWindowFn().windowCoder()))
            .triggering(new ReshuffleTrigger<>()) // can this be reused?
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
                new DoFn<KV<ShardedKey<K>, Iterable<TimestampedValue<V>>>, KV<ShardedKey<K>, TimestampedValue<V>>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<ShardedKey<K>, Iterable<TimestampedValue<V>>> element,
                      OutputReceiver<KV<ShardedKey<K>, TimestampedValue<V>>> r) {
                    ShardedKey<K> key = element.getKey();
                    for (TimestampedValue<V> value : element.getValue()) {
                      r.output(KV.of(key, value));
                    }
                  }
                }))
        .apply("RestoreOriginalTimestamps", ReifyTimestamps.extractFromValues());
  }

  /** Implementation of {@link #viaRandomKey()}. */
  public static class ViaRandomKey<K, V> extends PTransform<PCollection<KV<K, V>>, PCollection<KV<ShardedKey<K>, V>>> {
    public ViaRandomKey() {}

    @Override
    public PCollection<KV<ShardedKey<K>, V>>  expand(PCollection<KV<K, V>> input) {
      return input
          .apply("Pair element with a single autoshardable key", ParDo.of(new AssignShardFn<>())) // assign a single autoshardable key 
          .apply(AutoshardedKeyReshuffle.of()); // takes autosharded keyed input, ensures windows are correctly handled 
    }
  }

public static class AssignShardFn<K, V> extends DoFn<KV<K, V>, KV<ShardedKey<K>, V>> {
    private static final UUID workerUuid = UUID.randomUUID();

    @ProcessElement
    public void processElement(@Element KV<K, V> element, OutputReceiver<KV<ShardedKey<K>, V>> r) {
      long shard = Thread.currentThread().getId();
        ByteBuffer buffer = ByteBuffer.allocate(3 * Long.BYTES);
        buffer.putLong(workerUuid.getMostSignificantBits());
        buffer.putLong(workerUuid.getLeastSignificantBits());
        buffer.putLong(shard);
      r.output(KV.of(ShardedKey.of(element.getKey(), buffer.array()), element.getValue()));
    }
  }
}
