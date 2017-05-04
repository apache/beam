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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;

/**
 * A {@link PTransform} that returns a {@link PCollection} equivalent to its input but operationally
 * provides some of the side effects of a {@link GroupByKey}, in particular preventing fusion of
 * the surrounding transforms, checkpointing and deduplication by id (see
 * {@link ValueWithRecordId}).
 *
 * <p>Performs a {@link GroupByKey} so that the data is key-partitioned. Configures the
 * {@link WindowingStrategy} so that no data is dropped, but doesn't affect the need for
 * the user to specify allowed lateness and accumulation mode before a user-inserted GroupByKey.
 *
 * @param <K> The type of key being reshuffled on.
 * @param <V> The type of value being reshuffled.
 */
public class Reshuffle<K, V> extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {

  private Reshuffle() {
  }

  public static <K, V> Reshuffle<K, V> of() {
    return new Reshuffle<K, V>();
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
        Window.<KV<K, V>>into(new IdentityWindowFn<>(originalStrategy.getWindowFn().windowCoder()))
            .triggering(new ReshuffleTrigger<>())
            .discardingFiredPanes()
            .withTimestampCombiner(TimestampCombiner.EARLIEST)
            .withAllowedLateness(Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

    return input.apply(rewindow)
        .apply("ReifyOriginalTimestamps", ReifyTimestamps.<K, V>inValues())
        .apply(GroupByKey.<K, TimestampedValue<V>>create())
        // Set the windowing strategy directly, so that it doesn't get counted as the user having
        // set allowed lateness.
        .setWindowingStrategyInternal(originalStrategy)
        .apply("ExpandIterable", ParDo.of(
            new DoFn<KV<K, Iterable<TimestampedValue<V>>>, KV<K, TimestampedValue<V>>>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                K key = c.element().getKey();
                for (TimestampedValue<V> value : c.element().getValue()) {
                  c.output(KV.of(key, value));
                }
              }
            }))
        .apply("RestoreOriginalTimestamps", ReifyTimestamps.<K, V>extractFromValues());
  }
}
