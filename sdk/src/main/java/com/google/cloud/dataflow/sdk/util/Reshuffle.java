/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Duration;

/**
 * A {@link PTransform} to reshuffle the elements based on their key.
 *
 * <p> Performs a {@link GroupByKey} so that the data is key-partitioned. Configures the
 * {@link WindowingStrategy} so that no data is dropped, but doesn't affect the need for
 * the user to specify allowed lateness and accumulation mode before a user-inserted GroupByKey.
 *
 * @param <K> The type of key being reshuffled on.
 * @param <V> The type of value being reshuffled.
 */
public class Reshuffle<K, V>
  extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {

  private static final long serialVersionUID = 0L;

  private Reshuffle() {
  }

  public static <K, V> Reshuffle<K, V> of() {
    return new Reshuffle<K, V>();
  }

  @Override
  public PCollection<KV<K, V>> apply(PCollection<KV<K, V>> input) {
    WindowingStrategy<?, ?> originalStrategy = input.getWindowingStrategy();
    Window.Bound<KV<K, V>> rewindow = Window
        .<KV<K, V>>triggering(Repeatedly.<BoundedWindow>forever(
            AfterPane.<BoundedWindow>elementCountAtLeast(1)))
        .discardingFiredPanes();
    if (!originalStrategy.isAllowedLatenessSpecified()) {
      rewindow = rewindow.withAllowedLateness(
          Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
    }

    return input.apply(rewindow)
        .apply(GroupByKey.<K, V>create())
        // Set the windowing strategy directly, so that it doesn't get counted as the user having
        // set allowed lateness.
        .setWindowingStrategyInternal(originalStrategy)
        .apply(ParDo.named("ExpandIterable").of(
            new DoFn<KV<K, Iterable<V>>, KV<K, V>>() {
              private static final long serialVersionUID = 0;
              @Override
              public void processElement(ProcessContext c) {
                K key = c.element().getKey();
                for (V value : c.element().getValue()) {
                  c.output(KV.of(key, value));
                }
              }
            }));
  }
}
