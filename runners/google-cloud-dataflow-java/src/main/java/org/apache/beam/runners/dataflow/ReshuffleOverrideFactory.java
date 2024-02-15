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
package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.ReshuffleTrigger;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;

/**
 * A {@link PTransformOverrideFactory} that overrides {@link Reshuffle} with a version that does not
 * reify timestamps. Dataflow has special handling of the {@link ReshuffleTrigger} which never
 * buffers elements and outputs elements with their original timestamp.
 */
class ReshuffleOverrideFactory<K, V>
    extends SingleInputOutputOverrideFactory<
        PCollection<KV<K, V>>, PCollection<KV<K, V>>, Reshuffle<K, V>> {
  @Override
  public PTransformReplacement<PCollection<KV<K, V>>, PCollection<KV<K, V>>>
      getReplacementTransform(
          AppliedPTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>, Reshuffle<K, V>>
              transform) {
    return PTransformOverrideFactory.PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform), new ReshuffleWithOnlyTrigger<>());
  }

  private static class ReshuffleWithOnlyTrigger<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {
    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
      WindowingStrategy<?, ?> originalStrategy = input.getWindowingStrategy();
      // If the input has already had its windows merged, then the GBK that performed the merge
      // will have set originalStrategy.getWindowFn() to InvalidWindows, causing the GBK contained
      // here to fail. Instead, we install a valid WindowFn that leaves all windows unchanged.
      Window<KV<K, V>> rewindow =
          Window.<KV<K, V>>into(
                  new IdentityWindowFn<>(originalStrategy.getWindowFn().windowCoder()))
              .triggering(new ReshuffleTrigger<>())
              .discardingFiredPanes()
              .withTimestampCombiner(TimestampCombiner.EARLIEST)
              .withAllowedLateness(Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

      return input
          .apply(rewindow)
          .apply(GroupByKey.create())
          // Set the windowing strategy directly, so that it doesn't get counted as the user having
          // set allowed lateness.
          .setWindowingStrategyInternal(originalStrategy)
          .apply(
              "ExpandIterable",
              ParDo.of(
                  new DoFn<KV<K, Iterable<V>>, KV<K, V>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      K key = c.element().getKey();
                      for (V value : c.element().getValue()) {
                        c.output(KV.of(key, value));
                      }
                    }
                  }));
    }
  }
}
