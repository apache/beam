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

import java.util.Collections;
import org.apache.beam.runners.dataflow.internal.DataflowGroupByKey;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute.RedistributeByKey;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.ReshuffleTrigger;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;

class RedistributeByKeyOverrideFactory<K, V>
    extends SingleInputOutputOverrideFactory<
        PCollection<KV<K, V>>, PCollection<KV<K, V>>, RedistributeByKey<K, V>> {

  @Override
  public PTransformReplacement<PCollection<KV<K, V>>, PCollection<KV<K, V>>>
      getReplacementTransform(
          AppliedPTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>, RedistributeByKey<K, V>>
              transform) {
    return PTransformOverrideFactory.PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform),
        new DataflowRedistributeByKey<>(transform.getTransform()));
  }

  /** Specialized implementation of {@link RedistributeByKey} for Dataflow pipelines. */
  private static class DataflowRedistributeByKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {

    private final RedistributeByKey<K, V> originalTransform;

    private DataflowRedistributeByKey(RedistributeByKey<K, V> originalTransform) {
      this.originalTransform = originalTransform;
    }

    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
      WindowingStrategy<?, ?> originalStrategy = input.getWindowingStrategy();
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

      PCollection<KV<K, Iterable<ValueInSingleWindow<V>>>> grouped;
      if (originalTransform.getAllowDuplicates()) {
        grouped = reified.apply(DataflowGroupByKey.createWithAllowDuplicates());
      } else {
        grouped = reified.apply(DataflowGroupByKey.create());
      }

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
          .setWindowingStrategyInternal(originalStrategy);
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
}
