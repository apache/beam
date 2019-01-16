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

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

/**
 * {@link PTransform PTransforms} for reifying the timestamp of values and reemitting the original
 * value with the original timestamp.
 *
 * @deprecated Use {@link Reify}
 */
@Deprecated
class ReifyTimestamps {
  private ReifyTimestamps() {}

  /**
   * Create a {@link PTransform} that will output all input {@link KV KVs} with the timestamp inside
   * the value.
   *
   * @deprecated Use {@link Reify#timestampsInValue()}
   */
  @Deprecated
  public static <K, V>
      PTransform<PCollection<? extends KV<K, V>>, PCollection<KV<K, TimestampedValue<V>>>>
          inValues() {
    return new InValues<>();
  }

  /**
   * Create a {@link PTransform} that consumes {@link KV KVs} with a {@link TimestampedValue} as the
   * value, and outputs the {@link KV} of the input key and value at the timestamp specified by the
   * {@link TimestampedValue}.
   *
   * @deprecated Use {@link Reify#extractTimestampsFromValues()}.
   */
  @Deprecated
  public static <K, V>
      PTransform<PCollection<? extends KV<K, TimestampedValue<V>>>, PCollection<KV<K, V>>>
          extractFromValues() {
    return new ExtractTimestampsFromValues<>();
  }

  private static class RemoveWildcard<T>
      extends PTransform<PCollection<? extends T>, PCollection<T>> {
    @Override
    public PCollection<T> expand(PCollection<? extends T> input) {
      return input.apply(
          ParDo.of(
              new DoFn<T, T>() {
                @ProcessElement
                public void process(@Element T element, OutputReceiver<T> r) {
                  r.output(element);
                }
              }));
    }
  }

  private static class InValues<K, V>
      extends PTransform<PCollection<? extends KV<K, V>>, PCollection<KV<K, TimestampedValue<V>>>> {
    @Override
    public PCollection<KV<K, TimestampedValue<V>>> expand(PCollection<? extends KV<K, V>> input) {
      return input.apply(new RemoveWildcard<KV<K, V>>()).apply(Reify.timestampsInValue());
    }
  }

  private static class ExtractTimestampsFromValues<K, V>
      extends PTransform<PCollection<? extends KV<K, TimestampedValue<V>>>, PCollection<KV<K, V>>> {
    @Override
    public PCollection<KV<K, V>> expand(PCollection<? extends KV<K, TimestampedValue<V>>> input) {
      return input
          .apply(new RemoveWildcard<KV<K, TimestampedValue<V>>>())
          .apply(Reify.extractTimestampsFromValues());
    }
  }
}
