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
import org.joda.time.Duration;

/**
 * {@link PTransform PTransforms} for reifying the timestamp of values and reemitting the original
 * value with the original timestamp.
 *
 * @deprecated see {@link Reify}
 */
@Deprecated
class ReifyTimestamps {
  private ReifyTimestamps() {}

  /**
   * Create a {@link PTransform} that will output all input {@link KV KVs} with the timestamp inside
   * the value.
   *
   * @deprecated renamed to {@link Reify#timestampsInValue()}
   */
  @Deprecated
  public static <K, V>
      PTransform<PCollection<? extends KV<K, V>>, PCollection<KV<K, TimestampedValue<V>>>>
          inValues() {
    return (new InValues<>());
  }

  /**
   * Small composite transform to convert {@code ? extends KV<K, V>} into {@code KV<K, V>}
   * @param <K>
   * @param <V>
   */
  private static class InValues<K, V> extends
      PTransform<PCollection<? extends KV<K, V>>, PCollection<KV<K, TimestampedValue<V>>>> {
    @Override
    public PCollection<KV<K, TimestampedValue<V>>> expand(PCollection<? extends KV<K, V>> input) {
      return input
          // Get rid of wildcard type
          .apply(MapElements.via(new SimpleFunction<KV<K, V>, KV<K, V>>() {
            @Override
            public KV<K, V> apply(KV<K, V> input) {
              return input;
            }
          }))
          // Actually apply Reify
          .apply(Reify.<K, V>timestampsInValue());
    }
  }

  /**
   * Create a {@link PTransform} that consumes {@link KV KVs} with a {@link TimestampedValue} as the
   * value, and outputs the {@link KV} of the input key and value at the timestamp specified by the
   * {@link TimestampedValue}.
   */
  public static <K, V>
      PTransform<PCollection<? extends KV<K, TimestampedValue<V>>>, PCollection<KV<K, V>>>
          extractFromValues() {
    return ParDo.of(new ExtractTimestampedValueDoFn<K, V>());
  }

  private static class ExtractTimestampedValueDoFn<K, V>
      extends DoFn<KV<K, TimestampedValue<V>>, KV<K, V>> {
    @Override
    public Duration getAllowedTimestampSkew() {
      return Duration.millis(Long.MAX_VALUE);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<K, TimestampedValue<V>> kv = context.element();
      context.outputWithTimestamp(
          KV.of(kv.getKey(), kv.getValue().getValue()), kv.getValue().getTimestamp());
    }
  }
}
