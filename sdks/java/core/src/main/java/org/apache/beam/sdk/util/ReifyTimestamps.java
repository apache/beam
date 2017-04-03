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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

/**
 * {@link PTransform PTransforms} for reifying the timestamp of values and reemitting the original
 * value with the original timestamp.
 */
public class ReifyTimestamps {
  private ReifyTimestamps() {}

  /**
   * Create a {@link PTransform} that will output all input {@link KV KVs} with the timestamp inside
   * the value.
   */
  public static <K, V>
      PTransform<PCollection<? extends KV<K, V>>, PCollection<KV<K, TimestampedValue<V>>>>
          inValues() {
    return ParDo.of(new ReifyValueTimestampDoFn<K, V>());
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

  private static class ReifyValueTimestampDoFn<K, V>
      extends DoFn<KV<K, V>, KV<K, TimestampedValue<V>>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(
          KV.of(
              context.element().getKey(),
              TimestampedValue.of(context.element().getValue(), context.timestamp())));
    }
  }

  private static class ExtractTimestampedValueDoFn<K, V>
      extends DoFn<KV<K, TimestampedValue<V>>, KV<K, V>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<K, TimestampedValue<V>> kv = context.element();
      context.outputWithTimestamp(
          KV.of(kv.getKey(), kv.getValue().getValue()), kv.getValue().getTimestamp());
    }
  }
}
