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

import java.util.Map;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public final class KafkaReadWithRedistributeOverride {

  private KafkaReadWithRedistributeOverride() {}

  public static PTransformMatcher matcher() {
    return new PTransformMatcher() {
      @SuppressWarnings({
        "PatternMatchingInstanceof" // For compiling on older Java versions.
      })
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        if (application.getTransform() instanceof KafkaIO.Read) {
          return ((KafkaIO.Read) application.getTransform()).isRedistributed();
        }
        return false;
      }
    };
  }

  /**
   * {@link PTransformOverrideFactory} for {@link KafkaIO.Read} that enables {@code
   * withOffsetDeduplication} when {@code withRedistribute} is enabled.
   */
  static class Factory<K, V>
      implements PTransformOverrideFactory<
          PBegin, PCollection<KafkaRecord<K, V>>, KafkaIO.Read<K, V>> {

    @Override
    public PTransformReplacement<PBegin, PCollection<KafkaRecord<K, V>>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<KafkaRecord<K, V>>, KafkaIO.Read<K, V>> transform) {
      KafkaIO.Read<K, V> read = transform.getTransform();
      if (read.getOffsetDeduplication() == null) {
        return PTransformReplacement.of(
            transform.getPipeline().begin(), read.withOffsetDeduplication(true));
      }
      return PTransformReplacement.of(transform.getPipeline().begin(), read);
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<KafkaRecord<K, V>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }
}
