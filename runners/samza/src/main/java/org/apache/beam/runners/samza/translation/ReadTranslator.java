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
package org.apache.beam.runners.samza.translation;

import java.util.Map;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.samza.adapter.BoundedSourceSystem;
import org.apache.beam.runners.samza.adapter.UnboundedSourceSystem;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.util.SamzaCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;

/**
 * Translates {@link org.apache.beam.sdk.io.Read} to Samza input {@link
 * org.apache.samza.operators.MessageStream}.
 */
public class ReadTranslator<T> implements TransformTranslator<PTransform<PBegin, PCollection<T>>> {

  @Override
  public void translate(
      PTransform<PBegin, PCollection<T>> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {
    final PCollection<T> output = ctx.getOutput(transform);
    final Coder<WindowedValue<T>> coder = SamzaCoders.of(output);
    final Source<?> source =
        transform instanceof SplittableParDo.PrimitiveBoundedRead
            ? ((SplittableParDo.PrimitiveBoundedRead) transform).getSource()
            : ((SplittableParDo.PrimitiveUnboundedRead) transform).getSource();
    final String id = ctx.getIdForPValue(output);

    // Create system descriptor
    final GenericSystemDescriptor systemDescriptor;
    if (source instanceof BoundedSource) {
      systemDescriptor =
          new GenericSystemDescriptor(id, BoundedSourceSystem.Factory.class.getName());
    } else {
      systemDescriptor =
          new GenericSystemDescriptor(id, UnboundedSourceSystem.Factory.class.getName());
    }

    final Map<String, String> systemConfig =
        ImmutableMap.of(
            "source", Base64Serializer.serializeUnchecked(source),
            "coder", Base64Serializer.serializeUnchecked(coder),
            "stepName", node.getFullName());
    systemDescriptor.withSystemConfigs(systemConfig);

    // Create stream descriptor
    @SuppressWarnings("unchecked")
    final Serde<KV<?, OpMessage<T>>> kvSerde =
        (Serde) KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
    final GenericInputDescriptor<KV<?, OpMessage<T>>> inputDescriptor =
        systemDescriptor.getInputDescriptor(id, kvSerde);
    if (source instanceof BoundedSource) {
      inputDescriptor.isBounded();
    }

    ctx.registerInputMessageStream(output, inputDescriptor);
  }
}
