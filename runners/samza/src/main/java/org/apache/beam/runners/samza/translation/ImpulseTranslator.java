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

import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;

/**
 * Translate {@link org.apache.beam.sdk.transforms.Impulse} to a samza message stream produced by
 * {@link
 * org.apache.beam.runners.samza.translation.SamzaImpulseSystemFactory.SamzaImpulseSystemConsumer}.
 */
public class ImpulseTranslator
    implements TransformTranslator<PTransform<PBegin, PCollection<byte[]>>> {

  @Override
  public void translate(
      PTransform<PBegin, PCollection<byte[]>> transform, Node node, TranslationContext ctx) {
    final PCollection<byte[]> output = ctx.getOutput(transform);
    final String outputId = ctx.getIdForPValue(output);
    final GenericSystemDescriptor systemDescriptor =
        new GenericSystemDescriptor(outputId, SamzaImpulseSystemFactory.class.getName());

    // The KvCoder is needed here for Samza not to crop the key.
    final Serde<KV<?, OpMessage<byte[]>>> kvSerde = KVSerde.of(new NoOpSerde(), new NoOpSerde<>());
    final GenericInputDescriptor<KV<?, OpMessage<byte[]>>> inputDescriptor =
        systemDescriptor.getInputDescriptor(outputId, kvSerde);

    ctx.registerInputMessageStream(output, inputDescriptor);
  }

  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {

    final String outputId = ctx.getOutputId(transform);
    final GenericSystemDescriptor systemDescriptor =
        new GenericSystemDescriptor(outputId, SamzaImpulseSystemFactory.class.getName());

    // The KvCoder is needed here for Samza not to crop the key.
    final Serde<KV<?, OpMessage<byte[]>>> kvSerde = KVSerde.of(new NoOpSerde(), new NoOpSerde<>());
    final GenericInputDescriptor<KV<?, OpMessage<byte[]>>> inputDescriptor =
        systemDescriptor.getInputDescriptor(outputId, kvSerde);

    ctx.registerInputMessageStream(outputId, inputDescriptor);
  }
}
