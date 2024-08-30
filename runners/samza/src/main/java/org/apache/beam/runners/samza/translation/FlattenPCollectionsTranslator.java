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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpAdapter;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.samza.operators.MessageStream;

/**
 * Translates {@link org.apache.beam.sdk.transforms.Flatten.PCollections} to Samza merge operator.
 */
class FlattenPCollectionsTranslator<T> implements TransformTranslator<Flatten.PCollections<T>> {
  @Override
  public void translate(
      Flatten.PCollections<T> transform, TransformHierarchy.Node node, TranslationContext ctx) {
    doTranslate(transform, node, ctx);
  }

  private static <T> void doTranslate(
      Flatten.PCollections<T> transform, TransformHierarchy.Node node, TranslationContext ctx) {
    final PCollection<T> output = ctx.getOutput(transform);

    final List<MessageStream<OpMessage<T>>> inputStreams = new ArrayList<>();
    for (Map.Entry<TupleTag<?>, PCollection<?>> taggedPValue : node.getInputs().entrySet()) {
      @SuppressWarnings("unchecked")
      final PCollection<T> input = (PCollection<T>) taggedPValue.getValue();
      inputStreams.add(ctx.getMessageStream(input));
    }

    if (inputStreams.isEmpty()) {
      // for some of the validateRunner tests only
      final MessageStream<OpMessage<T>> noOpStream =
          ctx.getDummyStream()
              .flatMapAsync(
                  OpAdapter.adapt((Op<String, T, Void>) (inputElement, emitter) -> {}, ctx));
      ctx.registerMessageStream(output, noOpStream);
      return;
    }

    ctx.registerMessageStream(output, mergeInputStreams(inputStreams));
  }

  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    doTranslatePortable(transform, ctx);
  }

  private static <T> void doTranslatePortable(
      PipelineNode.PTransformNode transform, PortableTranslationContext ctx) {
    final List<MessageStream<OpMessage<T>>> inputStreams = ctx.getAllInputMessageStreams(transform);
    final String outputId = ctx.getOutputId(transform);

    // For portable api there should be at least the impulse as a dummy input
    // We will know once validateRunner tests are available for portable runners
    checkState(
        !inputStreams.isEmpty(), "no input streams defined for Flatten: %s", transform.getId());

    ctx.registerMessageStream(outputId, mergeInputStreams(inputStreams));
  }

  // Merge multiple input streams into one, as this is what "flatten" is meant to do
  private static <T> MessageStream<OpMessage<T>> mergeInputStreams(
      List<MessageStream<OpMessage<T>>> inputStreams) {
    if (inputStreams.size() == 1) {
      return Iterables.getOnlyElement(inputStreams);
    }
    final Set<MessageStream<OpMessage<T>>> streamsToMerge = new HashSet<>();
    inputStreams.forEach(
        stream -> {
          if (!streamsToMerge.add(stream)) {
            // Merge same streams. Make a copy of the current stream.
            streamsToMerge.add(stream.map(m -> m));
          }
        });

    return MessageStream.mergeAll(streamsToMerge);
  }
}
