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
package org.apache.beam.runners.kafka.streams.translation;

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.WireCoderSetting;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.util.construction.ReadTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

/**
 * Translates the deprecated primitive {@code Read} URN ({@code beam:transform:read:v1}) over a
 * {@link BoundedSource}.
 *
 * <p>The runner forces every {@code Read.Bounded} (including the one {@code Create} of two or more
 * elements expands to) into this primitive read before translation — see {@code
 * KafkaStreamsTestRunner.translate}, which applies {@code
 * SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads}. This deliberately avoids the
 * default {@code BoundedSourceAsSDFWrapperFn} splittable-DoFn expansion, which the runner cannot
 * execute yet (no SDF restriction protocol), as agreed with the mentor.
 *
 * <p>Adds the same three-node shape as {@link ImpulseTranslator}:
 *
 * <ul>
 *   <li>A {@code byte[]} source bound to a dedicated per-transform bootstrap topic (see {@link
 *       KafkaStreamsTranslationContext#getReadBootstrapTopic(String)}). Kafka Streams refuses to
 *       start a topology with no real source topic; records published to it are ignored by {@link
 *       ReadProcessor}.
 *   <li>The {@link ReadProcessor}, which reads the {@link BoundedSource} on a one-shot wall-clock
 *       punctuator and emits one data payload per element followed by a terminal watermark.
 *   <li>A per-processor persistent state store recording whether the read has already fired so task
 *       restarts do not duplicate elements.
 * </ul>
 *
 * <p>The processor emits elements in the runner-side wire form the downstream stage's SDK harness
 * expects, so it is handed the SDK-side and runner-side wire coders for the read's output
 * PCollection (see {@link ReadProcessor} for why). Only {@link
 * org.apache.beam.model.pipeline.v1.RunnerApi.IsBounded.Enum#BOUNDED bounded} sources are
 * supported; {@link ReadTranslation#boundedSourceFromProto} rejects an unbounded payload.
 */
class ReadTranslator implements PTransformTranslator {

  static final String SOURCE_SUFFIX = "-source";
  static final String STATE_STORE_SUFFIX = "-state";

  @Override
  public void translate(
      String transformId, RunnerApi.Pipeline pipeline, KafkaStreamsTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(transformId);
    // Read produces exactly one output PCollection; downstream consumers are separate PTransforms
    // whose inputs reference this PCollection id and are wired by their own translators.
    String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());
    addReadNodes(
        transformId,
        boundedSource(transform),
        pipeline.getComponents(),
        outputPCollectionId,
        context);
  }

  private static BoundedSource<?> boundedSource(RunnerApi.PTransform transform) {
    try {
      RunnerApi.ReadPayload payload =
          RunnerApi.ReadPayload.parseFrom(transform.getSpec().getPayload());
      return ReadTranslation.boundedSourceFromProto(payload);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to read the BoundedSource from transform " + transform.getUniqueName(), e);
    }
  }

  /**
   * Adds the source, {@link ReadProcessor}, and state store for the read. The type variable {@code
   * T} captures the {@link BoundedSource}'s element type so the processor and its wire coders are
   * built consistently.
   */
  private <T> void addReadNodes(
      String transformId,
      BoundedSource<T> source,
      RunnerApi.Components components,
      String outputPCollectionId,
      KafkaStreamsTranslationContext context) {
    PCollectionNode outputNode =
        PipelineNode.pCollection(
            outputPCollectionId, components.getPcollectionsOrThrow(outputPCollectionId));
    Coder<WindowedValue<T>> sdkWireCoder = sdkWireCoder(outputNode, components);
    Coder<WindowedValue<?>> runnerWireCoder = runnerWireCoder(outputNode, components);

    Topology topology = context.getTopology();
    String sourceNodeName = transformId + SOURCE_SUFFIX;
    String stateStoreName = transformId + STATE_STORE_SUFFIX;
    String bootstrapTopic = context.getReadBootstrapTopic(transformId);
    SerializablePipelineOptions options =
        new SerializablePipelineOptions(context.getPipelineOptions());

    topology.addSource(
        sourceNodeName,
        Serdes.ByteArray().deserializer(),
        Serdes.ByteArray().deserializer(),
        bootstrapTopic);
    topology.addProcessor(
        transformId,
        () ->
            new ReadProcessor<>(
                source, options, sdkWireCoder, runnerWireCoder, stateStoreName, transformId),
        sourceNodeName);
    KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(stateStoreName);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Boolean()), transformId);

    context.registerPCollectionProducer(outputPCollectionId, transformId);
  }

  /** The coder the SDK harness would use on the wire, keeping unknown element coders intact. */
  private static <T> Coder<WindowedValue<T>> sdkWireCoder(
      PCollectionNode outputNode, RunnerApi.Components components) {
    try {
      RunnerApi.Components.Builder builder = components.toBuilder();
      String coderId =
          WireCoders.addSdkWireCoder(outputNode, builder, WireCoderSetting.getDefaultInstance());
      @SuppressWarnings("unchecked")
      Coder<WindowedValue<T>> coder =
          (Coder<WindowedValue<T>>)
              RehydratedComponents.forComponents(builder.build()).getCoder(coderId);
      return coder;
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to build the SDK wire coder for PCollection " + outputNode.getId(), e);
    }
  }

  /** The coder the runner uses on the wire, replacing unknown element coders with byte arrays. */
  private static Coder<WindowedValue<?>> runnerWireCoder(
      PCollectionNode outputNode, RunnerApi.Components components) {
    try {
      @SuppressWarnings("unchecked")
      Coder<WindowedValue<?>> coder =
          (Coder<WindowedValue<?>>)
              (Coder<?>) WireCoders.instantiateRunnerWireCoder(outputNode, components);
      return coder;
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to build the runner wire coder for PCollection " + outputNode.getId(), e);
    }
  }
}
